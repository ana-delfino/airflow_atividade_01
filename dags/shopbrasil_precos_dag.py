"""# ShopBrasil · Panorama de preços por categoria

Substitui o script em cron que alimentava o painel de pricing.

**Fluxo**

```
ingestao:      preparar_execucao -> buscar_produtos -> validar_produtos  (linear)
analise:       agrupar_por_categoria -> calcular_metricas[categoria...]  (fan-out)
                                     -> consolidar_metricas              (fan-in)
persistencia:  salvar_snapshot (upsert) + salvar_historico (append idempotente)
qualidade:     checar_publicacao
```

- Agenda: todo dia às 06:00 (America/Sao_Paulo), `catchup=False`.
- `buscar_produtos` grava o retorno da API em `produtos_raw` e lê de lá; tem retry com
  backoff exponencial e callbacks de ciclo de vida.
- Reprocessar um dia passado usa os dados brutos daquele dia (a API só tem preços atuais).
- `calcular_metricas` usa Dynamic Task Mapping no pool `ecommerce_pool` (2 slots).
- `salvar_snapshot` é idempotente (UPSERT por data de referência + categoria).
- `checar_publicacao` confere no banco o que o painel vai ler.
- Param `simular_falha_api=true` força falhas nas 2 primeiras tentativas da
  ingestão, para demonstrar retry/backoff/callbacks.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import get_current_context

from shopbrasil import api, db, qualidade, transform
from shopbrasil.callbacks import alerta_falha, alerta_retry, alerta_sla, registrar_sucesso
from shopbrasil.exceptions import CatalogoIndisponivelError
from shopbrasil.operators import ValidarProdutosOperator

log = logging.getLogger(__name__)

TZ = pendulum.timezone("America/Sao_Paulo")
POSTGRES_CONN_ID = "postgres_dw"
POOL_METRICAS = "ecommerce_pool"
TENTATIVAS_SIMULADAS = 2


def _data_referencia(execucao: dict[str, str]) -> date:
    """Data de referência da execução, vinda do XCom de ``preparar_execucao``."""
    return date.fromisoformat(execucao["data_referencia"])


default_args = {
    "owner": "time-dados",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": alerta_falha,
}


@dag(
    dag_id="shopbrasil_precos_por_categoria",
    description="Coleta produtos da API de catálogo e publica métricas de preço por categoria",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=TZ),
    catchup=False,
    is_paused_upon_creation=False,  # já nasce ativa: roda sozinha às 06:00
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=alerta_sla,
    params={
        "simular_falha_api": Param(
            False, type="boolean", description="Força falha nas 2 primeiras tentativas da ingestão"
        ),
    },
    tags=["shopbrasil", "pricing", "fakestore"],
    doc_md=__doc__,
)
def shopbrasil_precos_por_categoria() -> None:
    """Monta a DAG: ingestão -> análise -> persistência -> qualidade."""

    # ------------------------------------------------------------------ #
    # 1. INGESTÃO (topologia linear)
    # ------------------------------------------------------------------ #
    @task_group(group_id="ingestao", tooltip="Coleta os produtos, guarda o bruto e valida")
    def ingestao() -> tuple[Any, Any]:
        @task
        def preparar_execucao(data_interval_end: pendulum.DateTime) -> dict[str, str]:
            """Garante as tabelas e decide a data de referência e a origem dos dados."""
            db.criar_tabelas(POSTGRES_CONN_ID)
            data_ref = data_interval_end.in_timezone(TZ).date()
            hoje = pendulum.now(TZ).date()
            origem = transform.definir_origem(data_ref, hoje)
            if origem == transform.ORIGEM_RAW:
                log.warning(
                    "Reprocessamento de %s (hoje é %s): os produtos virão dos dados brutos gravados "
                    "naquele dia, porque a API só devolve preços atuais.",
                    data_ref,
                    hoje,
                )
            else:
                log.info("Coleta do dia %s: os produtos virão da API.", data_ref)
            return {"data_referencia": data_ref.isoformat(), "origem": origem}

        @task(
            task_id="buscar_produtos",
            retries=5,
            retry_delay=timedelta(seconds=15),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=10),
            execution_timeout=timedelta(minutes=2),
            # Já vem do default_args; repetido aqui para deixar os 3 callbacks visíveis na task crítica.
            on_failure_callback=alerta_falha,
            on_retry_callback=alerta_retry,
            on_success_callback=registrar_sucesso,
        )
        def buscar_produtos(
            execucao: dict[str, str], params: dict[str, Any], ti: TaskInstance, run_id: str
        ) -> list[Any]:
            """Coleta da API (dia corrente) ou usa o bruto do dia (reprocessamento).

            Em ambos os casos os produtos são lidos de ``produtos_raw``: o cálculo usa
            exatamente o que ficou guardado.
            """
            data_ref = _data_referencia(execucao)
            if execucao["origem"] == transform.ORIGEM_API:
                try:
                    if params.get("simular_falha_api") and ti.try_number <= TENTATIVAS_SIMULADAS:
                        raise CatalogoIndisponivelError(
                            f"Falha simulada pelo parâmetro simular_falha_api (tentativa {ti.try_number}; "
                            f"as {TENTATIVAS_SIMULADAS} primeiras falham de propósito). "
                            "Desative o parâmetro para uma execução normal."
                        )
                    brutos = api.buscar_produtos()
                except CatalogoIndisponivelError:
                    if ti.try_number <= ti.max_tries:
                        log.warning(
                            "Tentativa %d de %d falhou; o Airflow fará nova tentativa com backoff "
                            "exponencial.",
                            ti.try_number,
                            ti.max_tries + 1,
                        )
                    else:
                        log.error(
                            "Tentativas esgotadas (%d); a execução será marcada como falha.", ti.try_number
                        )
                    raise  # relança a exceção original para o Airflow aplicar o retry
                db.salvar_produtos_raw(POSTGRES_CONN_ID, brutos, data_ref, run_id)

            brutos = db.ler_produtos_raw(POSTGRES_CONN_ID, data_ref)
            if not brutos:
                # Sem retry: esperar não faz aparecer o dado de um dia que não foi coletado.
                raise AirflowFailException(
                    f"Não há dados brutos de {data_ref} em produtos_raw, então não é possível "
                    "reprocessar esse dia: a API só devolve os preços atuais. O snapshot desse dia "
                    "não foi alterado."
                )
            log.info("%d produtos de %s lidos de produtos_raw", len(brutos), data_ref)
            return [transform.extrair_campos(p) for p in brutos]

        execucao = preparar_execucao()
        produtos = buscar_produtos(execucao)
        validados = ValidarProdutosOperator(
            task_id="validar_produtos",
            produtos=produtos,
            retries=0,  # erro de conteúdo: repetir com os mesmos dados não resolve
        )
        return execucao, validados.output

    # ------------------------------------------------------------------ #
    # 2. ANÁLISE (fan-out por categoria + fan-in)
    # ------------------------------------------------------------------ #
    @task_group(group_id="analise", tooltip="Calcula métricas por categoria em paralelo")
    def analise(produtos: Any) -> Any:
        @task
        def agrupar_por_categoria(produtos: list[dict[str, Any]]) -> list[dict[str, Any]]:
            """Gera um grupo por categoria; cada grupo vira uma task mapeada."""
            grupos = transform.agrupar_por_categoria(produtos)
            log.info("%d categorias encontradas: %s", len(grupos), [g["categoria"] for g in grupos])
            return grupos

        @task(pool=POOL_METRICAS, map_index_template="{{ categoria }}")
        def calcular_metricas(grupo: dict[str, Any]) -> dict[str, Any]:
            """Calcula as métricas de uma categoria (uma instância mapeada por categoria)."""
            # Nome amigável da instância mapeada na UI (em vez de 0, 1, 2...)
            get_current_context()["categoria"] = grupo["categoria"]
            return transform.calcular_metricas(grupo["categoria"], grupo["precos"])

        @task
        def consolidar_metricas(metricas: Any) -> list[dict[str, Any]]:
            """Junta os resultados de todas as categorias (fan-in)."""
            consolidadas = sorted(metricas, key=lambda m: m["categoria"])
            for m in consolidadas:
                log.info(
                    "%-20s qtd=%3d medio=%9.2f min=%9.2f max=%9.2f",
                    m["categoria"],
                    m["qtd_produtos"],
                    m["preco_medio"],
                    m["preco_minimo"],
                    m["preco_maximo"],
                )
            return consolidadas

        grupos = agrupar_por_categoria(produtos)
        metricas = calcular_metricas.expand(grupo=grupos)  # fan-out
        return consolidar_metricas(metricas)  # fan-in

    # ------------------------------------------------------------------ #
    # 3. PERSISTÊNCIA (PostgresHook + Connection "postgres_dw")
    # ------------------------------------------------------------------ #
    @task_group(group_id="persistencia", tooltip="Grava snapshot idempotente e histórico")
    def persistencia(metricas: Any, execucao: Any) -> tuple[Any, Any]:
        @task(sla=timedelta(minutes=30))  # painel precisa estar pronto até 06:30
        def salvar_snapshot(metricas: list[dict[str, Any]], execucao: dict[str, str], run_id: str) -> int:
            """Grava o snapshot idempotente do dia."""
            return db.salvar_snapshot(POSTGRES_CONN_ID, metricas, _data_referencia(execucao), run_id)

        @task
        def salvar_historico(
            metricas: list[dict[str, Any]], execucao: dict[str, str], run_id: str, ti: TaskInstance
        ) -> int:
            """Grava as métricas no histórico (append; idempotente por run_id)."""
            return db.salvar_historico(
                POSTGRES_CONN_ID,
                metricas,
                data_execucao=pendulum.now(TZ),
                data_referencia=_data_referencia(execucao),
                run_id=run_id,
                try_number=ti.try_number,
            )

        return salvar_snapshot(metricas, execucao), salvar_historico(metricas, execucao)

    # ------------------------------------------------------------------ #
    # 4. QUALIDADE (confere o que o painel vai ler)
    # ------------------------------------------------------------------ #
    @task_group(group_id="qualidade", tooltip="Confere no banco o que foi publicado")
    def checagem_qualidade(
        execucao: Any, metricas: Any, produtos_validos: Any, gravados_snapshot: Any, gravados_historico: Any
    ) -> None:
        @task
        def checar_publicacao(
            execucao: dict[str, str],
            metricas: list[dict[str, Any]],
            produtos_validos: list[dict[str, Any]],
            gravados_snapshot: int,
            gravados_historico: int,
            run_id: str,
        ) -> None:
            """Relê o snapshot e o histórico no banco e falha se algo estiver incoerente.

            ``gravados_snapshot`` e ``gravados_historico`` garantem que a checagem só roda
            depois das duas gravações.
            """
            data_ref = _data_referencia(execucao)
            problemas = qualidade.problemas_do_snapshot(
                snapshot=db.ler_snapshot(POSTGRES_CONN_ID, data_ref),
                categorias_esperadas=[m["categoria"] for m in metricas],
                qtd_produtos_validos=len(produtos_validos),
                qtd_linhas_historico=db.contar_historico_da_run(POSTGRES_CONN_ID, run_id),
            )
            qualidade.exigir_qualidade(problemas, data_ref)
            log.info(
                "Checagem de qualidade aprovada para %s: %d categorias, %d produtos, histórico ok "
                "(gravados: snapshot=%d, histórico=%d)",
                data_ref,
                len(metricas),
                len(produtos_validos),
                gravados_snapshot,
                gravados_historico,
            )

        checar_publicacao(execucao, metricas, produtos_validos, gravados_snapshot, gravados_historico)

    execucao, validados = ingestao()
    metricas = analise(validados)
    gravados_snapshot, gravados_historico = persistencia(metricas, execucao)
    checagem_qualidade(execucao, metricas, validados, gravados_snapshot, gravados_historico)


shopbrasil_precos_por_categoria()
