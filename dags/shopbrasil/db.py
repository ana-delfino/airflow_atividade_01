"""Persistência na base analítica via PostgresHook."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, datetime
from typing import Any, TypeVar

import psycopg2
import psycopg2.extensions
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json, execute_values

from shopbrasil.exceptions import PersistenciaError

log = logging.getLogger(__name__)

T = TypeVar("T")

DDL = """
-- Dados brutos: o retorno da API como chegou, 1 linha por produto por dia de referência.
CREATE TABLE IF NOT EXISTS produtos_raw (
    data_referencia DATE        NOT NULL,
    posicao         INTEGER     NOT NULL,
    payload         JSONB       NOT NULL,
    run_id          TEXT        NOT NULL,
    coletado_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_produtos_raw PRIMARY KEY (data_referencia, posicao)
);

-- Snapshot idempotente: 1 linha por (dia de referência, categoria).
CREATE TABLE IF NOT EXISTS metricas_categoria (
    data_referencia DATE          NOT NULL,
    categoria       TEXT          NOT NULL,
    preco_medio     NUMERIC(12,2) NOT NULL,
    preco_minimo    NUMERIC(12,2) NOT NULL,
    preco_maximo    NUMERIC(12,2) NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    run_id          TEXT          NOT NULL,
    atualizado_em   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT pk_metricas_categoria PRIMARY KEY (data_referencia, categoria)
);

-- Histórico: cada execução acrescenta linhas; a mesma execução reprocessada não duplica.
CREATE TABLE IF NOT EXISTS metricas_categoria_historico (
    id              BIGSERIAL     PRIMARY KEY,
    data_execucao   TIMESTAMPTZ   NOT NULL,
    data_referencia DATE          NOT NULL,
    categoria       TEXT          NOT NULL,
    preco_medio     NUMERIC(12,2) NOT NULL,
    preco_minimo    NUMERIC(12,2) NOT NULL,
    preco_maximo    NUMERIC(12,2) NOT NULL,
    qtd_produtos    INTEGER       NOT NULL,
    run_id          TEXT          NOT NULL,
    try_number      INTEGER       NOT NULL,
    inserido_em     TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_historico_categoria_data
    ON metricas_categoria_historico (categoria, data_referencia);
CREATE UNIQUE INDEX IF NOT EXISTS ux_historico_run_categoria
    ON metricas_categoria_historico (run_id, categoria);

-- Visão consumida pelo painel: sempre o dia mais recente.
CREATE OR REPLACE VIEW vw_painel_precos AS
SELECT data_referencia, categoria, preco_medio, preco_minimo, preco_maximo, qtd_produtos, atualizado_em
FROM metricas_categoria
WHERE data_referencia = (SELECT max(data_referencia) FROM metricas_categoria)
ORDER BY categoria;

-- Documentação no banco: aparece em qualquer cliente SQL (\\d+ no psql, DBeaver, etc.).
COMMENT ON TABLE produtos_raw IS
    'Retorno da API de catálogo como chegou, 1 linha por produto por dia. Fonte para recalcular '
    'as métricas de um dia passado. Recoletar o mesmo dia substitui as linhas daquele dia.';
COMMENT ON COLUMN produtos_raw.data_referencia IS 'Dia (Brasília) a que a coleta se refere.';
COMMENT ON COLUMN produtos_raw.posicao IS 'Posição do produto na resposta da API (0, 1, 2...).';
COMMENT ON COLUMN produtos_raw.payload IS 'Produto exatamente como a API devolveu (JSON).';
COMMENT ON COLUMN produtos_raw.run_id IS 'Execução do Airflow que fez a coleta.';
COMMENT ON COLUMN produtos_raw.coletado_em IS 'Momento em que a coleta foi gravada.';

COMMENT ON TABLE metricas_categoria IS
    'Snapshot das métricas de preço por categoria, 1 linha por (dia, categoria). Idempotente: '
    'reprocessar o dia substitui as linhas e remove categorias que não vieram na nova carga.';
COMMENT ON COLUMN metricas_categoria.data_referencia IS 'Dia (Brasília) a que as métricas se referem.';
COMMENT ON COLUMN metricas_categoria.categoria IS 'Categoria do produto, como vem da API.';
COMMENT ON COLUMN metricas_categoria.preco_medio IS
    'Média dos preços da categoria, arredondada meio para cima (2 casas).';
COMMENT ON COLUMN metricas_categoria.preco_minimo IS 'Menor preço da categoria.';
COMMENT ON COLUMN metricas_categoria.preco_maximo IS 'Maior preço da categoria.';
COMMENT ON COLUMN metricas_categoria.qtd_produtos IS
    'Produtos válidos e distintos da categoria (duplicados descartados).';
COMMENT ON COLUMN metricas_categoria.run_id IS 'Última execução do Airflow que gravou a linha.';
COMMENT ON COLUMN metricas_categoria.atualizado_em IS 'Momento da última gravação da linha.';

COMMENT ON TABLE metricas_categoria_historico IS
    'Histórico das métricas por execução, para acompanhar a evolução dos preços. Cada execução '
    'acrescenta linhas; reprocessar a mesma execução (run_id) atualiza em vez de duplicar.';
COMMENT ON COLUMN metricas_categoria_historico.data_execucao IS
    'Momento (Brasília) em que a execução gravou.';
COMMENT ON COLUMN metricas_categoria_historico.data_referencia IS
    'Dia (Brasília) a que as métricas se referem.';
COMMENT ON COLUMN metricas_categoria_historico.run_id IS 'Execução do Airflow; único junto com categoria.';
COMMENT ON COLUMN metricas_categoria_historico.try_number IS 'Tentativa da task que gravou a linha.';
COMMENT ON COLUMN metricas_categoria_historico.inserido_em IS 'Momento da última gravação da linha.';

COMMENT ON VIEW vw_painel_precos IS
    'Métricas do dia mais recente, consumidas pelo painel de pricing todas as manhãs.';
"""

DELETE_RAW_DO_DIA = "DELETE FROM produtos_raw WHERE data_referencia = %s;"

INSERT_RAW = """
INSERT INTO produtos_raw (data_referencia, posicao, payload, run_id)
VALUES %s;
"""

SELECT_RAW_DO_DIA = """
SELECT payload FROM produtos_raw
WHERE data_referencia = %s
ORDER BY posicao;
"""

UPSERT_SNAPSHOT = """
INSERT INTO metricas_categoria
    (data_referencia, categoria, preco_medio, preco_minimo, preco_maximo, qtd_produtos, run_id)
VALUES %s
ON CONFLICT (data_referencia, categoria) DO UPDATE SET
    preco_medio   = EXCLUDED.preco_medio,
    preco_minimo  = EXCLUDED.preco_minimo,
    preco_maximo  = EXCLUDED.preco_maximo,
    qtd_produtos  = EXCLUDED.qtd_produtos,
    run_id        = EXCLUDED.run_id,
    atualizado_em = now();
"""

# Mesma transação do UPSERT: o snapshot do dia passa a ter exatamente as categorias desta carga.
DELETE_CATEGORIAS_AUSENTES = """
DELETE FROM metricas_categoria
WHERE data_referencia = %s
  AND NOT (categoria = ANY(%s));
"""

UPSERT_HISTORICO = """
INSERT INTO metricas_categoria_historico
    (data_execucao, data_referencia, categoria, preco_medio, preco_minimo,
     preco_maximo, qtd_produtos, run_id, try_number)
VALUES %s
ON CONFLICT (run_id, categoria) DO UPDATE SET
    data_execucao   = EXCLUDED.data_execucao,
    data_referencia = EXCLUDED.data_referencia,
    preco_medio     = EXCLUDED.preco_medio,
    preco_minimo    = EXCLUDED.preco_minimo,
    preco_maximo    = EXCLUDED.preco_maximo,
    qtd_produtos    = EXCLUDED.qtd_produtos,
    try_number      = EXCLUDED.try_number,
    inserido_em     = now();
"""

SELECT_SNAPSHOT_DO_DIA = """
SELECT categoria, preco_medio, preco_minimo, preco_maximo, qtd_produtos
FROM metricas_categoria
WHERE data_referencia = %s
ORDER BY categoria;
"""

SELECT_QTD_HISTORICO_DA_RUN = "SELECT count(*) FROM metricas_categoria_historico WHERE run_id = %s;"


def criar_tabelas(conn_id: str) -> None:
    """Cria (se não existirem) as tabelas, a view e os comentários da base analítica.

    Raises:
        PersistenciaError: connection inexistente, banco inacessível ou sem permissão.
    """
    conn = _conectar(conn_id)
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()
    except psycopg2.Error as exc:
        conn.rollback()
        raise PersistenciaError(
            f"Não foi possível criar as tabelas da base analítica (connection {conn_id!r}: "
            f"{_resumo_erro(exc)}). Verifique se o usuário do banco tem permissão de CREATE."
        ) from exc
    finally:
        conn.close()
    log.info("Tabelas, view e comentários garantidos na base analítica (connection %r)", conn_id)


def salvar_produtos_raw(conn_id: str, produtos: list[Any], data_referencia: date, run_id: str) -> int:
    """Grava os produtos como vieram da API, substituindo a coleta anterior do mesmo dia.

    Returns:
        Quantidade de produtos gravados.

    Raises:
        PersistenciaError: nada para gravar, falha de conexão ou de gravação (a transação é desfeita).
    """
    linhas = [(data_referencia, posicao, Json(p), run_id) for posicao, p in enumerate(produtos)]

    def gravar(cur: psycopg2.extensions.cursor) -> int:
        cur.execute(DELETE_RAW_DO_DIA, (data_referencia,))
        substituidos = cur.rowcount
        execute_values(cur, INSERT_RAW, linhas)
        return substituidos

    substituidos = _gravar_em_transacao(
        conn_id, gravar, len(linhas), destino=f"dados brutos de {data_referencia}"
    )
    log.info(
        "Dados brutos de %s: %d produtos gravados (%d da coleta anterior do dia substituídos)",
        data_referencia,
        len(linhas),
        substituidos,
    )
    return len(linhas)


def ler_produtos_raw(conn_id: str, data_referencia: date) -> list[Any]:
    """Lê os produtos brutos de um dia, na ordem em que a API devolveu.

    Returns:
        Lista de payloads; vazia se não houver coleta para o dia.
    """
    linhas = _consultar(conn_id, SELECT_RAW_DO_DIA, (data_referencia,), f"dados brutos de {data_referencia}")
    return [payload for (payload,) in linhas]


def salvar_snapshot(conn_id: str, metricas: list[dict[str, Any]], data_referencia: date, run_id: str) -> int:
    """Grava o snapshot do dia, que passa a refletir exatamente esta carga.

    Numa única transação:
    1. UPSERT por (data_referencia, categoria): re-rodar não duplica linhas;
    2. remove as categorias do mesmo dia que não vieram nesta carga (ex.: categoria que saiu
       da API entre a execução das 06:00 e um reprocessamento), para o painel não mostrar
       números antigos de uma categoria que não existe mais.

    Returns:
        Quantidade de categorias gravadas.

    Raises:
        PersistenciaError: falha de conexão ou de gravação (a transação é desfeita).
    """
    linhas = [
        (
            data_referencia,
            m["categoria"],
            m["preco_medio"],
            m["preco_minimo"],
            m["preco_maximo"],
            m["qtd_produtos"],
            run_id,
        )
        for m in metricas
    ]
    categorias = [m["categoria"] for m in metricas]

    def gravar(cur: psycopg2.extensions.cursor) -> int:
        execute_values(cur, UPSERT_SNAPSHOT, linhas)
        cur.execute(DELETE_CATEGORIAS_AUSENTES, (data_referencia, categorias))
        return cur.rowcount

    removidas = _gravar_em_transacao(conn_id, gravar, len(linhas), destino=f"snapshot de {data_referencia}")
    log.info("Snapshot de %s: %d categorias gravadas (upsert)", data_referencia, len(linhas))
    if removidas:
        log.warning(
            "Snapshot de %s: %d categoria(s) removida(s) por não constarem nesta carga",
            data_referencia,
            removidas,
        )
    return len(linhas)


def salvar_historico(
    conn_id: str,
    metricas: list[dict[str, Any]],
    data_execucao: datetime,
    data_referencia: date,
    run_id: str,
    try_number: int,
) -> int:
    """Grava as métricas no histórico, para acompanhar a evolução dos preços.

    Cada execução acrescenta linhas (append). Reprocessar a mesma execução atualiza as linhas
    dela, com UPSERT por (run_id, categoria), em vez de duplicar.

    Returns:
        Quantidade de linhas gravadas.

    Raises:
        PersistenciaError: falha de conexão ou de gravação (a transação é desfeita).
    """
    linhas = [
        (
            data_execucao,
            data_referencia,
            m["categoria"],
            m["preco_medio"],
            m["preco_minimo"],
            m["preco_maximo"],
            m["qtd_produtos"],
            run_id,
            try_number,
        )
        for m in metricas
    ]
    _gravar_em_transacao(
        conn_id,
        lambda cur: execute_values(cur, UPSERT_HISTORICO, linhas),
        len(linhas),
        destino=f"histórico de {data_referencia}",
    )
    log.info("Histórico de %s (run %s): %d linhas gravadas", data_referencia, run_id, len(linhas))
    return len(linhas)


def ler_snapshot(conn_id: str, data_referencia: date) -> list[dict[str, Any]]:
    """Lê o snapshot publicado de um dia, como o painel o enxerga."""
    linhas = _consultar(conn_id, SELECT_SNAPSHOT_DO_DIA, (data_referencia,), f"snapshot de {data_referencia}")
    colunas = ("categoria", "preco_medio", "preco_minimo", "preco_maximo", "qtd_produtos")
    return [dict(zip(colunas, linha, strict=True)) for linha in linhas]


def contar_historico_da_run(conn_id: str, run_id: str) -> int:
    """Conta as linhas do histórico gravadas por uma execução."""
    ((qtd,),) = _consultar(conn_id, SELECT_QTD_HISTORICO_DA_RUN, (run_id,), f"histórico da run {run_id}")
    return qtd


def _conectar(conn_id: str) -> psycopg2.extensions.connection:
    """Abre conexão pela Connection do Airflow, traduzindo os erros mais comuns."""
    try:
        return PostgresHook(postgres_conn_id=conn_id).get_conn()
    except AirflowNotFoundException as exc:
        raise PersistenciaError(
            f"A connection {conn_id!r} não existe no Airflow. "
            "Crie-a em Admin → Connections ou rode de novo o serviço airflow-init "
            "(docker compose up airflow-init)."
        ) from exc
    except psycopg2.OperationalError as exc:
        raise PersistenciaError(
            f"Não foi possível conectar à base analítica (connection {conn_id!r}: {_resumo_erro(exc)}). "
            "Verifique se o container postgres_dw está no ar (docker compose ps) e se "
            "host, porta, usuário e senha da connection estão corretos."
        ) from exc


def _consultar(conn_id: str, sql: str, parametros: tuple[Any, ...], origem: str) -> list[tuple[Any, ...]]:
    """Executa uma consulta de leitura, traduzindo erros com contexto."""
    conn = _conectar(conn_id)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, parametros)
            return cur.fetchall()
    except psycopg2.Error as exc:
        raise PersistenciaError(
            f"Falha ao ler {origem} (connection {conn_id!r}: {_resumo_erro(exc)}). "
            "Verifique se as tabelas existem (task preparar_execucao) e se o banco está no ar."
        ) from exc
    finally:
        conn.close()


def _gravar_em_transacao(
    conn_id: str,
    gravar: Callable[[psycopg2.extensions.cursor], T],
    qtd_linhas: int,
    destino: str,
) -> T:
    """Executa ``gravar`` numa transação: tudo ou nada.

    Args:
        conn_id: connection do Airflow.
        gravar: função que recebe o cursor e faz as operações da transação.
        qtd_linhas: linhas a gravar (zero é tratado como erro).
        destino: descrição usada nas mensagens, ex.: ``"snapshot de 2026-11-22"``.

    Returns:
        O retorno de ``gravar``.

    Raises:
        PersistenciaError: nada para gravar, falha de conexão ou de gravação (com rollback).
    """
    if not qtd_linhas:
        raise PersistenciaError(f"Nenhuma linha para gravar ({destino}). Verifique a saída da task anterior.")
    conn = _conectar(conn_id)
    try:
        with conn.cursor() as cur:
            retorno = gravar(cur)
        conn.commit()
        return retorno
    except psycopg2.Error as exc:
        conn.rollback()
        raise PersistenciaError(
            f"Falha ao gravar {qtd_linhas} linhas em {destino} (connection {conn_id!r}: "
            f"{_resumo_erro(exc)}). A transação foi desfeita e nenhum dado foi gravado; "
            "corrija a causa e rode a task de novo."
        ) from exc
    finally:
        conn.close()


def _resumo_erro(exc: Exception) -> str:
    """Primeira linha da mensagem do erro, prefixada pelo tipo."""
    linhas = str(exc).strip().splitlines()
    return f"{type(exc).__name__}: {linhas[0] if linhas else 'sem detalhes'}"
