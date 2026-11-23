"""Regras de negócio puras (sem dependência do Airflow, fáceis de testar)."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from numbers import Real
from typing import Any

from shopbrasil.exceptions import DadosInvalidosError

CENTAVO = Decimal("0.01")
EXEMPLOS_NA_MENSAGEM = 3
CAMPOS_DA_API = ("id", "title", "price", "category")
ORIGEM_API = "api"
ORIGEM_RAW = "raw"


def definir_origem(data_referencia: date, hoje: date) -> str:
    """Decide de onde vêm os produtos de uma execução.

    A API só devolve os preços **atuais**. Por isso só a execução do dia corrente consulta a API;
    reprocessar um dia passado usa os dados brutos gravados naquele dia (``produtos_raw``),
    em vez de gravar os preços de hoje com a data antiga.

    Args:
        data_referencia: dia a que a execução se refere.
        hoje: data atual em Brasília.

    Returns:
        ``"api"`` para o dia corrente, ``"raw"`` para dias passados.

    Raises:
        DadosInvalidosError: data de referência no futuro.
    """
    if data_referencia > hoje:
        raise DadosInvalidosError(
            f"Data de referência {data_referencia} está no futuro (hoje é {hoje}). "
            "Confira a logical date informada ao disparar a DAG."
        )
    return ORIGEM_API if data_referencia == hoje else ORIGEM_RAW


def extrair_campos(produto: Any) -> Any:
    """Mantém só os campos que o pipeline usa (``id``, ``title``, ``price``, ``category``).

    Registros que não são objetos passam como estão, para a validação apontar o problema.
    """
    if not isinstance(produto, dict):
        return produto
    return {campo: produto.get(campo) for campo in CAMPOS_DA_API}


@dataclass(frozen=True)
class ResultadoValidacao:
    """Resultado da validação de um lote de produtos.

    Attributes:
        validos: produtos aprovados, só com os campos usados no processamento.
        invalidos: um motivo por produto reprovado, ex.: ``"id=3: price deve ser numérico..."``.
        duplicados: ids descartados por já terem aparecido antes no lote.
    """

    validos: list[dict[str, Any]] = field(default_factory=list)
    invalidos: list[str] = field(default_factory=list)
    duplicados: list[int] = field(default_factory=list)

    @property
    def total_unicos(self) -> int:
        """Produtos distintos avaliados (válidos + inválidos; duplicados não entram)."""
        return len(self.validos) + len(self.invalidos)

    @property
    def pct_invalidos(self) -> float:
        """Proporção de inválidos entre os produtos distintos (0 a 1)."""
        return len(self.invalidos) / self.total_unicos if self.total_unicos else 0.0


def erros_do_produto(produto: Any) -> list[str]:
    """Lista os problemas de schema de um produto.

    Args:
        produto: registro vindo da API.

    Returns:
        Uma mensagem por problema encontrado; lista vazia quando o produto é válido.
    """
    if not isinstance(produto, dict):
        return [f"registro deveria ser um objeto, recebido {_descrever(produto)}"]

    erros: list[str] = []

    pid = produto.get("id")
    if not isinstance(pid, int) or isinstance(pid, bool):
        erros.append(f"id deve ser inteiro, recebido {_descrever(pid)}")

    titulo = produto.get("title")
    if not isinstance(titulo, str) or not titulo.strip():
        erros.append(f"title deve ser texto não vazio, recebido {_descrever(titulo)}")

    preco = produto.get("price")
    if not isinstance(preco, Real) or isinstance(preco, bool):
        erros.append(f"price deve ser numérico, recebido {_descrever(preco)}")
    elif preco < 0:
        erros.append(f"price não pode ser negativo, recebido {preco}")

    categoria = produto.get("category")
    if not isinstance(categoria, str) or not categoria.strip():
        erros.append(f"category deve ser texto não vazio, recebido {_descrever(categoria)}")

    return erros


def validar_lote(produtos: Any) -> ResultadoValidacao:
    """Valida um lote de produtos, separando válidos, inválidos e duplicados.

    - Inválido: não respeita o schema (veja ``erros_do_produto``).
    - Duplicado: ``id`` já apareceu antes no lote; fica a primeira ocorrência, para que o
      produto não conte duas vezes nas métricas.
    - Os válidos saem só com ``id``, ``category`` (sem espaços nas pontas) e ``price``,
      para manter o XCom pequeno.

    Args:
        produtos: lista recebida da task de ingestão.

    Returns:
        O resultado da validação (não lança erro por produto inválido; veja ``exigir_tolerancia``).

    Raises:
        DadosInvalidosError: o lote não é uma lista ou está vazio.
    """
    if not isinstance(produtos, list) or not produtos:
        raise DadosInvalidosError(
            f"Nenhum produto recebido para validar (recebido: {_descrever(produtos)}). "
            "Verifique o retorno da task buscar_produtos."
        )

    resultado = ResultadoValidacao()
    ids_vistos: set[int] = set()
    for produto in produtos:
        erros = erros_do_produto(produto)
        if erros:
            ident = produto.get("id") if isinstance(produto, dict) else None
            resultado.invalidos.append(f"id={ident!r}: {'; '.join(erros)}")
        elif produto["id"] in ids_vistos:
            resultado.duplicados.append(produto["id"])
        else:
            ids_vistos.add(produto["id"])
            resultado.validos.append(
                {
                    "id": produto["id"],
                    "category": produto["category"].strip(),
                    "price": produto["price"],
                }
            )
    return resultado


def exigir_tolerancia(resultado: ResultadoValidacao, max_invalidos_pct: float) -> None:
    """Reprova o lote se não houver válidos ou se os inválidos passarem do limite.

    Args:
        resultado: saída de ``validar_lote``.
        max_invalidos_pct: tolerância de inválidos entre os produtos distintos, de 0 a 1.

    Raises:
        DadosInvalidosError: nenhum produto válido, ou inválidos acima do limite.
    """
    exemplos = " | ".join(resultado.invalidos[:EXEMPLOS_NA_MENSAGEM])
    acao = (
        "Provável mudança no contrato da API: confira a lista completa no log da task "
        "validar_produtos antes de ajustar a validação."
    )
    if not resultado.validos:
        raise DadosInvalidosError(
            f"Nenhum produto válido no lote: os {len(resultado.invalidos)} produtos recebidos "
            f"foram reprovados. Exemplos: {exemplos}. {acao}"
        )
    if resultado.pct_invalidos > max_invalidos_pct:
        raise DadosInvalidosError(
            f"Lote reprovado: {len(resultado.invalidos)} de {resultado.total_unicos} produtos inválidos "
            f"({resultado.pct_invalidos:.0%}), acima do limite de {max_invalidos_pct:.0%}. "
            f"Exemplos: {exemplos}. {acao}"
        )


def agrupar_por_categoria(produtos: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Agrupa os preços por categoria.

    O resultado alimenta o Dynamic Task Mapping: novas categorias na API geram novas
    tasks automaticamente.

    Args:
        produtos: produtos já validados por ``validar_lote``.

    Returns:
        Um item ``{"categoria": str, "precos": list[float]}`` por categoria, em ordem alfabética.

    Raises:
        DadosInvalidosError: lista vazia ou produto sem ``category``/``price``.
    """
    if not produtos:
        raise DadosInvalidosError(
            "Nenhum produto para agrupar por categoria. Verifique o resultado da task validar_produtos."
        )

    grupos: dict[str, list[float]] = defaultdict(list)
    for produto in produtos:
        try:
            grupos[produto["category"].strip()].append(float(produto["price"]))
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise DadosInvalidosError(
                f"Produto id={produto.get('id')!r} sem category/price utilizáveis "
                f"({type(exc).__name__}: {exc}). "
                "Os produtos devem passar pela validação antes do agrupamento."
            ) from exc
    return [{"categoria": cat, "precos": precos} for cat, precos in sorted(grupos.items())]


def calcular_metricas(categoria: str, precos: list[float]) -> dict[str, Any]:
    """Calcula preço médio, mínimo, máximo e quantidade de produtos de uma categoria.

    As contas são feitas em ``Decimal`` e arredondadas "meio para cima" (2,675 → 2,68),
    como se espera de valores monetários; ``float`` arredondaria 2,675 para 2,67.

    Args:
        categoria: nome da categoria.
        precos: preços dos produtos da categoria.

    Returns:
        Dicionário com ``categoria``, ``preco_medio``, ``preco_minimo``, ``preco_maximo``
        e ``qtd_produtos`` (valores monetários com 2 casas, como ``float`` para o XCom).

    Raises:
        DadosInvalidosError: a categoria não tem preços.
    """
    if not precos:
        raise DadosInvalidosError(
            f"A categoria {categoria!r} chegou sem preços, então não há métricas para calcular. "
            "Verifique a saída da task agrupar_por_categoria."
        )
    # str() evita levar o erro de representação do float para o Decimal (2.675 -> "2.675").
    valores = [Decimal(str(p)) for p in precos]
    return {
        "categoria": categoria,
        "preco_medio": _em_reais(sum(valores, Decimal(0)) / len(valores)),
        "preco_minimo": _em_reais(min(valores)),
        "preco_maximo": _em_reais(max(valores)),
        "qtd_produtos": len(valores),
    }


def _em_reais(valor: Decimal) -> float:
    """Arredonda para centavos (meio para cima) e devolve ``float`` serializável no XCom."""
    return float(valor.quantize(CENTAVO, rounding=ROUND_HALF_UP))


def _descrever(valor: Any, limite: int = 40) -> str:
    """Descreve tipo e valor de forma curta, ex.: ``str '10.5'``."""
    if valor is None:
        return "nada (campo ausente ou nulo)"
    texto = repr(valor)
    if len(texto) > limite:
        texto = f"{texto[:limite]}…"
    return f"{type(valor).__name__} {texto}"
