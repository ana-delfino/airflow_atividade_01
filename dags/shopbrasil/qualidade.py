"""Checagens de qualidade do que foi publicado (regras puras, testáveis sem Airflow).

A validação de entrada (``transform.validar_lote``) garante que os dados chegaram certos;
estas checagens garantem que o que o painel vai ler está coerente.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from shopbrasil.exceptions import DadosInvalidosError


def problemas_do_snapshot(
    snapshot: list[dict[str, Any]],
    categorias_esperadas: Iterable[str],
    qtd_produtos_validos: int,
    qtd_linhas_historico: int,
) -> list[str]:
    """Confere o snapshot publicado e o histórico da execução.

    Args:
        snapshot: linhas de ``metricas_categoria`` do dia, lidas do banco.
        categorias_esperadas: categorias calculadas nesta execução.
        qtd_produtos_validos: produtos aprovados na validação.
        qtd_linhas_historico: linhas do histórico gravadas por esta execução (run_id).

    Returns:
        Uma mensagem por problema encontrado; lista vazia quando está tudo certo.
    """
    esperadas = set(categorias_esperadas)
    if not snapshot:
        return ["o snapshot do dia está vazio: o painel não teria dados"]

    problemas: list[str] = []
    publicadas = {linha["categoria"] for linha in snapshot}
    if faltando := sorted(esperadas - publicadas):
        problemas.append(f"categorias calculadas mas ausentes no snapshot: {faltando}")
    if sobrando := sorted(publicadas - esperadas):
        problemas.append(f"categorias no snapshot que não vieram nesta carga: {sobrando}")

    for linha in snapshot:
        cat = linha["categoria"]
        minimo, medio, maximo = linha["preco_minimo"], linha["preco_medio"], linha["preco_maximo"]
        if not minimo <= medio <= maximo:
            problemas.append(
                f"{cat!r}: esperado mínimo ≤ médio ≤ máximo, mas está {minimo} / {medio} / {maximo}"
            )
        if minimo < 0:
            problemas.append(f"{cat!r}: preço mínimo negativo ({minimo})")
        if linha["qtd_produtos"] <= 0:
            problemas.append(f"{cat!r}: quantidade de produtos deveria ser maior que zero")

    total = sum(linha["qtd_produtos"] for linha in snapshot)
    if total != qtd_produtos_validos:
        problemas.append(
            f"soma de qtd_produtos no snapshot ({total}) difere dos produtos válidos ({qtd_produtos_validos})"
        )

    if qtd_linhas_historico != len(esperadas):
        problemas.append(
            f"histórico desta execução tem {qtd_linhas_historico} linhas, esperado {len(esperadas)} "
            "(uma por categoria)"
        )
    return problemas


def exigir_qualidade(problemas: list[str], data_referencia: Any) -> None:
    """Falha com todos os problemas listados, para o alerta já trazer o diagnóstico completo.

    Raises:
        DadosInvalidosError: há pelo menos um problema.
    """
    if problemas:
        raise DadosInvalidosError(
            f"Checagem de qualidade reprovou o snapshot de {data_referencia} "
            f"({len(problemas)} problema(s)): {' | '.join(problemas)}. "
            "O painel pode estar com dados incorretos: investigue antes do time de pricing começar."
        )
