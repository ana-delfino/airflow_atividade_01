"""Testes da checagem de qualidade do que foi publicado."""

from decimal import Decimal

import pytest

from shopbrasil.exceptions import DadosInvalidosError
from shopbrasil.qualidade import exigir_qualidade, problemas_do_snapshot


def linha(categoria, minimo, medio, maximo, qtd):
    return {
        "categoria": categoria,
        "preco_minimo": Decimal(str(minimo)),
        "preco_medio": Decimal(str(medio)),
        "preco_maximo": Decimal(str(maximo)),
        "qtd_produtos": qtd,
    }


SNAPSHOT_OK = [linha("bags", 50.5, 75.25, 100, 2), linha("jewelery", 10, 10, 10, 1)]


def conferir(snapshot=SNAPSHOT_OK, esperadas=("bags", "jewelery"), validos=3, historico=2):
    return problemas_do_snapshot(snapshot, esperadas, validos, historico)


def test_snapshot_coerente_nao_tem_problemas():
    assert conferir() == []


def test_snapshot_vazio():
    assert conferir(snapshot=[]) == ["o snapshot do dia está vazio: o painel não teria dados"]


def test_categoria_faltando_e_sobrando():
    problemas = conferir(esperadas=("bags", "electronics"))
    assert "categorias calculadas mas ausentes no snapshot: ['electronics']" in problemas
    assert "categorias no snapshot que não vieram nesta carga: ['jewelery']" in problemas


def test_medio_fora_do_intervalo_min_max():
    problemas = conferir(snapshot=[linha("bags", 50, 120, 100, 2), SNAPSHOT_OK[1]])
    assert "'bags': esperado mínimo ≤ médio ≤ máximo, mas está 50 / 120 / 100" in problemas


def test_soma_de_produtos_diferente_dos_validos():
    assert "soma de qtd_produtos no snapshot (3) difere dos produtos válidos (4)" in conferir(validos=4)


def test_historico_duplicado_na_mesma_run():
    problemas = conferir(historico=4)
    assert "histórico desta execução tem 4 linhas, esperado 2 (uma por categoria)" in problemas


def test_exigir_qualidade_lista_todos_os_problemas_na_mensagem():
    with pytest.raises(DadosInvalidosError, match=r"2026-11-22 \(2 problema\(s\)\): a \| b"):
        exigir_qualidade(["a", "b"], "2026-11-22")


def test_exigir_qualidade_sem_problemas_nao_lanca():
    exigir_qualidade([], "2026-11-22")
