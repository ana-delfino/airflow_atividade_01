"""Testes das regras de negócio (rodam sem Airflow: `pytest`)."""

from datetime import date

import pytest

from shopbrasil.exceptions import DadosInvalidosError, ShopBrasilError
from shopbrasil.transform import (
    ORIGEM_API,
    ORIGEM_RAW,
    agrupar_por_categoria,
    calcular_metricas,
    definir_origem,
    erros_do_produto,
    exigir_tolerancia,
    extrair_campos,
    validar_lote,
)

PRODUTOS = [
    {"id": 1, "title": "Mochila", "price": 100.0, "category": "bags"},
    {"id": 2, "title": "Bolsa", "price": 50.5, "category": "bags"},
    {"id": 3, "title": "Anel", "price": 10, "category": "jewelery"},
]


def test_produto_valido_nao_tem_erros():
    assert erros_do_produto(PRODUTOS[0]) == []


@pytest.mark.parametrize(
    ("produto", "mensagem_esperada"),
    [
        ({"id": "1", "title": "x", "price": 1, "category": "a"}, "id deve ser inteiro, recebido str '1'"),
        (
            {"id": 1, "title": "", "price": 1, "category": "a"},
            "title deve ser texto não vazio, recebido str ''",
        ),
        ({"id": 1, "title": "x", "price": "1", "category": "a"}, "price deve ser numérico, recebido str '1'"),
        ({"id": 1, "title": "x", "price": -1, "category": "a"}, "price não pode ser negativo, recebido -1"),
        (
            {"id": 1, "title": "x", "price": True, "category": "a"},
            "price deve ser numérico, recebido bool True",
        ),
        (
            {"id": 1, "title": "x", "price": 1},
            "category deve ser texto não vazio, recebido nada (campo ausente ou nulo)",
        ),
    ],
)
def test_produto_invalido_explica_o_problema(produto, mensagem_esperada):
    assert mensagem_esperada in erros_do_produto(produto)


def test_registro_que_nao_e_objeto():
    assert erros_do_produto("abc") == ["registro deveria ser um objeto, recebido str 'abc'"]


def test_agrupar_cria_um_grupo_por_categoria():
    grupos = agrupar_por_categoria(PRODUTOS)
    assert [g["categoria"] for g in grupos] == ["bags", "jewelery"]
    assert grupos[0]["precos"] == [100.0, 50.5]


def test_nova_categoria_gera_novo_grupo():
    novos = [*PRODUTOS, {"id": 4, "title": "TV", "price": 999, "category": "electronics"}]
    assert len(agrupar_por_categoria(novos)) == 3


def test_agrupar_lista_vazia_explica_onde_olhar():
    with pytest.raises(DadosInvalidosError, match="validar_produtos"):
        agrupar_por_categoria([])


def test_agrupar_produto_sem_validacao_identifica_o_produto():
    with pytest.raises(DadosInvalidosError, match=r"id=9 .*validação"):
        agrupar_por_categoria([{"id": 9, "price": 1}])


def test_calcular_metricas():
    assert calcular_metricas("bags", [100.0, 50.5]) == {
        "categoria": "bags",
        "preco_medio": 75.25,
        "preco_minimo": 50.5,
        "preco_maximo": 100.0,
        "qtd_produtos": 2,
    }


def test_calcular_metricas_sem_precos_cita_a_categoria():
    with pytest.raises(DadosInvalidosError, match="'vazia' chegou sem preços"):
        calcular_metricas("vazia", [])


def test_erros_de_dados_sao_do_dominio_e_value_error():
    assert issubclass(DadosInvalidosError, ShopBrasilError)
    assert issubclass(DadosInvalidosError, ValueError)


# --------------------------------------------------------------------------- #
# validar_lote / exigir_tolerancia
# --------------------------------------------------------------------------- #
INVALIDO = {"id": 99, "title": "Quebrado", "price": "10,5", "category": "bags"}


def test_validar_lote_devolve_so_campos_do_processamento():
    resultado = validar_lote([{**PRODUTOS[0], "category": "  bags  ", "image": "x.png"}])
    assert resultado.validos == [{"id": 1, "category": "bags", "price": 100.0}]


def test_validar_lote_descarta_duplicados_mantendo_o_primeiro():
    repetido = {**PRODUTOS[0], "price": 1.0}
    resultado = validar_lote([*PRODUTOS, repetido])
    assert [p["id"] for p in resultado.validos] == [1, 2, 3]
    assert resultado.validos[0]["price"] == 100.0
    assert resultado.duplicados == [1]


def test_duplicados_nao_contam_como_invalidos():
    resultado = validar_lote([*PRODUTOS, PRODUTOS[0], PRODUTOS[1]])
    assert resultado.invalidos == []
    assert resultado.pct_invalidos == 0
    exigir_tolerancia(resultado, max_invalidos_pct=0)  # não lança


def test_validar_lote_registra_motivo_do_invalido():
    resultado = validar_lote([*PRODUTOS, INVALIDO])
    assert resultado.invalidos == ["id=99: price deve ser numérico, recebido str '10,5'"]
    assert resultado.total_unicos == 4


@pytest.mark.parametrize("entrada", [[], None, {"id": 1}])
def test_validar_lote_sem_lista_explica_onde_olhar(entrada):
    with pytest.raises(DadosInvalidosError, match="buscar_produtos"):
        validar_lote(entrada)


def test_tolerancia_dentro_do_limite_passa():
    resultado = validar_lote([*PRODUTOS, {**PRODUTOS[0], "id": 4}, INVALIDO])  # 1 de 5 = 20%
    exigir_tolerancia(resultado, max_invalidos_pct=0.2)


def test_tolerancia_acima_do_limite_reprova_com_exemplos():
    resultado = validar_lote([PRODUTOS[0], INVALIDO])  # 50%
    with pytest.raises(DadosInvalidosError, match=r"1 de 2 produtos inválidos \(50%\).*limite de 20%.*id=99"):
        exigir_tolerancia(resultado, max_invalidos_pct=0.2)


def test_nenhum_valido_tem_mensagem_propria_mesmo_com_limite_de_100():
    resultado = validar_lote([INVALIDO])
    with pytest.raises(DadosInvalidosError, match="Nenhum produto válido no lote"):
        exigir_tolerancia(resultado, max_invalidos_pct=1)


# --------------------------------------------------------------------------- #
# arredondamento monetário
# --------------------------------------------------------------------------- #
def test_arredonda_dinheiro_meio_para_cima():
    assert round(2.675, 2) == 2.67  # o problema do float que motivou o Decimal
    metricas = calcular_metricas("x", [2.675])
    assert (metricas["preco_medio"], metricas["preco_minimo"], metricas["preco_maximo"]) == (2.68, 2.68, 2.68)


def test_media_sem_erro_acumulado_do_float():
    assert sum([0.1] * 3) / 3 != 0.1
    assert calcular_metricas("x", [0.1, 0.1, 0.1])["preco_medio"] == 0.1
    assert calcular_metricas("x", [1.0, 2.35])["preco_medio"] == 1.68  # 1,675 -> 1,68


# --------------------------------------------------------------------------- #
# origem dos dados e seleção de campos
# --------------------------------------------------------------------------- #
HOJE = date(2026, 11, 22)


def test_dia_corrente_vem_da_api():
    assert definir_origem(HOJE, HOJE) == ORIGEM_API


def test_dia_passado_vem_do_bruto_e_nao_da_api():
    assert definir_origem(date(2026, 11, 20), HOJE) == ORIGEM_RAW


def test_data_no_futuro_e_recusada():
    with pytest.raises(DadosInvalidosError, match="2026-11-23 está no futuro"):
        definir_origem(date(2026, 11, 23), HOJE)


def test_extrair_campos_descarta_o_que_o_pipeline_nao_usa():
    bruto = {"id": 1, "title": "t", "price": 1.0, "category": "c", "image": "x", "rating": {"rate": 4}}
    assert extrair_campos(bruto) == {"id": 1, "title": "t", "price": 1.0, "category": "c"}


def test_extrair_campos_preserva_registro_estranho_para_a_validacao_apontar():
    assert extrair_campos("abc") == "abc"
    assert erros_do_produto(extrair_campos("abc")) == ["registro deveria ser um objeto, recebido str 'abc'"]
