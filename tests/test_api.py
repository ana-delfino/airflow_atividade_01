"""Testes das mensagens de erro do cliente da API (sem rede: `requests.get` é simulado)."""

import pytest
import requests

from shopbrasil import api
from shopbrasil.exceptions import CatalogoIndisponivelError


class RespostaFalsa:
    def __init__(self, status=200, corpo=None, texto="", content_type="application/json"):
        self.status_code = status
        self._corpo = corpo
        self.text = texto
        self.headers = {"Content-Type": content_type}

    def json(self):
        if isinstance(self._corpo, Exception):
            raise self._corpo
        return self._corpo


@pytest.fixture
def simular_get(monkeypatch):
    def _simular(resultado):
        def falso_get(*_args, **_kwargs):
            if isinstance(resultado, Exception):
                raise resultado
            return resultado

        monkeypatch.setattr(api.requests, "get", falso_get)

    return _simular


def test_sucesso_devolve_o_payload_como_veio_para_o_bruto(simular_get):
    payload = [{"id": 1, "title": "t", "price": 1.0, "category": "c", "image": "x"}, "registro estranho"]
    simular_get(RespostaFalsa(corpo=payload))
    assert api.buscar_produtos() == payload


@pytest.mark.parametrize(
    ("resultado", "trechos"),
    [
        (requests.Timeout(), ["Tempo esgotado", "conexão 5s, leitura 30s", "retentada"]),
        (requests.ConnectionError(), ["Não foi possível conectar", "FAKESTORE_API_URL"]),
        (
            RespostaFalsa(status=503, texto="Service Unavailable"),
            ["HTTP 503", "'Service Unavailable'", "retentada"],
        ),
        (RespostaFalsa(status=429), ["HTTP 429", "Limite de requisições"]),
        (RespostaFalsa(status=404, texto="Not Found"), ["HTTP 404", "endpoint /products"]),
        (
            RespostaFalsa(corpo=ValueError("x"), texto="<html>manutenção</html>", content_type="text/html"),
            ["não é JSON válido", "text/html", "<html>manutenção</html>"],
        ),
        (RespostaFalsa(corpo={"erro": "x"}), ["devolveu dict em vez de uma lista"]),
        (RespostaFalsa(corpo=[]), ["lista vazia", "retentada"]),
    ],
)
def test_erros_explicam_causa_contexto_e_acao(simular_get, resultado, trechos):
    simular_get(resultado)
    with pytest.raises(CatalogoIndisponivelError) as erro:
        api.buscar_produtos()
    mensagem = str(erro.value)
    assert "/products" in mensagem  # sempre cita a URL consultada
    for trecho in trechos:
        assert trecho in mensagem
