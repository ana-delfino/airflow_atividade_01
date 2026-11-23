"""Cliente da API de catálogo (representada pela FakeStore API)."""

from __future__ import annotations

import logging
import os
from typing import Any

import requests

from shopbrasil.exceptions import CatalogoIndisponivelError

log = logging.getLogger(__name__)

BASE_URL = os.getenv("FAKESTORE_API_URL", "https://fakestoreapi.com").rstrip("/")
TIMEOUT_CONEXAO_S = 5
TIMEOUT_LEITURA_S = 30
HEADERS = {"Accept": "application/json", "User-Agent": "shopbrasil-pipeline/1.0"}


def buscar_produtos() -> list[Any]:
    """Busca todos os produtos do catálogo, exatamente como a API devolve.

    O retorno é gravado como dado bruto (``produtos_raw``); a seleção dos campos usados no
    pipeline acontece depois, em ``transform.extrair_campos``.

    Returns:
        Lista de produtos como vieram da API (inclusive registros fora do padrão, que a
        validação vai apontar).

    Raises:
        CatalogoIndisponivelError: API fora do ar, lenta, com erro HTTP ou com resposta
            que não é uma lista de produtos. A task deve relançar para acionar o retry.
    """
    url = f"{BASE_URL}/products"
    log.info("Consultando catálogo: GET %s", url)

    resposta = _get(url)
    _checar_status(resposta, url)
    dados = _ler_json(resposta, url)

    if not isinstance(dados, list):
        raise CatalogoIndisponivelError(
            f"A API de catálogo devolveu {type(dados).__name__} em vez de uma lista de produtos "
            f"(GET {url}, início da resposta: {_trecho(resposta.text)}). "
            "Confira se FAKESTORE_API_URL aponta para a API correta."
        )
    if not dados:
        raise CatalogoIndisponivelError(
            f"A API de catálogo devolveu uma lista vazia (GET {url}). "
            "Pode ser instabilidade momentânea: a task será retentada automaticamente."
        )

    log.info("Catálogo recebido: %d produtos", len(dados))
    return dados


def _get(url: str) -> requests.Response:
    """Faz o GET convertendo erros de rede em ``CatalogoIndisponivelError``."""
    try:
        return requests.get(url, timeout=(TIMEOUT_CONEXAO_S, TIMEOUT_LEITURA_S), headers=HEADERS)
    except requests.Timeout as exc:
        raise CatalogoIndisponivelError(
            f"Tempo esgotado ao consultar a API de catálogo (GET {url}, limites: "
            f"conexão {TIMEOUT_CONEXAO_S}s, leitura {TIMEOUT_LEITURA_S}s). "
            "A API está lenta: a task será retentada automaticamente."
        ) from exc
    except requests.ConnectionError as exc:
        raise CatalogoIndisponivelError(
            f"Não foi possível conectar à API de catálogo (GET {url}: {type(exc).__name__}). "
            "Verifique se o container tem acesso à internet e se FAKESTORE_API_URL está correta."
        ) from exc
    except requests.RequestException as exc:
        raise CatalogoIndisponivelError(
            f"Erro inesperado na requisição à API de catálogo (GET {url}: {type(exc).__name__}: {exc}). "
            "A task será retentada automaticamente."
        ) from exc


def _checar_status(resposta: requests.Response, url: str) -> None:
    """Explica o erro HTTP de acordo com a faixa do status code."""
    status = resposta.status_code
    if status < 400:
        return
    contexto = f"(GET {url}, resposta: {_trecho(resposta.text)})"
    if status == 429:
        acao = "Limite de requisições da API atingido: a task aguardará o backoff e tentará de novo."
    elif status >= 500:
        acao = "Instabilidade do lado da API: a task será retentada automaticamente."
    else:
        acao = "Erro na requisição: confira FAKESTORE_API_URL e o endpoint /products."
    raise CatalogoIndisponivelError(f"A API de catálogo respondeu HTTP {status} {contexto}. {acao}")


def _ler_json(resposta: requests.Response, url: str) -> Any:
    """Decodifica o corpo como JSON, explicando quando não é JSON."""
    try:
        return resposta.json()
    except ValueError as exc:
        tipo = resposta.headers.get("Content-Type", "desconhecido")
        raise CatalogoIndisponivelError(
            f"A API de catálogo respondeu HTTP {resposta.status_code}, mas o corpo não é JSON válido "
            f"(GET {url}, Content-Type: {tipo}, início: {_trecho(resposta.text)}). "
            "Costuma ser uma página de erro ou manutenção: a task será retentada automaticamente."
        ) from exc


def _trecho(texto: str, limite: int = 120) -> str:
    """Resume um texto em uma linha, para caber na mensagem de erro."""
    texto = " ".join((texto or "").split())
    return repr(texto if len(texto) <= limite else f"{texto[:limite]}…")
