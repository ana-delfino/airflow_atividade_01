"""Exceções do domínio do pipeline.

Toda exceção do projeto herda de ``ShopBrasilError`` e a mensagem segue o formato:

    <o que aconteceu> (<contexto>). <o que fazer>.

Exemplo:
    "A API de catálogo respondeu HTTP 503 (GET https://fakestoreapi.com/products).
    Instabilidade do lado da API: a task será retentada automaticamente."
"""

from __future__ import annotations


class ShopBrasilError(Exception):
    """Base de todos os erros do pipeline."""


class CatalogoIndisponivelError(ShopBrasilError):
    """A API de catálogo não respondeu ou respondeu algo inutilizável.

    É um erro **transitório**: a task deve relançá-lo para o Airflow aplicar retry.
    """


class DadosInvalidosError(ShopBrasilError, ValueError):
    """Os dados recebidos não respeitam o contrato esperado.

    É um erro **de conteúdo**: repetir a task com os mesmos dados não resolve.
    """


class PersistenciaError(ShopBrasilError):
    """Falha ao ler ou gravar na base analítica."""
