"""Módulos de apoio do pipeline de preços da ShopBrasil.

Organização:
    api.py        -> acesso à API de catálogo (FakeStore)
    transform.py  -> regras de negócio puras (validação, agrupamento, métricas)
    operators.py  -> ValidarProdutosOperator (operador customizado)
    db.py         -> DDL e gravação no PostgreSQL via PostgresHook
    callbacks.py  -> callbacks de ciclo de vida e alertas
"""
