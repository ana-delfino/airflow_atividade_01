"""Operador customizado de validação de schema dos produtos."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context

from shopbrasil.transform import exigir_tolerancia, validar_lote


class ValidarProdutosOperator(BaseOperator):
    """Valida o schema dos produtos antes do processamento.

    A regra fica em ``transform.validar_lote`` e ``transform.exigir_tolerancia`` (testáveis sem
    Airflow); o operador só registra o resultado no log e devolve os válidos.

    - Inválidos e duplicados são descartados e cada um é registrado no log.
    - Se a proporção de inválidos passar de ``max_invalidos_pct``, a task falha:
      provavelmente a API mudou de contrato e não queremos publicar um painel errado.
    - O retorno (produtos válidos, só com ``id``/``category``/``price``) vira XCom.

    Args:
        produtos: lista de produtos (aceita XComArg ou template Jinja).
        max_invalidos_pct: tolerância de registros inválidos, entre 0 e 1.
    """

    template_fields: Sequence[str] = ("produtos",)
    ui_color = "#fde68a"

    def __init__(self, *, produtos: Any, max_invalidos_pct: float = 0.2, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if not 0 <= max_invalidos_pct <= 1:
            raise ValueError(
                f"max_invalidos_pct deve estar entre 0 e 1, recebido {max_invalidos_pct!r} "
                f"(task {kwargs.get('task_id')!r}). Use, por exemplo, 0.2 para 20%."
            )
        self.produtos = produtos
        self.max_invalidos_pct = max_invalidos_pct

    def execute(self, context: Context) -> list[dict[str, Any]]:
        """Valida os produtos e devolve só os válidos.

        Raises:
            DadosInvalidosError: lote vazio, sem válidos ou com inválidos acima do limite.
        """
        resultado = validar_lote(self.produtos)

        for motivo in resultado.invalidos:
            self.log.warning("Produto descartado por schema inválido (%s)", motivo)
        for pid in resultado.duplicados:
            self.log.warning("Produto descartado por duplicidade (id=%r já recebido neste lote)", pid)

        self.log.info(
            "Validação concluída: %d válidos, %d inválidos (%.0f%%; limite %.0f%%), %d duplicados",
            len(resultado.validos),
            len(resultado.invalidos),
            resultado.pct_invalidos * 100,
            self.max_invalidos_pct * 100,
            len(resultado.duplicados),
        )
        exigir_tolerancia(resultado, self.max_invalidos_pct)
        return resultado.validos
