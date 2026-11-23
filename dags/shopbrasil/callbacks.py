"""Callbacks de ciclo de vida e alertas (simulados).

Os alertas são escritos no log da task com destaque. Se a variável de ambiente
``ALERT_WEBHOOK_URL`` estiver definida (ex.: webhook do Slack/Teams), a mensagem
também é enviada por HTTP POST. Uma falha no envio nunca derruba a task.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import requests
from airflow.utils.context import Context

log = logging.getLogger(__name__)

TIMEOUT_WEBHOOK_S = 10


def alerta_falha(context: Context) -> None:
    """``on_failure_callback``: avisa que a task falhou de vez (tentativas esgotadas)."""
    ti = context["task_instance"]
    _enviar_alerta(
        "FALHA",
        f"{_identificar(context)} falhou e não será retentada. "
        f"Erro: {_descrever_erro(context)}. Log: {ti.log_url}",
    )


def alerta_retry(context: Context) -> None:
    """``on_retry_callback``: avisa que a task falhou, mas terá nova tentativa."""
    _enviar_alerta(
        "RETRY",
        f"{_identificar(context)} falhou e será retentada com backoff. Erro: {_descrever_erro(context)}",
    )


def registrar_sucesso(context: Context) -> None:
    """``on_success_callback``: registra a conclusão (sem alerta)."""
    log.info("[OK] %s concluída com sucesso", _identificar(context))


def alerta_sla(dag: Any, task_list: str, blocking_task_list: str, slas: Any, blocking_tis: Any) -> None:
    """``sla_miss_callback``: avisa que o painel não ficou pronto no prazo."""
    _enviar_alerta(
        "SLA",
        f"DAG {dag.dag_id} estourou o SLA: o painel pode não estar atualizado. "
        f"Tasks atrasadas: {task_list.strip() or '-'}. Bloqueando: {blocking_task_list.strip() or '-'}",
    )


def _identificar(context: Context) -> str:
    ti = context["task_instance"]
    return (
        f"Task {ti.task_id} (DAG {ti.dag_id}, run {context['run_id']}, "
        f"tentativa {ti.try_number} de {ti.max_tries + 1})"
    )


def _descrever_erro(context: Context) -> str:
    erro = context.get("exception")
    if erro is None:
        return "não informado (veja o log da task)"
    return f"{type(erro).__name__}: {erro}"


def _enviar_alerta(nivel: str, mensagem: str) -> None:
    banner = "=" * 70
    log.warning("\n%s\n[ALERTA %s] %s\n%s", banner, nivel, mensagem, banner)

    webhook = os.getenv("ALERT_WEBHOOK_URL")
    if not webhook:
        return
    try:
        resposta = requests.post(webhook, json={"text": f"[{nivel}] {mensagem}"}, timeout=TIMEOUT_WEBHOOK_S)
        resposta.raise_for_status()
    except requests.RequestException as exc:
        log.error(
            "Alerta %s não entregue ao webhook (%s: %s). Confira ALERT_WEBHOOK_URL; "
            "o alerta continua registrado acima neste log.",
            nivel,
            type(exc).__name__,
            exc,
        )
