"""Cerebral × AIOpsLab adapter — dataplane-mode result relay.

This is an AIOpsLab ``Agent`` that carries **no** perception or reasoning of its
own. It bridges two systems over HTTP only (stdlib ``urllib``, importing none of
the engine code):

  * AIOpsLab drives the agent one action per turn:
        get_action(obs) -> ```exec_shell(...)``` / ```submit(...)```
  * the Cerebral engine investigates autonomously: the live dataplane detects
    the injected fault, the engine's relay triggers a full pipeline run, and the
    result lands as an incident.

The adapter does not trigger or drive anything. It waits out the initial deploy
churn, polls the engine's incidents (GET /api/incidents), picks this episode's
incident for the benchmark namespace, and submits the engine's terminal answer
in the shape the task scores. Where the engine can't answer, it submits the
honest empty ([], {}).

Env (all optional):
  CEREBRAL_ENGINE_URL              default http://localhost:8080
  CEREBRAL_ENGINE_TIMEOUT          default 240   (per-request seconds)
  CEREBRAL_PIPELINE_TIMEOUT        default = ENGINE_TIMEOUT (max wait for an incident)
  CEREBRAL_PIPELINE_POLL_INTERVAL  default 5     (sleep between polls)
  CEREBRAL_SETTLE_SECONDS          default 60    (ignore incidents during deploy churn)
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

ENGINE_URL = os.getenv("CEREBRAL_ENGINE_URL", "http://localhost:8080").rstrip("/")
ENGINE_TIMEOUT = float(os.getenv("CEREBRAL_ENGINE_TIMEOUT", "240"))
POLL_INTERVAL = float(os.getenv("CEREBRAL_PIPELINE_POLL_INTERVAL", "5"))
PIPELINE_TIMEOUT = float(os.getenv("CEREBRAL_PIPELINE_TIMEOUT", str(ENGINE_TIMEOUT)))
# AIOpsLab redeploys the whole app per problem, so the first ~30-60s is deploy
# churn (pods ContainerCreating -> spurious `availability` incidents). Wait for
# that to pass and the injected fault to manifest, then take the latest incident.
SETTLE_SECONDS = float(os.getenv("CEREBRAL_SETTLE_SECONDS", "60"))


# --------------------------------------------------------------------------- #
# HTTP plumbing (stdlib only — no engine imports)
# --------------------------------------------------------------------------- #
def _get(url: str, timeout: float) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _bare(node_id: str) -> str:
    return node_id.rsplit("/", 1)[-1] if node_id else node_id


# A ReplicaSet pod is "<workload>-<pod-template-hash>-<suffix>"; a StatefulSet
# pod is "<workload>-<ordinal>". The localization grader expects the WORKLOAD
# (service) name, e.g. "user", not the concrete pod "user-76f8ddf8bb-76gg9".
_POD_RS_HASH = re.compile(r"-[a-z0-9]{8,10}-[a-z0-9]{5}$")
_POD_ORDINAL = re.compile(r"-\d+$")


def _workload(node_id: str) -> str:
    """Reduce an engine component ref to its workload (service) name.

    The RCA may name the concrete Pod ("ns/Pod/user-76f8ddf8bb-76gg9") while the
    grader scores service names ("user"). Strip the pod-template-hash / ordinal
    so the right diagnosis isn't lost on naming granularity. Deployment / Service
    / StatefulSet refs are already workload-level and pass through unchanged.
    """
    if not node_id:
        return node_id
    parts = node_id.split("/")
    kind = parts[-2].lower() if len(parts) >= 2 else ""
    name = parts[-1]
    if kind in ("", "pod"):
        stripped = _POD_RS_HASH.sub("", name)
        if stripped != name:
            return stripped
        if kind == "pod":  # only strip a bare ordinal when we know it's a Pod
            return _POD_ORDINAL.sub("", name)
    return name


# --------------------------------------------------------------------------- #
# AIOpsLab Agent contract
# --------------------------------------------------------------------------- #
def _fence(call: str) -> str:
    # The parser requires exactly one ```-fenced block holding one API call.
    return f"```\n{call}\n```"


def _exec(cmd: str) -> str:
    return _fence(f"exec_shell({json.dumps(cmd)})")


class Agent:
    """AIOpsLab agent that relays the Cerebral engine's autonomous incident."""

    def __init__(self) -> None:
        self.task_kind = "detection"
        self.namespace = "default"
        self.submitted = False
        self.run_id: str | None = None
        self.done: dict[str, Any] | None = None
        self._mitigation_acted = False
        self._wait_started = time.time()
        self._created_after = self._wait_started

    def init_context(self, problem_desc: str, instructions: str, apis: dict[str, str]) -> None:
        # Read only the namespace out of the task; the engine localizes itself.
        self.namespace = self._field(problem_desc, "Namespace") or "default"
        self.task_kind = self._detect_task_kind(f"{problem_desc}\n{instructions}".lower())
        # Only match incidents created after this episode actually started (post
        # deploy + wait_for_ready + fault injection). No lookback — a lookback
        # window catches the pre-ready deploy churn we want to exclude.
        self._wait_started = time.time()
        self._created_after = self._wait_started

    async def get_action(self, _input: str) -> str:
        if self.submitted:
            return _fence('submit("")')
        if self.done is None:
            ev = self._advance()
            if ev is None:
                return _exec(f"sleep {int(POLL_INTERVAL)}")  # not ready -> wait & re-poll
            self.done = ev
        return self._final_action()

    # --- wait for, then read, the engine's incident ------------------------- #
    def _advance(self) -> dict[str, Any] | None:
        try:
            if not self.run_id:
                # Let deploy churn pass + the injected fault manifest before matching.
                if time.time() - self._wait_started < SETTLE_SECONDS:
                    return None
                doc = self._latest_namespace_incident()
                if doc:
                    self.run_id = doc.get("run_id")
                    return self._incident_to_done(doc)
                if time.time() - self._wait_started > PIPELINE_TIMEOUT:
                    return {"kind": "done", "anomaly": False}
                return None
            doc = _get(f"{ENGINE_URL}/api/incidents/{urllib.parse.quote(self.run_id)}", ENGINE_TIMEOUT)
            return self._incident_to_done(doc)
        except (urllib.error.URLError, OSError, ValueError):
            # Engine unreachable / errored — terminate honestly (no anomaly).
            return {"kind": "done", "anomaly": False}

    def _latest_namespace_incident(self) -> dict[str, Any] | None:
        listing = _get(f"{ENGINE_URL}/api/incidents?limit=50", ENGINE_TIMEOUT)
        candidates = []
        for doc in listing.get("incidents", []):
            if doc.get("namespace") != self.namespace:
                continue
            if not self._is_recent(doc.get("created_at") or doc.get("updated_at")):
                continue
            candidates.append(doc)
        if not candidates:
            return None
        # Deploy-churn incidents form first; the injected fault's incident forms
        # later, so the latest post-episode incident is the fault.
        candidates.sort(key=lambda d: self._ts(d.get("created_at") or d.get("updated_at")), reverse=True)
        return candidates[0]

    @staticmethod
    def _ts(ts: Any) -> float:
        if not isinstance(ts, str) or not ts:
            return 0.0
        try:
            p = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if p.tzinfo is None:
                p = p.replace(tzinfo=timezone.utc)
            return p.timestamp()
        except ValueError:
            return 0.0

    def _incident_to_done(self, doc: dict[str, Any]) -> dict[str, Any] | None:
        status = doc.get("status")
        if status in {"active", ""}:
            if time.time() - self._wait_started > PIPELINE_TIMEOUT:
                return {"kind": "done", "anomaly": False}
            return None
        if status == "no_trigger":
            return {"kind": "done", "anomaly": False}

        decision = doc.get("decision") or {}
        rca = self._rca_from_events(doc.get("events") or [])
        service = doc.get("service") or ""
        localized = _workload(rca.get("component") or service)
        # The engine emits {system_level, fault_type} on the RCA; relay it for the
        # analysis task. Fall back to a top-level incident taxonomy if present.
        taxonomy = doc.get("taxonomy") or {}
        if rca.get("system_level") and rca.get("fault_type"):
            taxonomy = {"system_level": rca["system_level"], "fault_type": rca["fault_type"]}
        done = {
            "kind": "done",
            "anomaly": status == "completed" or bool(service),
            "service": service,
            "localized": localized,
            "decision_type": decision.get("decision_type"),
            "reasoning": decision.get("reasoning"),
            "rca": rca or None,
            "taxonomy": taxonomy,
        }
        cmd = self._remediation_from_decision(decision)
        if cmd:
            done["remediation"] = {"command": cmd, "action": decision.get("decision_type")}
        return done

    @staticmethod
    def _rca_from_events(events: list[dict[str, Any]]) -> dict[str, Any]:
        for ev in reversed(events):
            if ev.get("event_type") == "rca_synthesis":
                return ev.get("summary") or {}
        return {}

    def _remediation_from_decision(self, decision: dict[str, Any]) -> str:
        plan = decision.get("execution_plan") or {}
        # Prefer the engine's synthesized fix command; the engine already
        # validated it as a single, safe kubectl invocation.
        cmd = (plan.get("command") or "").strip()
        if cmd:
            return cmd
        if decision.get("decision_type") != "restart_deployment":
            return ""
        params = plan.get("parameters") or {}
        svc = _workload(params.get("service") or self.done_service_fallback())
        ns = params.get("namespace") or self.namespace
        return f"kubectl rollout restart deployment/{svc} -n {ns}" if svc else ""

    def done_service_fallback(self) -> str:
        return (self.done or {}).get("service") or ""

    def _final_action(self) -> str:
        d = self.done or {}
        if self.task_kind == "detection":
            self.submitted = True
            return _fence(f'submit("{"Yes" if d.get("anomaly") else "No"}")')

        if self.task_kind == "localization":
            self.submitted = True
            origin = _workload(d.get("localized") or "")
            return _fence(f"submit({json.dumps([origin] if origin else [])})")

        if self.task_kind == "analysis":
            self.submitted = True
            return _fence(f"submit({json.dumps(d.get('taxonomy') or {})})")

        # mitigation: run the engine's recommended fix, then submit.
        if not self._mitigation_acted:
            self._mitigation_acted = True
            cmd = ((d.get("remediation") or {}).get("command") or "").strip()
            if cmd:
                return _exec(cmd)
        self.submitted = True
        return _fence("submit()")

    # --- helpers ------------------------------------------------------------ #
    @staticmethod
    def _field(text: str, label: str) -> str:
        m = re.search(rf"{re.escape(label)}\s*:\s*(\S+)", text)
        return m.group(1).strip() if m else ""

    @staticmethod
    def _detect_task_kind(blob: str) -> str:
        if "system_level" in blob or "fault_type" in blob or "fault type" in blob:
            return "analysis"
        if "localiz" in blob or "which service" in blob or "faulty component" in blob:
            return "localization"
        if "mitigat" in blob or "remediat" in blob or "resolve the" in blob or "fix the" in blob:
            return "mitigation"
        return "detection"

    def _is_recent(self, ts: Any) -> bool:
        if not isinstance(ts, str) or not ts:
            return True
        try:
            parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp() >= self._created_after
        except ValueError:
            return True
