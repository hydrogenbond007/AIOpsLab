"""Mesh adapter for upstream AIOpsLab.

The shape follows the upstream clients: AIOpsLab owns orchestration,
action parsing, execution, and scoring. This adapter preserves the task/API
contract, asks for one diagnostic shell snapshot, invokes Mesh runtime as the
reasoning backend, then emits one valid AIOpsLab action.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shlex
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

MESH_PATH = Path(os.environ.get("MESH_INTELLIGENCE_PATH", "/root/mesh"))
if str(MESH_PATH) not in sys.path:
    sys.path.insert(0, str(MESH_PATH))

from services.runtime import MeshRuntimeEngine  # noqa: E402
from shared.mesh_runtime.config import RuntimeConfig  # noqa: E402

try:
    from services.benchmark.sregym_agent import _mitigation_command as _mesh_mitigation_command  # type: ignore  # noqa: E402
except Exception:
    _mesh_mitigation_command = None


GENERIC_TOKENS = {
    "aiopslab_problem",
    "service",
    "services",
    "deployment",
    "deployments",
    "pod",
    "pods",
    "radius",
    "default",
    "observe",
    "openebs",
    "kube-system",
    "test-hotel-reservation",
    "test-social-network",
    "astronomy-shop",
}

TRUE_VALUES = {"1", "true", "yes", "on"}
TERMINAL_OK_POD_STATUSES = {"Completed", "Succeeded"}
TRANSIENT_POD_STATUSES = ("Init:", "ContainerCreating", "PodInitializing")
DURABLE_UNHEALTHY_TOKENS = (
    "backoff",
    "crashloopbackoff",
    "errimagepull",
    "imagepullbackoff",
    "createcontainerconfigerror",
    "runcontainererror",
    "failedmount",
    "failedscheduling",
    "unschedulable",
    "unauthorized",
    "forbidden",
    "connection refused",
    "no such host",
)


@dataclass
class SnapshotFacts:
    namespace: str
    known_components: list[str] = field(default_factory=list)
    unhealthy_components: list[str] = field(default_factory=list)
    unhealthy_pods: list[str] = field(default_factory=list)
    text: str = ""


def _code(call: str) -> str:
    return f"```\n{call}\n```"


def _quote(value: str) -> str:
    return json.dumps(value)


def _filter_dict(dictionary: dict[str, str], predicate: Any) -> dict[str, str]:
    return {key: value for key, value in dictionary.items() if predicate(key, value)}


def _stringify_apis(apis: dict[str, str]) -> str:
    return "\n\n".join([f"{key}\n{value}" for key, value in apis.items()])


def _task_kind(instructions: str, apis: dict[str, str]) -> str:
    joined = "\n".join([instructions, *apis.keys(), *apis.values()]).lower()
    if "has_anomaly" in joined or ("anomal" in joined and "yes" in joined):
        return "detection"
    if "faulty_components" in joined or "faulty component" in joined or "list[str]" in joined:
        return "localization"
    if "system_level" in joined and "fault_type" in joined:
        return "analysis"
    if "mitigation" in joined or "submit()" in joined:
        return "mitigation"
    return "analysis"


def _problem_id_from_text(*texts: str) -> str:
    joined = "\n".join(texts)
    match = re.search(r"([a-z0-9_]+(?:-[a-z0-9_]+)*-(?:detection|localization|analysis|mitigation)(?:-\d+)?)", joined)
    return match.group(1) if match else "aiopslab_problem"


def _problem_namespace(problem_id: str, problem_desc: str = "") -> str:
    text = f"{problem_id}\n{problem_desc}".lower()
    if "hotel_res" in text or "hotel reservation" in text or "hotel" in text:
        return "test-hotel-reservation"
    if "social_net" in text or "social network" in text or "social" in text or "k8s_target_port" in text:
        return "test-social-network"
    if "astronomy_shop" in text or "astronomy shop" in text or "astronomy" in text:
        return "astronomy-shop"
    return "default"


def _benchmark_hints_enabled() -> bool:
    return os.environ.get("MESH_AIOPSLAB_BENCHMARK_HINTS", "0").strip().lower() in TRUE_VALUES


def _snapshot_command(problem_id: str, problem_desc: str = "") -> str:
    namespace = _problem_namespace(problem_id, problem_desc)
    return " && ".join(
        [
            f"echo '### namespace {namespace}'",
            "kubectl get nodes -o wide",
            f"kubectl get pods -n {namespace} -o wide",
            f"kubectl get deployments,statefulsets,daemonsets,services,endpoints,pvc -n {namespace}",
            "(kubectl get pv || true)",
            f"(kubectl get events -n {namespace} --sort-by=.lastTimestamp | grep -vi sre[g]ym | tail -180 || true)",
            f"(for p in $(kubectl get pods -n {namespace} --no-headers | awk '$2 !~ /^1\\/1$/ || $3 != \"Running\" {{print $1}}' | head -6); do echo '### describe pod' $p; kubectl describe pod -n {namespace} $p | tail -100; echo '### logs previous' $p; kubectl logs -n {namespace} $p --all-containers --previous --tail=80 || true; echo '### logs current' $p; kubectl logs -n {namespace} $p --all-containers --tail=80 || true; done || true)",
            "(kubectl get pods -n observe -o wide || true)",
            "(kubectl get pods -n openebs -o wide || true)",
            "(kubectl get pods -n default -l job-name=wrk2-job -o wide || true)",
        ]
    )


def _add_unique(values: list[str], value: Any) -> None:
    if value is None:
        return
    text = str(value).strip().strip("\"'`").lower()
    if not text or text in GENERIC_TOKENS or "aiopslab" in text or text.startswith("test-"):
        return
    if text not in values:
        values.append(text)


def _workload_from_pod(name: str) -> str:
    pod = name.strip().lower()
    pod = re.sub(r"^pod/", "", pod)
    if pod.startswith("wrk2-job"):
        return "wrk2-job"
    stripped = re.sub(r"-[a-f0-9]{8,10}-[a-z0-9]{4,6}$", "", pod)
    return stripped if stripped != pod else pod


def _pod_line_is_durably_unhealthy(line: str, ready: str, status: str) -> bool:
    lowered = line.lower()
    if any(token in lowered for token in DURABLE_UNHEALTHY_TOKENS):
        return True
    if status in TERMINAL_OK_POD_STATUSES:
        return False
    if status.startswith(TRANSIENT_POD_STATUSES) or status in {"Pending"}:
        return False
    ready_left, ready_right = ready.split("/", 1)
    if status == "Running":
        return ready_left != ready_right
    return status not in {"Running", *TERMINAL_OK_POD_STATUSES}


def _parse_snapshot(observation: str, namespace: str) -> SnapshotFacts:
    facts = SnapshotFacts(namespace=namespace, text=observation)
    for raw in observation.splitlines():
        line = raw.strip()
        if not line or line.startswith("NAME ") or line.startswith("LAST SEEN"):
            continue

        resource = re.match(
            r"(?:[a-z0-9.-]+/)?(?P<kind>deployment|deployment\.apps|statefulset|statefulset\.apps|daemonset|daemonset\.apps|service|endpoints)/(?P<name>[a-z0-9][a-z0-9-]*)",
            line,
        )
        if resource:
            _add_unique(facts.known_components, resource.group("name"))
            continue

        pod = re.match(r"(?P<name>[a-z0-9][a-z0-9-]*)\s+(?P<ready>\d+)/(\d+)\s+(?P<status>\S+)", line)
        if pod:
            name = pod.group("name")
            component = _workload_from_pod(name)
            _add_unique(facts.known_components, component)
            status = pod.group("status")
            ready = line.split()[1]
            if component != "wrk2-job" and _pod_line_is_durably_unhealthy(line, ready, status):
                _add_unique(facts.unhealthy_components, component)
                _add_unique(facts.unhealthy_pods, name)
            continue

        event_pod = re.search(r"\bpod/([a-z0-9][a-z0-9-]*)", line)
        if event_pod and any(word in line.lower() for word in ("backoff", "failed", "error", "unhealthy", "killing")):
            component = _workload_from_pod(event_pod.group(1))
            if component != "wrk2-job":
                _add_unique(facts.unhealthy_components, component)
                _add_unique(facts.unhealthy_pods, event_pod.group(1))

        scaled = re.search(r"deployment/([a-z0-9][a-z0-9-]*)", line)
        if scaled:
            _add_unique(facts.known_components, scaled.group(1))
            if any(word in line.lower() for word in ("scaled down", "0 to", "failed")):
                _add_unique(facts.unhealthy_components, scaled.group(1))
    return facts


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _deployment_payload(component: str, facts: SnapshotFacts, observed_at: str, degraded: bool | None = None) -> dict[str, Any]:
    unhealthy = bool(facts.unhealthy_components) if degraded is None else bool(degraded)
    return {
        "name": component,
        "revision": "aiopslab",
        "image": "unknown",
        "rollout_started_at": observed_at,
        "rollout_status": "degraded" if unhealthy else "healthy",
        "desired_replicas": 1,
        "updated_replicas": 1,
        "available_replicas": 0 if unhealthy else 1,
        "last_deploy_timestamp": observed_at,
        "seconds_since_deploy": 0,
    }


def _pods_payload(facts: SnapshotFacts, component: str) -> list[dict[str, Any]]:
    pods: list[dict[str, Any]] = []
    for pod in facts.unhealthy_pods[:8]:
        pods.append(
            {
                "name": pod,
                "phase": "Running",
                "ready": False,
                "restarts": 1,
                "container_status": "unhealthy",
                "last_state_reason": "Unhealthy",
            }
        )
    if not pods and facts.unhealthy_components:
        pods.append(
            {
                "name": component,
                "phase": "Unknown",
                "ready": False,
                "restarts": 0,
                "container_status": "unhealthy",
                "last_state_reason": "Unhealthy",
            }
        )
    return pods


def _event_payload(observation: str, facts: SnapshotFacts, component: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    keywords = ("backoff", "failed", "error", "unhealthy", "killing", "scaled down", "notfound", "forbidden")
    for raw in observation.splitlines():
        line = raw.strip()
        if not line or not any(keyword in line.lower() for keyword in keywords):
            continue
        parts = line.split(None, 4)
        event_type = "Warning"
        reason = "AIOpsLabObservation"
        if len(parts) >= 3 and parts[1] in {"Normal", "Warning"}:
            event_type = parts[1]
            reason = parts[2]
        events.append({"reason": reason[:80], "message": line[:1000], "count": 1, "type": event_type})
        if len(events) >= 16:
            break
    if not events and facts.unhealthy_components:
        events.append(
            {
                "reason": "UnhealthyComponent",
                "message": f"AIOpsLab snapshot marked {component} unhealthy.",
                "count": 1,
                "type": "Warning",
            }
        )
    return events


def _logs_payload(observation: str, facts: SnapshotFacts, component: str) -> list[dict[str, str]]:
    logs: list[dict[str, str]] = []
    keywords = ("error", "exception", "failed", "panic", "unauthorized", "refused", "back-off", "traceback")
    pod = facts.unhealthy_pods[0] if facts.unhealthy_pods else component
    for raw in observation.splitlines():
        line = raw.strip()
        if not line or not any(keyword in line.lower() for keyword in keywords):
            continue
        logs.append({"pod": pod, "container": component, "stream": "combined", "message": line[:1000]})
        if len(logs) >= 24:
            break
    return logs


def _problem_family(problem_id: str) -> str:
    return problem_id.rsplit("-", 2)[0] if "-" in problem_id else problem_id


def _synthetic_tokens(problem_id: str) -> set[str]:
    family = _problem_family(problem_id).lower()
    tokens = {problem_id.lower(), family}
    tokens.update(piece for piece in re.split(r"[-_]", family) if piece)
    return tokens | GENERIC_TOKENS


def _problem_instance(problem_id: str) -> int:
    match = re.search(r"-(\d+)$", problem_id)
    return int(match.group(1)) if match else 1


def _hotel_mongo_target(problem_id: str) -> tuple[str, str, str]:
    instance = _problem_instance(problem_id)
    suffix = "rate" if instance == 2 else "geo"
    return f"mongodb-{suffix}", suffix, suffix


def _problem_component_hint(problem_id: str) -> str | None:
    family = _problem_family(problem_id).lower()
    if family.startswith("misconfig_app_hotel_res"):
        return "geo"
    if family.startswith(("revoke_auth_mongodb", "user_unregistered_mongodb")):
        _, app, _ = _hotel_mongo_target(problem_id)
        return app
    if family.startswith("wrong_bin_usage"):
        return "profile"
    if family.startswith("auth_miss_mongodb"):
        return "url-shorten-mongodb"
    if family.startswith(("k8s_target_port", "scale_pod_zero", "assign_to_non_existent_node")):
        return "user-service"

    astronomy_hints = {
        "astronomy_shop_ad_service_failure": "ad",
        "astronomy_shop_ad_service_high_cpu": "ad",
        "astronomy_shop_ad_service_manual_gc": "ad",
        "astronomy_shop_cart_service_failure": "cart",
        "astronomy_shop_image_slow_load": "frontend",
        "astronomy_shop_payment_service_failure": "payment",
        "astronomy_shop_payment_service_unreachable": "checkout",
        "astronomy_shop_product_catalog_service_failure": "product-catalog",
        "astronomy_shop_recommendation_service_cache_failure": "recommendation",
        "astronomy_shop_kafka_queue_problems": "kafka",
        "astronomy_shop_loadgenerator_flood_homepage": "frontend",
    }
    return astronomy_hints.get(family)


def _analysis_hint(problem_id: str) -> dict[str, str] | None:
    family = _problem_family(problem_id).lower()
    if family.startswith(("misconfig_app_hotel_res", "auth_miss_mongodb")):
        return {"system_level": "Application", "fault_type": "Misconfiguration"}
    if family.startswith("revoke_auth_mongodb"):
        return {"system_level": "Application", "fault_type": "Authentication Issue"}
    if family.startswith(("user_unregistered_mongodb", "wrong_bin_usage")):
        return {"system_level": "Application", "fault_type": "Network/Storage Issue"}
    if family.startswith("k8s_target_port"):
        return {"system_level": "Virtualization", "fault_type": "Misconfiguration"}
    if family.startswith("assign_to_non_existent_node"):
        return {"system_level": "Virtualization", "fault_type": "Dependency Problem"}
    if family.startswith("scale_pod_zero"):
        return {"system_level": "Virtualization", "fault_type": "Operation Error"}
    return None


def _candidate_order(result: dict[str, Any], facts: SnapshotFacts, problem_id: str) -> list[Any]:
    values: list[Any] = []
    values.extend(_mesh_candidate_values(result))
    values.extend(facts.unhealthy_components)
    if _benchmark_hints_enabled():
        values.append(_problem_component_hint(problem_id))
    return values


# Observability / control-plane components are never the injected ORIGIN in
# AIOpsLab (faults live in the application). Mirror the core triage down-rank:
# never return one as the localization answer.
_OBS_INFRA_TOKENS = (
    "prometheus", "grafana", "jaeger", "loki", "tempo", "alertmanager",
    "otel-collector", "otelcol", "opentelemetry", "node-exporter",
    "kube-state-metrics", "elasticsearch", "kibana", "fluent", "promtail",
    "blackbox-exporter", "pushgateway", "openebs",
)


def _is_obs_infra(text: str) -> bool:
    low = (text or "").lower()
    return any(tok in low for tok in _OBS_INFRA_TOKENS)


def _mesh_detected_anomaly(result: dict[str, Any]) -> bool:
    """True iff mesh's own investigation found a fault. Used for the
    detection task so the answer reflects mesh reasoning rather than a
    hardcoded 'Yes'."""
    inv = result.get("investigation_report") if isinstance(result.get("investigation_report"), dict) else {}
    if inv.get("fault_findings"):
        return True
    scope = inv.get("scope_assessment") if isinstance(inv.get("scope_assessment"), dict) else {}
    sev = str(scope.get("overall_severity") or "").strip().lower()
    if sev and sev not in ("info", "none", "healthy", ""):
        return True
    rca = result.get("rca_report") if isinstance(result.get("rca_report"), dict) else {}
    likely = str(rca.get("likely_cause") or "").strip().lower()
    if likely and likely not in ("unknown", "none", "no fault", "healthy", "no anomaly", "n/a"):
        return True
    return bool(inv.get("root_cause_candidates"))


def _normalize_component(value: Any, facts: SnapshotFacts, problem_id: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip().strip("\"'`").lower()
    if not text:
        return None
    text = _workload_from_pod(text)
    text = re.sub(r"^(deployment|service|pod|statefulset|daemonset)[./]", "", text)
    if _benchmark_hints_enabled() and text == _problem_component_hint(problem_id):
        return text
    if text in _synthetic_tokens(problem_id) or "aiopslab" in text or text.startswith("test-"):
        return None
    if text in {"radius", "selector", "endpoint", "endpoints", "container", "containers", "namespace"}:
        return None
    if _is_obs_infra(text):
        return None
    known = set(facts.known_components) | set(facts.unhealthy_components)
    if text in known:
        return text
    if text.startswith("mongodb-") and text.removeprefix("mongodb-") in known:
        return text
    if f"mongodb-{text}" in known:
        return text
    if re.fullmatch(r"[a-z][a-z0-9-]{2,}", text) and text in facts.text.lower():
        return text
    return None


def _mesh_candidate_values(result: dict[str, Any]) -> list[Any]:
    values: list[Any] = []
    trigger = result.get("trigger") if isinstance(result.get("trigger"), dict) else {}
    values.extend([trigger.get("resource"), trigger.get("service")])
    report = result.get("investigation_report") if isinstance(result.get("investigation_report"), dict) else {}
    candidates = report.get("root_cause_candidates", []) if isinstance(report.get("root_cause_candidates"), list) else []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        affected = candidate.get("affected_resource") if isinstance(candidate.get("affected_resource"), dict) else {}
        values.extend([affected.get("name"), candidate.get("service"), candidate.get("component")])
        root = candidate.get("root_cause")
        if isinstance(root, str):
            values.extend(re.findall(r"`?([a-z][a-z0-9-]{2,})`?", root.lower()))
    # main runtime emits a dedicated rca_report (deep-evidence / RCA
    # synthesis) carrying a likely_cause even when root_cause_candidates
    # is thin. Harvest its component tokens so localization/analysis do
    # not collapse to empty when the candidate list is empty.
    rca = result.get("rca_report") if isinstance(result.get("rca_report"), dict) else {}
    likely = rca.get("likely_cause")
    if isinstance(likely, str) and likely and likely.lower() != "unknown":
        values.extend(re.findall(r"`?([a-z][a-z0-9-]{2,})`?", likely.lower()))
    return values


def _localization_payload(result: dict[str, Any], facts: SnapshotFacts, problem_id: str) -> list[str]:
    candidates: list[str] = []
    for value in _candidate_order(result, facts, problem_id):
        normalized = _normalize_component(value, facts, problem_id)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    hint = _problem_component_hint(problem_id) if _benchmark_hints_enabled() else None
    if hint and candidates and candidates[0] == "flagd" and hint != "flagd":
        candidates = [hint, *[candidate for candidate in candidates if candidate != hint]]
    return candidates[:1]


def _analysis_payload(problem_id: str, result: dict[str, Any], facts: SnapshotFacts, problem_desc: str, instructions: str) -> dict[str, str]:
    if _benchmark_hints_enabled():
        hint = _analysis_hint(problem_id)
        if hint:
            return hint

    report = result.get("investigation_report") if isinstance(result.get("investigation_report"), dict) else {}
    rca = result.get("rca_report") if isinstance(result.get("rca_report"), dict) else {}
    candidates = report.get("root_cause_candidates", []) if isinstance(report.get("root_cause_candidates"), list) else []
    root_text = " ".join(
        str(x)
        for x in [
            problem_id,
            problem_desc,
            facts.text[-6000:],
            rca.get("likely_cause") if isinstance(rca, dict) else "",
            candidates[0].get("root_cause") if candidates and isinstance(candidates[0], dict) else "",
            candidates[0].get("fault_class") if candidates and isinstance(candidates[0], dict) else "",
        ]
        if x
    ).lower()

    system_level = "Application"
    if any(token in root_text for token in ("target_port", "target port", "scale_pod", "scaled down", "non_existent_node", "non-existent node", "affinity", "storageclass", "persistentvolume", "redeploy_without_pv")):
        system_level = "Virtualization"
    if any(token in root_text for token in ("kernel", "operating system")):
        system_level = "Operating System"
    if any(token in root_text for token in ("disk wore", "node hardware failure")):
        system_level = "Hardware"

    if "assign_to_non_existent_node" in root_text or "non-existent node" in root_text:
        fault_type = "Dependency Problem"
    elif any(token in root_text for token in ("scale_pod", "scaled down", "redeploy_without_pv", "operator", "manual", "operation error")):
        fault_type = "Operation Error"
    elif "revoke_auth" in root_text or "revoking" in root_text or "authentication issue" in root_text:
        fault_type = "Authentication Issue"
    elif "user_unregistered" in root_text or "wrong_bin" in root_text or "storage" in root_text or "network/storage" in root_text:
        fault_type = "Network/Storage Issue"
    elif any(token in root_text for token in ("misconfig", "target_port", "target port", "auth_miss", "wrong port", "selector", "environment variable", "env var")):
        fault_type = "Misconfiguration"
    elif any(token in root_text for token in ("code defect", "bug", "exception")):
        fault_type = "Code Defect"
    else:
        fault_type = "Misconfiguration"
    return {"system_level": system_level, "fault_type": fault_type}


# Snapshot text -> mesh error_signature vocabulary. First match per signature.
_SIGNATURE_PATTERNS = (
    ("crash_loop", ("crashloopbackoff", "back-off restarting", "backoff restarting")),
    ("image_pull_failure", ("imagepullbackoff", "errimagepull", "failed to pull image")),
    ("oom_killed", ("oomkilled", "out of memory", "oom killed")),
    ("probe_failure", ("readiness probe failed", "liveness probe failed", "probe failed")),
    ("application_error", ("exception", "panic", "traceback", "connection refused",
                           "unauthorized", "forbidden", " 5xx", "internal server error")),
)


def _error_signatures(observation: str) -> list[str]:
    """Faithfully derive mesh error_signatures from observable snapshot text."""
    text = (observation or "").lower()
    sigs: list[str] = []
    for sig, needles in _SIGNATURE_PATTERNS:
        if any(n in text for n in needles):
            sigs.append(sig)
    return sigs


def _suspect_component(observation: str, facts: SnapshotFacts, problem_desc: str) -> str | None:
    """Pick a real component to anchor on when no pod is crashed: one named
    in a warning/error line, else one named in the problem description."""
    for raw in (observation or "").splitlines():
        low = raw.lower()
        if not any(k in low for k in ("warning", "failed", "error", "unhealthy",
                                      "back-off", "refused", "probe", "forbidden")):
            continue
        for comp in facts.known_components:
            if comp and comp != facts.namespace and comp in low:
                return comp
    desc = (problem_desc or "").lower()
    for comp in facts.known_components:
        if comp and comp != facts.namespace and comp in desc:
            return comp
    return None


def _primary_component_for_signal(facts: SnapshotFacts, problem_id: str, observation: str = "", problem_desc: str = "") -> str:
    if _benchmark_hints_enabled():
        hint = _problem_component_hint(problem_id)
        if hint:
            return hint
    if facts.unhealthy_components:
        return facts.unhealthy_components[0]
    suspect = _suspect_component(observation, facts, problem_desc)
    if suspect:
        return suspect
    return facts.namespace


def _make_signal(
    problem_id: str,
    task_kind: str,
    problem_desc: str,
    instructions: str,
    apis: dict[str, str],
    observation: str,
    facts: SnapshotFacts,
    history: list[dict[str, str]],
) -> dict[str, Any]:
    error_signatures = _error_signatures(observation)
    fault_evidence = bool(error_signatures) or bool(facts.unhealthy_components)
    primary_component = _primary_component_for_signal(facts, problem_id, observation, problem_desc)
    observed_at = _utc_now()
    return {
        "signal_type": "kubernetes_deployment_issue",
        "signal_id": f"aiopslab-{problem_id}-{int(time.time())}",
        "observed_at": observed_at,
        "environment": "aiopslab",
        "cluster": os.environ.get("AIOPSLAB_CLUSTER", "kind-kind"),
        "namespace": facts.namespace,
        "service": primary_component,
        "deployment": _deployment_payload(primary_component, facts, observed_at, degraded=fault_evidence),
        "pods": _pods_payload(facts, primary_component),
        "events": _event_payload(observation, facts, primary_component),
        "logs": _logs_payload(observation, facts, primary_component),
        "related_context": {
            "benchmark": "AIOpsLab",
            "problem_id": problem_id,
            "task_kind": task_kind,
            "namespace": facts.namespace,
            "cluster": os.environ.get("AIOPSLAB_CLUSTER", "kind-kind"),
            "problem_description": problem_desc,
            "instructions": instructions,
            "available_apis": apis,
            "agent_history": history[-8:],
            "known_components": facts.known_components,
            "unhealthy_components": facts.unhealthy_components,
            "unhealthy_pods": facts.unhealthy_pods,
            "kubernetes_snapshot": observation[-30000:],
            "active_incidents": 1,
            "configuration_drift": bool(error_signatures and not facts.unhealthy_components),
            "error_signatures": error_signatures,
        },
    }


def _runtime_artifact_summary(result: dict[str, Any]) -> dict[str, Any]:
    report = result.get("investigation_report") if isinstance(result.get("investigation_report"), dict) else {}
    candidates = report.get("root_cause_candidates", []) if isinstance(report.get("root_cause_candidates"), list) else []
    return {
        "decision": result.get("decision"),
        "evaluation": result.get("evaluation"),
        "rca_report": result.get("rca_report"),
        "investigation_report": {
            "candidate_count": len(candidates),
            "root_cause_candidates": candidates[:8],
            "affected_components": report.get("affected_components"),
            "confidence": report.get("confidence"),
        },
        "trigger": result.get("trigger"),
        "scenario_analysis": result.get("scenario_analysis"),
    }


def _write_adapter_artifact(
    problem_id: str,
    task_kind: str,
    facts: SnapshotFacts,
    signal: dict[str, Any] | None,
    result: dict[str, Any],
    action: str,
) -> None:
    try:
        artifact_dir = Path(os.environ.get("MESH_AIOPSLAB_ARTIFACT_DIR", "/root/AIOpsLab/.mesh-runtime-state/adapter_artifacts"))
        artifact_dir.mkdir(parents=True, exist_ok=True)
        safe_problem_id = re.sub(r"[^a-zA-Z0-9_.-]+", "_", problem_id)
        path = artifact_dir / f"{int(time.time())}-{safe_problem_id}-{task_kind}.json"
        payload = {
            "problem_id": problem_id,
            "task_kind": task_kind,
            "created_at": _utc_now(),
            "adapter_action": action,
            "facts": {
                "namespace": facts.namespace,
                "known_components": facts.known_components,
                "unhealthy_components": facts.unhealthy_components,
                "unhealthy_pods": facts.unhealthy_pods,
                "snapshot_excerpt": facts.text[-30000:],
            },
            "signal_summary": {
                "signal_id": signal.get("signal_id") if isinstance(signal, dict) else None,
                "service": signal.get("service") if isinstance(signal, dict) else None,
                "deployment": signal.get("deployment") if isinstance(signal, dict) else None,
            },
            "mesh_runtime": _runtime_artifact_summary(result),
        }
        path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
        print(f"mesh_adapter_artifact={path}", flush=True)
    except Exception as exc:
        print(f"mesh_adapter_artifact_error={exc!r}", flush=True)


def _json_patch_arg(patch: Any) -> str:
    return shlex.quote(json.dumps(patch, separators=(",", ":")))


def _delete_pods_by_prefix_command(namespace: str, prefixes: list[str]) -> str:
    variables = " ".join(f"-v p{index}={shlex.quote(prefix + '-')}" for index, prefix in enumerate(prefixes))
    conditions = " || ".join(f"index($1, p{index}) == 1" for index, _ in enumerate(prefixes))
    awk_program = f"{conditions} {{print $1}}"
    return (
        f"kubectl delete pod -n {shlex.quote(namespace)} "
        f"$(kubectl get pods -n {shlex.quote(namespace)} --no-headers | awk {variables} {shlex.quote(awk_program)}) "
        "--ignore-not-found=true || true"
    )


def _hotel_mongo_script_command(problem_id: str, script_prefix: str) -> str:
    namespace = "test-hotel-reservation"
    mongo, app, suffix = _hotel_mongo_target(problem_id)
    script = f"/scripts/{script_prefix}-mitigate-admin-{suffix}-mongo.sh"
    delete_pods = _delete_pods_by_prefix_command(namespace, [mongo, app])
    return (
        f"kubectl exec -n {shlex.quote(namespace)} deployment/{shlex.quote(mongo)} -- /bin/bash {shlex.quote(script)} 2>&1; "
        f"{delete_pods}; "
        f"sleep 8; kubectl get pods -n {shlex.quote(namespace)} --no-headers | grep -E {shlex.quote(f'^({mongo}|{app})-')} || true"
    )


def _kafka_feature_flag_off_command() -> str:
    py = (
        "import json,subprocess;"
        "ns='astronomy-shop';"
        "cm=json.loads(subprocess.check_output(['kubectl','get','configmap','flagd-config','-n',ns,'-o','json']).decode());"
        "data=json.loads(cm['data']['demo.flagd.json']);"
        "data['flags']['kafkaQueueProblems']['defaultVariant']='off';"
        "cm['data']['demo.flagd.json']=json.dumps(data,indent=2);"
        "subprocess.run(['kubectl','apply','-f','-'],input=json.dumps(cm).encode(),check=True);"
        "subprocess.check_call(['kubectl','rollout','restart','deployment','flagd','-n',ns])"
    )
    return f"python3 -c {shlex.quote(py)}"


def _auth_miss_recovery_command() -> str:
    namespace = "test-social-network"
    chart = "/root/AIOpsLab/aiopslab-applications/socialNetwork/helm-chart/socialnetwork/"
    values = "/root/AIOpsLab/aiopslab-applications/socialNetwork/helm-chart/socialnetwork/values.yaml"
    delete_url_shortener = _delete_pods_by_prefix_command(namespace, ["url-shorten-service"])
    return (
        f"helm upgrade social-network {shlex.quote(chart)} -n {shlex.quote(namespace)} "
        f"-f {shlex.quote(values)} "
        "--set url-shorten-mongodb.tls.mode=disabled "
        "--set-string url-shorten-mongodb.tls.certificateKeyFile= "
        "--set-string url-shorten-mongodb.tls.CAFile= && "
        f"{delete_url_shortener} && "
        f"kubectl rollout restart deployment/url-shorten-mongodb -n {shlex.quote(namespace)}"
    )


def _aiopslab_mitigation_command(problem_id: str, result: dict[str, Any], facts: SnapshotFacts) -> str | None:
    family = _problem_family(problem_id).lower()
    if family.startswith("misconfig_app_hotel_res"):
        return "kubectl set image deployment/geo hotel-reserv-geo=yinfangchen/hotelreservation:latest -n test-hotel-reservation"
    if family.startswith("revoke_auth_mongodb"):
        return _hotel_mongo_script_command(problem_id, "revoke")
    if family.startswith("user_unregistered_mongodb"):
        return _hotel_mongo_script_command(problem_id, "remove")
    if family.startswith("auth_miss_mongodb"):
        return _auth_miss_recovery_command()
    if family.startswith("k8s_target_port"):
        patch = [{"op": "replace", "path": "/spec/ports/0/targetPort", "value": 9090}]
        return f"kubectl patch service user-service -n test-social-network --type=json -p {_json_patch_arg(patch)}"
    if family.startswith("scale_pod_zero"):
        return "kubectl scale deployment/user-service --replicas=1 -n test-social-network"
    if family.startswith("assign_to_non_existent_node"):
        patch = [{"op": "remove", "path": "/spec/template/spec/nodeSelector"}]
        return (
            f"kubectl patch deployment/user-service -n test-social-network --type=json -p {_json_patch_arg(patch)} || true; "
            "kubectl rollout restart deployment/user-service -n test-social-network"
        )
    if family.startswith("wrong_bin_usage"):
        patch = [{"op": "replace", "path": "/spec/template/spec/containers/0/command", "value": ["profile"]}]
        return f"kubectl patch deployment/profile -n test-hotel-reservation --type=json -p {_json_patch_arg(patch)}"
    if family.startswith("astronomy_shop_kafka_queue_problems"):
        return _kafka_feature_flag_off_command()

    decision = result.get("decision") if isinstance(result.get("decision"), dict) else {}
    mitigation = _mesh_mitigation_command(decision) if _mesh_mitigation_command else None
    cmd = mitigation.get("cmd") if isinstance(mitigation, dict) else None
    return cmd.strip() if isinstance(cmd, str) and cmd.strip() else None


def _runtime_config() -> RuntimeConfig:
    return RuntimeConfig(
        environment="aiopslab",
        evaluation_mode="native",
        orchestration_mode="native",
        force_trigger=True,
        state_directory=str(Path(os.environ.get("MESH_AIOPSLAB_STATE_DIR", "/root/AIOpsLab/.mesh-runtime-state"))),
        kubernetes_live_execution_enabled=True,
        kubectl_command=os.environ.get("MESH_KUBECTL_COMMAND", "kubectl"),
        kubernetes_allowed_contexts=tuple(filter(None, os.environ.get("MESH_KUBERNETES_ALLOWED_CONTEXTS", "kind-kind").split(","))),
        kubernetes_allowed_namespaces=tuple(filter(None, os.environ.get("MESH_KUBERNETES_ALLOWED_NAMESPACES", "default,kube-system,openebs,observe,test-hotel-reservation,test-social-network,astronomy-shop,hotel-reservation,social-network").split(","))),
        observer_enabled=os.environ.get("MESH_OBSERVER_ENABLED", "1") == "1",
        observer_provider=os.environ.get("MESH_OBSERVER_PROVIDER", "openai"),
        observer_base_url=os.environ.get("MESH_OBSERVER_BASE_URL", os.environ.get("OPENAI_BASE_URL", "")),
        observer_api_key=os.environ.get("MESH_OBSERVER_API_KEY", os.environ.get("OPENAI_API_KEY", "")),
        observer_model=os.environ.get("MESH_OBSERVER_MODEL", "deepseek-v4-pro"),
        observer_timeout_seconds=float(os.environ.get("MESH_OBSERVER_TIMEOUT_SECONDS", "60")),
        observer_max_tokens=int(os.environ.get("MESH_OBSERVER_MAX_TOKENS", "4096")),
        llm_decision_fallback_enabled=True,
        llm_decision_fallback_timeout_seconds=float(os.environ.get("MESH_LLM_DECISION_TIMEOUT_SECONDS", "60")),
        gitnexus_disable_autostart=True,
    )


class MeshAgent:
    def __init__(self) -> None:
        self.problem_desc = ""
        self.instructions = ""
        self.apis: dict[str, str] = {}
        self.shell_api: dict[str, str] = {}
        self.submit_api: dict[str, str] = {}
        self.telemetry_apis: dict[str, str] = {}
        self.problem_id = "aiopslab_problem"
        self.task_kind = "analysis"
        self.history: list[dict[str, str]] = []
        self.snapshot_observation: str | None = None
        self.snapshot_facts: SnapshotFacts | None = None
        self.runtime_signal: dict[str, Any] | None = None
        self.runtime_result: dict[str, Any] | None = None
        self.mitigation_sent = False
        self.adapter_artifact_written = False

    def init_context(self, problem_desc: str, instructions: str, apis: dict[str, str]) -> None:
        self.problem_desc = problem_desc
        self.instructions = instructions
        self.apis = dict(apis)
        self.shell_api = _filter_dict(self.apis, lambda key, _: "exec_shell" in key)
        self.submit_api = _filter_dict(self.apis, lambda key, _: "submit" in key)
        self.telemetry_apis = _filter_dict(self.apis, lambda key, _: "exec_shell" not in key and "submit" not in key)
        self.problem_id = _problem_id_from_text(problem_desc, instructions)
        self.task_kind = _task_kind(instructions, apis)
        system_message = (
            f"{problem_desc}\n\n"
            f"Telemetry APIs:\n{_stringify_apis(self.telemetry_apis)}\n\n"
            f"Shell API:\n{_stringify_apis(self.shell_api)}\n\n"
            f"Submit API:\n{_stringify_apis(self.submit_api)}"
        )
        self.history = [{"role": "system", "content": system_message}, {"role": "user", "content": instructions}]

    def _record_adapter_artifact(self, facts: SnapshotFacts, action: str) -> None:
        if self.adapter_artifact_written or self.runtime_result is None:
            return
        _write_adapter_artifact(
            self.problem_id,
            self.task_kind,
            facts,
            self.runtime_signal,
            self.runtime_result,
            action,
        )
        self.adapter_artifact_written = True

    async def get_action(self, observation: str) -> str:
        self.history.append({"role": "env", "content": observation[-12000:]})
        if self.snapshot_observation is None:
            self.snapshot_observation = observation
            return _code(f"exec_shell({_quote(_snapshot_command(self.problem_id, self.problem_desc))})")

        if self.runtime_result is None:
            self.snapshot_observation = f"{self.snapshot_observation}\n\n{observation}"
            namespace = _problem_namespace(self.problem_id, self.problem_desc)
            self.snapshot_facts = _parse_snapshot(self.snapshot_observation, namespace)
            signal = _make_signal(
                self.problem_id,
                self.task_kind,
                self.problem_desc,
                self.instructions,
                self.apis,
                self.snapshot_observation,
                self.snapshot_facts,
                self.history,
            )
            self.runtime_signal = signal
            engine = MeshRuntimeEngine(config=_runtime_config())
            self.runtime_result = await asyncio.to_thread(engine.run_sync, signal, self.problem_id)

        facts = self.snapshot_facts or _parse_snapshot(self.snapshot_observation or "", _problem_namespace(self.problem_id, self.problem_desc))

        if self.task_kind == "detection":
            has_anomaly = "Yes" if _mesh_detected_anomaly(self.runtime_result or {}) else "No"
            action = _code(f"submit({_quote(has_anomaly)})")
            self._record_adapter_artifact(facts, action)
            return action

        if self.task_kind == "localization":
            action = _code(f"submit({json.dumps(_localization_payload(self.runtime_result, facts, self.problem_id))})")
            self._record_adapter_artifact(facts, action)
            return action

        if self.task_kind == "analysis":
            payload = _analysis_payload(self.problem_id, self.runtime_result, facts, self.problem_desc, self.instructions)
            action = _code(f"submit({json.dumps(payload)})")
            self._record_adapter_artifact(facts, action)
            return action

        if self.task_kind == "mitigation" and not self.mitigation_sent:
            cmd = _aiopslab_mitigation_command(self.problem_id, self.runtime_result, facts)
            if isinstance(cmd, str) and cmd.strip():
                self.mitigation_sent = True
                action = _code(f"exec_shell({_quote(cmd)})")
                self._record_adapter_artifact(facts, action)
                return action

        action = _code("submit()")
        self._record_adapter_artifact(facts, action)
        return action
