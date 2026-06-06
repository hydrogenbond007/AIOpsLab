from __future__ import annotations

import argparse
import asyncio
import json
import traceback
import os
from pathlib import Path
from typing import Any

from aiopslab.orchestrator import Orchestrator
from aiopslab.orchestrator.problems.registry import ProblemRegistry
from clients.mesh import MeshAgent
from dotenv import load_dotenv

load_dotenv()

DEFAULT_PROBLEMS = [
    "misconfig_app_hotel_res-detection-1",
    "misconfig_app_hotel_res-localization-1",
    "misconfig_app_hotel_res-analysis-1",
    "misconfig_app_hotel_res-mitigation-1",
    "revoke_auth_mongodb-detection-1",
    "revoke_auth_mongodb-localization-1",
    "revoke_auth_mongodb-analysis-1",
    "revoke_auth_mongodb-mitigation-1",
    "revoke_auth_mongodb-detection-2",
    "revoke_auth_mongodb-localization-2",
    "revoke_auth_mongodb-analysis-2",
    "revoke_auth_mongodb-mitigation-2",
    "user_unregistered_mongodb-detection-1",
    "user_unregistered_mongodb-localization-1",
    "user_unregistered_mongodb-analysis-1",
    "user_unregistered_mongodb-mitigation-1",
    "user_unregistered_mongodb-detection-2",
    "user_unregistered_mongodb-localization-2",
    "user_unregistered_mongodb-analysis-2",
    "user_unregistered_mongodb-mitigation-2",
    "assign_to_non_existent_node_social_net-detection-1",
    "assign_to_non_existent_node_social_net-localization-1",
    "assign_to_non_existent_node_social_net-analysis-1",
    "assign_to_non_existent_node_social_net-mitigation-1",
    "auth_miss_mongodb-detection-1",
    "auth_miss_mongodb-localization-1",
    "auth_miss_mongodb-analysis-1",
    "auth_miss_mongodb-mitigation-1",
    "k8s_target_port-misconfig-detection-1",
    "k8s_target_port-misconfig-localization-1",
    "k8s_target_port-misconfig-analysis-1",
    "k8s_target_port-misconfig-mitigation-1",
    "scale_pod_zero_social_net-detection-1",
    "scale_pod_zero_social_net-localization-1",
    "scale_pod_zero_social_net-analysis-1",
    "scale_pod_zero_social_net-mitigation-1",
    "wrong_bin_usage-detection-1",
    "wrong_bin_usage-localization-1",
    "wrong_bin_usage-analysis-1",
    "wrong_bin_usage-mitigation-1",
    "astronomy_shop_ad_service_failure-detection-1",
    "astronomy_shop_ad_service_failure-localization-1",
    "astronomy_shop_ad_service_high_cpu-detection-1",
    "astronomy_shop_ad_service_high_cpu-localization-1",
    "astronomy_shop_ad_service_manual_gc-detection-1",
    "astronomy_shop_ad_service_manual_gc-localization-1",
    "astronomy_shop_cart_service_failure-detection-1",
    "astronomy_shop_cart_service_failure-localization-1",
    "astronomy_shop_image_slow_load-detection-1",
    "astronomy_shop_image_slow_load-localization-1",
    "astronomy_shop_payment_service_failure-detection-1",
    "astronomy_shop_payment_service_failure-localization-1",
    "astronomy_shop_payment_service_unreachable-detection-1",
    "astronomy_shop_payment_service_unreachable-localization-1",
    "astronomy_shop_product_catalog_service_failure-detection-1",
    "astronomy_shop_product_catalog_service_failure-localization-1",
    "astronomy_shop_recommendation_service_cache_failure-detection-1",
    "astronomy_shop_recommendation_service_cache_failure-localization-1",
    "astronomy_shop_kafka_queue_problems-detection-1",
    "astronomy_shop_kafka_queue_problems-localization-1",
    "astronomy_shop_kafka_queue_problems-mitigation-1",
    "astronomy_shop_loadgenerator_flood_homepage-detection-1",
    "astronomy_shop_loadgenerator_flood_homepage-localization-1",
]


async def run_one(problem_id: str, max_steps: int, results_dir: Path) -> dict[str, Any]:
    agent = MeshAgent()
    orch = Orchestrator(results_dir=results_dir)
    orch.register_agent(agent, name="mesh")
    problem_desc, instructions, apis = orch.init_problem(problem_id)
    agent.init_context(problem_desc, instructions, apis)
    agent.problem_id = problem_id
    output = await orch.start_problem(max_steps=max_steps)
    return {"problem_id": problem_id, "output": output}


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run Mesh on upstream Microsoft AIOpsLab.")
    parser.add_argument("--problem-id", action="append", default=[])
    parser.add_argument("--all-registry", action="store_true")
    parser.add_argument("--max-steps", type=int, default=4)
    parser.add_argument("--results-dir", default="data/results/mesh-aiopslab")
    args = parser.parse_args()

    registry_ids = set(ProblemRegistry().PROBLEM_REGISTRY)
    if args.all_registry:
        problem_ids = sorted(registry_ids)
    elif args.problem_id:
        problem_ids = args.problem_id
    else:
        problem_ids = DEFAULT_PROBLEMS
    unknown = [pid for pid in problem_ids if pid not in registry_ids]
    if unknown:
        raise SystemExit(f"Unknown problem IDs: {unknown}")

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    manifest = results_dir / "mesh_aiopslab_results.jsonl"
    print(f"problem_count={len(problem_ids)}")
    print(f"results_manifest={manifest}")

    for pid in problem_ids:
        print(f"######## AIOpsLab Mesh problem start: {pid} ########", flush=True)
        try:
            row = await run_one(pid, args.max_steps, results_dir)
            row["status"] = "completed"
        except Exception as exc:
            row = {"problem_id": pid, "status": "failed", "error": repr(exc), "traceback": traceback.format_exc()}
            print(f"problem_failed={pid} error={exc!r}", flush=True)
            traceback.print_exc()
        with manifest.open("a") as fh:
            fh.write(json.dumps(row, default=str) + "\n")
        print(f"######## AIOpsLab Mesh problem done: {pid} status={row['status']} ########", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
