from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import subprocess
import time
import traceback
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from aiopslab.orchestrator import Orchestrator
from aiopslab.orchestrator.problems.registry import ProblemRegistry
from clients.utils.templates import DOCS_SHELL_ONLY

load_dotenv("/root/.env")
load_dotenv()

DEFAULT_ACTION_TIMEOUT = float(os.environ.get("CLAUDE_AIOPSLAB_ACTION_TIMEOUT", "180"))
DEFAULT_PROBLEM_TIMEOUT = float(os.environ.get("CLAUDE_AIOPSLAB_PROBLEM_TIMEOUT", "900"))
DEFAULT_MODEL = os.environ.get("CLAUDE_CODE_MODEL") or os.environ.get("ANTHROPIC_MODEL") or "sonnet"
DEFAULT_BUDGET = os.environ.get("CLAUDE_AIOPSLAB_MAX_BUDGET_USD", "1.00")
CLAUDE_BIN = os.environ.get("CLAUDE_BIN", "/root/.local/bin/claude")

API_CALL_RE = re.compile(
    r"(?s)(submit\s*\(.*?\)|exec_shell\s*\(.*?\)|get_logs\s*\(.*?\)|get_metrics\s*\(.*?\)|get_traces\s*\(.*?\))"
)


class ClaudeCodeAIOpsAgent:
    def __init__(self, *, model: str, logs_dir: Path, max_budget_usd: str, action_timeout: float):
        self.model = model
        self.logs_dir = logs_dir
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.max_budget_usd = max_budget_usd
        self.action_timeout = action_timeout
        self.history: list[dict[str, str]] = []
        self.problem_id = "unknown"
        self.step = 0

    def init_context(self, problem_desc: str, instructions: str, apis: dict[str, str]):
        shell_api = {k: v for k, v in apis.items() if "exec_shell" in k}
        submit_api = {k: v for k, v in apis.items() if "submit" in k}
        stringify = lambda d: "\n\n".join([f"{k}\n{v}" for k, v in d.items()])
        system = DOCS_SHELL_ONLY.format(
            prob_desc=problem_desc,
            shell_api=stringify(shell_api),
            submit_api=stringify(submit_api),
        )
        system += (
            "\n\nYou are being run through Claude Code non-interactively inside AIOpsLab. "
            "For every turn, return exactly one AIOpsLab API call in a markdown code block and nothing else. "
            "Valid calls are exec_shell(...), submit(...), get_logs(...), get_metrics(...), or get_traces(...). "
            "Do not explain. Do not mention commands unless they are inside exec_shell."
        )
        self.history = [
            {"role": "system", "content": system},
            {"role": "user", "content": instructions},
        ]

    async def get_action(self, input: str) -> str:
        self.step += 1
        self.history.append({"role": "user", "content": input})
        prompt = self._build_prompt()
        result = await asyncio.to_thread(self._run_claude, prompt)
        self.history.append({"role": "assistant", "content": result})
        return result

    def _build_prompt(self) -> str:
        parts = []
        for msg in self.history[-10:]:
            parts.append(f"[{msg['role'].upper()}]\n{msg['content']}")
        parts.append("[ASSISTANT]\nReturn exactly one markdown fenced API call now.")
        return "\n\n".join(parts)

    def _run_claude(self, prompt: str) -> str:
        step_dir = self.logs_dir / self.problem_id
        step_dir.mkdir(parents=True, exist_ok=True)
        prompt_path = step_dir / f"step_{self.step:02d}_prompt.txt"
        raw_path = step_dir / f"step_{self.step:02d}_claude_raw.json"
        err_path = step_dir / f"step_{self.step:02d}_stderr.txt"
        prompt_path.write_text(prompt)

        env = os.environ.copy()
        env["PATH"] = "/root/.local/bin:" + env.get("PATH", "")
        env["CLAUDE_CONFIG_DIR"] = str(step_dir / "claude_config")

        cmd = [
            CLAUDE_BIN,
            "-p",
            prompt,
            "--output-format",
            "json",
            "--model",
            self.model,
            "--no-session-persistence",
            "--max-budget-usd",
            self.max_budget_usd,
            "--allowedTools",
            "Read",
            "LS",
            "Grep",
            "Glob",
        ]
        try:
            proc = subprocess.run(
                cmd,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=self.action_timeout,
                stdin=subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired as exc:
            err_path.write_text(f"timeout after {self.action_timeout}s\n{exc!r}\n")
            return "```\nsubmit(\"No\")\n```"

        raw_path.write_text(proc.stdout or "")
        err_path.write_text(proc.stderr or "")
        if proc.returncode != 0:
            return "```\nsubmit(\"No\")\n```"

        try:
            payload = json.loads(proc.stdout)
            text = str(payload.get("result") or "")
        except Exception:
            text = proc.stdout.strip()

        match = API_CALL_RE.search(text)
        if match:
            call = match.group(1).strip()
        else:
            call = text.strip() or "submit(\"No\")"
        if "```" in call:
            return call
        return f"```\n{call}\n```"


async def run_one(problem_id: str, max_steps: int, results_dir: Path, args: argparse.Namespace) -> dict[str, Any]:
    agent = ClaudeCodeAIOpsAgent(
        model=args.model,
        logs_dir=results_dir / "claude_code_logs",
        max_budget_usd=args.max_budget_usd,
        action_timeout=args.action_timeout,
    )
    agent.problem_id = problem_id
    orch = Orchestrator(results_dir=results_dir)
    orch.register_agent(agent, name="claude-code")
    problem_desc, instructions, apis = orch.init_problem(problem_id)
    agent.init_context(problem_desc, instructions, apis)
    output = await orch.start_problem(max_steps=max_steps)
    return {"problem_id": problem_id, "output": output}


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run Claude Code on upstream Microsoft AIOpsLab.")
    parser.add_argument("--problem-id", action="append", default=[])
    parser.add_argument("--all-registry", action="store_true")
    parser.add_argument("--max-steps", type=int, default=8)
    parser.add_argument("--results-dir", default="data/results/claude-code-aiopslab")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--max-budget-usd", default=DEFAULT_BUDGET)
    parser.add_argument("--action-timeout", type=float, default=DEFAULT_ACTION_TIMEOUT)
    parser.add_argument("--problem-timeout", type=float, default=DEFAULT_PROBLEM_TIMEOUT)
    parser.add_argument("--skip-existing", action="store_true")
    args = parser.parse_args()

    registry_ids = set(ProblemRegistry().PROBLEM_REGISTRY)
    if args.all_registry:
        problem_ids = sorted(registry_ids)
    elif args.problem_id:
        problem_ids = args.problem_id
    else:
        problem_ids = sorted(registry_ids)[:3]
    unknown = [pid for pid in problem_ids if pid not in registry_ids]
    if unknown:
        raise SystemExit(f"Unknown problem IDs: {unknown}")

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    manifest = results_dir / "claude_code_aiopslab_results.jsonl"
    if args.skip_existing and manifest.exists():
        seen = set()
        for line in manifest.read_text().splitlines():
            if not line.strip():
                continue
            try:
                seen.add(json.loads(line).get("problem_id"))
            except Exception:
                pass
        problem_ids = [pid for pid in problem_ids if pid not in seen]
        print(f"skip_existing_seen={len(seen)} remaining={len(problem_ids)}")
    print(f"problem_count={len(problem_ids)}")
    print(f"model={args.model}")
    print(f"action_timeout={args.action_timeout}")
    print(f"problem_timeout={args.problem_timeout}")
    print(f"results_manifest={manifest}")

    for pid in problem_ids:
        print(f"######## AIOpsLab ClaudeCode problem start: {pid} ########", flush=True)
        started = time.time()
        try:
            row = await asyncio.wait_for(
                run_one(pid, args.max_steps, results_dir, args), timeout=args.problem_timeout
            )
            row["status"] = "completed"
        except Exception as exc:
            row = {"problem_id": pid, "status": "failed", "error": repr(exc), "traceback": traceback.format_exc()}
            print(f"problem_failed={pid} error={exc!r}", flush=True)
            traceback.print_exc()
        row["wall_seconds"] = time.time() - started
        with manifest.open("a") as fh:
            fh.write(json.dumps(row, default=str) + "\n")
        print(f"######## AIOpsLab ClaudeCode problem done: {pid} status={row['status']} ########", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
