# AIOpsLab bench runbook — adapters, parallel runs, sharding

Everything runs from **`/root/AIOpsLab`** (the run scripts `cd` here). Python is
`/root/AIOpsLab/.venv/bin/python`. Secrets live in `/root/.env` (sourced via
`set -a; . /root/.env; set +a`). Problems come from
`aiopslab.orchestrator.problems.registry.ProblemRegistry` (~87 task ids like
`k8s_target_port-misconfig-detection-1`).

## 1. The two runners (entry points)
| Runner | Agent under test | Key flags |
|--------|------------------|-----------|
| `run_mesh_aiopslab.py` | Mesh (`clients/mesh.py`) | `--all-registry` \| `--problem-id ID` (repeatable) · `--max-steps` · `--results-dir` |
| `run_claude_code_aiopslab.py` | Claude Code (`cerebral_agent.py`) | + `--model` · `--max-budget-usd` · `--action-timeout` · `--problem-timeout` · `--skip-existing` |

Both take EITHER `--all-registry` (every task) OR one or more `--problem-id` (a
subset). Results are JSONL under `--results-dir`; re-runs into the same dir with
`--skip-existing` (claude runner) skip completed ids.

## 2. Writing / pointing the adapter
- **Mesh adapter** = `clients/mesh.py`. The runner imports it; the *Mesh build*
  it exercises is chosen by env, NOT by the adapter file:
  - `MESH_INTELLIGENCE_PATH=/root/mesh` (or `/root/mesh-main-bench`) — which
    mesh checkout to import. This is the single most important knob: confirm it
    points at the commit you think you're testing.
  - Observer: `MESH_OBSERVER_ENABLED=1`, `MESH_OBSERVER_PROVIDER=openai`,
    `MESH_OBSERVER_BASE_URL=https://api.deepseek.com`, `MESH_OBSERVER_MODEL=deepseek-v4-pro`,
    `MESH_OBSERVER_API_KEY=$DEEPSEEK_API_KEY`, `*_TIMEOUT_SECONDS`, `*_MAX_TOKENS`.
  - Scoring honesty: `MESH_AIOPSLAB_BENCHMARK_HINTS=0` (hints OFF),
    `MESH_AIOPSLAB_PROBLEM_ANCHOR_ENABLED=1`.
- **Claude Code / Cerebral adapter** = `cerebral_agent.py` (currently only in the
  non-git `aiopslab-fix/`; copy into the repo and track it). Tunables via
  `CLAUDE_AIOPSLAB_ACTION_TIMEOUT`, `CLAUDE_AIOPSLAB_PROBLEM_TIMEOUT`,
  `CLAUDE_AIOPSLAB_MAX_BUDGET_USD`, `CLAUDE_CODE_MODEL`/`ANTHROPIC_MODEL`.
- To add a new agent: copy a runner, swap the agent class it constructs, keep the
  `ProblemRegistry` enumeration + JSONL writer untouched so merge/scoring still work.

## 3. Split ONE benchmark into N parts (sharding)
There is no `--shard` flag yet; shard by splitting the id list. Build the list
once, `split` it, and feed each chunk to its own runner + results dir.

```bash
cd /root/AIOpsLab
# 3a. dump every problem id, deterministically sorted
.venv/bin/python - <<'PY' > /tmp/ids.txt
from aiopslab.orchestrator.problems.registry import ProblemRegistry
print("\n".join(sorted(ProblemRegistry().PROBLEM_REGISTRY)))
PY
wc -l /tmp/ids.txt                      # ~87
# 3b. split into N near-equal chunks (here N=4 -> ids.part-aa..ad)
split -n l/4 /tmp/ids.txt /tmp/ids.part-
# 3c. turn a chunk into repeated --problem-id flags
flags() { sed 's/^/--problem-id /' "$1" | tr '\n' ' '; }
```

## 4. Run the parts in parallel (one tmux session per shard)
Each shard gets its OWN `--results-dir` so writers never collide.
```bash
TS=$(date -u +%m%d_%H%M)
i=0; for part in /tmp/ids.part-*; do
  i=$((i+1)); RD="data/results/mesh-aiopslab-${TS}-shard${i}"
  tmux new-session -d -s "aiops-sh${i}" \
    "cd /root/AIOpsLab && set -a; . /root/.env; set +a; \
     export MESH_INTELLIGENCE_PATH=/root/mesh-main-bench MESH_AIOPSLAB_BENCHMARK_HINTS=0 \
       MESH_OBSERVER_ENABLED=1 MESH_OBSERVER_PROVIDER=openai \
       MESH_OBSERVER_BASE_URL=https://api.deepseek.com MESH_OBSERVER_MODEL=deepseek-v4-pro \
       MESH_OBSERVER_API_KEY=\$DEEPSEEK_API_KEY; \
     .venv/bin/python run_mesh_aiopslab.py $(flags "$part") \
       --results-dir $RD 2>&1 | tee logs/${TS}-shard${i}.log"
done
tmux ls                                  # watch: aiops-sh1..N
```
Sizing: each task is ~3-6 min and spins up real microservices on the shared
cluster. 3-4 shards is the sweet spot here; more risks cluster contention, not
speedup. Keep `MESH_INTELLIGENCE_PATH` identical across shards so every part
measures the same build.

## 5. Merge shard results + score
```bash
cd /root/AIOpsLab
cat data/results/mesh-aiopslab-${TS}-shard*/*.jsonl > data/results/mesh-aiopslab-${TS}-merged.jsonl
wc -l data/results/mesh-aiopslab-${TS}-merged.jsonl   # expect == total task count
```
Then run the usual scoring/aggregation over the merged JSONL.

## 6. Rerun only failures
The repo already uses this pattern (`.remaining_aiopslab_ids`, `run_mitfix_rerun.sh`,
`watch-*-rerun` tmux sessions): collect failed ids into a file and feed them back as
`--problem-id` flags into a fresh results dir, then merge.
```bash
# example: rerun a hand-picked subset with more steps
.venv/bin/python run_mesh_aiopslab.py \
  --problem-id auth_miss_mongodb-mitigation-1 \
  --problem-id revoke_auth_mongodb-mitigation-2 \
  --max-steps 20 --results-dir data/results/rerun-$(date -u +%m%d_%H%M)
```

## 7. Pre-run checklist (avoid wasting a paid run)
1. `git -C $MESH_INTELLIGENCE_PATH rev-parse --short HEAD` == the commit you mean.
2. `MESH_INTELLIGENCE_PATH` set the same in every shard (kickoff env > shell export).
3. No conflicting run already live: `tmux ls` and `pgrep -af run_mesh_aiopslab`.
4. Launch detached, then after ~15s confirm the log advanced past instantiation
   (a constructor/harness mismatch dies on task 1 — catch it in seconds).

## Future hardening (nice-to-have)
- Add a native `--shard i/N` flag to both runners (hash ids, keep deterministic).
- A `merge_results.py` that dedups by id and prints the per-category table.
- Move `cerebral_agent.py` + the run_*.sh into the repo so the harness is versioned,
  not living as untracked files / in the non-git aiopslab-fix copy.
