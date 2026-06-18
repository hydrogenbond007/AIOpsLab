#!/usr/bin/env bash
# Handoff monitor for the hints-off AIOpsLab bench. Waits for completion,
# computes accuracy (overall + per task-type), then hands the qualitative
# reasoning analysis to box-local claude (READ-ONLY). Writes reports under
# the run's results dir. Never touches the cluster or the running bench.
set -uo pipefail

RD=/root/AIOpsLab/data/results/mesh-aiopslab-hintsoff-run
RES="$RD/mesh_aiopslab_results.jsonl"
LOG=/root/AIOpsLab/logs/mesh-aiopslab-hintsoff-run.log
REPORT="$RD/REPORT.md"
MONLOG="$RD/monitor.log"
TOTAL=89

mkdir -p "$RD"
echo "[monitor] started $(date -u +%FT%TZ); waiting for bench to finish ($TOTAL problems)" | tee -a "$MONLOG"

# Wait until the bench tmux session ends OR all results are present.
while tmux has-session -t aiopslab-hintsoff 2>/dev/null; do
  done=$(wc -l < "$RES" 2>/dev/null || echo 0)
  echo "[monitor] $(date -u +%H:%M:%S) progress: ${done}/${TOTAL}" >> "$MONLOG"
  if [ "${done:-0}" -ge "$TOTAL" ]; then break; fi
  sleep 120
done
sleep 15
echo "[monitor] bench finished; computing accuracy" | tee -a "$MONLOG"

# Deterministic accuracy aggregation from the AIOpsLab eval blocks in the log.
python3 - "$LOG" "$RES" "$REPORT" <<'PY'
import json, re, sys, collections
log_path, res_path, report_path = sys.argv[1], sys.argv[2], sys.argv[3]

# Pair each problem with its eval "Results: {...}" dict from the log.
text = open(log_path, errors="ignore").read()
blocks = re.split(r"#{4,} AIOpsLab Mesh problem start: ", text)
rows = []
for b in blocks[1:]:
    pid = b.split(" ", 1)[0].strip()
    m = re.search(r"Results:\s*\{(.+?)\}", b, re.DOTALL)
    success = None
    acc = None
    if m:
        frag = "{" + m.group(1) + "}"
        try:
            d = json.loads(frag.replace("'", '"'))
            success = bool(d.get("success"))
            for k in ("Localization Accuracy", "Accuracy", "Detection Accuracy"):
                if k in d:
                    acc = d[k]; break
        except Exception:
            success = "True" in (re.search(r"'success':\s*(\w+)", frag) or [None, ""])[1] if re.search(r"'success':\s*(\w+)", frag) else None
    task = pid.rsplit("-", 2)[-2] if pid.count("-") >= 2 else "other"
    rows.append((pid, task, success, acc))

by_task = collections.defaultdict(lambda: [0, 0])  # task -> [success, total]
ok = tot = 0
for pid, task, success, acc in rows:
    if success is None:
        continue
    by_task[task][1] += 1; tot += 1
    if success:
        by_task[task][0] += 1; ok += 1

with open(report_path, "w") as f:
    f.write("# AIOpsLab bench — hints OFF, /root/mesh@main, deepseek-v4-pro\n\n")
    f.write(f"Run: `{res_path}`\n\n")
    f.write(f"**Overall success: {ok}/{tot}" + (f" = {100*ok/tot:.1f}%" if tot else "") + "**\n\n")
    f.write("Context: prior 'openebsfix' run scored ~0% and ran with benchmark HINTS ON (canned answers). "
            "This run is the first with hints OFF + the signal-clarity / rca-harvest fixes, so it measures real mesh reasoning.\n\n")
    f.write("| task type | success/total | rate |\n|---|---|---|\n")
    for task in sorted(by_task):
        s, t = by_task[task]
        f.write(f"| {task} | {s}/{t} | {100*s/t:.0f}% |\n" if t else f"| {task} | 0/0 | - |\n")
    f.write("\n## Sample submitted answers\n\n")
    try:
        recs = [json.loads(l) for l in open(res_path)]
        for d in recs[:6]:
            o = d.get("output", {}); h = o.get("history", []) if isinstance(o, dict) else []
            subs = [str(t) for t in h if "submit(" in str(t)]
            f.write(f"- `{d.get('problem_id')}` -> {subs[-1][:160] if subs else '(no submit)'}\n")
    except Exception as e:
        f.write(f"(could not read results: {e})\n")
print("wrote", report_path)
PY

echo "[monitor] accuracy written; handing qualitative analysis to box-local claude (read-only)" | tee -a "$MONLOG"

# Handoff: box-local claude does the reasoning-quality narrative. READ-ONLY
# toolset (no Bash -> cannot touch the cluster or the bench).
CLAUDE=/root/.local/bin/claude
if [ -x "$CLAUDE" ]; then
  "$CLAUDE" -p "You are auditing a completed AIOpsLab benchmark run of the Mesh system.
Read these files: ${REPORT} and ${RES} (JSONL; each row has problem_id, status, output.history with the agent's exec_shell/submit turns).
Write a concise reasoning-quality report to ${RD}/REPORT-claude.md covering:
1. Did mesh produce REAL root-cause reasoning vs the trivial fallback (recent_deploy->rollback, unknown->escalate)? Cite specific submitted answers.
2. Per task-type (detection/localization/analysis/mitigation): how did it do, and vs the prior ~0% hints-on 'openebsfix' baseline.
3. Where does it still collapse, and the likely root cause (e.g. healthy-pod app faults, observer not firing, wrong localization anchor).
4. Top 3 concrete next fixes, mesh-CORE-first.
Be specific and honest. READ-ONLY: do not run kubectl, do not modify anything except writing REPORT-claude.md." \
    --allowedTools "Read,Grep,Glob,Write" \
    --output-format text > "$RD/REPORT-claude.log" 2>&1 \
    && echo "[monitor] claude analysis -> $RD/REPORT-claude.md" | tee -a "$MONLOG" \
    || echo "[monitor] claude analysis failed (see REPORT-claude.log); deterministic REPORT.md still valid" | tee -a "$MONLOG"
else
  echo "[monitor] claude binary not found; deterministic REPORT.md only" | tee -a "$MONLOG"
fi

# Publish reports to a git branch so the scheduled cloud check-in (which
# cannot SSH this box) can read them. Use a separate worktree so the live
# /root/mesh checkout the bench imports from is NEVER touched.
echo "[monitor] publishing reports to Cerebral-Systems/mesh:aiopslab-bench-reports" | tee -a "$MONLOG"
WT=/tmp/bench-reports-wt
git -C /root/mesh worktree remove --force "$WT" 2>/dev/null || true
rm -rf "$WT"
git -C /root/mesh worktree prune 2>/dev/null || true
if git -C /root/mesh fetch origin aiopslab-bench-reports 2>/dev/null; then
  git -C /root/mesh worktree add --force -B aiopslab-bench-reports "$WT" origin/aiopslab-bench-reports 2>/dev/null \
    || git -C /root/mesh worktree add --force -b aiopslab-bench-reports "$WT"
else
  git -C /root/mesh worktree add --force -b aiopslab-bench-reports "$WT"
fi
mkdir -p "$WT/bench-reports"
cp "$REPORT" "$WT/bench-reports/REPORT.md" 2>/dev/null || true
cp "$RD/REPORT-claude.md" "$WT/bench-reports/REPORT-claude.md" 2>/dev/null || true
cp "$MONLOG" "$WT/bench-reports/monitor.log" 2>/dev/null || true
git -C "$WT" add bench-reports
git -C "$WT" -c user.email=bench@mesh.local -c user.name=mesh-bench commit -m "aiopslab bench reports $(date -u +%FT%TZ)" 2>&1 | tail -1 | tee -a "$MONLOG"
git -C "$WT" push -u origin aiopslab-bench-reports 2>&1 | tail -2 | tee -a "$MONLOG"
git -C /root/mesh worktree remove --force "$WT" 2>/dev/null || true

echo "[monitor] DONE $(date -u +%FT%TZ): $REPORT" | tee -a "$MONLOG"
