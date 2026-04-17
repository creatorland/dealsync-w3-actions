# A/B Eval Framework

Evaluate and compare deal classification prompts against ground truth. Runs on W3 testnet.

## Directory Structure

```
eval/
  README.md              ← you are here
  baseline.json          ← current baseline eval result (10 runs)
  ground-truth.json      ← labeled email threads for evaluation
  thresholds.json        ← configurable pass/fail criteria
  history/               ← archived eval comparisons
    YYYY-MM-DD-<name>/
      README.md          ← findings, decision
      system-a.md        ← baseline prompt
      system-b.md        ← candidate prompt
      user-a.md, user-b.md
      result-a.json      ← baseline eval result
      result-b.json      ← candidate eval result
```

## Workflows

Three W3 workflows are available on testnet:

| Workflow | Purpose |
|----------|---------|
| `dealsync-eval` | Run eval on one prompt variant. Outputs result JSON. |
| `dealsync-eval-compare` | Run eval on a candidate, then compare against `baseline.json`. |
| `dealsync-compare` | Compare two existing result JSONs. No API calls. |

## How to Test a New Prompt

### Option 1: Paste prompt directly

1. Trigger `dealsync-eval` with `system_prompt` input set to your prompt text.
2. Grab the result JSON from the job output.
3. Trigger `dealsync-compare` with `result_b` set to your result JSON. It compares against the bundled `baseline.json`.
4. Read the verdict and report.

### Option 2: Commit prompt and use hash

1. Edit `prompts/system.md` on a branch, commit, note the hash.
2. Trigger `dealsync-eval` with `prompt_hash` set to your commit hash.
3. Same as above — grab result, run compare.

### Option 3: One-shot eval + compare

1. Trigger `dealsync-eval-compare` with your prompt (via `system_prompt` or `prompt_hash`).
2. It runs the eval and comparison in one workflow. Report is in the compare job output.

## Workflow Inputs

### dealsync-eval / dealsync-eval-compare

| Input | Default | Description |
|-------|---------|-------------|
| `model` | `deepseek/deepseek-chat-v3-0324` | Model to evaluate |
| `prompt_hash` | (empty) | Git commit hash for prompts. Empty = bundled. |
| `system_prompt` | (empty) | Inline system prompt. Takes priority over `prompt_hash`. |
| `user_prompt` | (empty) | Inline user prompt. Takes priority over `prompt_hash`. |
| `runs` | `10` | Number of eval runs (higher = more stable metrics) |
| `temperature` | `0` | AI temperature |
| `batch_size` | `5` | Threads per API call |

### dealsync-compare

| Input | Default | Description |
|-------|---------|-------------|
| `result_a` | (empty) | Eval result JSON for variant A. Empty = bundled `baseline.json`. |
| `result_b` | (required) | Eval result JSON for variant B. |

## Available Models (OpenRouter)

| Model | Notes |
|-------|-------|
| `deepseek/deepseek-chat-v3-0324` | Default, current production |
| `deepseek/deepseek-r1-0528` | Reasoning, newest |
| `deepseek/deepseek-r1` | Reasoning |
| `qwen/qwen3-coder-480b-a35b-instruct` | Coder, newest |
| `meta-llama/llama-3.3-70b-instruct` | Smallest |

## Metrics

All metrics are computed per run, then aggregated with mean/min/max/stddev.

### Detection (most important)

- **Recall** — % of real deals correctly identified. Most important metric.
- **Precision** — % of predicted deals that are actually deals.
- **F2 Score** — Harmonic mean weighted toward recall (beta=2). Missing a deal costs more than a false positive.

### Sub-Metrics

- **Category Accuracy** — How often predicted category matches ground truth (deal threads only).
- **Urgency Scoring** — % of threads where `ai_score` falls in the expected range.
- **Scam Detection** — Accuracy on known scam threads.
- **JSON Health** — Clean parse rate, retry rate, failures. Should be 100% clean with `json_schema`.

## Pass/Fail Criteria

Defined in `thresholds.json`. All criteria must pass for a PASS verdict.

| Criterion | Default | Description |
|-----------|---------|-------------|
| `min_recall` | 0.95 | Candidate recall must meet this minimum |
| `min_precision` | 0.40 | Candidate precision must meet this minimum |
| `max_recall_stddev` | 0.03 | Candidate must be consistent across runs |
| `require_f2_non_regression` | true | F2 must not drop vs baseline |
| `require_no_new_missed_deals` | true | Must not miss deals the baseline catches |
| `require_no_scam_regression` | true | Must not miss scams the baseline catches |
| `require_no_category_regression` | true | Category accuracy must not drop |

Set any boolean to `false` in `thresholds.json` to disable that criterion.

## Updating the Baseline

When a prompt variant passes evaluation and is promoted to production:

1. Run `dealsync-eval` with the new prompt (10 runs, batch_size=5).
2. Copy the result JSON to `eval/baseline.json`.
3. PR it to main.

The baseline is bundled in the action at build time. Future comparisons automatically use it.

## Archiving Eval Results

After each evaluation, save results to `eval/history/`:

1. Create `eval/history/YYYY-MM-DD-<name>/`
2. Copy both prompts (system + user for each variant)
3. Copy both result JSONs
4. Write a `README.md` with findings, verdict, and decision

See `eval/history/2026-04-17-v1-vs-v3/` for an example.

## Ground Truth

`ground-truth.json` contains 60 labeled email threads (38 usable after filtering empty bodies and static rules).

### Adding entries

1. Add to the array in `ground-truth.json`.
2. Use next sequential ID (`gt-NNN`).
3. Include full email content — the AI classifies the complete body.
4. Set `expected`: `is_deal`, `category`, `likely_scam`, `score_range`.
5. Run eval to verify the new entry doesn't break existing metrics.

### Format

```json
{
  "id": "gt-001",
  "description": "Short description of the scenario",
  "emails": [{
    "messageId": "gt-001-msg-1",
    "threadId": "gt-001",
    "topLevelHeaders": [
      { "name": "from", "value": "brand@company.com" },
      { "name": "to", "value": "creator@example.com" },
      { "name": "subject", "value": "Campaign Proposal" },
      { "name": "date", "value": "Fri, 01 Mar 2026 09:15:00 +0000" }
    ],
    "body": "Full email body..."
  }],
  "expected": {
    "is_deal": true,
    "category": "new",
    "likely_scam": false,
    "score_range": [5, 7]
  }
}
```

## Required Secrets

| Secret | Description |
|--------|-------------|
| `AI_API_KEY` | OpenRouter API key |
| `AI_API_URL` | OpenRouter endpoint (`https://openrouter.ai/api/v1/chat/completions`) |

Set in W3 environment `0x248d...d140`.
