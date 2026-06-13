# ai-pipeline-orchestrator

Stateful webhook orchestrator for the [GitHub AI automation pipeline](https://github.com/asfar95/ai-agent-playground). Receives all GitHub events through a single webhook URL and sequences three AI agents in the correct order — triage → autofix → code review → review-fix — without race conditions.

---

## The Problem It Solves

GitHub fires webhooks the moment events happen. If you point webhooks directly at each agent, you get a race condition:

```
Issue opened  ──► triage agent starts
Issue labeled ──► autofix agent starts  ← fires immediately, before triage finishes
                  └─ triage and autofix both run the same issue in parallel
```

The naive fix — a startup delay in the autofix agent — breaks under any load: two simultaneous issues, a slow triage run, or a brief network hiccup will still cause overlap.

---

## The Solution: Option C — Stateful Orchestrator

The router is the **only** component that receives GitHub webhooks. Agents are dumb executors: they do their job and POST a completion signal back to the router when done. The router owns all sequencing decisions.

```
GitHub
  │
  ▼
┌─────────────────────────────────────────────────────┐
│           ai-pipeline-orchestrator :3000            │
│                                                     │
│  pipelineState Map                                  │
│  "owner/repo#42" → { triageDone, pendingAutofix }   │
└──┬──────────────┬──────────────┬────────────────────┘
   │              │              │
   ▼              ▼              ▼
triage        autofix        review bot
:3002          :3003           :3001
   │              │
   └── POST /internal/triage-done
                  └── POST /internal/autofix-done
```

### State machine per issue

```
issues.opened
    │
    ├─► forward to triage ──────────────────────────────────────► [triage running]
    │                                                                    │
    │                                                         POST /internal/triage-done
    │                                                                    │
issues.labeled "bug"                                                     ▼
    │                                                           state.triageDone = true
    ├─ if triageDone ──────────────────────────────────────────► forward to autofix immediately
    │
    └─ if NOT triageDone ──────────────────────────────────────► store in state.pendingAutofix
                                                                         │
                                                              (when triage-done arrives)
                                                                         │
                                                                         ▼
                                                              fire the queued autofix payload
```

### Why this is safe under concurrent issues

Each issue gets its own entry in `pipelineState` keyed by `owner/repo#issueNumber`. Two issues arriving simultaneously get two independent state entries — they never interfere with each other.

---

## Event Routing

| GitHub Event | Condition | Action |
|---|---|---|
| `issues.opened` | — | Forward to triage agent |
| `issues.reopened` | — | Forward to triage agent |
| `issues.labeled` | label = `"bug"` | Forward to autofix immediately if triage done, otherwise queue |
| `pull_request.opened` | — | Forward to code review bot |
| `pull_request.synchronize` | — | Forward to code review bot |
| `pull_request_review.submitted` | branch starts with `fix/issue-` | Forward to review-fix agent |
| `pull_request_review.submitted` | any other branch | Skip — not an autofix PR |
| everything else | — | Ignored |

### The branch name filter

Review-fix only triggers on branches named `fix/issue-{n}-{slug}`. This is the opt-in mechanism — the autofix agent always creates branches in this format, so review-fix automation is automatic for AI-generated PRs and off by default for everything else.

---

## Internal Endpoints

Agents call these when they finish. The router uses them to advance the pipeline.

### `POST /internal/triage-done`

```json
{ "owner": "asfar95", "repo": "ai-agent-playground", "issueNumber": 42 }
```

Marks triage complete for the issue. If a `bug` label event already arrived and is queued in `pendingAutofix`, the router fires the autofix agent immediately.

### `POST /internal/autofix-done`

```json
{ "owner": "asfar95", "repo": "ai-agent-playground", "issueNumber": 42, "pullNumber": 7 }
```

Currently used for logging only (the PR number is recorded). Future: could trigger review bot directly rather than waiting for GitHub's `pull_request.opened` webhook.

---

## The 3-Minute Fallback

When a `bug` label arrives before triage-done, the event is queued and a 3-minute timer starts:

```js
setTimeout(() => {
  const current = pipelineState.get(key);
  if (current?.pendingAutofix) {
    // triage-done never arrived — fire autofix anyway
    forward(AUTOFIX_PORT, pendingRawBody, pendingHeaders);
  }
}, 3 * 60 * 1000);
```

This handles two failure scenarios:
- **Triage agent crashed** — the issue still gets an autofix attempt
- **Agent running without `TRIAGE_DONE_URL` set** (standalone mode) — the signal is never sent, so the fallback fires after 3 minutes

The fallback is a safety net, not the happy path. In normal operation, `triage-done` arrives in ~30 seconds.

---

## Standalone Mode

Every agent works independently without the router. The completion signal URLs are optional — if `TRIAGE_DONE_URL` or `AUTOFIX_DONE_URL` are not set in an agent's `.env`, the `signalRouter()` call is a no-op and the agent runs normally.

This means:
- You can run any agent directly via CLI for testing without the router
- The router only matters when all agents are running together end-to-end

---

## Setup

### 1. Start all agents first

Each agent listens on its own port. Start them before the router:

```bash
# Terminal 1
cd github-issue-triage-agent && npm start   # :3002

# Terminal 2
cd github-autofix-agent && npm start        # :3003

# Terminal 3
cd ai-code-review-bot/backend && npm start  # :3001
```

### 2. Start the router

```bash
cd ai-pipeline-orchestrator
node index.js
```

Output on startup:
```
🔀 Webhook Router (Orchestrator)
   Listening : http://localhost:3000/webhook
   Internal  : http://localhost:3000/internal/triage-done
               http://localhost:3000/internal/autofix-done

   Pipeline  :
     issues.opened          → triage (3002)
     issues.labeled "bug"   → queued until triage-done → autofix (3003)
     pull_request           → review bot (3001)
     pull_request_review    → review-fix (3003) immediately
```

### 3. Expose it with ngrok

```bash
ngrok http 3000
```

Point **one** GitHub webhook at the ngrok URL: `https://<ngrok-id>.ngrok.io/webhook`

Events to subscribe: `Issues`, `Pull requests`, `Pull request reviews`

### 4. Configure agents to signal the router

In `github-issue-triage-agent/.env`:
```
TRIAGE_DONE_URL=http://localhost:3000/internal/triage-done
```

In `github-autofix-agent/.env`:
```
AUTOFIX_DONE_URL=http://localhost:3000/internal/autofix-done
```

---

## Related Repos

| Repo | Purpose |
|------|---------|
| [ai-agent-playground](https://github.com/asfar95/ai-agent-playground) | Demo target — open issues here to trigger the pipeline |
| [github-issue-triage-agent](https://github.com/asfar95/github-issue-triage-agent) | Classifies issues and adds labels |
| [github-autofix-agent](https://github.com/asfar95/github-autofix-agent) | Writes code fixes, opens PRs, addresses review comments |
| [ai-code-review-bot](https://github.com/asfar95/ai-code-review-bot) | Reviews PRs and posts inline diff comments |
