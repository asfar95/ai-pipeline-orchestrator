// Webhook orchestrator — single entry point for all GitHub events.
// Owns pipeline sequencing: triage → autofix → review-fix.
// Agents signal completion back here; the router decides what runs next.
//
// Usage: node ~/webhook-router.js
// Then point ONE GitHub webhook at: https://<ngrok-url>/webhook

const http = require('http');

const PORT = 3000;
const TRIAGE_PORT = 3002;
const AUTOFIX_PORT = 3003;
const REVIEW_PORT  = 3001;

// Per-issue pipeline state.
// key: `owner/repo#issueNumber`  value: { triageDone, pendingAutofix }
const pipelineState = new Map();

// ── Helpers ────────────────────────────────────────────────────────────────────

function issueKey(owner, repo, issueNumber) {
  return `${owner}/${repo}#${issueNumber}`;
}

function readBody(req) {
  return new Promise(resolve => {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks)));
  });
}

function parsePayload(rawBody) {
  try {
    let s = rawBody.toString();
    if (s.startsWith('payload=')) s = decodeURIComponent(s.slice(8));
    return JSON.parse(s);
  } catch {
    return null;
  }
}

function forward(port, rawBody, headers) {
  return new Promise((resolve) => {
    const { 'transfer-encoding': _te, 'content-length': _cl, host: _h, ...rest } = headers;
    const req = http.request({
      hostname: 'localhost', port, path: '/webhook', method: 'POST',
      headers: { ...rest, host: `localhost:${port}`, 'content-length': Buffer.byteLength(rawBody) },
    }, res => { res.resume(); resolve({ port, status: res.statusCode }); });
    req.on('error', () => resolve({ port, status: 'error' }));
    req.write(rawBody);
    req.end();
  });
}

// ── Server ─────────────────────────────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  let rawBody;
  try {
    rawBody = await readBody(req);
  } catch {
    res.writeHead(500);
    res.end('body read error');
    return;
  }

  // ── /internal/triage-done ───────────────────────────────────────────────────
  if (req.method === 'POST' && req.url === '/internal/triage-done') {
    const { owner, repo, issueNumber } = JSON.parse(rawBody.toString());
    const key = issueKey(owner, repo, issueNumber);

    console.log(`\n📡 triage-done: ${key}`);

    const state = pipelineState.get(key) || {};
    state.triageDone = true;
    pipelineState.set(key, state);

    if (state.pendingAutofix) {
      console.log(`  ▶️  Firing queued autofix for ${key}`);
      const { pendingRawBody, pendingHeaders } = state.pendingAutofix;
      state.pendingAutofix = null;
      const r = await forward(AUTOFIX_PORT, pendingRawBody, pendingHeaders);
      console.log(`   ✅ autofix (${AUTOFIX_PORT}) → ${r.status}`);
    } else {
      console.log(`  ℹ️  No queued autofix for ${key} (not labeled "bug" yet, or not a bug)`);
    }

    res.writeHead(200);
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  // ── /internal/autofix-done ─────────────────────────────────────────────────
  if (req.method === 'POST' && req.url === '/internal/autofix-done') {
    const { owner, repo, issueNumber, pullNumber } = JSON.parse(rawBody.toString());
    const key = issueKey(owner, repo, issueNumber);
    console.log(`\n📡 autofix-done: ${key} → PR #${pullNumber ?? 'none'}`);
    res.writeHead(200);
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  // ── /webhook — GitHub events ───────────────────────────────────────────────
  if (req.method !== 'POST' || req.url !== '/webhook') {
    res.writeHead(200);
    res.end(JSON.stringify({ status: 'webhook-router running' }));
    return;
  }

  const event = req.headers['x-github-event'];
  const payload = parsePayload(rawBody);

  if (!payload) {
    res.writeHead(400);
    res.end(JSON.stringify({ error: 'Invalid payload' }));
    return;
  }

  const owner = payload.repository?.owner?.login;
  const repo  = payload.repository?.name;

  // issues.opened / issues.reopened → triage only
  if (event === 'issues' && (payload.action === 'opened' || payload.action === 'reopened')) {
    const issueNumber = payload.issue.number;
    const key = issueKey(owner, repo, issueNumber);
    console.log(`\n📨 issues.${payload.action}: ${key} — "${payload.issue.title}"`);

    if (!pipelineState.has(key)) {
      pipelineState.set(key, { triageDone: false, pendingAutofix: null });
    }

    res.writeHead(200);
    res.end(JSON.stringify({ message: 'forwarded to triage', issue: issueNumber }));

    const r = await forward(TRIAGE_PORT, rawBody, req.headers);
    console.log(`   ✅ triage (${TRIAGE_PORT}) → ${r.status}`);
    return;
  }

  // issues.labeled "bug" → hold until triage-done, then fire autofix
  if (event === 'issues' && payload.action === 'labeled' && payload.label?.name === 'bug') {
    const issueNumber = payload.issue.number;
    const key = issueKey(owner, repo, issueNumber);
    console.log(`\n📨 issues.labeled "bug": ${key}`);

    const state = pipelineState.get(key) || { triageDone: false, pendingAutofix: null };
    pipelineState.set(key, state);

    res.writeHead(200);
    res.end(JSON.stringify({ message: 'queuing or forwarding to autofix', issue: issueNumber }));

    if (state.triageDone) {
      console.log(`  ▶️  Triage already done — forwarding immediately to autofix`);
      const r = await forward(AUTOFIX_PORT, rawBody, req.headers);
      console.log(`   ✅ autofix (${AUTOFIX_PORT}) → ${r.status}`);
    } else {
      console.log(`  ⏳ Triage still running — queuing autofix payload`);
      state.pendingAutofix = { pendingRawBody: rawBody, pendingHeaders: { ...req.headers } };

      // Safety net: if triage-done never arrives (agent crashed, no ROUTER_URL set),
      // fire autofix after 3 minutes rather than holding the event forever.
      setTimeout(() => {
        const current = pipelineState.get(key);
        if (current?.pendingAutofix) {
          console.warn(`  ⚠️  triage-done timeout for ${key} — firing autofix anyway`);
          const { pendingRawBody: rb, pendingHeaders: rh } = current.pendingAutofix;
          current.pendingAutofix = null;
          forward(AUTOFIX_PORT, rb, rh);
        }
      }, 3 * 60 * 1000);
    }
    return;
  }

  // pull_request.opened / synchronize → review bot
  if (event === 'pull_request' && (payload.action === 'opened' || payload.action === 'synchronize')) {
    const pullNumber = payload.pull_request.number;
    console.log(`\n📨 pull_request.${payload.action}: PR #${pullNumber}`);

    res.writeHead(200);
    res.end(JSON.stringify({ message: 'forwarded to review bot', pr: pullNumber }));

    const r = await forward(REVIEW_PORT, rawBody, req.headers);
    console.log(`   ✅ review bot (${REVIEW_PORT}) → ${r.status}`);
    return;
  }

  // pull_request_review.submitted → review-fix for fix/issue-* branches only
  // Branch naming is the opt-in: autofix always uses fix/issue-{n}-{slug},
  // humans who follow the same convention get the automation too.
  // Sequencing is guaranteed by GitHub: this event only exists after a review
  // is submitted, so code-review-bot is always done by the time it arrives.
  if (event === 'pull_request_review' && payload.action === 'submitted') {
    const pullNumber = payload.pull_request.number;
    const reviewer   = payload.review?.user?.login;
    const branch     = payload.pull_request?.head?.ref;
    console.log(`\n📨 pull_request_review.submitted: PR #${pullNumber} by ${reviewer} (branch: ${branch})`);

    if (!branch?.startsWith('fix/issue-')) {
      console.log(`  ⏭  Skipping — not a fix/issue-* branch`);
      res.writeHead(200);
      res.end(JSON.stringify({ message: 'skipped — not a fix branch', pr: pullNumber }));
      return;
    }

    res.writeHead(200);
    res.end(JSON.stringify({ message: 'forwarded to review-fix', pr: pullNumber }));

    const r = await forward(AUTOFIX_PORT, rawBody, req.headers);
    console.log(`   ✅ review-fix (${AUTOFIX_PORT}) → ${r.status}`);
    return;
  }

  // Everything else — ignore
  console.log(`⏭  Ignored: ${event}.${payload.action || '?'}`);
  res.writeHead(200);
  res.end(JSON.stringify({ message: `Ignored: ${event}.${payload.action}` }));
});

server.listen(PORT, () => {
  console.log(`\n🔀 Webhook Router (Orchestrator)`);
  console.log(`   Listening : http://localhost:${PORT}/webhook`);
  console.log(`   Internal  : http://localhost:${PORT}/internal/triage-done`);
  console.log(`               http://localhost:${PORT}/internal/autofix-done`);
  console.log(`\n   Pipeline  :`);
  console.log(`     issues.opened          → triage (${TRIAGE_PORT})`);
  console.log(`     issues.labeled "bug"   → queued until triage-done → autofix (${AUTOFIX_PORT})`);
  console.log(`     pull_request           → review bot (${REVIEW_PORT})`);
  console.log(`     pull_request_review    → review-fix (${AUTOFIX_PORT}) immediately\n`);
});
