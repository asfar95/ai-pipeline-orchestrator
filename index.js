// Webhook orchestrator — single entry point for all GitHub events.
// Owns pipeline sequencing: triage → autofix → review-fix.
// Agents signal completion back here; the router decides what runs next.
//
// Usage: GITHUB_WEBHOOK_SECRET=<secret> node index.js
// Then point ONE GitHub webhook at: https://<ngrok-url>/webhook

const http   = require('http');
const crypto = require('crypto');

const PORT         = parseInt(process.env.PORT || '3000', 10);
const TRIAGE_PORT  = parseInt(process.env.TRIAGE_PORT  || '3002', 10);
const AUTOFIX_PORT = parseInt(process.env.AUTOFIX_PORT || '3003', 10);
const REVIEW_PORT  = parseInt(process.env.REVIEW_PORT  || '3001', 10);

if (!process.env.GITHUB_WEBHOOK_SECRET) {
  console.error('❌ Fatal: GITHUB_WEBHOOK_SECRET is not set.');
  process.exit(1);
}

// Per-issue pipeline state.
// key: `owner/repo#issueNumber`  value: { triageDone, pendingAutofix, createdAt }
const pipelineState = new Map();

// Evict entries older than 6 hours to prevent unbounded memory growth.
// Issues that never triggered autofix (not labeled "bug") would otherwise stay forever.
setInterval(() => {
  const cutoff = Date.now() - 6 * 60 * 60 * 1000;
  for (const [key, state] of pipelineState) {
    if (state.createdAt < cutoff) pipelineState.delete(key);
  }
}, 60 * 60 * 1000).unref();

// ── Helpers ────────────────────────────────────────────────────────────────────

function issueKey(owner, repo, issueNumber) {
  return `${owner}/${repo}#${issueNumber}`;
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end',  () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function verifySignature(rawBody, signature) {
  if (!signature) return false;
  const digest = 'sha256=' + crypto.createHmac('sha256', process.env.GITHUB_WEBHOOK_SECRET).update(rawBody).digest('hex');
  return crypto.timingSafeEqual(Buffer.from(digest), Buffer.from(signature));
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

function send(res, status, body) {
  const data = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(data) });
  res.end(data);
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
    send(res, 500, { error: 'body read error' });
    return;
  }

  // ── Health check ────────────────────────────────────────────────────────────
  if (req.url === '/health') {
    send(res, 200, { status: 'ok', pipeline_entries: pipelineState.size });
    return;
  }

  // ── /internal/triage-done ───────────────────────────────────────────────────
  if (req.method === 'POST' && req.url === '/internal/triage-done') {
    let body;
    try { body = JSON.parse(rawBody.toString()); } catch {
      send(res, 400, { error: 'Invalid JSON' });
      return;
    }
    const { owner, repo, issueNumber } = body;
    if (!owner || !repo || !issueNumber) {
      send(res, 400, { error: 'Missing owner, repo, or issueNumber' });
      return;
    }
    const key = issueKey(owner, repo, issueNumber);
    console.log(`\n📡 triage-done: ${key}`);

    const state = pipelineState.get(key) || { createdAt: Date.now() };
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

    send(res, 200, { ok: true });
    return;
  }

  // ── /internal/autofix-done ─────────────────────────────────────────────────
  if (req.method === 'POST' && req.url === '/internal/autofix-done') {
    let body;
    try { body = JSON.parse(rawBody.toString()); } catch {
      send(res, 400, { error: 'Invalid JSON' });
      return;
    }
    const { owner, repo, issueNumber, pullNumber } = body;
    const key = issueKey(owner, repo, issueNumber);
    console.log(`\n📡 autofix-done: ${key} → PR #${pullNumber ?? 'none'}`);
    send(res, 200, { ok: true });
    return;
  }

  // ── /webhook — GitHub events ───────────────────────────────────────────────
  if (req.method !== 'POST' || req.url !== '/webhook') {
    send(res, 200, { status: 'webhook-router running' });
    return;
  }

  // Verify GitHub webhook signature before processing
  if (!verifySignature(rawBody, req.headers['x-hub-signature-256'])) {
    console.warn('❌ Invalid webhook signature — rejected');
    send(res, 401, { error: 'Invalid signature' });
    return;
  }

  const event   = req.headers['x-github-event'];
  const payload = parsePayload(rawBody);

  if (!payload) {
    send(res, 400, { error: 'Invalid payload' });
    return;
  }

  const owner = payload.repository?.owner?.login;
  const repo  = payload.repository?.name;

  // issues.opened / issues.reopened → triage only
  if (event === 'issues' && (payload.action === 'opened' || payload.action === 'reopened')) {
    const issueNumber = payload.issue?.number;
    if (!owner || !repo || !issueNumber) {
      send(res, 400, { error: 'Malformed payload: missing owner, repo, or issue number' });
      return;
    }
    const key = issueKey(owner, repo, issueNumber);
    console.log(`\n📨 issues.${payload.action}: ${key} — "${payload.issue.title}"`);

    if (!pipelineState.has(key)) {
      pipelineState.set(key, { triageDone: false, pendingAutofix: null, createdAt: Date.now() });
    }

    send(res, 200, { message: 'forwarded to triage', issue: issueNumber });

    const r = await forward(TRIAGE_PORT, rawBody, req.headers);
    console.log(`   ✅ triage (${TRIAGE_PORT}) → ${r.status}`);
    return;
  }

  // issues.labeled "bug" → hold until triage-done, then fire autofix
  if (event === 'issues' && payload.action === 'labeled' && payload.label?.name === 'bug') {
    const issueNumber = payload.issue?.number;
    if (!owner || !repo || !issueNumber) {
      send(res, 400, { error: 'Malformed payload: missing owner, repo, or issue number' });
      return;
    }
    const key = issueKey(owner, repo, issueNumber);
    console.log(`\n📨 issues.labeled "bug": ${key}`);

    const state = pipelineState.get(key) || { triageDone: false, pendingAutofix: null, createdAt: Date.now() };
    pipelineState.set(key, state);

    send(res, 200, { message: 'queuing or forwarding to autofix', issue: issueNumber });

    if (state.triageDone) {
      console.log(`  ▶️  Triage already done — forwarding immediately to autofix`);
      const r = await forward(AUTOFIX_PORT, rawBody, req.headers);
      console.log(`   ✅ autofix (${AUTOFIX_PORT}) → ${r.status}`);
    } else {
      console.log(`  ⏳ Triage still running — queuing autofix payload`);
      state.pendingAutofix = { pendingRawBody: rawBody, pendingHeaders: { ...req.headers } };

      // Safety net: if triage-done never arrives (agent crashed, TRIAGE_DONE_URL not set),
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
    const pullNumber = payload.pull_request?.number;
    if (!owner || !repo || !pullNumber) {
      send(res, 400, { error: 'Malformed payload: missing owner, repo, or PR number' });
      return;
    }
    console.log(`\n📨 pull_request.${payload.action}: PR #${pullNumber}`);

    send(res, 200, { message: 'forwarded to review bot', pr: pullNumber });

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
    const pullNumber = payload.pull_request?.number;
    const reviewer   = payload.review?.user?.login;
    const branch     = payload.pull_request?.head?.ref;

    if (!owner || !repo || !pullNumber) {
      send(res, 400, { error: 'Malformed payload: missing owner, repo, or PR number' });
      return;
    }

    console.log(`\n📨 pull_request_review.submitted: PR #${pullNumber} by ${reviewer} (branch: ${branch})`);

    if (!branch?.startsWith('fix/issue-')) {
      console.log(`  ⏭  Skipping — not a fix/issue-* branch`);
      send(res, 200, { message: 'skipped — not a fix branch', pr: pullNumber });
      return;
    }

    send(res, 200, { message: 'forwarded to review-fix', pr: pullNumber });

    const r = await forward(AUTOFIX_PORT, rawBody, req.headers);
    console.log(`   ✅ review-fix (${AUTOFIX_PORT}) → ${r.status}`);
    return;
  }

  // Everything else — ignore
  console.log(`⏭  Ignored: ${event}.${payload.action || '?'}`);
  send(res, 200, { message: `Ignored: ${event}.${payload.action}` });
});

server.listen(PORT, () => {
  console.log(`\n🔀 Webhook Router (Orchestrator)`);
  console.log(`   Listening : http://localhost:${PORT}/webhook`);
  console.log(`   Health    : http://localhost:${PORT}/health`);
  console.log(`   Internal  : http://localhost:${PORT}/internal/triage-done`);
  console.log(`               http://localhost:${PORT}/internal/autofix-done`);
  console.log(`\n   Pipeline  :`);
  console.log(`     issues.opened          → triage (${TRIAGE_PORT})`);
  console.log(`     issues.labeled "bug"   → queued until triage-done → autofix (${AUTOFIX_PORT})`);
  console.log(`     pull_request           → review bot (${REVIEW_PORT})`);
  console.log(`     pull_request_review    → review-fix (${AUTOFIX_PORT}) immediately\n`);
});

function shutdown(signal) {
  console.log(`\n⏳ ${signal} received — shutting down gracefully`);
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 30_000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
