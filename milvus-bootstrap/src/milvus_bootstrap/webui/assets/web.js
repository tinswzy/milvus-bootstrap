// Minimal vanilla renderer for the Milvus Admin WebUI (read-only overview).
const NAV = [
  { id: 'overview', label: 'Overview', href: 'index.html' },
  { id: 'milvus',   label: 'Milvus 实例', href: 'milvus.html' },
  { id: 'deps',     label: 'Dependencies', href: 'deps.html' },
  { id: 'compat',   label: '版本依赖', href: 'compat.html' },
  { id: 'install',  label: '安装向导', href: 'install.html' },
];
const LVL = { PASS: 'ok', WARN: 'warn', FAIL: 'err', SKIP: 'idle' };

function esc(s) { return String(s == null ? '' : s).replace(/[&<>]/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;' }[c])); }

const NAV_ICON = {
  overview: '<path d="M3 3h7v7H3zM14 3h7v7h-7zM14 14h7v7h-7zM3 14h7v7H3z"/>',
  milvus: '<path d="M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/>',
  deps: '<path d="M6 3h12v6H6zM6 15h12v6H6zM12 9v6"/>',
  compat: '<path d="M3 7l9-4 9 4-9 4-9-4zM3 12l9 4 9-4M3 17l9 4 9-4"/>',
  install: '<path d="M12 3v12M8 11l4 4 4-4M4 21h16"/>',
};
function svgIco(path, size) {
  return `<svg class="ic" viewBox="0 0 24 24" width="${size}" height="${size}" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">${path}</svg>`;
}

function shell(active) {
  const rail = document.getElementById('rail');
  if (rail) rail.innerHTML =
    '<div class="brand"><span class="mark">' +
    svgIco('<path d="M12 2l8 5v10l-8 5-8-5V7z" fill="rgba(255,255,255,.15)"/><path d="M8 9l4 6 4-6M12 15v4"/>', 20) +
    '</span><span class="word"><b>Milvus Admin</b><span>WebUI</span></span></div>' +
    '<nav class="nav">' +
    NAV.map(n => `<a class="${n.id === active ? 'active' : ''}" href="${n.href}">${svgIco(NAV_ICON[n.id] || '', 17)}<span>${esc(n.label)}</span></a>`).join('') +
    '</nav>';
  const top = document.getElementById('topbar');
  if (top) top.innerHTML = `<div class="crumbs">Milvus Admin <span class="sep">/</span> <b>${esc({ compat: '版本依赖', install: '安装向导', milvus: 'Milvus 实例', deps: 'Dependencies' }[active] || 'Overview')}</b></div>`;
}

async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(url + ' -> HTTP ' + r.status);
  return r.json();
}

async function postJSON(url, body) {
  const r = await fetch(url, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  let data = null;
  try { data = await r.json(); } catch (e) { /* empty body */ }
  return { status: r.status, data };
}

function badge(level, text) {
  return `<span class="badge b-${LVL[level] || 'idle'}"><span class="d"></span>${esc(text || level)}</span>`;
}

async function renderCompat() {
  shell('compat');
  const err = document.getElementById('err');
  err.style.display = 'none';
  try {
    const r = await getJSON('api/compat-rules');
    document.getElementById('mq-rules').innerHTML =
      '<table class="tbl"><thead><tr><th>MQ</th><th>WAL</th><th>最低 milvus</th><th>依赖</th><th>说明</th></tr></thead><tbody>' +
      r.mq_rules.map(m => `<tr><td>${esc(m.label)}</td><td>${esc(m.wal)}</td><td class="mono">${esc(m.min_milvus)}</td>` +
        `<td>${esc(m.dep_kind || '嵌入')}${m.standalone_only ? ' · 仅standalone' : ''}</td><td class="muted">${esc(m.note)}</td></tr>`).join('') +
      '</tbody></table>';
    document.getElementById('constraints').innerHTML =
      '<table class="tbl"><thead><tr><th>组件</th><th>规则</th><th>下限</th><th>milvus 区间</th><th>强度</th><th>来源</th></tr></thead><tbody>' +
      r.constraints.map(c => `<tr><td>${esc(c.component)}</td><td>${esc(c.rule)}</td><td class="mono">${esc(c.min || '—')}</td>` +
        `<td class="mono">${esc(c.milvus_range || '任意')}</td><td>${badge(c.severity === 'hard' ? 'FAIL' : 'WARN', c.severity)}</td><td class="muted">${esc(c.source)}</td></tr>`).join('') +
      '</tbody></table>';
    document.getElementById('upgrade-paths').innerHTML =
      '<table class="tbl"><thead><tr><th>目标 ≥</th><th>需当前 ≥</th><th>说明</th></tr></thead><tbody>' +
      r.upgrade_paths.map(u => `<tr><td class="mono">${esc(u.target_min)}</td><td class="mono">${esc(u.requires_current_min)}</td><td class="muted">${esc(u.reason)}</td></tr>`).join('') +
      '</tbody></table>';
  } catch (e) {
    err.style.display = 'block';
    err.textContent = '加载失败：' + e.message;
  }
}

async function renderOverview() {
  shell('overview');
  const err = document.getElementById('err');
  err.style.display = 'none';
  try {
    const doc = await getJSON('api/doctor');
    // environment rows
    document.getElementById('env-list').innerHTML =
      '<table class="tbl"><tbody>' + doc.env.map(f =>
        `<tr><td>${esc(f.rule || f.component)}</td><td>${badge(f.level)}</td><td class="muted">${esc(f.reason)}</td></tr>`
      ).join('') + '</tbody></table>';
    // k8s connection
    const cluster = doc.env.find(f => f.component === 'cluster');
    const connected = cluster && cluster.level === 'PASS';
    document.getElementById('conn').innerHTML = connected
      ? `<div class="conn ok">✅ 已连接　<span class="muted">${esc(cluster.reason)}</span></div>`
      : `<div class="conn bad">❌ 未连接　<span class="muted">${esc(cluster ? cluster.reason : '未探测')}</span></div>`;
    // versions (only if connected)
    document.getElementById('versions').innerHTML = connected
      ? '<table class="tbl"><tbody>' + Object.entries(doc.versions).map(([k, v]) =>
          `<tr><td>${esc(k)}</td><td class="mono">${esc(v)}</td></tr>`).join('') +
        (Object.keys(doc.versions).length ? '' : '<tr><td class="muted" colspan="2">未探测到组件版本</td></tr>') +
        '</tbody></table>'
      : '<div class="muted">连接集群后展示</div>';
  } catch (e) {
    err.style.display = 'block';
    err.textContent = '加载失败：' + e.message;
  }
}

const INSTALL_KINDS = ['etcd', 'minio', 'kafka', 'pulsar', 'milvus'];
const INSTALL_DEFAULTS = {
  etcd: {}, minio: {}, kafka: {}, pulsar: {},
  milvus: { mq: 'kafka', image: 'milvusdb/milvus:v2.6.18',
            storageEndpoint: 'minio.default.svc:80', kafkaBrokers: 'kafka-dev.default.svc:9092' },
};

function paramRow(k, v) {
  const row = document.createElement('div');
  row.className = 'prow';
  row.innerHTML = `<input class="pk" placeholder="key"><span>=</span><input class="pv" placeholder="value"><button class="btn btn-ghost btn-sm pdel">删</button>`;
  row.querySelector('.pk').value = k || '';
  row.querySelector('.pv').value = v || '';
  row.querySelector('.pdel').onclick = () => row.remove();
  return row;
}

function fillParams(kind) {
  const box = document.getElementById('inst-params');
  box.innerHTML = '';
  const d = INSTALL_DEFAULTS[kind] || {};
  const entries = Object.entries(d);
  if (!entries.length) box.appendChild(paramRow('', ''));
  else entries.forEach(([k, v]) => box.appendChild(paramRow(k, v)));
}

function collectParams() {
  const out = {};
  document.querySelectorAll('#inst-params .prow').forEach(r => {
    const k = r.querySelector('.pk').value.trim();
    if (k) out[k] = r.querySelector('.pv').value.trim();
  });
  return out;
}

function renderTaskResult(task) {
  const st = { succeeded: 'PASS', failed: 'FAIL', rolled_back: 'FAIL' }[task.status] || 'WARN';
  return `<div style="margin-bottom:8px">总状态：${badge(st, task.status)}${task.dry_run ? ' <span class="muted">(dry-run)</span>' : ''}</div>` +
    '<table class="tbl"><thead><tr><th>步骤</th><th>状态</th><th>详情/计划</th></tr></thead><tbody>' +
    task.steps.map(s => {
      const lvl = { ok: 'PASS', failed: 'FAIL', skipped: 'SKIP', planned: 'WARN', running: 'WARN' }[s.status] || 'WARN';
      return `<tr><td>${esc(s.name)}</td><td>${badge(lvl, s.status)}</td><td class="muted">${esc(s.detail || s.plan)}</td></tr>`;
    }).join('') + '</tbody></table>';
}

function installBody(dryRun, force) {
  return {
    kind: document.getElementById('inst-kind').value,
    name: document.getElementById('inst-name').value.trim(),
    namespace: document.getElementById('inst-ns').value.trim() || 'default',
    params: collectParams(), dry_run: dryRun, force: !!force,
  };
}

async function pollInstall(taskId, resultEl) {
  const started = Date.now();
  while (true) {
    let j;
    try { j = await getJSON('api/task/' + taskId); }
    catch (e) { resultEl.innerHTML = '<span class="conn bad">轮询失败：' + esc(e.message) + '</span>'; return; }
    if (j.state === 'running') {
      resultEl.innerHTML = `<span class="muted">安装中… ${Math.round((Date.now() - started) / 1000)}s</span>`;
      await new Promise(r => setTimeout(r, 1500));
      continue;
    }
    if (j.state === 'error') { resultEl.innerHTML = '<span class="conn bad">执行出错：' + esc(j.error) + '</span>'; return; }
    resultEl.innerHTML = renderTaskResult(j.task);
    return;
  }
}

async function submitInstall(dryRun, force) {
  const err = document.getElementById('err'); err.style.display = 'none';
  const resultEl = document.getElementById('inst-result');
  const body = installBody(dryRun, force);
  if (!body.name) { err.style.display = 'block'; err.textContent = '请填实例名'; return; }
  resultEl.innerHTML = '<span class="muted">提交中…</span>';
  let status, data;
  try {
    ({ status, data } = await postJSON('api/install', body));
  } catch (e) {
    resultEl.innerHTML = '';
    err.style.display = 'block'; err.textContent = '提交失败：' + esc(e.message);
    return;
  }
  if (status === 200) { resultEl.innerHTML = renderTaskResult(data.task); return; }
  if (status === 202) { await pollInstall(data.task_id, resultEl); return; }
  if (status === 409) {
    resultEl.innerHTML = `<div class="conn bad">被兼容门禁拦截：${esc((data && data.reason) || '兼容门禁拦截')}</div>` +
      `<button class="btn btn-primary btn-sm" id="inst-force" style="margin-top:8px">强制安装 --force</button>`;
    document.getElementById('inst-force').onclick = () => {
      if (confirm('确认跳过兼容门禁强制安装？')) submitInstall(dryRun, true);
    };
    return;
  }
  resultEl.innerHTML = '';
  err.style.display = 'block';
  err.textContent = '失败（HTTP ' + status + '）：' + esc((data && data.reason) || '未知错误');
}

function renderInstall() {
  shell('install');
  const sel = document.getElementById('inst-kind');
  sel.innerHTML = INSTALL_KINDS.map(k => `<option value="${k}">${k}</option>`).join('');
  sel.onchange = () => fillParams(sel.value);
  fillParams(sel.value);
  document.getElementById('inst-addparam').onclick = () =>
    document.getElementById('inst-params').appendChild(paramRow('', ''));
  document.getElementById('inst-dryrun').onclick = () => submitInstall(true, false);
  document.getElementById('inst-apply').onclick = () => submitInstall(false, false);
}
