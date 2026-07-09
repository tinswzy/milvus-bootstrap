// Minimal vanilla renderer for the Milvus Admin WebUI (read-only overview).
const NAV = [
  { id: 'overview', label: 'Overview', href: 'index.html' },
  { id: 'milvus',   label: 'Milvus 实例', href: 'milvus.html' },
  { id: 'deps',     label: 'Dependencies', href: 'deps.html' },
  { id: 'compat',   label: '版本依赖', href: 'compat.html' },
  { id: 'install',  label: '安装向导', href: 'install.html' },
];
const LVL = { PASS: 'ok', WARN: 'warn', FAIL: 'err', SKIP: 'idle' };

function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c])); }

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

let INSTANCES_CACHE = null;
async function loadInstances() {
  if (INSTANCES_CACHE) return INSTANCES_CACHE;
  try { INSTANCES_CACHE = (await getJSON('api/instances')).instances; }
  catch (e) { INSTANCES_CACHE = []; }
  return INSTANCES_CACHE;
}
function depOptions(instances, kind) {
  const opts = instances.filter(i => i.kind === kind).map(i => {
    const ep = depEndpoint(kind, i.name, i.namespace);
    return `<option value="${esc(ep)}">${esc(i.name)} (${esc(i.namespace)})</option>`;
  }).join('');
  return opts + '<option value="__custom__">自定义…</option>';
}
function selVal(selId, custId) {
  const s = document.getElementById(selId);
  if (!s) return '';
  if (s.value === '__custom__') { const c = document.getElementById(custId); return c ? c.value.trim() : ''; }
  return s.value;
}
function wireCustom(selId, custId) {
  const s = document.getElementById(selId), c = document.getElementById(custId);
  if (!s || !c) return;
  const sync = () => { c.style.display = s.value === '__custom__' ? '' : 'none'; };
  s.onchange = sync; sync();
}

async function fillParams(kind) {
  const box = document.getElementById('inst-params');
  const head = document.getElementById('inst-params-head');
  if (head) head.style.display = (kind === 'milvus') ? 'none' : '';
  if (kind !== 'milvus') {
    box.innerHTML = '';
    const d = INSTALL_DEFAULTS[kind] || {};
    Object.entries(d).forEach(([k, v]) => box.appendChild(paramRow(k, v)));
    return;
  }
  const insts = await loadInstances();
  const isoField = (id, label, title) =>
    `<div class="iso-in"><label title="${esc(title)}">${esc(label)} <span class="q">?</span></label>` +
    `<input id="${esc(id)}" class="f-in"></div>`;
  const mqLogoFor = t => ({ kafka: '🌊', pulsar: '📡', rocksmq: '🪨' })[t] || (String(t).startsWith('woodpecker') ? '🪶' : '📨');
  const mqInst = mk => `<select id="inst-mq" class="f-in"><option value="">—</option>${depOptions(insts, mk)}</select>`
    + `<input id="inst-mq-custom" class="f-in" placeholder="host:port" style="display:none;margin-top:7px">`;
  // Configure-as-topology: the install form IS the topology card you'll get —
  // etcd ▸ [new Milvus] ▸ store, MQ hanging below, with the same flow connectors.
  box.innerHTML =
    `<div class="topo topo-edit">` +
      `<div class="box cell-etcd">` +
        `<div class="bt"><span class="lo">🗄️</span><div><div class="nm">etcd</div><div class="role">元数据</div></div></div>` +
        `<select id="inst-etcd" class="f-in bind-sel">${depOptions(insts, 'etcd')}</select>` +
        `<input id="inst-etcd-custom" class="f-in" placeholder="etcd.default.svc:2379" style="display:none;margin-top:7px">` +
        isoField('inst-etcd-root', 'rootPath', 'etcd.rootPath —— Milvus 在 etcd 存元数据的根路径。共用同一 etcd 时用它区分不同 Milvus；默认=实例名。') +
      `</div>` +
      `<div class="flow-h col2"></div>` +
      `<div class="box box-mv">` +
        `<div class="bt"><span class="lo">M</span><div><div class="nm" id="mv-name">新 Milvus</div><div class="role">向量数据库内核 · MixCoord</div></div></div>` +
        `<div class="mv-fields">` +
          `<label class="mvl">镜像</label><input id="inst-image" class="f-in" value="milvusdb/milvus:v2.6.18">` +
        `</div>` +
      `</div>` +
      `<div class="flow-h col4"></div>` +
      `<div class="box cell-store">` +
        `<div class="bt"><span class="lo">🪣</span><div><div class="nm">对象存储</div><div class="role">Object Storage</div></div></div>` +
        `<select id="inst-storage" class="f-in bind-sel">${depOptions(insts, 'minio')}</select>` +
        `<input id="inst-storage-custom" class="f-in" placeholder="minio.default.svc:80" style="display:none;margin-top:7px">` +
        isoField('inst-store-bucket', 'bucket', 'minio.bucketName —— Milvus 对象存储用的桶名，各 Milvus 一个桶；默认=实例名。') +
        isoField('inst-store-root', 'rootPath', 'minio.rootPath —— 桶内子路径前缀；想多个 Milvus 共用一个桶又互不干扰时改它；默认=实例名。') +
      `</div>` +
      `<div class="flow-v"></div>` +
      `<div class="box cell-mq">` +
        `<div class="bt"><span class="lo" id="mq-logo">🌊</span><div><div class="nm">消息队列</div><div class="role">WAL · MQ</div></div></div>` +
        `<select id="inst-mqtype" class="f-in bind-sel">` +
        ['kafka', 'pulsar', 'woodpecker-service', 'woodpecker-embedded', 'rocksmq'].map(o => `<option value="${o}">${o}</option>`).join('') +
        `</select><div id="inst-mqinst-row" style="margin-top:7px">${mqInst('kafka')}</div>` +
        isoField('inst-mq-prefix', 'cluster', 'msgChannel.chanNamePrefix.cluster —— MQ topic/channel 名前缀，共用同一 kafka/pulsar 时避免撞名；默认=实例名。') +
      `</div>` +
    `</div>`;
  wireCustom('inst-etcd', 'inst-etcd-custom');
  wireCustom('inst-storage', 'inst-storage-custom');
  const mqtype = document.getElementById('inst-mqtype');
  const row = document.getElementById('inst-mqinst-row');
  const mqLogoEl = document.getElementById('mq-logo');
  const syncMq = () => {
    const t = mqtype.value;
    mqLogoEl.textContent = mqLogoFor(t);
    if (t === 'kafka' || t === 'pulsar') {
      row.style.display = '';
      row.innerHTML = mqInst(t);
      wireCustom('inst-mq', 'inst-mq-custom');
    } else { row.style.display = 'none'; row.innerHTML = ''; }
  };
  mqtype.onchange = syncMq; syncMq();
  const nameEl = document.getElementById('inst-name');
  const mvNameEl = document.getElementById('mv-name');
  const isoFields = ['inst-etcd-root', 'inst-store-bucket', 'inst-store-root', 'inst-mq-prefix']
    .map(id => document.getElementById(id));
  isoFields.forEach(el => {
    el.value = nameEl.value.trim();
    el.oninput = () => { el.dataset.dirty = '1'; };
  });
  nameEl.oninput = () => {
    isoFields.forEach(el => { if (!el.dataset.dirty) el.value = nameEl.value.trim(); });
    mvNameEl.textContent = nameEl.value.trim() || '新 Milvus';
  };
  mvNameEl.textContent = nameEl.value.trim() || '新 Milvus';
}

function collectParams() {
  if (document.getElementById('inst-kind').value !== 'milvus') {
    const out = {};
    document.querySelectorAll('#inst-params .prow').forEach(r => {
      const k = r.querySelector('.pk').value.trim();
      if (k) out[k] = r.querySelector('.pv').value.trim();
    });
    return out;
  }
  const p = { image: (document.getElementById('inst-image') || {}).value || '' };
  const etcd = selVal('inst-etcd', 'inst-etcd-custom'); if (etcd) p.etcdEndpoints = etcd;
  const store = selVal('inst-storage', 'inst-storage-custom'); if (store) p.storageEndpoint = store;
  const mq = (document.getElementById('inst-mqtype') || {}).value || 'kafka';
  p.mq = mq;
  if (mq === 'kafka') { const v = selVal('inst-mq', 'inst-mq-custom'); if (v) p.kafkaBrokers = v; }
  if (mq === 'pulsar') { const v = selVal('inst-mq', 'inst-mq-custom'); if (v) p.pulsarEndpoint = v; }
  p.etcdRootPath = (document.getElementById('inst-etcd-root') || {}).value || '';
  p.minioBucket = (document.getElementById('inst-store-bucket') || {}).value || '';
  p.minioRootPath = (document.getElementById('inst-store-root') || {}).value || '';
  p.mqChanPrefix = (document.getElementById('inst-mq-prefix') || {}).value || '';
  return p;
}

const STEP_ICON = { ok: '✓', failed: '✗', skipped: '⤼', running: '⏳', planned: '○', pending: '·' };

function logPanel(task, running) {
  const head = running
    ? '<div class="loghead run">⏳ 执行中…</div>'
    : (task && task.status === 'succeeded'
        ? '<div class="loghead ok">✅ 完成</div>'
        : '<div class="loghead bad">❌ 出错</div>');
  const steps = (task && task.steps) ? task.steps.slice().reverse() : [];   // newest on top
  const rows = steps.map(s => {
    const ic = STEP_ICON[s.status] || '·';
    const cmd = s.plan ? `<div class="logcmd">▸ ${esc(s.plan)}</div>` : '';
    const det = s.detail ? `<div class="logdet">${esc(s.detail)}</div>` : '';
    return `<div class="logrow st-${esc(s.status)}"><span class="ic">${ic}</span>` +
           `<div class="logbody"><b>${esc(s.name)}</b>${cmd}${det}</div></div>`;
  }).join('') || '<div class="muted" style="padding:8px">暂无步骤…</div>';
  return head + `<div class="logpanel">${rows}</div>`;
}

function renderTaskResult(task) { return logPanel(task, false); }

async function pollTask(taskId, el, onDone) {
  while (true) {
    let j;
    try { j = await getJSON('api/task/' + taskId); }
    catch (e) { el.innerHTML = '<span class="conn bad">轮询失败：' + esc(e.message) + '</span>'; return; }
    if (j.state === 'running') {
      el.innerHTML = logPanel(j.task, true);
      await new Promise(r => setTimeout(r, 800));
      continue;
    }
    if (j.state === 'error') {
      el.innerHTML = logPanel(j.task, false) +
        '<div class="conn bad" style="margin-top:8px">执行出错：' + esc(j.error) + '</div>';
      return;
    }
    el.innerHTML = logPanel(j.task, false);
    if (onDone) onDone(j.task);
    return;
  }
}

function installBody(dryRun, force) {
  return {
    kind: document.getElementById('inst-kind').value,
    name: document.getElementById('inst-name').value.trim(),
    namespace: document.getElementById('inst-ns').value.trim() || 'default',
    params: collectParams(), dry_run: dryRun, force: !!force,
  };
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
  if (status === 202) { await pollTask(data.task_id, resultEl); return; }
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
  sel.onchange = () => { fillParams(sel.value); };
  fillParams(sel.value);
  document.getElementById('inst-addparam').onclick = () =>
    document.getElementById('inst-params').appendChild(paramRow('', ''));
  document.getElementById('inst-dryrun').onclick = () => submitInstall(true, false);
  document.getElementById('inst-apply').onclick = () => submitInstall(false, false);
}

// Honest, no-poll delete: submit + hand off to operator; verify by refreshing
// the list (card gone = deleted; still there = not done / failed). Same on-demand
// model as the upgrade flow — no auto-poll, no false "删除成功".
function openDelete(name, onDone) {
  const m = openModal('删除 · ' + name,
    `<div>确认删除实例 <b>${esc(name)}</b>？<span class="muted">（依赖 / PVC 默认保留）</span></div>` +
    `<div style="margin-top:12px"><button class="btn btn-primary btn-sm" id="del-go">确认删除</button></div>` +
    `<div id="del-result" style="margin-top:12px"></div>`);
  const res = m.body.querySelector('#del-result');
  m.body.querySelector('#del-go').onclick = async () => {
    res.innerHTML = '<span class="muted">提交中…</span>';
    let resp;
    try { resp = await postJSON('api/delete', { instance: name }); }
    catch (e) { res.innerHTML = '<span class="conn bad">提交失败：' + esc(e.message) + '</span>'; return; }
    const { status, data } = resp;
    if (status === 202) {
      res.innerHTML = '<div class="conn ok">已提交删除 · operator 正在处理</div>' +
        '<div class="muted" style="margin:6px 0 10px">刷新列表确认：卡片消失 = 删除成功；仍在 = 尚未完成或失败。</div>' +
        '<button class="btn btn-ghost btn-sm" id="del-refresh">🔄 刷新列表</button>';
      document.getElementById('del-refresh').onclick = () => { closeModal(); onDone(); };
      return;
    }
    res.innerHTML = '<span class="conn bad">失败（HTTP ' + status + '）：' + esc((data && data.reason) || '未知错误') + '</span>';
  };
}

function mqLogo(mq) { return ({ kafka: '🌊', pulsar: '📡', woodpecker: '🪶', rocksmq: '🪨' })[mq] || '📨'; }

function depBox(cls, logo, name, role, id) {
  return `<div class="box ${cls}">` +
    `<div class="bt"><span class="lo">${esc(logo)}</span><div><div class="nm">${esc(name)}</div><div class="role">${esc(role)}</div></div></div>` +
    `<div class="id"><span class="d" style="background:#3fb950"></span>${esc(id || '—')}</div></div>`;
}

function tagOf(ref) {
  if (!ref) return '';
  const noDigest = String(ref).split('@')[0];
  return noDigest.includes(':') ? noDigest.split(':').pop() : noDigest;
}
function imageCell(i) {
  const title = i.image ? (i.image + (i.image_id ? ' @ ' + i.image_id : '')) : '';
  return `<span class="img" title="${esc(title)}">${esc(tagOf(i.image) || '—')}</span>`;
}
function ownBadge(o) {
  return o === 'managed'
    ? '<span class="badge b-accent">managed</span>'
    : '<span class="badge b-muted">external</span>';
}
function delButton(i) {
  return i.ownership === 'managed'
    ? `<button class="btn btn-ghost btn-sm" data-del="${esc(i.name)}">删除</button>`
    : `<button class="btn btn-ghost btn-sm" disabled title="external：mb 未安装，不可删除/升级">删除</button>`;
}

async function renderMilvus() {
  shell('milvus');
  const box = document.getElementById('milvus-list');
  try {
    const inst = await getJSON('api/instances');
    const rows = inst.instances.filter(i => i.kind === 'milvus');
    const head = `<div style="margin-bottom:14px"><a class="btn btn-primary btn-sm" href="install.html">+ 新建 Milvus</a></div>`;
    const ph = t => `<button class="btn btn-ghost btn-sm" disabled title="下一切面">${t}</button>`;
    box.innerHTML = head + (rows.length ? rows.map(i => {
      const d = i.deps || {};
      return `<div class="card inst">` +
        `<div class="inst-head"><span class="mvdot">M</span>` +
        `<div><div class="nm">${esc(i.name)}</div><div class="ns">ns: ${esc(i.namespace)} · ${esc(i.image || '—')}</div></div>` +
        `<div class="right">${ownBadge(i.ownership)} ${statusPill(i)}</div></div>` +
        `<div class="topo">` +
          depBox('cell-etcd', '🗄️', 'etcd', '元数据', d.etcd || ('etcd.' + i.namespace + '.svc:2379')) +
          `<div class="flow-h col2"></div>` +
          `<div class="box box-mv">` +
            `<div class="bt"><span class="lo">M</span><div><div class="nm">${esc(i.name)}</div><div class="role">向量数据库内核 · MixCoord</div></div></div>` +
            `<div class="id"><span class="d" style="background:#3fb950"></span>${esc(i.name)} · ${imageCell(i)}</div>` +
            `<div class="mvmeta"><span class="badge b-accent"><span class="d"></span>MQ: ${esc(d.mq || '—')}</span></div>` +
            `<div class="mv-actions">${upgradeButton(i)}${ph('配置')}${podsButton(i)}${ph('切换 MQ')}${delButton(i)}</div>` +
          `</div>` +
          `<div class="flow-h col4"></div>` +
          depBox('cell-store', '🪣', '对象存储', 'Object Storage', d.storage) +
          `<div class="flow-v"></div>` +
          depBox('cell-mq', mqLogo(d.mq), d.mq || 'MQ', '消息队列 · WAL', d.mq_endpoint) +
        `</div></div>`;
    }).join('') : '<div class="card"><div class="card-pad muted">暂无 Milvus 实例</div></div>');
    box.querySelectorAll('[data-del]').forEach(b => { b.onclick = () => openDelete(b.getAttribute('data-del'), renderMilvus); });
    box.querySelectorAll('[data-pods]').forEach(b => { b.onclick = () => openPods(b.getAttribute('data-pods')); });
    box.querySelectorAll('[data-upgrade]').forEach(b => { b.onclick = () => openUpgrade(b.getAttribute('data-upgrade'), b.getAttribute('data-image')); });
    box.querySelectorAll('[data-progress]').forEach(b => { b.onclick = () => openProgress(b.getAttribute('data-progress')); });
  } catch (e) {
    box.innerHTML = '<div class="conn bad">加载失败：' + esc(e.message) + '</div>';
  }
}

function closeModal() {
  const o = document.getElementById('modal-overlay');
  if (o) o.remove();
  document.onkeydown = null;
}
function openModal(title, bodyHTML) {
  closeModal();
  const o = document.createElement('div');
  o.id = 'modal-overlay';
  o.className = 'modal-overlay';
  o.innerHTML = `<div class="modal"><div class="modal-head"><h3>${esc(title)}</h3>` +
    `<button class="modal-x" id="modal-x">✕</button></div><div class="modal-body">${bodyHTML}</div></div>`;
  document.body.appendChild(o);
  o.onclick = e => { if (e.target === o) closeModal(); };
  document.getElementById('modal-x').onclick = closeModal;
  document.onkeydown = e => { if (e.key === 'Escape') closeModal(); };
  return { root: o, body: o.querySelector('.modal-body'), close: closeModal };
}
function ageOf(iso) {
  if (!iso) return '—';
  const s = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 3600) return Math.round(s / 60) + 'm';
  if (s < 86400) return Math.round(s / 3600) + 'h';
  return Math.round(s / 86400) + 'd';
}
async function openPods(name) {
  const m = openModal('Pods · ' + name, '<div id="pods-body" class="muted">加载中…</div>');
  const el = m.body.querySelector('#pods-body');
  let d;
  try { d = await getJSON('api/pods?instance=' + encodeURIComponent(name)); }
  catch (e) { el.innerHTML = '<div class="conn bad">加载失败：' + esc(e.message) + '</div>'; return; }
  const pods = d.pods || [];
  el.innerHTML = pods.length
    ? '<table class="tbl"><thead><tr><th>Pod</th><th>状态</th><th>Ready</th><th>重启</th><th>龄</th></tr></thead><tbody>' +
      pods.map(p => `<tr><td class="mono">${esc(p.pod)}</td>` +
        `<td>${badge(p.phase === 'Running' ? 'PASS' : 'WARN', p.phase)}</td>` +
        `<td>${esc(p.ready)}</td><td>${esc(String(p.restarts))}</td><td>${esc(ageOf(p.created))}</td></tr>`).join('') +
      '</tbody></table>'
    : `<div class="muted">ns:${esc(d.namespace)} 下未找到该实例的 pod（或未连接集群）</div>`;
}
function podsButton(i) {
  return i.ownership === 'managed'
    ? `<button class="btn btn-ghost btn-sm" data-pods="${esc(i.name)}">Pods</button>`
    : `<button class="btn btn-ghost btn-sm" disabled title="external：仅 managed 可查">Pods</button>`;
}

const DEP_KINDS = ['etcd', 'minio', 'kafka', 'pulsar'];
const DEP_META = {
  etcd: { logo: '🗄️', name: 'etcd', role: '元数据' },
  minio: { logo: '🪣', name: 'MinIO', role: '对象存储' },
  kafka: { logo: '🌊', name: 'Kafka', role: '消息队列' },
  pulsar: { logo: '📡', name: 'Pulsar', role: '消息队列' },
};
function depEndpoint(kind, name, ns) {
  return ({
    etcd: `${name}.${ns}.svc:2379`,
    minio: `${name}.${ns}.svc:80`,
    kafka: `${name}.${ns}.svc:9092`,
    pulsar: `${name}-broker.${ns}.svc:6650`,
  })[kind] || `${name}.${ns}.svc`;
}

function upgradeButton(i) {
  return i.ownership === 'managed'
    ? `<button class="btn btn-ghost btn-sm" data-upgrade="${esc(i.name)}" data-image="${esc(i.image || '')}">升级</button>`
    : `<button class="btn btn-ghost btn-sm" disabled title="external：mb 未安装，不可升级">升级</button>`;
}
async function submitUpgrade(name, image, dryRun, force, resultEl) {
  resultEl.innerHTML = '<span class="muted">提交中…</span>';
  let resp;
  try { resp = await postJSON('api/upgrade', { instance: name, image: image, dry_run: dryRun, force: !!force }); }
  catch (e) { resultEl.innerHTML = '<span class="conn bad">提交失败：' + esc(e.message) + '</span>'; return; }
  const { status, data } = resp;
  if (status === 200) { resultEl.innerHTML = renderTaskResult(data.task); return; }
  if (status === 202) {
    resultEl.innerHTML = '<div class="conn ok">已提交升级 · operator 正在滚动</div>' +
      '<button class="btn btn-primary btn-sm" id="up-prog" style="margin-top:8px">查看进展</button>';
    document.getElementById('up-prog').onclick = () => { closeModal(); openProgress(name); };
    renderMilvus();
    return;
  }
  if (status === 409) {
    resultEl.innerHTML = `<div class="conn bad">被兼容门禁拦截：${esc((data && data.reason) || '兼容门禁')}</div>` +
      `<button class="btn btn-primary btn-sm" id="up-force" style="margin-top:8px">强制升级 --force</button>`;
    document.getElementById('up-force').onclick = () => {
      if (confirm('确认跳过兼容门禁强制升级？')) submitUpgrade(name, image, false, true, resultEl);
    };
    return;
  }
  resultEl.innerHTML = '<span class="conn bad">失败（HTTP ' + status + '）：' + esc((data && data.reason) || '未知错误') + '</span>';
}
function openUpgrade(name, curImage) {
  const m = openModal('升级 · ' + name,
    `<label class="mvl">新镜像</label><input id="up-image" class="f-in" value="${esc(curImage || '')}">` +
    `<div style="margin-top:12px;display:flex;gap:8px"><button class="btn btn-ghost btn-sm" id="up-dry">dry-run 预览</button>` +
    `<button class="btn btn-primary btn-sm" id="up-go">确认升级</button></div>` +
    `<div id="up-result" style="margin-top:12px"></div>`);
  const img = () => m.body.querySelector('#up-image').value.trim();
  const res = m.body.querySelector('#up-result');
  m.body.querySelector('#up-dry').onclick = () => { if (img()) submitUpgrade(name, img(), true, false, res); };
  m.body.querySelector('#up-go').onclick = () => { if (img()) submitUpgrade(name, img(), false, false, res); };
}

function statusPill(i) {
  if (i.rolling) {
    return `<button class="btn btn-ghost btn-sm rollpill" data-progress="${esc(i.name)}">` +
      `🔄 升级中 ${i.pods_upgraded || 0}/${i.pods_total || 0} · 查看进展</button>`;
  }
  return i.status === 'Healthy'
    ? '<span class="badge b-ok"><span class="d"></span>正常运行</span>'
    : (i.status ? badge('WARN', i.status) : '<span class="muted">状态 —</span>');
}
function progPct(u, t) { return t > 0 ? Math.round(100 * u / t) : 0; }
async function openProgress(name) {
  const m = openModal('升级进度 · ' + name,
    '<div id="prog-body" class="muted">加载中…</div>' +
    '<div style="margin-top:12px"><button class="btn btn-ghost btn-sm" id="prog-refresh">🔄 刷新</button></div>');
  const el = m.body.querySelector('#prog-body');
  const render = async () => {
    el.innerHTML = '<span class="muted">读取中…</span>';
    let d;
    try { d = await getJSON('api/pods?instance=' + encodeURIComponent(name)); }
    catch (e) { el.innerHTML = '<div class="conn bad">加载失败：' + esc(e.message) + '</div>'; return; }
    const desired = d.desired_image || '';
    const pods = d.pods || [];
    const total = pods.length;
    const upgraded = pods.filter(p => tagOf(p.image) === tagOf(desired)).length;
    const done = total > 0 && upgraded === total;
    el.innerHTML =
      (done ? '<div class="conn ok" style="margin-bottom:10px">✅ 升级完成，实例正常运行</div>' : '') +
      '<div class="f-sect">阶段一 · CR 已提交</div>' +
      `<div class="mono muted" style="margin-bottom:8px">✓ 目标镜像：${esc(tagOf(desired) || '—')}</div>` +
      `<div class="f-sect">阶段二 · 节点升级 ${upgraded}/${total}（${progPct(upgraded, total)}%）</div>` +
      `<div class="progbar"><i style="width:${progPct(upgraded, total)}%"></i></div>` +
      (total
        ? '<table class="tbl" style="margin-top:10px"><thead><tr><th>Pod</th><th>当前镜像</th><th>状态</th><th>Ready</th></tr></thead><tbody>' +
          pods.map(p => {
            const ok = tagOf(p.image) === tagOf(desired);
            return `<tr><td class="mono">${esc(p.pod)}</td><td class="mono">${esc(tagOf(p.image) || '—')}</td>` +
              `<td>${ok ? '<span class="conn ok">✓已升级</span>' : '<span class="muted">⏳待升级</span>'}</td>` +
              `<td>${esc(p.ready)}</td></tr>`;
          }).join('') + '</tbody></table>'
        : '<div class="muted">未找到该实例的 pod（或未连接集群）</div>');
  };
  m.body.querySelector('#prog-refresh').onclick = render;
  render();
}

async function renderDeps() {
  shell('deps');
  const box = document.getElementById('deps-list');
  try {
    const inst = await getJSON('api/instances');
    box.innerHTML = DEP_KINDS.map(kind => {
      const meta = DEP_META[kind];
      const rows = inst.instances.filter(i => i.kind === kind);
      const body = rows.length ? rows.map(i =>
        `<div class="dep-row"><span class="nm">${esc(i.name)}</span>` +
        `${ownBadge(i.ownership)}` +
        `<span class="muted">ns:${esc(i.namespace)}</span>` +
        `${imageCell(i)}` +
        `<span class="mono muted">${esc(depEndpoint(kind, i.name, i.namespace))}</span>` +
        `${delButton(i)}</div>`).join('')
        : '<div class="muted">无实例</div>';
      return `<div class="card acc open">` +
        `<div class="acc-head"><span class="lo">${esc(meta.logo)}</span>` +
        `<div><div class="nm">${esc(meta.name)}</div><div class="sub">${rows.length} 个实例</div></div>` +
        `<div class="right"><a class="btn btn-ghost btn-sm" href="install.html">+ 新建</a><span class="chev">▾</span></div></div>` +
        `<div class="acc-body">${body}</div></div>`;
    }).join('');
    box.querySelectorAll('.acc-head').forEach(h => {
      h.onclick = e => { if (e.target.closest('a,button')) return; h.parentElement.classList.toggle('open'); };
    });
    box.querySelectorAll('[data-del]').forEach(b => { b.onclick = () => openDelete(b.getAttribute('data-del'), renderDeps); });
  } catch (e) {
    box.innerHTML = '<div class="conn bad">加载失败：' + esc(e.message) + '</div>';
  }
}
