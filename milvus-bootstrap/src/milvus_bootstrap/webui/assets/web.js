// Minimal vanilla renderer for the Milvus Admin WebUI (read-only overview).
const NAV = [
  { id: 'overview', label: 'Overview', href: 'index.html' },
  { id: 'compat',   label: '版本依赖', href: 'compat.html' },
  { id: 'install',  label: '安装向导（待做）', disabled: true },
];
const LVL = { PASS: 'ok', WARN: 'warn', FAIL: 'err', SKIP: 'idle' };

function esc(s) { return String(s == null ? '' : s).replace(/[&<>]/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;' }[c])); }

function shell(active) {
  const rail = document.getElementById('rail');
  if (rail) rail.innerHTML = '<div class="brand">Milvus Admin</div><nav class="nav">' +
    NAV.map(n => n.disabled
      ? `<span class="navitem disabled">${esc(n.label)}</span>`
      : `<a class="navitem${n.id === active ? ' active' : ''}" href="${n.href}">${esc(n.label)}</a>`
    ).join('') + '</nav>';
  const top = document.getElementById('topbar');
  if (top) top.innerHTML = `<div class="crumbs">Milvus Admin <span class="sep">/</span> <b>${esc(active === 'compat' ? '版本依赖' : 'Overview')}</b></div>`;
}

async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(url + ' -> HTTP ' + r.status);
  return r.json();
}

function badge(level, text) {
  return `<span class="badge b-${LVL[level] || 'idle'}"><span class="d"></span>${esc(text || level)}</span>`;
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
    // instances (only if connected)
    if (connected) {
      const inst = (await getJSON('api/instances')).instances;
      document.getElementById('instances').innerHTML = inst.length
        ? '<table class="tbl"><thead><tr><th>名称</th><th>类型</th><th>命名空间</th><th>Ownership</th></tr></thead><tbody>' +
          inst.map(i => `<tr><td>${esc(i.name)}</td><td>${esc(i.kind)}</td><td>${esc(i.namespace)}</td><td>${esc(i.ownership)}</td></tr>`).join('') +
          '</tbody></table>'
        : '<div class="muted">该集群下暂无本工具登记的实例</div>';
    } else {
      document.getElementById('instances').innerHTML = '<div class="muted">连接集群后展示</div>';
    }
  } catch (e) {
    err.style.display = 'block';
    err.textContent = '加载失败：' + e.message;
  }
}
