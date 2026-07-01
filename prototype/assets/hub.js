/* ============================================================
   Milvus Admin WebUI — prototype shell & fake engine
   Builds the rail + topbar on every page, holds the (fake) data,
   and provides toast / drawer / modal helpers. No real backend.
   ============================================================ */
const Hub = (function () {

  /* ---------- fake component inventory ---------- */
  const COMPONENTS = [
    { id:'milvus',     name:'Milvus',        kind:'core / mixcoord+nodes', logo:'M', accent:true,
      status:'ok',   version:'3.0.0',  latest:'3.0.1',  replicas:'8/8',  ns:'milvus', mgr:'Milvus Operator',
      role:'向量数据库内核：MixCoord + Proxy + Query/Data/Streaming Node' },
    { id:'woodpecker', name:'Woodpecker',    kind:'message queue (WAL)', logo:'🪶',
      status:'ok',   version:'0.4.0',  latest:'0.5.0',  replicas:'4/4',  ns:'milvus', mgr:'Woodpecker Operator', active:true,
      role:'Milvus 原生 WAL / 内置消息队列，基于对象存储' },
    { id:'kafka',      name:'Kafka',         kind:'message queue', logo:'🌊',
      status:'idle',  version:'—',     latest:'3.7.0',  replicas:'0/0',  ns:'milvus', mgr:'Strimzi',
      role:'分布式消息队列（可选 MQ 后端，未部署）' },
    { id:'pulsar',     name:'Pulsar',        kind:'message queue', logo:'📡',
      status:'idle',  version:'—',     latest:'3.0.6',  replicas:'0/0',  ns:'milvus', mgr:'Pulsar Operator',
      role:'分布式消息队列（可选 MQ 后端，未部署）' },
    { id:'etcd',       name:'etcd',          kind:'metadata store', logo:'🗄️',
      status:'ok',   version:'3.5.14', latest:'3.5.16', replicas:'3/3',  ns:'milvus', mgr:'etcd-operator',
      role:'元数据存储与服务发现' },
    { id:'minio',      name:'MinIO',         kind:'object storage', logo:'🪣',
      status:'ok',   version:'2024-08', latest:'2024-08', replicas:'4/4', ns:'milvus', mgr:'MinIO Operator',
      role:'对象存储：segment / 索引 / Woodpecker 日志' },
    { id:'s3',         name:'External S3',   kind:'object storage', logo:'☁️',
      status:'idle',  version:'external', latest:'—',    replicas:'—',    ns:'—', mgr:'External',
      role:'外部对象存储（替代内置 MinIO）' },
    { id:'attu',       name:'Attu',          kind:'tool / GUI', logo:'📊',
      status:'ok',   version:'2.4.10', latest:'2.4.12', replicas:'1/1',  ns:'milvus', mgr:'Tool',
      role:'Milvus 可视化管理客户端' },
    { id:'birdwatcher',name:'Birdwatcher',   kind:'tool / diagnostics', logo:'🔭',
      status:'idle',  version:'—',     latest:'1.0.6',  replicas:'0/0',  ns:'milvus', mgr:'Tool',
      role:'元数据诊断 / 集群运维 CLI' },
    { id:'logexport',  name:'Log Export',    kind:'tool / observability', logo:'📤',
      status:'idle',  version:'—',     latest:'1.2.0',  replicas:'0/0',  ns:'milvus', mgr:'Tool',
      role:'一键导出各组件日志用于排障' },
  ];
  const byId = id => COMPONENTS.find(c => c.id === id);

  /* ---------- fake Milvus instances (the center of gravity) ---------- */
  const INSTANCES = [
    { name:'milvus-prod',      ns:'milvus',     status:'ok',   version:'3.0.0', nodes:'8/8',
      etcd:'etcd-cluster-a1b2', store:{ kind:'minio', id:'minio-tenant-7f3c' }, mq:{ kind:'woodpecker', id:'woodpecker-svc-9d2e' } },
    { name:'milvus-staging',   ns:'milvus-stg', status:'ok',   version:'2.6.3', nodes:'5/5',
      etcd:'etcd-cluster-c3d4', store:{ kind:'minio', id:'minio-tenant-1a8b' }, mq:{ kind:'kafka', id:'kafka-cluster-4b7a' } },
    { name:'milvus-analytics', ns:'milvus-ana', status:'warn', version:'2.6.0', nodes:'6/7',
      etcd:'etcd-cluster-e5f6', store:{ kind:'s3',    id:'s3-external-prod' }, mq:{ kind:'pulsar', id:'pulsar-cluster-2c9f' } },
  ];

  /* existing dependency instances available for reuse in the install wizard */
  const POOL = {
    etcd:       ['etcd-cluster-a1b2','etcd-cluster-c3d4','etcd-cluster-e5f6'],
    minio:      ['minio-tenant-7f3c','minio-tenant-1a8b'],
    s3:         ['s3-external-prod'],
    woodpecker: ['woodpecker-svc-9d2e'],
    kafka:      ['kafka-cluster-4b7a'],
    pulsar:     ['pulsar-cluster-2c9f'],
  };

  const MQMETA    = { woodpecker:{logo:'🪶',name:'Woodpecker'}, kafka:{logo:'🌊',name:'Kafka'}, pulsar:{logo:'📡',name:'Pulsar'} };
  const STOREMETA = { minio:{logo:'🪣',name:'MinIO'}, s3:{logo:'☁️',name:'External S3'} };
  const statusDot = s => ({ok:'var(--ok)',warn:'var(--warn)',err:'var(--err)',idle:'var(--idle)'}[s]||'var(--idle)');

  /* ---------- inline icons ---------- */
  const I = {
    grid:'<path d="M3 3h7v7H3zM14 3h7v7h-7zM14 14h7v7h-7zM3 14h7v7H3z"/>',
    install:'<path d="M12 3v12m0 0l-4-4m4 4l4-4M4 17v2a2 2 0 002 2h12a2 2 0 002-2v-2"/>',
    swap:'<path d="M7 4L3 8l4 4M3 8h13M17 20l4-4-4-4M21 16H8"/>',
    upgrade:'<path d="M12 20V8m0 0l-5 5m5-5l5 5M5 4h14"/>',
    sliders:'<path d="M4 6h10M18 6h2M4 12h2M10 12h10M4 18h7M15 18h5M14 4v4M6 10v4M11 16v4"/>',
    cube:'<path d="M12 2l8 4.5v9L12 22l-8-6.5v-9zM12 2v20M4 6.5l8 4.5 8-4.5"/>',
    tool:'<path d="M14 7a4 4 0 01-5 5L4 17l3 3 5-5a4 4 0 005-5l-2 2-3-3 2-2z"/>',
    bell:'<path d="M6 8a6 6 0 1112 0c0 7 3 8 3 8H3s3-1 3-8M10 21a2 2 0 004 0"/>',
    search:'<circle cx="11" cy="11" r="7"/><path d="M21 21l-4-4"/>',
    chev:'<path d="M6 9l6 6 6-6"/>', layers:'<path d="M12 2l9 5-9 5-9-5 9-5zM3 12l9 5 9-5M3 17l9 5 9-5"/>',
    check:'<path d="M20 6L9 17l-5-5"/>', x:'<path d="M18 6L6 18M6 6l12 12"/>',
    warn:'<path d="M12 9v4m0 4h.01M10.3 3.9L1.8 18a2 2 0 001.7 3h17a2 2 0 001.7-3L13.7 3.9a2 2 0 00-3.4 0z"/>',
    info:'<circle cx="12" cy="12" r="9"/><path d="M12 8h.01M11 12h1v4h1"/>',
    rocket:'<path d="M5 13c-1 3-1 6-1 6s3 0 6-1m-5-5a16 16 0 019-9 9 9 0 011 5 16 16 0 01-9 9zM9 15l-1-1m6-6a2 2 0 100-4 2 2 0 000 4z"/>',
    restart:'<path d="M21 12a9 9 0 11-3-6.7M21 4v4h-4"/>',
  };
  const svg = (p,s=18)=>`<svg class="ic" width="${s}" height="${s}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">${p}</svg>`;

  /* ---------- status helpers ---------- */
  const ST = { ok:['b-ok','运行中'], warn:['b-warn','降级'], err:['b-err','异常'], idle:['b-idle','未部署'] };
  function badge(s, pulse) {
    const [cls,txt] = ST[s] || ST.idle;
    return `<span class="badge ${cls}"><span class="d ${pulse&&s==='ok'?'':''}"></span>${txt}</span>`;
  }
  function compLogo(c, size=38) {
    if (c.accent) return `<span class="lo" style="font-family:var(--font-display);font-weight:700;color:#fff;
      background:linear-gradient(150deg,var(--accent),var(--indigo));border:none;width:${size}px;height:${size}px">${c.logo}</span>`;
    return `<span class="lo" style="width:${size}px;height:${size}px">${c.logo}</span>`;
  }

  /* ---------- navigation ---------- */
  const NAV = [
    { grp:'实例' },
    { id:'index',     label:'Milvus Instances',       icon:I.grid,   href:'index.html' },
    { id:'upgrade',   label:'Dependencies', icon:I.layers, href:'upgrade.html' },
    { grp:'其他' },
    { id:'install',   label:'安装向导',       icon:I.install,  href:'install.html' },
    { id:'configmap', label:'ConfigMap 管理', icon:I.sliders,  href:'configmap.html' },
    { id:'tools',     label:'工具箱',         icon:I.tool,     href:'tools.html' },
    { id:'help',      label:'帮助 · 架构',    icon:I.info,     href:'help.html' },
    { grp:'方案讨论' },
    { id:'plan-compare', label:'方案对比',     icon:I.layers,  href:'plan-compare.html' },
    { id:'plan-1',       label:'方案一 · 扩展 helm/operator（过渡）', icon:I.sliders, href:'plan-1.html' },
    { id:'plan-2',       label:'方案二 · 维持现状 · k8s hack',        icon:I.swap,   href:'plan-2.html' },
    { id:'plan-3',       label:'方案三 · WebUI（终态）',              icon:I.cube,   href:'plan-3.html' },
    { id:'summary',      label:'讨论小结 · 实施计划',                  icon:I.check,  href:'summary.html' },
    { id:'design-arch',  label:'总体设计 · 架构/模块/流程',            icon:I.layers, href:'design-arch.html' },
    { id:'design-runtime', label:'运行时 · 部署/状态/平台',            icon:I.rocket, href:'design-runtime.html' },
    { id:'design-abstraction', label:'分层抽象 · 组件×平台',           icon:I.grid,   href:'design-abstraction.html' },
    { id:'phase1-plan',   label:'阶段一 · 实施计划',                   icon:I.cube,   href:'phase1-plan.html' },
    { grp:'阶段一 · 逐步验收' },
    { id:'phase1-setup',  label:'环境准备 · 远程 minikube',  icon:I.rocket,  href:'phase1-setup.html' },
    { id:'phase1-step1',  label:'① kafka/pulsar dry-run',   icon:I.sliders, href:'phase1-step1.html' },
    { id:'phase1-step2',  label:'② kafka/pulsar live',      icon:I.install, href:'phase1-step2.html' },
    { id:'phase1-step3',  label:'③ milvus 连 kafka dry-run', icon:I.sliders, href:'phase1-step3.html' },
    { id:'phase1-step4',  label:'④ milvus 连 kafka live',    icon:I.install, href:'phase1-step4.html' },
    { id:'phase1-step5',  label:'⑤ milvus 连 pulsar live',   icon:I.install, href:'phase1-step5.html' },
    { id:'phase1-step6',  label:'⑥ switch kafka→pulsar',     icon:I.swap,    href:'phase1-step6.html' },
    { id:'phase1-step7',  label:'⑦ kafka 独立升级',          icon:I.upgrade, href:'phase1-step7.html' },
    { id:'phase1-step8',  label:'⑧ milvus 升级',             icon:I.upgrade, href:'phase1-step8.html' },
    { id:'phase1-step9',  label:'⑨ 兼容矩阵',                icon:I.layers,  href:'phase1-step9.html' },
    { grp:'工具打磨' },
    { id:'mb-doctor',     label:'mb doctor · 环境/版本/兼容', icon:I.info,   href:'mb-doctor.html' },
  ];

  /* ---------- shell render ---------- */
  function railHTML(active) {
    let nav = '';
    NAV.forEach(n => {
      if (n.grp) { nav += `<div class="grp">${n.grp}</div>`; return; }
      const a = n.id === active ? ' active' : '';
      nav += `<a class="${a.trim()}" href="${n.href}">${svg(n.icon,17)}<span>${n.label}</span>${n.tag?`<span class="tag">${n.tag}</span>`:''}</a>`;
    });
    return `
      <div class="brand">
        <span class="mark">${svg('<path d="M12 2l8 5v10l-8 5-8-5V7z" fill="rgba(255,255,255,.15)"/><path d="M8 9l4 6 4-6M12 15v4"/>',20)}</span>
        <span class="word"><b>Milvus Admin</b><span>WebUI</span></span>
      </div>
      <div class="scope" onclick="Hub.toast('演示原型','集群/命名空间切换为假数据','info')">
        <div class="lbl">集群 · 命名空间</div>
        <div class="val">${svg(I.layers,14)} prod-cluster · milvus <span class="chev">${svg(I.chev,14)}</span></div>
      </div>
      <nav class="nav">${nav}</nav>
      <div class="foot">
        <span class="ava">JM</span>
        <span class="who"><b>Justin M.</b><span>平台管理员</span></span>
      </div>`;
  }

  function topHTML(crumbs) {
    const c = (crumbs||['总览']).map((x,i,a)=> i===a.length-1?`<b>${x}</b>`:`${x} <span class="sep">/</span>`).join(' ');
    return `
      <div class="crumbs">Milvus Admin <span class="sep">/</span> ${c}</div>
      <div class="spacer"></div>
      <div class="env-pill" onclick="Hub.toast('环境','prod / staging / dev 切换（假）','info')"><span class="dot"></span> prod</div>
      <button class="icon-btn" onclick="Hub.notifs()">${svg(I.bell,17)}<span class="bdg">3</span></button>
      <button class="icon-btn" onclick="Hub.toast('搜索','跨组件搜索（假）','info')">${svg(I.search,17)}</button>`;
  }

  function mount({ page, crumbs }) {
    const rail = document.getElementById('rail'); if (rail) rail.innerHTML = railHTML(page);
    const top  = document.getElementById('topbar'); if (top) top.innerHTML = topHTML(crumbs);
    // stagger reveal
    document.querySelectorAll('[data-rise]').forEach((el,i)=>{ el.classList.add('reveal'); el.style.animationDelay=(i*55)+'ms'; });
  }

  /* ---------- toast ---------- */
  function toast(title, sub, type='ok', ms=3200) {
    let box = document.getElementById('toasts');
    if (!box) { box = document.createElement('div'); box.id='toasts'; document.body.appendChild(box); }
    const t = document.createElement('div'); t.className = 'toast '+type;
    const ic = {ok:I.check, info:I.info, warn:I.warn, err:I.x}[type] || I.check;
    t.innerHTML = `<span class="ic">${svg(ic,14)}</span><div><b>${title}</b>${sub?`<span>${sub}</span>`:''}</div>`;
    box.appendChild(t);
    setTimeout(()=>{ t.style.transition='.3s'; t.style.opacity='0'; t.style.transform='translateX(30px)'; setTimeout(()=>t.remove(),300); }, ms);
  }

  /* ---------- drawer ---------- */
  function drawer({ title, body, footer }) {
    closeOverlays();
    const scrim = el('div','scrim'); scrim.onclick = closeOverlays;
    const d = el('div','drawer');
    d.innerHTML = `<div class="dh"><h3>${title}</h3><button class="close-x" onclick="Hub.closeOverlays()">${svg(I.x,16)}</button></div>
      <div class="db">${body}</div>${footer?`<div class="df">${footer}</div>`:''}`;
    document.body.append(scrim, d); window._ov=[scrim,d];
  }

  /* ---------- modal / confirm ---------- */
  function modal({ title, msg, icon='info', body='', confirmText='确认', danger=false, onConfirm }) {
    closeOverlays();
    const scrim = el('div','scrim');
    const m = el('div','modal');
    const tone = danger?'err':icon;
    const bg = {info:'var(--info-soft)',err:'var(--err-soft)',warn:'var(--warn-soft)',ok:'var(--ok-soft)'}[tone];
    const fg = {info:'var(--info)',err:'var(--err)',warn:'var(--warn)',ok:'var(--ok)'}[tone];
    const ic = {info:I.info,err:I.warn,warn:I.warn,ok:I.check}[tone];
    m.innerHTML = `<div class="box">
      <div class="mh"><span class="ic" style="background:${bg};color:${fg}">${svg(ic,20)}</span>
        <div><h3>${title}</h3><p>${msg}</p></div></div>
      ${body?`<div class="mb">${body}</div>`:''}
      <div class="mf"><button class="btn btn-ghost" onclick="Hub.closeOverlays()">取消</button>
        <button class="btn ${danger?'btn-danger':'btn-primary'}" id="mok">${confirmText}</button></div></div>`;
    document.body.append(scrim, m); window._ov=[scrim,m];
    m.querySelector('#mok').onclick = ()=>{ closeOverlays(); onConfirm && onConfirm(); };
    scrim.onclick = closeOverlays;
  }

  function notifs() {
    drawer({ title:'通知 · 3', body:`
      <div class="timeline">
        <div class="ev warn"><div class="t">12 分钟前</div><div class="m"><b>etcd</b> 有新版本 3.5.16 可用（当前 3.5.14）</div></div>
        <div class="ev"><div class="t">1 小时前</div><div class="m"><b>Woodpecker</b> 0.5.0 发布，建议升级以获得更优 flush 策略</div></div>
        <div class="ev"><div class="t">今天 09:14</div><div class="m">集群 <b>prod-cluster</b> 巡检通过，8/8 组件健康</div></div>
      </div>`, footer:`<button class="btn btn-ghost btn-sm" onclick="Hub.closeOverlays()">关闭</button>
        <a class="btn btn-primary btn-sm" href="upgrade.html">前往升级中心</a>` });
  }

  /* ---------- utils ---------- */
  function el(t,c){ const e=document.createElement(t); if(c)e.className=c; return e; }
  function closeOverlays(){ (window._ov||[]).forEach(n=>n&&n.remove()); window._ov=[]; }
  document.addEventListener('keydown',e=>{ if(e.key==='Escape') closeOverlays(); });

  return { COMPONENTS, byId, INSTANCES, POOL, MQMETA, STOREMETA, statusDot,
           mount, toast, drawer, modal, notifs, closeOverlays, badge, compLogo, svg, I };
})();
