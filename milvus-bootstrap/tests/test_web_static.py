import re

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MB_HOME", str(tmp_path))
    monkeypatch.setenv("MB_ADAPTER", "fake")
    from milvus_bootstrap.server.app import app
    with TestClient(app) as c:
        yield c


def test_root_serves_overview_html(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    body = r.text
    assert 'id="rail"' in body and 'id="env-list"' in body
    assert 'id="instances-card"' not in body        # instances moved off overview


def test_assets_served(client):
    assert client.get("/assets/web.css").status_code == 200
    js = client.get("/assets/web.js")
    assert js.status_code == 200 and "renderOverview" in js.text


def test_api_routes_win_over_static(client):
    # /healthz is a real route, must not be shadowed by the static mount at "/"
    assert client.get("/healthz").json() == {"ok": True}


def test_compat_page_served(client):
    r = client.get("/compat.html")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert 'id="mq-rules"' in r.text and 'id="constraints"' in r.text and 'id="upgrade-paths"' in r.text
    assert "renderCompat" in client.get("/assets/web.js").text


def test_install_page_served(client):
    r = client.get("/install.html")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert 'id="inst-kind"' in r.text and 'id="inst-params"' in r.text and 'id="inst-result"' in r.text
    js = client.get("/assets/web.js").text
    assert "renderInstall" in js and "postJSON" in js
    assert "安装向导（待做）" not in js       # nav item enabled, not the disabled placeholder


def test_nav_has_instance_pages(client):
    js = client.get("/assets/web.js").text
    assert "milvus.html" in js and "deps.html" in js
    assert "renderOverview" in js                    # overview still exists


def test_deps_page_served(client):
    r = client.get("/deps.html")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert 'id="deps-list"' in r.text
    js = client.get("/assets/web.js").text
    assert "renderDeps" in js and "openDelete" in js


def test_delete_flow_is_honest_no_poll(client):
    js = client.get("/assets/web.js").text
    # modal-based confirm + "已提交" prompt + manual refresh, mirroring the upgrade flow
    assert "function openDelete" in js
    assert "已提交删除" in js and "del-refresh" in js
    # honest / no-polling: the delete flow must not auto-poll the task endpoint
    body = js.split("function openDelete", 1)[1].split("\nfunction ", 1)[0]
    assert "api/task/" not in body and "setTimeout" not in body


def test_milvus_page_served(client):
    r = client.get("/milvus.html")
    assert r.status_code == 200 and "text/html" in r.headers["content-type"]
    assert 'id="milvus-list"' in r.text
    assert "renderMilvus" in client.get("/assets/web.js").text


def test_milvus_card_topology_markup(client):
    js = client.get("/assets/web.js").text
    assert "renderMilvus" in js
    for marker in ['class="card inst"', 'inst-head', 'class="topo"', 'box box-mv', 'flow-h', 'mv-actions', 'function depBox']:
        assert marker in js, marker
    assert 'disabled title="下一切面"' in js       # deferred action placeholders


def test_deps_accordion_markup_and_css(client):
    js = client.get("/assets/web.js").text
    assert "renderDeps" in js
    for marker in ['class="card acc open"', 'acc-head', 'acc-body', 'class="img"', 'function depEndpoint']:
        assert marker in js, marker
    css = client.get("/assets/web.css").text
    for c in ['.acc-head', '.acc-body', '.img', '.acc.open']:
        assert c in css, c


def test_esc_encodes_quotes(client):
    """esc() must encode " and ' (used in data-del=... attribute contexts), not only &<>."""
    js = client.get("/assets/web.js").text
    # the esc() replacement must cover quotes so attribute interpolation can't break out
    assert "&quot;" in js and "&#39;" in js
    m = re.search(r"function esc\(s\).*?replace\(/\[([^\]]*)\]/g", js)
    assert m and '"' in m.group(1) and "'" in m.group(1), m and m.group(1)


def test_overview_has_no_versions_card(client):
    body = client.get("/").text
    assert 'id="versions-card"' not in body and 'id="versions"' not in body
    js = client.get("/assets/web.js").text
    assert "renderOverview" in js and "env-list" in js     # overview still renders env


def test_milvus_card_ownership_and_image_hover(client):
    js = client.get("/assets/web.js").text
    for m in ['function tagOf', 'function imageCell', 'function ownBadge', 'ownBadge(i.ownership)', 'imageCell(i)']:
        assert m in js, m
    # external rows get a disabled delete with an explanatory title
    assert "external：mb 未安装" in js
    css = client.get("/assets/web.css").text
    assert ".badge.b-muted" in css or ".b-muted" in css


def test_deps_rows_have_image_and_ownership(client):
    js = client.get("/assets/web.js").text
    # renderDeps no longer fetches doctor versions, and rows carry ownership + image
    deps_src = js[js.index("function renderDeps"):]
    deps_src = deps_src[:deps_src.index("async function renderMilvus")] if "async function renderMilvus" in deps_src else deps_src
    assert "api/doctor" not in deps_src
    assert "ownBadge(i.ownership)" in deps_src and "imageCell(i)" in deps_src and "delButton(i)" in deps_src


def test_install_milvus_form_has_dep_dropdowns(client):
    js = client.get("/assets/web.js").text
    for m in ['function depOptions', '__custom__', 'inst-etcd', 'inst-storage', 'inst-mqtype', 'inst-mq', 'function selVal']:
        assert m in js, m


def test_install_milvus_per_dep_isolation(client):
    js = client.get("/assets/web.js").text
    for m in ["inst-etcd-root", "inst-store-bucket", "inst-store-root", "inst-mq-prefix",
              "etcdRootPath", "minioBucket", "minioRootPath", "mqChanPrefix", "title="]:
        assert m in js, m
    assert "isolationPrefix" not in js and 'id="inst-iso"' not in js and 'id="iso-preview"' not in js


def test_milvus_card_pods_modal(client):
    js = client.get("/assets/web.js").text
    for m in ["function openModal", "function closeModal", "function openPods", "data-pods", "api/pods", "function ageOf"]:
        assert m in js, m
    assert ".modal" in client.get("/assets/web.css").text


def test_milvus_card_upgrade_modal(client):
    js = client.get("/assets/web.js").text
    for m in ["function openUpgrade", "function submitUpgrade", "data-upgrade", "api/upgrade", "up-force"]:
        assert m in js, m


def test_milvus_card_rollout_progress(client):
    js = client.get("/assets/web.js").text
    for m in ["function statusPill", "function openProgress", "data-progress", "api/pods?instance=", "prog-refresh", "升级中"]:
        assert m in js, m
    assert ".progbar" in client.get("/assets/web.css").text


def test_upgrade_apply_reframed(client):
    js = client.get("/assets/web.js").text
    up = js[js.index("function submitUpgrade"):]
    up = up[:up.index("function openUpgrade")] if "function openUpgrade" in up else up
    assert "已提交升级" in up and "openProgress" in up
    assert "pollInstall" not in up   # apply path no longer polls the (falsely-succeeding) task


def test_exec_log_panel_present(client):
    js = client.get("/assets/web.js").text
    assert "function logPanel" in js and "function pollTask" in js
    assert ".slice().reverse()" in js          # newest-on-top
    assert "logcmd" in js                       # command shown mono
    assert "function pollInstall" not in js     # old countdown poller removed


def test_upgrade_streams_then_handoff(client):
    js = client.get("/assets/web.js").text
    body = js.split("async function submitUpgrade", 1)[1].split("\nasync function ", 1)[0]
    assert "pollTask(" in body                 # streams the apply steps
    assert "已提交升级" in body and "查看进展" in body   # honest handoff kept
    assert "openProgress(" in body             # progress modal still reachable


def test_delete_has_dryrun_and_streams(client):
    js = client.get("/assets/web.js").text
    body = js.split("function openDelete", 1)[1].split("\nfunction ", 1)[0]
    assert "预演" in body                       # dry-run button
    assert "pollTask(" in body                  # confirm path streams steps
    assert "dry_run: true" in body              # dry-run request
    assert "刷新列表" in body                    # honest handoff kept


def test_config_ui_present(client):
    js = client.get("/assets/web.js").text
    assert "function openConfig" in js and "function collectKv" in js and "function configButton" in js
    assert "api/config" in js and "data-config" in js
    assert "cfg-view" in js                       # collapsed current-config view
    css = client.get("/assets/web.css").text
    assert ".cfg-view" in css


def test_log_panel_css_and_readme(client):
    css = client.get("/assets/web.css").text
    assert ".logpanel" in css and ".logcmd" in css and ".logrow" in css
    import pathlib
    readme = pathlib.Path(__file__).resolve().parents[2] / "README.md"
    text = readme.read_text(encoding="utf-8")
    assert "透明" in text and "黑盒" in text


def test_switch_mq_ui_present(client):
    js = client.get("/assets/web.js").text
    assert "function submitSwitchMq" in js and "function switchMqButton" in js
    assert "api/switch-mq" in js and "data-switch" in js
    # modal retired: openSwitchMq gone; card navigates to the dedicated page
    assert "function openSwitchMq" not in js
    assert "switch-mq.html?instance=" in js
    body = js.split("function submitSwitchMq", 1)[1].split("\nfunction ", 1)[0]
    assert "pollTask(" in body and "409" in body and "已提交 MQ 切换" in body   # stream + gate + honest handoff


def test_overview_resources_present(client):
    html = client.get("/index.html").text
    assert 'id="host-info"' in html and 'id="k8s-res"' in html
    js = client.get("/assets/web.js").text
    assert "api/resources" in js and "function resBar" in js
    assert "host-info" in js and "k8s-res" in js
    assert "setInterval" not in js               # no-polling: no timers anywhere
    css = client.get("/assets/web.css").text
    assert ".resbar" in css


def test_pod_logs_ui_present(client):
    js = client.get("/assets/web.js").text
    assert "function openLogs" in js
    assert "api/logs" in js and "data-log-pod" in js and "log-view" in js
    assert "最后 100 条" in js
    # openPods table wires a log button; still no timers anywhere (single-shot)
    assert "data-log-pod" in js.split("function openPods", 1)[1].split("function ", 1)[0]
    assert "setInterval" not in js
    css = client.get("/assets/web.css").text
    assert ".logview" in css


def test_pod_logs_newest_first(client):
    js = client.get("/assets/web.js").text
    body = js.split("async function openLogs", 1)[1].split("\nfunction ", 1)[0]
    assert ".reverse()" in body and "最新在上" in body   # newest-on-top


def test_config_view_pretty_yaml(client):
    js = client.get("/assets/web.js").text
    assert "function cfgView" in js
    assert "cfg-file" in js and "JSON.stringify(cur" not in js   # per-file <pre>, not JSON blob
    css = client.get("/assets/web.css").text
    assert ".cfg-fn" in css


def test_instance_resources_ui_present(client):
    js = client.get("/assets/web.js").text
    assert "function resLine" in js and "resLine(i.res)" in js   # card resource line
    body = js.split("async function openPods", 1)[1].split("\nfunction ", 1)[0]
    assert "CPU请求" in body and "合计" in body and "rmap" in body   # Pods resource columns + total
    assert "setInterval" not in js
    css = client.get("/assets/web.css").text
    assert ".restot" in css


def test_switch_mq_page_present(client):
    html = client.get("/switch-mq.html").text
    assert 'class="sw-tri"' in html and 'id="sw-target"' in html and 'id="sw-stepper"' in html
    assert 'tri-hub' in html and 'id="sw-ack"' in html and "renderSwitchMq()" in html
    assert 'id="sw-targets"' not in html          # old .sw-opt card list removed
    js = client.get("/assets/web.js").text
    assert "function renderSwitchMq" in js
    assert "api/switch-mq/targets" in js and "setStep" in js and "getElementById('sw-target')" in js
    assert "location.reload()" in js.split("function submitSwitchMq", 1)[1].split("\nfunction ", 1)[0]
    assert "setInterval" not in js


def test_switch_mq_topology_css(client):
    css = client.get("/assets/web.css").text
    assert ".sw-tri" in css and ".box-dark" in css and ".tri-hub" in css
    assert ".tri-arm.l" in css and ".tri-arm.r" in css   # solid=current(left), dashed=target(right)
    assert "prefers-reduced-motion" in css        # animation guarded


def test_switch_mq_grouped_dropdown(client):
    js = client.get("/assets/web.js").text
    body = js.split("async function renderSwitchMq", 1)[1].split("\nasync function ", 1)[0]
    assert "optgroup" in body and "data-inst" in body           # grouped by type, instances carried
    assert "无可复用实例" in body and "嵌入，无独立实例" in body   # empty-external + embedded copy
    assert "setInterval" not in js
