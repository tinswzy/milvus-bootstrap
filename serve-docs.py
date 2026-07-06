#!/usr/bin/env python3
"""Zero-dependency static docs server.

Serves the repo so you can browse the prototype HTML pages and renders any
.md file (spec/plan docs) as GitHub-styled HTML in the browser.

Usage:  python3 serve-docs.py [PORT]   (default 8899, binds 0.0.0.0)
Open:   http://<this-machine-ip>:<port>/
Stop:   Ctrl-C  (or: kill the process / fuser -k <port>/tcp)
"""
from __future__ import annotations

import html
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8899

# Landing page: hand-picked entry points so you don't have to guess paths.
LINKS = [
    ("阶段一 · 实施计划 (hub)", "/prototype/index.html"),
    ("阶段一 · 计划 + 逐步验收清单", "/prototype/phase1-plan.html"),
    ("④ milvus 连 kafka (live)", "/prototype/phase1-step4.html"),
    ("⑤ milvus 连 pulsar (live)", "/prototype/phase1-step5.html"),
    ("⑧ milvus 升级", "/prototype/phase1-step8.html"),
    ("mb doctor · 环境/版本/兼容 (设计页)", "/prototype/mb-doctor.html"),
    ("spec · mb doctor 设计", "/docs/superpowers/specs/2026-07-01-mb-doctor-design.md"),
    ("plan · mb doctor 实现", "/docs/superpowers/plans/2026-07-01-mb-doctor.md"),
    ("plan · 版本约束增量实现", "/docs/superpowers/plans/2026-07-02-mb-doctor-constraints.md"),
]

_MD_SHELL = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/github-markdown-css@5/github-markdown.min.css">
<style>body{{margin:0;background:#0d1117}}.markdown-body{{box-sizing:border-box;max-width:900px;
margin:0 auto;padding:32px 20px;color:#e6edf3}}a.back{{display:inline-block;margin:14px 20px;color:#58a6ff}}</style>
</head><body>
<a class="back" href="/">← 文档首页</a>
<article id="c" class="markdown-body">加载中…</article>
<script src="https://cdn.jsdelivr.net/npm/marked@12/marked.min.js"></script>
<script>
fetch(location.pathname + "?raw=1").then(r=>r.text()).then(md=>{{
  marked.setOptions({{gfm:true, breaks:false}});
  document.getElementById("c").innerHTML = marked.parse(md);
}}).catch(e=>{{document.getElementById("c").textContent = "加载失败: " + e;}});
</script></body></html>"""


def _index_html() -> bytes:
    rows = "\n".join(
        f'<li><a href="{html.escape(href)}">{html.escape(label)}</a></li>'
        for label, href in LINKS
    )
    page = f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>milvus-admin-webui 文档</title>
<style>body{{font-family:system-ui,'IBM Plex Sans',sans-serif;background:#0d1117;color:#e6edf3;
max-width:760px;margin:0 auto;padding:40px 22px}}h1{{font-size:20px}}li{{margin:10px 0}}
a{{color:#58a6ff;text-decoration:none;font-size:15px}}a:hover{{text-decoration:underline}}
.hint{{color:#8b949e;font-size:13px;margin:18px 0}}</style></head><body>
<h1>milvus-admin-webui · 文档 &amp; 步骤进展</h1>
<p class="hint">HTML 页直接渲染；.md 文档经 marked.js 渲染。也可直接浏览任意路径（如 /prototype/、/docs/）。</p>
<ul>{rows}</ul></body></html>"""
    return page.encode("utf-8")


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if path in ("/", "/index"):
            body = _index_html()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        # A .md request without ?raw=1 -> return the rendering shell (HTML).
        if path.endswith(".md") and "raw=1" not in (parsed.query or ""):
            body = _MD_SHELL.format(title=html.escape(path.rsplit("/", 1)[-1])).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        # ?raw=1 (or anything else) -> serve the file as-is (md as text/plain).
        super().do_GET()

    def guess_type(self, path):
        if path.endswith(".md"):
            return "text/plain; charset=utf-8"
        return super().guess_type(path)


if __name__ == "__main__":
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Serving {__import__('os').getcwd()} on http://0.0.0.0:{PORT}  (Ctrl-C to stop)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()
