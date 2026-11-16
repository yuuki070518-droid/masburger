"""index.html 内のインラインスクリプトを変更したら実行する。"""
from pathlib import Path
import base64, hashlib, re
path = Path(__file__).resolve().parent.parent / 'index.html'
html = path.read_text()
html = re.sub(r'<meta http-equiv="Content-Security-Policy"[^>]*>\n?', '', html)
scripts = re.findall(r'<script>([\s\S]*?)</script>', html)
hashes = ' '.join("'sha256-" + base64.b64encode(hashlib.sha256(s.encode()).digest()).decode() + "'" for s in scripts)
policy = ("default-src 'self'; script-src 'self' " + hashes + "; "
          "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
          "font-src 'self' https://fonts.gstatic.com; img-src 'self' data: blob:; "
          "media-src 'self' data: blob:; worker-src 'self' blob:; "
          "connect-src 'self' data: blob: wss://broker.emqx.io:8084 wss://broker.hivemq.com:8884; "
          "object-src 'none'; base-uri 'none'; form-action 'self'")
html = html.replace('<meta charset="utf-8">', '<meta charset="utf-8">\n<meta http-equiv="Content-Security-Policy" content="' + policy + '">', 1)
path.write_text(html)
print('CSP updated:', len(scripts), 'inline scripts')
