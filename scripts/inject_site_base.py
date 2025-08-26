# scripts/inject_site_base.py
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SITE_BASE = "https://www.center-court.net"
INJECT_SNIPPET = f'<script>const SITE_BASE = "{SITE_BASE}";</script>\n'

def inject_into_file(p: Path):
    txt = p.read_text(encoding='utf-8')
    if 'const SITE_BASE' in txt:
        return False
    # prefer to inject just before closing </head>, else before first <script>
    if '</head>' in txt:
        txt = txt.replace('</head>', INJECT_SNIPPET + '</head>')
    else:
        # fallback: before first <script
        idx = txt.find('<script')
        if idx != -1:
            txt = txt[:idx] + INJECT_SNIPPET + txt[idx:]
        else:
            # else put at top
            txt = INJECT_SNIPPET + txt
    p.write_text(txt, encoding='utf-8')
    return True

count = 0
for d in (ROOT / 'maps_html', ROOT / 'docs'):
    if not d.exists(): continue
    for p in sorted(d.glob('*.html')):
        try:
            if inject_into_file(p):
                print("Injected SITE_BASE into", p)
                count += 1
        except Exception as e:
            print("Failed for", p, ":", e)
print("Total injected:", count)
