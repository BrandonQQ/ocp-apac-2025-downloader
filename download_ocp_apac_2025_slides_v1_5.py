#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OCP APAC Summit 2025 Slide Downloader v1.5
- 嗅探檔頭與 Content-Type，自動用正確副檔名（pdf / pptx 等）
- 若伺服器回 HTML（需登入、配額、錯頁），標記失敗並落地到 debug_html/
- 區塊切片逐連結解析（Slides / G-Drive / Dropbox / href 網域），降低漏抓
- 支援 Google Drive confirm、重試退避、--insecure、--dry-run、--save-html
"""
import argparse, csv, os, re, sys, time, zipfile, logging, html, hashlib
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup, NavigableString, Tag
from tqdm import tqdm

DEFAULT_URL = "https://www.opencompute.org/events/past-events/2025-ocp-apac-summit"

def build_logger(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=level)

def sanitize(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name or "")
    name = re.sub(r"\s+", " ", name).strip()
    return name[:200] if len(name) > 200 else name

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def short_hash(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()[:8]

def dropbox_direct(url: str) -> str:
    if "dropbox.com" not in url: return url
    if "?dl=0" in url: return url.replace("?dl=0","?dl=1")
    if "?dl=1" in url: return url
    return url + ("&dl=1" if ("?" in url) else "?dl=1")

def gdrive_direct(url: str) -> str:
    if "drive.google.com" not in url: return url
    m = re.search(r"/file/d/([a-zA-Z0-9_-]+)/", url) or re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    if m:
        fid = m.group(1)
        return f"https://drive.google.com/uc?export=download&id={fid}"
    return url

def fetch(url: str, session: requests.Session, verify_ssl: bool, save_html=False):
    r = session.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=60, verify=verify_ssl)
    r.raise_for_status()
    if save_html:
        with open("debug_page.html","w",encoding="utf-8") as f:
            f.write(r.text)
    return BeautifulSoup(r.text, "lxml")

def iter_track_headers(soup: BeautifulSoup):
    headers = soup.find_all(["h2","h3"])
    for h in headers:
        title = (h.get_text() or "").strip()
        if title and any(title.lower().startswith(prefix.lower()) for prefix in [
            "Keynotes","Adoption of OCP Recognized Equipment","AI Clusters","Chiplets",
            "Cooling Environments","Future Technologies Symposium","Networking",
            "Optical Communication Networks","Rack & Power","Server","Storage",
        ]):
            yield title, h

def slice_region(start_header: Tag):
    region_nodes = []
    node = start_header.next_sibling
    while node:
        if isinstance(node, Tag) and node.name in ("h2","h3"): break
        region_nodes.append(node)
        node = node.next_sibling
    return region_nodes

def is_slides_anchor(a: Tag):
    if not isinstance(a, Tag) or a.name != "a": return False
    txt = (a.get_text() or "").strip().lower()
    href = (a.get("href") or "").lower()
    if txt.startswith("slides") or "slide deck" in txt or txt == "pdf": return True
    if txt in ("g-drive","gdrive","g drive","dropbox"): return True
    if "drive.google.com" in href or "dropbox.com" in href: return True
    return False

def find_slides_anchors(region_nodes):
    anchors = []
    for n in region_nodes:
        if isinstance(n, Tag):
            for a in n.find_all("a"):
                if is_slides_anchor(a): anchors.append(a)
    return anchors

NEG_TITLE = re.compile(r"^(video|slides|slide deck|pdf|back to the top)$", re.I)

def nearest_title_for_anchor(a: Tag):
    row = a
    for _ in range(6):
        if not row or not isinstance(row, Tag): break
        if row.name in ("li","p","tr","div","section","article"): break
        row = row.parent
    if isinstance(row, Tag):
        for sel in [["strong","b","h4","h5"], ["em","i"]]:
            cand = row.find(sel)
            if cand:
                t = (cand.get_text(" ", strip=True) or "").strip()
                if t and not NEG_TITLE.match(t) and len(t)>=6: return t
        sib = row.previous_sibling; steps=0
        while sib and steps<8:
            steps+=1
            t = (sib.get_text(" ", strip=True) if isinstance(sib,Tag) else (sib.strip() if sib else "")) or ""
            if t and not NEG_TITLE.match(t) and len(t)>=6: return t
            sib = sib.previous_sibling
    for prev in a.find_all_previous(string=False, limit=20):
        t = (prev.get_text(" ", strip=True) or "").strip()
        if t and not NEG_TITLE.match(t) and len(t)>=6: return t
    return "Untitled"

def collect_items_for_track(header: Tag):
    region = slice_region(header); anchors = find_slides_anchors(region)
    items, seen = [], set()
    for a in anchors:
        href = a.get("href") or ""
        if not href: continue
        title = sanitize(nearest_title_for_anchor(a))
        key = (title, href)
        if key in seen: continue
        seen.add(key)
        row = a
        for _ in range(6):
            if row and isinstance(row, Tag) and row.name in ("li","p","tr","div","section","article"): break
            row = row.parent
        gdrive = dropbox = ""
        if isinstance(row, Tag):
            for aa in row.find_all("a"):
                h = aa.get("href") or ""
                if "drive.google.com" in h: gdrive = h
                if "dropbox.com" in h: dropbox = h
        if not gdrive and "drive.google.com" in href: gdrive = href
        if not dropbox and "dropbox.com" in href: dropbox = href
        items.append({"title": title, "gdrive": gdrive, "dropbox": dropbox})
    return items

def sniff_extension_and_validate(head_bytes: bytes, content_type: str):
    if head_bytes.startswith(b"%PDF-"): return ".pdf", True
    if head_bytes.startswith(b"PK\x03\x04"):
        if content_type and ("presentationml" in content_type or "officedocument" in content_type): return ".pptx", True
        return ".zip", True
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct=="application/pdf": return ".pdf", True
        if ct=="application/vnd.openxmlformats-officedocument.presentationml.presentation": return ".pptx", True
        if ct=="application/vnd.ms-powerpoint": return ".ppt", True
        if ct.startswith("text/html"): return ".html", False
    return ".bin", False

def resolve_filename_from_headers(resp, fallback_name, ext_from_sniff):
    cd = resp.headers.get("Content-Disposition","")
    fn = None
    for key in ["filename*","filename"]:
        if key in cd:
            m = re.search(r'%s=([^;]+)' % key, cd, re.I)
            if m:
                val = m.group(1).strip().strip('"')
                val = val.split("''")[-1]
                fn = sanitize(val); break
    if not fn: fn = sanitize(fallback_name)
    root, ext = os.path.splitext(fn)
    if not ext: fn = root + ext_from_sniff
    return fn

def save_html_debug(content, debug_dir, base_name):
    ensure_dir(debug_dir); path = os.path.join(debug_dir, base_name + ".html")
    with open(path,"wb") as f: f.write(content)
    return path

def google_drive_confirm_url(html_text):
    m = re.search(r'href="([^"]+confirm=[^"]+)"', html_text)
    if m: return html.unescape(m.group(1))
    m2 = re.search(r'name="confirm" value="([^"]+)"', html_text)
    if m2:
        token = m2.group(1); m3 = re.search(r'form action="([^"]+)"', html_text)
        if m3:
            action = html.unescape(m3.group(1)); join = urljoin("https://drive.google.com/", action)
            return join + ("&" if "?" in join else "?") + "confirm=" + token
    return None

def google_drive_download(session, url, out_dir, base_name, verify_ssl, max_retries=3):
    debug_dir = os.path.join(out_dir, "_debug_html"); last_err = "download_failed"
    for attempt in range(max_retries):
        try:
            r = session.get(url, stream=True, timeout=90, verify=verify_ssl)
            ct = r.headers.get("Content-Type","").lower()
            head = r.raw.read(16384, decode_content=True) if hasattr(r.raw,"read") else r.content[:16384]
            ext, okbin = sniff_extension_and_validate(head, ct)
            if not okbin:
                text = r.text; c2 = google_drive_confirm_url(text)
                if c2:
                    r = session.get(urljoin("https://drive.google.com/", c2), stream=True, timeout=90, verify=verify_ssl)
                    ct = r.headers.get("Content-Type","").lower()
                    head = r.raw.read(16384, decode_content=True) if hasattr(r.raw,"read") else r.content[:16384]
                    ext, okbin = sniff_extension_and_validate(head, ct)
                if not okbin:
                    save_html_debug(text.encode("utf-8","ignore"), debug_dir, short_hash(url)+"_gdrive")
                    last_err = "html_not_file"; time.sleep(2**attempt); continue
            fname = resolve_filename_from_headers(r, base_name, ext); path = os.path.join(out_dir, fname)
            ensure_dir(out_dir)
            with open(path,"wb") as f:
                if head: f.write(head)
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk: f.write(chunk)
            if os.path.getsize(path) > 0: return True, "ok", path, fname, ct
            last_err = "0-byte"
        except Exception as e:
            last_err = str(e); time.sleep(2**attempt)
    return False, last_err, "", "", ""

def generic_download(session, url, out_dir, base_name, verify_ssl, max_retries=3):
    debug_dir = os.path.join(out_dir, "_debug_html"); last_err = "download_failed"
    for attempt in range(max_retries):
        try:
            r = session.get(url, stream=True, timeout=90, verify=verify_ssl, allow_redirects=True)
            ct = r.headers.get("Content-Type","").lower()
            content_iter = r.iter_content(chunk_size=8192)
            first = next(content_iter, b""); head = first
            ext, okbin = sniff_extension_and_validate(head, ct)
            if not okbin:
                html_bytes = first + b"".join(list(content_iter))
                save_html_debug(html_bytes, debug_dir, short_hash(url)+"_generic")
                last_err = "html_not_file"; time.sleep(2**attempt); continue
            fname = resolve_filename_from_headers(r, base_name, ext); path = os.path.join(out_dir, fname)
            ensure_dir(out_dir)
            with open(path,"wb") as f:
                if head: f.write(head)
                for chunk in content_iter:
                    if chunk: f.write(chunk)
            if os.path.getsize(path) > 0: return True, "ok", path, fname, ct
            last_err = "0-byte"
        except Exception as e:
            last_err = str(e); time.sleep(2**attempt)
    return False, last_err, "", "", ""

def main():
    ap = argparse.ArgumentParser(description="OCP APAC Summit 2025 slide downloader v1.5")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--out", default="OCP_APAC_2025_Slides")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--prefer", choices=["dropbox","gdrive"], default="dropbox")
    ap.add_argument("--group-by", choices=["track","speaker","company"], default="track")
    ap.add_argument("--insecure", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--track-filter", default="")
    ap.add_argument("--save-html", action="store_true")
    ap.add_argument("--filename-template", default="{index:02d} - {title}")
    args = ap.parse_args()

    build_logger(args.verbose)
    verify_ssl = not args.insecure
    session = requests.Session()
    soup = fetch(args.url, session, verify_ssl, save_html=args.save_html)

    ensure_dir(args.out)
    manifest_path = "manifest.csv"
    parsed_path = "parsed_items.csv"

    all_items = []
    for track_title, header in iter_track_headers(soup):
        if args.track_filter and args.track_filter.lower() not in track_title.lower(): continue
        for it in collect_items_for_track(header):
            all_items.append([track_title, it["title"], it.get("gdrive",""), it.get("dropbox","")])

    with open(parsed_path,"w",newline="",encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["track","title","gdrive","dropbox"]); w.writerows(all_items)
    if args.dry_run:
        print(f"Parsed {len(all_items)} items. See {parsed_path}"); return 0

    per_track_counter = {}; rows = []; tasks = []

    def build_base_filename(track, title):
        idx = per_track_counter.get(track, 0) + 1
        per_track_counter[track] = idx
        return sanitize(args.filename_template.format(index=idx, title=title, track=track))

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for track, title, gdr, dbx in all_items:
            folder = sanitize(track.split("(")[0].strip())
            out_dir = os.path.join(args.out, folder); ensure_dir(out_dir)
            chosen = alt = ""; source = ""
            if args.prefer == "dropbox" and dbx:
                chosen = dropbox_direct(dbx); alt = gdrive_direct(gdr) if gdr else ""; source = "dropbox"
            elif gdr:
                chosen = gdrive_direct(gdr); alt = dropbox_direct(dbx) if dbx else ""; source = "gdrive"
            elif dbx:
                chosen = dropbox_direct(dbx); source = "dropbox"
            else:
                rows.append([track, title, "", "", "", "", "", "no_link", "", ""]); continue

            base = build_base_filename(track, title)

            def task(track=track, title=title, chosen=chosen, alt=alt, out_dir=out_dir, base=base):
                if "drive.google.com" in chosen:
                    ok, note, path, fname, ct = google_drive_download(session, chosen, out_dir, base, verify_ssl=verify_ssl, max_retries=3)
                else:
                    ok, note, path, fname, ct = generic_download(session, chosen, out_dir, base, verify_ssl=verify_ssl, max_retries=3)
                used = chosen
                if not ok and alt:
                    if "drive.google.com" in alt:
                        ok2, note2, path2, fname2, ct2 = google_drive_download(session, alt, out_dir, base, verify_ssl=verify_ssl, max_retries=3)
                    else:
                        ok2, note2, path2, fname2, ct2 = generic_download(session, alt, out_dir, base, verify_ssl=verify_ssl, max_retries=3)
                    used = alt if ok2 else chosen
                    ok, note, path, fname, ct = (ok2, note2, path2, fname2, ct2) if ok2 else (ok, note, path, fname, ct)
                status = "ok" if ok else "failed"
                return [track, title, chosen, alt, used, fname, path, status, note, ct]

            tasks.append(ex.submit(task))

        for fut in tqdm(as_completed(tasks), total=len(tasks), desc="Downloading"):
            rows.append(fut.result())

    with open(manifest_path,"w",newline="",encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["track","title","source","source_alt","chosen_used","saved_name","saved_path","status","notes","content_type"]); w.writerows(rows)

    zip_name = args.out.rstrip("/\\") + ".zip"
    with zipfile.ZipFile(zip_name,"w",compression=zipfile.ZIP_DEFLATED) as z:
        for root,_,files in os.walk(args.out):
            for fn in files:
                fp = os.path.join(root,fn)
                z.write(fp, arcname=os.path.relpath(fp, args.out))
    print(f"Done. See {args.out}/, {zip_name}, manifest.csv, parsed_items.csv")
    return 0

if __name__ == "__main__":
    sys.exit(main())
