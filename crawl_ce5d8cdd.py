# -*- coding: utf-8 -*-
"""
crawl_ce5d8cdd.py —— ce5d8cdd 系列站点采集器（优化版）
- 外层温和并发 + 内层章节并发
- 失败队列重试（退避+抖动；带 UA 轮换）
- 代理/UA 池（每请求随机）
- 封面本地化（使用 referer），失败自动生成 300x400 封面
- YAML 分类映射加载：--category-map
- 【新增】优先解析分页条（<ul class="pagination" id="pagelink">...<a class="next">>）
- 【增强】翻页兜底支持 /{n}.html（例：/list/10/3.html）
- 【增强】书籍链接判定放宽：/book/{id}/、/book/{id}.html、/book/{id}/index.html
"""

import argparse, sys, re, traceback, threading as _t, time, random
from pathlib import Path
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from parsel import Selector

# 导入共用组件
sys.path.append(str(Path(__file__).resolve().parents[1]))
from crawl.common import (  # type: ignore
    ROOT, ensure_dir, http_session, fetch_text, save_json,
    localize_cover, ProxyPool, generate_cover_image
)
from site_ce5d8cdd import (  # type: ignore
    parse_book_meta, parse_toc, parse_chapter_content
)

_THREAD_LOCAL = _t.local()

try:
    import brotli as _br  # noqa
    _ACCEPT_ENCODING = "gzip, deflate, br"
except Exception:
    _ACCEPT_ENCODING = "gzip, deflate"

_BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/117 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": _ACCEPT_ENCODING,
    "Referer": "",
    "Connection": "keep-alive",
}

def _get_session(rate: float, pool: int, base: str, proxy_pool=None, ua_pool=None):
    s = getattr(_THREAD_LOCAL, "s", None)
    if s is None:
        headers = dict(_BASE_HEADERS); headers["Referer"] = base.rstrip("/") + "/"
        try:
            s = http_session(rate=rate, pool_maxsize=max(2, pool), base_headers=headers,
                             ua_pool=ua_pool, proxy_pool=proxy_pool)
        except TypeError:
            try: s = http_session(rate=rate)
            except TypeError: s = http_session()
            try: s.headers.update(headers)
            except Exception: pass
        _THREAD_LOCAL.s = s
    return s

def _same_site(base_netloc: str, target_netloc: str) -> bool:
    if not target_netloc:
        return True
    if target_netloc == base_netloc:
        return True
    base_host = base_netloc.split(":", 1)[0]
    host = target_netloc.split(":", 1)[0]
    return host == base_host or host.endswith("." + base_host)

def _looks_like_book(u: str, base_netloc: str) -> bool:
    """放宽为 /book/{id}/ 、/book/{id}.html、/book/{id}/index.html"""
    try:
        p = urlparse(u)
        if not _same_site(base_netloc, p.netloc):
            return False
        return bool(re.search(r"^/book/\d+(?:/|/index\.html|\.html)$", p.path, flags=re.I))
    except Exception:
        return False

def list_books_in_category(html: str, base_url: str):
    sel = Selector(text=html)
    selectors = [
        'a[href*="/book/"]::attr(href)',
        '.booklist a::attr(href)',
        '.novellist a::attr(href)',
        '#newscontent a::attr(href)',
        '.ranklist a::attr(href)',
        '.toplist a::attr(href)',
        '.topbox a::attr(href)',
        '.list a::attr(href)',
        'ul li a::attr(href)',
    ]
    hrefs = []
    for css in selectors:
        try: hrefs += sel.css(css).getall() or []
        except Exception: pass
    if not hrefs:
        hrefs = re.findall(r'href=[\'"]([^\'"]*/book/\d+[^\'"]*)[\'"]', html, flags=re.I) or \
                re.findall(r'(/book/\d+/\S*)', html, flags=re.I)
    base_netloc = urlparse(base_url).netloc
    out, seen = [], set()
    for h in hrefs:
        u = urljoin(base_url, h)
        if _looks_like_book(u, base_netloc) and u not in seen:
            out.append(u); seen.add(u)
    return out

def _find_next_page(html: str, page_url: str) -> Optional[str]:
    """
    解析分页条“下一页”：
    1) <ul class="pagination" id="pagelink"> ... <a class="next"> &gt; </a>
    2) a[rel="next"]
    3) li.active 的后一项
    4) 文本为 ">" 的链接（限定到 pagination）
    """
    sel = Selector(text=html)

    href = sel.css('ul.pagination#pagelink a.next::attr(href)').get()
    if href:
        return urljoin(page_url, href.strip())

    href = sel.css('a[rel="next"]::attr(href)').get()
    if href:
        return urljoin(page_url, href.strip())

    href = sel.css('ul.pagination#pagelink li.active + li a::attr(href)').get()
    if href:
        return urljoin(page_url, href.strip())

    href = sel.xpath("//ul[contains(@class,'pagination')][@id='pagelink']//a[normalize-space(text())='>']/@href").get()
    if href:
        return urljoin(page_url, href.strip())

    href = sel.xpath("//ul[contains(@class,'pagination')]//a[normalize-space(text())='>']/@href").get()
    if href:
        return urljoin(page_url, href.strip())

    return None

def _category_root(cat_url: str) -> str:
    """
    归一化分类根路径：
      /list/10/2.html、/list/10/page/2、/list/10/index_2.html  ->  /list/10
    也适用于完整 URL。
    """
    u = cat_url.rstrip('/')
    u = re.sub(r"/index_\d+\.html$", "", u, flags=re.I)
    u = re.sub(r"/(?:page/)?\d+(?:\.html)?$", "", u, flags=re.I)
    return u

def try_paged_category(s, cat_url: str, max_books: int = 0):
    """
    模板穷举兜底：当分页条解析失败或不足量时启用。
    支持 /{n}.html（例：/list/10/3.html）
    """
    base_root = _category_root(cat_url)

    patterns = [
        base_root + "/{n}.html",        # ✅ /list/10/3.html
        base_root + "/index_{n}.html",  #    /list/10/index_3.html
        base_root + "/page/{n}",        #    /list/10/page/3
        base_root + "?page={n}",        #    /list/10?page=3
    ]

    found, seen = [], set(); n = 1
    while True:
        n += 1  # 从 2 开始，避免重复第一页
        hit = 0
        for fmt in patterns:
            url = fmt.format(n=n)
            try:
                html = fetch_text(s, url)
            except Exception:
                continue
            items = list_books_in_category(html, cat_url)
            for it in items:
                if it not in seen:
                    found.append(it); seen.add(it); hit += 1
                    if max_books and len(found) >= max_books:
                        return found[:max_books]
        if hit == 0:
            return found

def _looks_like_error_page(html: str) -> bool:
    if not html:
        return True
    return bool(re.search(
        r"(出现错误|出错|错误|访问过于频繁|频繁|验证|安全|禁止访问|Access Denied|Just a moment)",
        html, flags=re.I
    ))

def _fetch_with_retry(s, url: str, ua_pool=None, tries: int = 3, base_sleep: float = 1.2):
    """抓到错误页就退避重试，若提供 ua_pool 则轮换 UA。"""
    last_html = ""
    for i in range(tries):
        try:
            html = fetch_text(s, url)
            last_html = html
        except Exception:
            html = ""
        if html and not _looks_like_error_page(html):
            return html
        time.sleep(base_sleep * (i + 1) + random.uniform(0.2, 0.9))
        if ua_pool:
            try:
                s.headers["User-Agent"] = random.choice(ua_pool)
            except Exception:
                pass
    return last_html

def save_chapter_text(book_dir: Path, no: int, slug_part: str, paras):
    pad = f"{no:04d}"
    out = book_dir / "chapters" / f"{pad}-{(slug_part or pad)}.txt"
    ensure_dir(out.parent)
    out.write_text("\n".join(paras), encoding="utf-8")
    return out

def write_chapters_index(book_dir: Path, toc: list):
    if not toc: return
    chap_list, source_map = [], {}
    for idx, ch in enumerate(toc, 1):
        try: no = int(ch.get("no") or idx)
        except Exception: no = idx
        title = (ch.get("title") or f"第{no}章").strip() or f"第{no}章"
        slug4 = f"{no:04d}"
        chap_list.append({"no": no, "title": title, "slug": slug4})
        href = ch.get("href") or ""
        if href: source_map[slug4] = {"href": href, "site_id": ch.get("site_id", 0)}
    save_json(book_dir / "chapters.json", {"list": chap_list, "source_map": source_map})

# ---- YAML 可控的目录段 -> 分类 hint（可被 --category-map 覆盖）----
CATEGORY_HINT_MAP = {
    "dushi": "dushi", "xuanhuan": "xuanhuan", "xianxia": "xianxia", "wuxia": "xianxia",
    "yanqing": "yanqing", "mm": "yanqing", "lishi": "lishi", "kehuan": "kehuan",
    "wangyou": "wangyou", "xuanyi": "xuanyi",
}

def _hint_from_category_path(cat_path: str) -> str:
    seg = (cat_path or "").strip("/").split("/", 1)[0].lower()
    return CATEGORY_HINT_MAP.get(seg, "")

def process_book(book_url: str, base: str, args, proxy_pool=None, ua_pool=None):
    s0 = _get_session(args.rate, args.pool, base, proxy_pool=proxy_pool, ua_pool=ua_pool)

    # 1) 详情页（带重试）
    b_html = _fetch_with_retry(s0, book_url, ua_pool=ua_pool, tries=3, base_sleep=1.2)

    cat_hint = _hint_from_category_path(args.category or "")
    meta = parse_book_meta(b_html, book_url, category_hint=cat_hint if cat_hint else "")

    if args.local_covers and meta.get("cover") is not None:
        try:
            meta["cover"] = localize_cover(
                s0, meta.get("cover",""), meta["slug"],
                referer=book_url,
                title=meta.get("title",""),
                author=meta.get("author",""),
                category_id=meta.get("category_id","")
            )
        except Exception as e:
            print("[warn] cover localize failed, using generated:", e)
            meta["cover"] = generate_cover_image(meta["slug"], meta.get("title",""), meta.get("author",""), meta.get("category_id",""))

    book_dir = ROOT / "data" / "novels" / meta["slug"]
    ensure_dir(book_dir)

    # 2) 目录（失败则尝试 index.html，再解析）
    toc = parse_toc(b_html, book_url)
    if not toc:
        alt = urljoin(book_url, "index.html")
        try:
            toc_html = _fetch_with_retry(s0, alt, ua_pool=ua_pool, tries=2, base_sleep=1.0)
            if toc_html:
                toc = parse_toc(toc_html, book_url)
        except Exception:
            pass

    save_json(book_dir / "meta.json", meta)
    save_json(book_dir / "toc.json", toc)
    write_chapters_index(book_dir, toc)

    if not args.fetch_chapters or not toc:
        print("[chapters] skipped:", meta.get("title"))
        return 1

    existing_prefix = set()
    chap_dir = book_dir / "chapters"; ensure_dir(chap_dir)
    if not args.overwrite:
        for p in chap_dir.glob("*.txt"):
            try:
                if p.stat().st_size > 10:
                    existing_prefix.add(p.name.split("-")[0])
            except Exception:
                pass

    fails = []
    def fetch_one(ch):
        try:
            href = (ch.get("href") or "").strip().lower()
            if not href or href.startswith(("javascript:", "#", "mailto:", "tel:")) or "void(0" in href:
                return (ch.get("no", 0), None, href)
            url = urljoin(book_url, href)
            s_local = _get_session(args.rate, args.pool, base, proxy_pool=proxy_pool, ua_pool=ua_pool)
            html = _fetch_with_retry(s_local, url, ua_pool=ua_pool, tries=3, base_sleep=0.8)
            paras = parse_chapter_content(html)
            return (ch.get("no", 0), paras, href)
        except Exception:
            return (ch.get("no", 0), None, ch.get("href"))

    inner_workers = max(8, min(args.threads, 24))
    with ThreadPoolExecutor(max_workers=inner_workers) as ex:
        futs = []
        for ch in toc:
            pad = f"{int(ch.get('no', 0)):04d}"
            if not args.overwrite and pad in existing_prefix:
                continue
            futs.append(ex.submit(fetch_one, ch))
        for fu in as_completed(futs):
            no, paras, href = fu.result()
            if not no or not paras:
                if href: fails.append((int(no or 0), href))
                continue
            out = save_chapter_text(book_dir, int(no), f"{int(no):04d}", paras)
            print("[chapter] saved:", out)

    for rd in range(max(0, args.retry_rounds)):
        if not fails: break
        wait = args.retry_sleep * (1.5 ** rd) + random.uniform(0, 1.5)
        print(f"[retry] {meta.get('title','')}: round {rd+1}/{args.retry_rounds}, size={len(fails)}, sleep={wait:.1f}s")
        time.sleep(wait)
        f2 = []
        with ThreadPoolExecutor(max_workers=inner_workers) as ex:
            futs = [ex.submit(fetch_one, {"no": no, "href": href}) for (no, href) in fails]
            for fu in as_completed(futs):
                no, paras, href = fu.result()
                if paras:
                    out = save_chapter_text(book_dir, int(no), f"{int(no):04d}", paras)
                    print("[retry] saved:", out)
                else:
                    f2.append((int(no or 0), href))
        fails = f2

    if fails:
        print(f"[warn] still failed chapters: {len(fails)} (book={meta.get('title','')})")
    return 1

def _load_lines_or_csv(val: str):
    p = Path(val)
    if p.exists():
        return [x.strip() for x in p.read_text(encoding="utf-8").splitlines() if x.strip()]
    return [x.strip() for x in val.split(",") if x.strip()]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="站点基域，如 https://www.ce5d8cdd.icu 或 https://www.dd4bd5.xyz")
    ap.add_argument("--category", help="分类路径或完整URL，如 /xuanhuan/ 或 https://.../xuanhuan/")
    ap.add_argument("--rate", type=float, default=0.08)
    ap.add_argument("--max-books", type=int, default=0)
    ap.add_argument("--threads", type=int, default=24)
    ap.add_argument("--pool", type=int, default=256)
    ap.add_argument("--fetch-chapters", dest="fetch_chapters", action="store_true", default=True)
    ap.add_argument("--no-fetch-chapters", dest="fetch_chapters", action="store_false")
    ap.add_argument("--local-covers", type=int, default=1)
    ap.add_argument("--overwrite", type=int, default=0)
    # 代理/UA/重试参数
    ap.add_argument("--proxies", help="逗号分隔或文件路径（每行一个）")
    ap.add_argument("--ua-file", help="UA 列表文件路径（每行一个）")
    ap.add_argument("--retry-rounds", type=int, default=2, help="失败队列重试轮数（默认2轮）")
    ap.add_argument("--retry-sleep", type=float, default=4.0, help="两轮重试之间的基础等待秒")
    # 分类映射
    ap.add_argument("--category-map", help="分类映射文件（YAML/JSON），含 category_rules/keyword_rules/default_id 与可选 category_hint_map")
    args = ap.parse_args()

    # 加载 YAML 分类映射（含目录段 hint 覆盖）
    global CATEGORY_HINT_MAP
    if args.category_map:
        try:
            import json, yaml
            p = Path(args.category_map)
            data = {}
            if p.suffix.lower() in (".yml", ".yaml"):
                data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
            else:
                data = json.loads(p.read_text(encoding="utf-8"))
            from crawl.common import set_category_map  # type: ignore
            set_category_map(data.get("category_map") or data)
            hint_map = data.get("category_hint_map") or (data.get("category_map") or {}).get("category_hint_map")
            if isinstance(hint_map, dict) and hint_map:
                CATEGORY_HINT_MAP = {k.lower(): str(v) for k, v in hint_map.items()}
        except Exception as e:
            print("[warn] 加载分类映射失败：", e)

    base = args.base.rstrip('/')
    cat = args.category or '/xuanhuan/'
    cat_url = cat if cat.startswith('http') else base + ('' if cat.startswith('/') else '/') + cat

    proxy_pool = None
    ua_pool = None
    if args.proxies:
        proxies = _load_lines_or_csv(args.proxies)
        if proxies:
            proxy_pool = ProxyPool(proxies, cooldown=60, fail_threshold=2)
    if args.ua_file:
        try:
            ua_pool = [x.strip() for x in Path(args.ua_file).read_text(encoding="utf-8").splitlines() if x.strip()]
        except Exception:
            ua_pool = None

    s = _get_session(args.rate, args.pool, base, proxy_pool=proxy_pool, ua_pool=ua_pool)

    # ====== 分类页：第一页 ======
    found = []
    try:
        html = fetch_text(s, cat_url)
    except Exception as e:
        print('[category] fetch failed:', e)
        html = ''
    books = list_books_in_category(html, cat_url)
    for u in books:
        if u not in found:
            found.append(u)
        if args.max_books and len(found) >= args.max_books:
            found = found[:args.max_books]; books = []; break

    # ====== 优先：按分页条“下一页”链式翻页 ======
    visited_pages = {cat_url}
    next_url = _find_next_page(html, cat_url)
    while next_url and next_url not in visited_pages and (not args.max_books or len(found) < args.max_books):
        visited_pages.add(next_url)
        try:
            nh = fetch_text(s, next_url)
        except Exception:
            break
        more = list_books_in_category(nh, next_url)
        for u in more:
            if u not in found:
                found.append(u)
                if args.max_books and len(found) >= args.max_books:
                    break
        if args.max_books and len(found) >= args.max_books:
            break
        next_url = _find_next_page(nh, next_url)

    # ====== 仍不足：回退 URL 模板穷举兜底 ======
    if not args.max_books or len(found) < args.max_books:
        more = try_paged_category(s, cat_url, max_books=(args.max_books - len(found)) if args.max_books else 0)
        for u in more:
            if u not in found:
                found.append(u)
            if args.max_books and len(found) >= args.max_books:
                found = found[:args.max_books]; break

    print(f"[category] collected books: {len(found)}")

    # ====== 并发抓取每本书 ======
    book_concurrency = max(4, min(12, (args.threads // 2) or 4))
    print(f"[books] start concurrent: {book_concurrency}")
    ok = 0
    with ThreadPoolExecutor(max_workers=book_concurrency) as ex:
        futs = [ex.submit(process_book, u, base, args, proxy_pool, ua_pool) for u in found]
        for fu in as_completed(futs):
            try:
                ok += fu.result()
            except KeyboardInterrupt:
                print("[abort] user interrupted"); raise
            except Exception as e:
                print("[book] failed:", e)
                traceback.print_exc()

    print(f"[done] books: {ok}/{len(found)}")

if __name__ == "__main__":
    main()