# -*- coding: utf-8 -*-
"""
site_ce5d8cdd.py
- 为 ce5d8cdd 站点系列提供解析函数（meta 优先 + YAML 分类映射 + 安全兜底）：
  * parse_book_meta(html, book_url, category_hint="") -> dict
  * parse_toc(html, book_url) -> list[{"no","title","href","site_id"}]   # no 为顺序编号
  * parse_chapter_content(html) -> list[str]
"""

import re, hashlib, datetime
from urllib.parse import urljoin, urlparse
from parsel import Selector

from crawl.common import (  # type: ignore
    map_category_name, match_category_by_keywords, get_default_category_id
)

def _clean_text(x: str) -> str:
    if not x: return ""
    x = re.sub(r"<[^>]+>", "", x)
    x = re.sub(r"&nbsp;?", " ", x)
    x = re.sub(r"\s+", " ", x)
    return x.strip()

def _first(sel: Selector, css: str, default=""):
    try:
        v = sel.css(css).get()
        return v if v is not None else default
    except Exception:
        return default

def _first_txt(sel: Selector, css: str, default=""):
    return _clean_text(_first(sel, css, default))

def _find_number(text: str, default=0):
    if not text: return default
    m = re.search(r"(\d[\d,\.]*)", text)
    if not m: return default
    t = m.group(1).replace(",", "")
    try:
        return int(float(t))
    except Exception:
        return default

def _iso_now():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _stable_slug(seed: str, n=5) -> str:
    h = hashlib.md5(seed.encode("utf-8")).hexdigest()
    num = int(h[:10], 16)
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    out = []
    for _ in range(n):
        out.append(alphabet[num % len(alphabet)])
        num //= len(alphabet)
    return "".join(out)

def _extract_site_book_id(book_url: str) -> str:
    m = re.search(r"/book/(\d+)(?:/|\.html)?", book_url)
    return m.group(1) if m else ""

def parse_book_meta(html: str, book_url: str, category_hint: str = "") -> dict:
    """
    优先从 <meta property="og:*"> 取书籍信息；无则回退到面包屑/信息区。
    分类：YAML 的 category_rules > keyword_rules > category_hint > default_id
    """
    sel = Selector(text=html)

    def _norm_status(s: str) -> str:
        s = (s or "").strip()
        if not s: return ""
        if "完" in s: return "已完结"
        if "连载" in s: return "连载中"
        return s

    title = (
        _first_txt(sel, 'meta[property="og:novel:book_name"]::attr(content)')
        or _first_txt(sel, 'meta[property="og:title"]::attr(content)')
        or _first_txt(sel, "h1::text")
        or _first_txt(sel, ".book .info h1::text")
        or _first_txt(sel, ".book-info h1::text")
        or _first_txt(sel, ".bookname h1::text")
        or _first_txt(sel, "title::text")
    )
    author = (
        _first_txt(sel, 'meta[property="og:novel:author"]::attr(content)')
        or _first_txt(sel, '.book .info .author::text')
        or _first_txt(sel, '.book-info .author::text')
        or _first_txt(sel, 'a[rel="author"]::text')
        or _first_txt(sel, '.author a::text')
        or _first_txt(sel, '.author::text')
    )
    if not author:
        t = _first_txt(sel, "body::text")
        m = re.search(r"作者[:：]\s*([^\s/|]+)", t)
        if m: author = m.group(1)

    cover = (
        _first(sel, 'meta[property="og:image"]::attr(content)')
        or _first(sel, ".book .info .cover img::attr(src)")
        or _first(sel, ".book .cover img::attr(src)")
        or _first(sel, ".book-info .cover img::attr(src)")
    )
    if cover and not cover.startswith(("http://", "https://", "/")):
        cover = urljoin(book_url, cover)

    intro = (
        _first_txt(sel, 'meta[property="og:description"]::attr(content)')
        or _first_txt(sel, "#intro")
        or _first_txt(sel, ".intro")
        or _first_txt(sel, "#bookintro")
        or _first_txt(sel, ".book-intro")
        or _first_txt(sel, 'meta[name="description"]::attr(content)')
    )

    # 分类：从页面分类名开始
    cat_name = _first_txt(sel, 'meta[property="og:novel:category"]::attr(content)')
    if not cat_name:
        cat_name = (
            _first_txt(sel, '.breadcrumb a:nth-last-child(2)::text') or
            _first_txt(sel, '.book .info .sort a::text') or
            _first_txt(sel, '.book-info .sort a::text') or
            _first_txt(sel, '.bookdata .sort a::text') or
            _first_txt(sel, '.book .info .category a::text') or
            _first_txt(sel, '.book-info .category a::text')
        )

    category_id = map_category_name(cat_name)

    if category_id == get_default_category_id():
        kw_blob = " ".join(filter(None, [
            cat_name,
            _first_txt(sel, 'meta[name="keywords"]::attr(content)'),
            _first_txt(sel, 'meta[property="og:description"]::attr(content)'),
            _first_txt(sel, 'meta[name="description"]::attr(content)'),
        ]))
        hit = match_category_by_keywords(kw_blob)
        if hit:
            category_id = hit

    if category_id == get_default_category_id() and category_hint:
        category_id = category_hint

    status = (
        _norm_status(_first_txt(sel, 'meta[property="og:novel:status"]::attr(content)'))
        or _norm_status(_first_txt(sel, ".book .info .status::text"))
        or _norm_status(_first_txt(sel, ".book-info .status::text"))
    )
    words_text = (
        _first_txt(sel, ".book .info .words::text")
        or _first_txt(sel, ".book-info .words::text")
        or _first_txt(sel, ".bookdata .words::text")
    )
    words = _find_number(words_text, 0)

    up_text = (
        _first_txt(sel, 'meta[property="og:novel:update_time"]::attr(content)')
        or _first_txt(sel, ".book .info .update::text")
        or _first_txt(sel, ".book-info .update::text")
        or _first_txt(sel, ".bookdata .update::text")
    )
    update_time = None
    m = re.search(r"(\d{4}-\d{1,2}-\d{1,2})(?:\s+(\d{1,2}:\d{2}(?::\d{2})?))?", up_text or "")
    if m:
        d = m.group(1); t = m.group(2) or "00:00:00"
        try:
            if len(t) == 5: t += ":00"
            dt = datetime.datetime.strptime(d + " " + t, "%Y-%m-%d %H:%M:%S")
            update_time = dt.replace(tzinfo=None).isoformat() + "Z"
        except Exception:
            update_time = None
    if not update_time:
        update_time = _iso_now()

    site_book_id = _extract_site_book_id(book_url)
    if not site_book_id:
        og_read = _first_txt(sel, 'meta[property="og:novel:read_url"]::attr(content)')
        if og_read:
            site_book_id = _extract_site_book_id(og_read)

    slug_seed = site_book_id or (urlparse(book_url).netloc + "/" + (title or ""))
    slug = _stable_slug(slug_seed, 5)

    meta_keywords = _first_txt(Selector(text=html), 'meta[name="keywords"]::attr(content)')
    tags = [t.strip() for t in re.split(r"[，,/\s]+", meta_keywords) if t.strip()] if meta_keywords else []

    return {
        "id": title or site_book_id or slug,
        "slug": slug,
        "title": title or slug,
        "author": author or "",
        "category_id": category_id,
        "status": status or "",
        "cover": cover or "",
        "intro": intro or "",
        "words": int(words) if isinstance(words, int) else 0,
        "update_time": update_time,
        "source": {
            "site": "ce5d8cdd",
            "book_url": book_url,
            "site_book_id": site_book_id
        },
        "tags": tags,
        "rating": 0
    }

# ---------------- 目录（顺序编号 + 放宽 URL 形态） ----------------

def parse_toc(html: str, book_url: str):
    from urllib.parse import urlparse
    sel = Selector(text=html)

    anchors = sel.css(
        '#list a, #list-chapterAll a, .listmain a, .chapterlist a, '
        '.reader-list a, .dirlist a, .chapter a'
    )

    def _is_bad_href(h: str) -> bool:
        if not h: return True
        h = h.strip().lower()
        return (
            h.startswith('javascript:') or
            h.startswith('#') or
            h.startswith('mailto:') or
            h.startswith('tel:') or
            'void(0' in h
        )

    def _looks_like_chapter(u: str, text: str) -> bool:
        p = urlparse(u); path = (p.path or "").lower()
        patterns = [
            r'/book/\d+/\d+\.html',   # 原有形态
            r'/\d+/\d+\.html',        # /123/456.html
            r'/\d+_\d+\.html',        # /12345_67890.html
            r'/read/\d+(?:_\d+)?\.html',
            r'/book/\d+/\d+/?$',      # 目录式章节 /book/123/456/
        ]
        for pat in patterns:
            if re.search(pat, path, flags=re.I):
                return True
        t = (text or "").strip()
        if t and len(t) <= 20:
            if re.search(r'(第\s*\d+\s*[章节回节部卷])', t): return True
            if re.match(r'^\d{1,4}\s*$', t): return True
        return False

    prelim = []
    for a in anchors:
        href = (a.attrib.get('href') or a.attrib.get('data-href') or a.attrib.get('data-url') or '').strip()
        if _is_bad_href(href):
            continue
        u = urljoin(book_url, href)
        t = _clean_text(a.css('::text').get() or '')
        if not _looks_like_chapter(u, t):
            continue
        m = re.search(r'/(\d+)\.html$', u) or re.search(r'/book/\d+/(\d+)/?$', u)
        site_id = int(m.group(1)) if m else 0
        prelim.append({"href": u, "title": t or "", "site_id": site_id})

    items = []
    for i, row in enumerate(prelim, 1):
        title = row["title"] or f"第{i}章"
        items.append({
            "no": i,                 # 顺序号用于保存/排序
            "title": title,
            "href": row["href"],
            "site_id": row["site_id"]  # 仅记录
        })
    return items

# ---------------- 正文清洗 ----------------

def parse_chapter_content(html: str):
    sel = Selector(text=html)
    containers = [
        "#htmlContent", ".htmlContent",                 # 新增两种常见容器
        "#chaptercontent", ".ReadAjax_content", ".Readarea",
        "#content", ".content",
        ".read-content", ".chapter_content",
        "#chaptercontent1", "#chapterContent", "article",
    ]
    node_html = ""
    for css in containers:
        v = sel.css(css).get()
        if v:
            node_html = v; break
    if not node_html:
        node_html = sel.css("body").get() or html
    node_html = re.sub(r"(?is)<script[^>]*>.*?</script>", "", node_html)
    node_html = re.sub(r"(?is)<style[^>]*>.*?</style>", "", node_html)
    node_html = re.sub(r'(?is)<a[^>]+id=[\'"](pb_prev|pb_mulu|pb_next)[\'"][^>]*>.*?</a>', '', node_html)
    node_html = re.sub(r'(?is)<(div|p|section)[^>]+class=[\'"][^"\']*(Readpage|link)[^"\']*[\'"][^>]*>.*?</\1>', '', node_html)
    node_html = re.sub(r"(?i)<br\s*/?>", "\n", node_html)
    node_html = re.sub(r"(?is)<[^>]+>", "", node_html)

    def _noise_line(ln: str) -> bool:
        if ln in ("上一章", "下一章", "目录"): return True
        if ln.startswith("新书推荐"): return True
        if "加入书签" in ln or "点此报错" in ln: return True
        if "请收藏本站" in ln and ("www." in ln or "https://" in ln or "http://" in ln): return True
        if "手机版：" in ln and ("https://" in ln or "http://" in ln): return True
        return False

    lines = [re.sub(r"\u00a0+", " ", ln.strip()) for ln in re.split(r"\r?\n", node_html)]
    cleaned = [re.sub(r"[ \t]{2,}", " ", ln) for ln in lines if ln and not _noise_line(ln)]
    paras = []
    for ln in cleaned:
        if ln == "" and (not paras or paras[-1] == ""): continue
        paras.append(ln)
    while paras and paras[0] == "": paras.pop(0)
    while paras and paras[-1] == "": paras.pop()

    MIN_LEN = 80
    if len("".join(paras)) < MIN_LEN:
        txt = ""
        for css in containers:
            n = sel.css(css)
            if n:
                v = n.xpath("normalize-space(string())").get() or ""
                if v and len(v) > len(txt):
                    txt = v
        if not txt:
            txt = sel.xpath("normalize-space(string(//body))").get() or ""
        parts, paras = [], []
        for chunk in re.split(r"[\n\r]+|(?<=[。！？!?\.;])\s+", txt):
            chunk = chunk.strip()
            if not chunk: continue
            if _noise_line(chunk): continue
            parts.append(chunk)
        for ln in parts:
            if ln == "" and (not paras or paras[-1] == ""): continue
            paras.append(ln)
        while paras and paras[0] == "": paras.pop(0)
        while paras and paras[-1] == "": paras.pop()
    return paras