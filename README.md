1) 项目概览
这是一个纯静态输出的小说站系统：采集/生成 → data/ 入库 → build.php 渲染模板 → public/ 静态 HTML。

无数据库依赖，所有数据均为 json/txt + 静态文件。
支持并行/增量静态化、章节分页、封面本地化、站点地图、伪静态 URL。
支持“关键词生成书”模块，且可控制其在首页/分类/搜索/排行是否显示。
2) 环境要求（必要）
PHP ≥ 8.0（CLI），启用 mbstring；
Python ≥ 3.8：
pip install requests parsel charset_normalizer pyyaml pillow
2) 环境要求（可选）
# 可选：brotli （若要解析 br 压缩，站点返回 Content-Encoding: br 时提升解压兼容性）
pip install brotli
Socks5 代理支持（如果 proxies.txt 里有 socks5://...）
pip install "requests[socks]"
 # 等价于安装 PySocks
中文字体（生成 300×400 封面时更好看；没有也能用系统默认，Debian/Ubuntu：）
apt-get install fonts-noto-cjk
或
apt-get install fonts-wqy-zenhei
也可设置环境变量 COVER_FONT=/path/to/your.ttf
Nginx 任意版本（只负责静态文件分发；与采集无强依赖）

例子：
joelcreese.com

[笔趣阁](https://www.joelcreese.com/)

https://www.joelcreese.com

sitemap-index.xml 格式:

<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<sitemap>
<loc>https://www.joelcreese.com/sitemap/static.xml</loc>
</sitemap>
<sitemap>
<loc>https://www.joelcreese.com/sitemap/books/013th.xml</loc>
</sitemap>
<sitemap>
<loc>https://www.joelcreese.com/sitemap/books/01kah.xml</loc>
</sitemap>
<sitemap>
<loc>https://www.joelcreese.com/sitemap/books/02bst.xml</loc>
</sitemap>
<sitemap>
<loc>https://www.joelcreese.com/sitemap/books/03ki0.xml</loc>
<sitemap>
<loc>https://www.joelcreese.com/sitemap/books/zzshl.xml</loc>
</sitemap>
</sitemapindex>
