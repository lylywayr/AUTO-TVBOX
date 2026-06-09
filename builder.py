#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
影视仓自动化多仓更新脚本
功能：每日采集、验证、打包多仓线路，支持jar备份与替换、源质量淘汰、告警等
内部请求使用原始 raw 地址，对外输出使用 jsdelivr CDN 地址
"""

import os
import sys
import json
import re
import time
import hashlib
import random
import subprocess
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ===================== 配置区域 =====================

# 内置知名多仓源（原始 raw 地址或可还原的代理地址） 共计 35+ 个
DEFAULT_SOURCES = [
    "https://raw.githubusercontent.com/liu673cn/box/main/m.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/xiaobai.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/mao.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/p.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/n.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/w.txt",
    "https://raw.githubusercontent.com/liu673cn/box/main/4k.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/zy.json",
    "https://raw.githubusercontent.com/mcp2016/TVBox/main/urls.json",
    "https://raw.githubusercontent.com/zhanghong1983/tvbox/main/MyBox.json",
    "https://raw.githubusercontent.com/zhanghong1983/tvbox/main/MyBox_m3u8.json",
    "https://agit.ai/nbwzlyd/zyd/raw/branch/master/0.json",
    "https://agit.ai/nbwzlyd/zyd/raw/branch/master/1.json",
    "https://bitbucket.org/xduo/cool/raw/main/room.json",
    "https://bitbucket.org/xduo/cool/raw/main/line.json",
    "http://www.饭太硬.com/tv",
    "http://fty.xxooo.cf/tv",
    "http://肥猫.com/",
    "http://小白.love",
    "http://我不是.肥猫.live/",
    "http://tvbox.王二小放牛娃.top",
    "https://qixing.myhkw.com/DC.txt",
    "https://11256.kstore.space/qxys/禁止传播.json",
    "http://cdn.qiaoji8.com/tvbox.json",
    "https://github.moeyy.xyz/https://raw.githubusercontent.com/xyq254245/xyqonlinerule/main/XYQTVBox.json",
    "http://xhztv.top/xhz",
    "http://home.jundie.top:81/top98.json",
    "http://itvbox.cc/云星日记",
    "https://gitlink.org.cn/api/hailin/aishangtv5/raw/tvbox/aishang.json?ref=master",
    "https://gh.con.sh/https://raw.githubusercontent.com/guot55/yg/main/ygbox.json",
    "http://我不是.摸鱼儿.top",
    "http://ok321.top/ok",
    "http://meowtv.top/tv",
    "https://龙伊.top",
    "https://raw.liucn.cc/box/m.json",
    "https://jikefuye.cn/tvbox.json",
    "https://毒盒.com/tv",
    "https://dxawi.github.io/0/0.json",
]

# 内置社区源列表（用于发现新多仓）共计 50+ 个
DEFAULT_COMMUNITY_SOURCES = [
    "https://agit.ai/nbwzlyd/zyd/raw/branch/master/0.json",
    "https://agit.ai/nbwzlyd/zyd/raw/branch/master/1.json",
    "https://agit.ai/nbwzlyd/zyd/raw/branch/master/2.json",
    "https://bitbucket.org/xduo/cool/raw/main/room.json",
    "https://bitbucket.org/xduo/cool/raw/main/line.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/m.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/xiaobai.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/mao.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/p.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/n.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/w.txt",
    "https://raw.githubusercontent.com/liu673cn/box/main/4k.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/zy.json",
    "https://raw.githubusercontent.com/mcp2016/TVBox/main/urls.json",
    "https://raw.githubusercontent.com/zhanghong1983/tvbox/main/MyBox.json",
    "https://raw.githubusercontent.com/zhanghong1983/tvbox/main/MyBox_m3u8.json",
    "https://raw.githubusercontent.com/zhanghong1983/tvbox/main/MyBox_xiaoya.json",
    "https://github.moeyy.xyz/https://raw.githubusercontent.com/xyq254245/xyqonlinerule/main/XYQTVBox.json",
    "https://gh.con.sh/https://raw.githubusercontent.com/guot55/yg/main/ygbox.json",
    "https://gitlink.org.cn/api/hailin/aishangtv5/raw/tvbox/aishang.json?ref=master",
    "http://www.饭太硬.com/tv",
    "http://fty.xxooo.cf/tv",
    "http://fty.888484.xyz/tv",
    "http://肥猫.com/",
    "http://小白.love",
    "http://我不是.肥猫.live/",
    "http://tvbox.王二小放牛娃.top",
    "https://qixing.myhkw.com/DC.txt",
    "https://11256.kstore.space/qxys/禁止传播.json",
    "http://cdn.qiaoji8.com/tvbox.json",
    "http://xhztv.top/xhz",
    "http://home.jundie.top:81/top98.json",
    "http://itvbox.cc/云星日记",
    "http://我不是.摸鱼儿.top",
    "http://ok321.top/ok",
    "http://meowtv.top/tv",
    "https://龙伊.top",
    "https://raw.liucn.cc/box/m.json",
    "https://jikefuye.cn/tvbox.json",
    "https://毒盒.com/tv",
    "https://dxawi.github.io/0/0.json",
    "https://raw.githubusercontent.com/liu673cn/box/main/duo.txt",
    "https://raw.githubusercontent.com/liu673cn/box/main/jiance.txt",
    "https://raw.githubusercontent.com/liu673cn/box/main/feimao.txt",
    "https://raw.githubusercontent.com/tangsan99999/Tvbox/main/tvbox.json",
    "https://raw.githubusercontent.com/yzq9/box/main/box.json",
    "https://raw.githubusercontent.com/2hacc/TVBox/main/box.json",
    "https://raw.githubusercontent.com/weixine/tvbox/main/box.json",
    "https://raw.githubusercontent.com/q215613905/TVBoxOS/main/box.json",
]

# GitHub 搜索关键词
SEARCH_QUERIES = [
    "tvbox multibox",
    "storeHouse urls",
    "多仓 线路",
    "影视仓 接口",
]

# 代理还原规则（将各种代理地址还原为原始 raw 地址）
PROXY_PATTERNS = [
    (r'^https?://cdn\.jsdelivr\.net/gh/([^@]+)@(.+?)/(.+)$',
     lambda m: f'https://raw.githubusercontent.com/{m.group(1)}/{m.group(2)}/{m.group(3)}'),
    (r'^https?://ghproxy\.net/(raw\.githubusercontent\.com/.+)$',
     lambda m: f'https://{m.group(1)}'),
    (r'^https?://github\.moeyy\.xyz/(https?://raw\.githubusercontent\.com/.+)$',
     lambda m: m.group(1)),
    (r'^https?://raw\.fastgit\.org/(.+)$',
     lambda m: f'https://raw.githubusercontent.com/{m.group(1)}'),
    (r'^https?://raw\.staticdn\.net/(.+)$',
     lambda m: f'https://raw.githubusercontent.com/{m.group(1)}'),
    (r'^https?://[^/]+/(raw\.githubusercontent\.com/.+)$',
     lambda m: f'https://{m.group(1)}'),
]

# ===================== 全局会话 =====================

_session = None
def get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        retries = Retry(total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        _session.mount('http://', HTTPAdapter(max_retries=retries))
        _session.mount('https://', HTTPAdapter(max_retries=retries))
    return _session

def log(msg, level="info"):
    if os.environ.get("DEBUG", "false").lower() == "true" or level == "error":
        print(f"[{level.upper()}] {msg}")

# ===================== 地址规范化 =====================

def restore_github_raw_url(url):
    """将各种代理地址还原为原始 raw.githubusercontent.com 地址（用于内部请求）"""
    for pattern, repl_func in PROXY_PATTERNS:
        m = re.match(pattern, url, re.I)
        if m:
            new_url = repl_func(m)
            log(f"还原代理: {url} -> {new_url}")
            return new_url
    return url

def to_jsdelivr_url(raw_url):
    """仅用于最终输出：将 raw 地址转换为 jsdelivr CDN 地址（非 GitHub 源则原样返回）"""
    pattern = r'^https?://raw\.githubusercontent\.com/([^/]+)/([^/]+)/([^/]+)/(.+)$'
    m = re.match(pattern, raw_url)
    if m:
        user, repo, branch, path = m.group(1), m.group(2), m.group(3), m.group(4)
        return f'https://cdn.jsdelivr.net/gh/{user}/{repo}@{branch}/{path}'
    return raw_url

def normalize_for_request(url):
    """内部请求使用：只还原代理，不转CDN"""
    return restore_github_raw_url(url)

# ===================== 文件加载 =====================

def load_file_sources(filepath, description):
    sources = []
    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    sources.append(line)
        log(f"加载{description}: {len(sources)} 个")
    else:
        log(f"{description}文件不存在，跳过")
    return sources

def load_known_sources():
    sources = DEFAULT_SOURCES.copy()
    external = load_file_sources(Path("known_sources.txt"), "外部已知源")
    sources.extend(external)
    seen = set()
    unique = []
    for s in sources:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    log(f"已知多仓源总计: {len(unique)} 个 (内置 {len(DEFAULT_SOURCES)}, 外部 {len(external)})")
    return unique

def load_community_sources():
    sources = DEFAULT_COMMUNITY_SOURCES.copy()
    external = load_file_sources(Path("community_sources.txt"), "外部社区源")
    sources.extend(external)
    seen = set()
    unique = []
    for s in sources:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    log(f"社区源总计: {len(unique)} 个 (内置 {len(DEFAULT_COMMUNITY_SOURCES)}, 外部 {len(external)})")
    return unique

# ===================== 缓存请求 =====================

def fetch_json_with_cache(url, state, use_cache=True):
    cache_key = f"cache_{url}"
    headers = {}
    if use_cache and "cache_headers" in state and cache_key in state["cache_headers"]:
        cached = state["cache_headers"][cache_key]
        if cached.get("etag"):
            headers["If-None-Match"] = cached["etag"]
        if cached.get("last_modified"):
            headers["If-Modified-Since"] = cached["last_modified"]
    try:
        resp = get_session().get(url, timeout=int(os.environ.get("REQUEST_TIMEOUT", 15)), headers=headers)
        if resp.status_code == 304:
            log(f"缓存未变化: {url}")
            return None
        if resp.status_code == 200:
            if "cache_headers" not in state:
                state["cache_headers"] = {}
            state["cache_headers"][cache_key] = {
                "etag": resp.headers.get("ETag"),
                "last_modified": resp.headers.get("Last-Modified"),
                "last_accessed": time.time()
            }
            return resp.json()
    except Exception as e:
        log(f"请求失败 {url}: {e}", "error")
    return None

def download_file(url, max_size, timeout=30):
    try:
        resp = get_session().get(url, timeout=timeout, stream=True)
        if resp.status_code == 200:
            content = b''
            for chunk in resp.iter_content(chunk_size=8192):
                content += chunk
                if len(content) > max_size:
                    log(f"文件过大超过限制 ({max_size} bytes): {url}", "error")
                    return None
            return content
    except Exception as e:
        log(f"下载失败 {url}: {e}", "error")
    return None

# ===================== 社区源采集 =====================

def extract_urls_from_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    urls = set()
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.startswith('http') and any(ext in href for ext in ['.json', '.txt', 'raw.githubusercontent', 'gitlab', 'agit', 'bitbucket']):
            urls.add(href)
    text_urls = re.findall(r'https?://[^\s"\'<>]+', html_content)
    for u in text_urls:
        if any(ext in u for ext in ['.json', '.txt', 'raw.githubusercontent', 'gitlab', 'agit', 'bitbucket']):
            urls.add(u)
    return list(urls)

def fetch_community_new_sources(state):
    if not os.environ.get("ENABLE_COMMUNITY_FETCH", "true").lower() == "true":
        return []
    last_fetch = state.get("last_community_fetch")
    interval_days = int(os.environ.get("COMMUNITY_FETCH_INTERVAL_DAYS", 7))
    if last_fetch:
        last = datetime.fromisoformat(last_fetch)
        if (datetime.now() - last).days < interval_days:
            log(f"距离上次社区采集不足{interval_days}天，跳过")
            return []
    community_urls = load_community_sources()
    new_sources = set()
    max_new = 50
    for url in community_urls:
        if len(new_sources) >= max_new:
            break
        req_url = normalize_for_request(url)
        log(f"采集社区源: {req_url}")
        try:
            resp = get_session().get(req_url, timeout=15)
            if resp.status_code != 200:
                continue
            content = resp.text
        except Exception as e:
            log(f"请求失败: {e}", "error")
            continue
        if '.html' in req_url or resp.headers.get('Content-Type', '').startswith('text/html'):
            extracted = extract_urls_from_html(content)
        else:
            extracted = re.findall(r'https?://[^\s"\'<>]+', content)
        for ext_url in extracted:
            if len(new_sources) >= max_new:
                break
            ext_raw = normalize_for_request(ext_url)
            if ('raw.githubusercontent' in ext_raw or 'gitlab' in ext_raw or 'agit.ai' in ext_raw or 'bitbucket' in ext_raw) and \
               (ext_raw.endswith('.json') or ext_raw.endswith('.txt') or 'storeHouse' in ext_raw or 'urls' in ext_raw):
                new_sources.add(ext_raw)
    state["last_community_fetch"] = datetime.now().isoformat()
    log(f"从社区源发现 {len(new_sources)} 个新多仓地址")
    return list(new_sources)

# ===================== GitHub 搜索 =====================

def github_search_new_sources(token, state):
    if not token:
        return []
    last_search = state.get("last_search_date")
    if last_search:
        last = datetime.strptime(last_search, "%Y-%m-%d")
        if (datetime.now() - last).days < 7:
            log(f"距离上次GitHub搜索不足7天，跳过")
            return []
    headers = {'Authorization': f'token {token}'}
    new_sources = set()
    for query in SEARCH_QUERIES:
        page = 1
        while page <= 3:
            url = f"https://api.github.com/search/code?q={query}+extension:json&per_page=30&page={page}"
            try:
                resp = get_session().get(url, headers=headers, timeout=10)
                if resp.status_code != 200:
                    log(f"GitHub搜索失败: {resp.status_code}", "error")
                    break
                remaining = int(resp.headers.get('X-RateLimit-Remaining', 0))
                if remaining == 0:
                    log("GitHub API 限流，停止搜索", "error")
                    break
                data = resp.json()
                for item in data.get('items', []):
                    raw_url = item.get('html_url', '').replace('github.com', 'raw.githubusercontent.com').replace('/blob/', '/')
                    req_url = normalize_for_request(raw_url)
                    content = fetch_json_with_cache(req_url, state, use_cache=False)
                    if content and isinstance(content, dict):
                        if ('storeHouse' in content and isinstance(content['storeHouse'], list) and len(content['storeHouse']) > 0) or \
                           ('urls' in content and isinstance(content['urls'], list) and len(content['urls']) > 0):
                            new_sources.add(req_url)
                if 'next' not in resp.links:
                    break
                page += 1
            except Exception as e:
                log(f"搜索出错 {query}: {e}", "error")
                break
    state["last_search_date"] = datetime.now().strftime("%Y-%m-%d")
    log(f"GitHub搜索发现 {len(new_sources)} 个新多仓")
    return list(new_sources)

# ===================== 解析源提取单线路 =====================

def extract_single_line_urls(source_url, state):
    urls = set()
    req_url = normalize_for_request(source_url)
    data = fetch_json_with_cache(req_url, state)
    if not data:
        return urls
    if isinstance(data, dict) and 'storeHouse' in data and isinstance(data['storeHouse'], list):
        for item in data['storeHouse']:
            if isinstance(item, dict) and 'sourceUrl' in item:
                sub_urls = extract_single_line_urls(item['sourceUrl'], state)
                urls.update(sub_urls)
    elif isinstance(data, dict) and 'urls' in data and isinstance(data['urls'], list):
        for item in data['urls']:
            if isinstance(item, dict) and 'url' in item:
                urls.add(item['url'])
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and 'url' in item:
                urls.add(item['url'])
    return urls

# ===================== 验证函数 =====================

def verify_live(url):
    start = time.time()
    try:
        req_url = normalize_for_request(url)
        resp = get_session().head(req_url, timeout=int(os.environ.get("REQUEST_TIMEOUT", 10)), allow_redirects=True)
        if resp.status_code == 405:
            resp = get_session().get(req_url, timeout=10, stream=True)
            resp.close()
        elapsed = (time.time() - start) * 1000
        return 200 <= resp.status_code < 400, elapsed
    except:
        elapsed = (time.time() - start) * 1000
        return False, elapsed

def verify_functional(url, state, max_size):
    try:
        req_url = normalize_for_request(url)
        resp = get_session().get(req_url, timeout=int(os.environ.get("REQUEST_TIMEOUT", 15)), stream=True)
        if resp.status_code != 200:
            return False, None, None
        content = b''
        for chunk in resp.iter_content(chunk_size=8192):
            content += chunk
            if len(content) > max_size:
                log(f"功能验证: 文件过大 {url}", "error")
                return False, None, None
        data = json.loads(content.decode('utf-8'))
        if not isinstance(data, dict):
            return False, None, None
        if 'sites' not in data or not isinstance(data['sites'], list) or len(data['sites']) == 0:
            return False, None, None
        first_site = data['sites'][0]
        if not isinstance(first_site, dict) or 'key' not in first_site:
            return False, None, None
        jar_url = data.get('spider')
        content_hash = hashlib.sha256(content).hexdigest()
        return True, jar_url, content_hash
    except Exception as e:
        log(f"功能验证异常 {url}: {e}", "error")
        return False, None, None

# ===================== jar 备份与替换 =====================

def get_local_base_url():
    try:
        repo_url = subprocess.check_output(['git', 'config', '--get', 'remote.origin.url']).decode().strip()
        match = re.search(r'github\.com[:/](.+?)(\.git)?$', repo_url)
        if match:
            repo_path = match.group(1)
            return f"https://cdn.jsdelivr.net/gh/{repo_path}@main"
    except:
        pass
    log("警告：无法自动获取 LOCAL_BASE_URL，jar替换功能将禁用", "error")
    return None

def backup_jar(jar_url, state):
    if not jar_url:
        return None
    raw_jar = normalize_for_request(jar_url)
    jar_hash = hashlib.sha256(raw_jar.encode()).hexdigest()
    jar_dir = Path("jars")
    jar_dir.mkdir(exist_ok=True)
    jar_path = jar_dir / f"{jar_hash}.jar"
    need_update = not jar_path.exists()
    if not need_update and "jar_update_record" in state:
        last_update = state["jar_update_record"].get(jar_hash, 0)
        if (time.time() - last_update) > 30 * 86400:
            need_update = True
    if not need_update:
        log(f"jar已存在且未过期: {jar_path}")
        return jar_hash
    max_jar_size = int(os.environ.get("MAX_JAR_SIZE", 10485760))
    content = download_file(raw_jar, max_jar_size)
    if content:
        with open(jar_path, 'wb') as f:
            f.write(content)
        if "jar_update_record" not in state:
            state["jar_update_record"] = {}
        state["jar_update_record"][jar_hash] = time.time()
        log(f"jar备份成功: {jar_path}")
        return jar_hash
    else:
        log(f"jar备份失败: {jar_url}", "error")
        return None

def create_local_line(line_url, original_json_url, jar_backup_hash, state):
    base_cdn = get_local_base_url()
    if not base_cdn:
        return None
    raw_json_url = normalize_for_request(original_json_url)
    try:
        resp = get_session().get(raw_json_url, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
    except:
        return None
    new_jar_url = f"{base_cdn}/jars/{jar_backup_hash}.jar"
    data['spider'] = new_jar_url
    line_hash = hashlib.sha256(line_url.encode()).hexdigest()
    lines_dir = Path("lines")
    lines_dir.mkdir(exist_ok=True)
    local_path = lines_dir / f"{line_hash}.json"
    with open(local_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log(f"已生成本地线路: {local_path}")
    return str(local_path)

def revert_local_line(line_url, state):
    line_state = state.get("valid_lines", {}).get(line_url, {})
    local_path = line_state.get("local_line_path")
    if local_path and Path(local_path).exists():
        Path(local_path).unlink()
        log(f"已删除本地线路: {local_path}")

# ===================== 源质量统计与淘汰 =====================

def update_source_quality(state, source_url, parse_success):
    if "source_quality" not in state:
        state["source_quality"] = {}
    qual = state["source_quality"].get(source_url, {"total": 0, "success": 0, "last_parse": 0})
    qual["total"] += 1
    if parse_success:
        qual["success"] += 1
    qual["last_parse"] = time.time()
    state["source_quality"][source_url] = qual

def evict_low_quality_sources(state, known_sources, max_sources):
    if not os.environ.get("ENABLE_SOURCE_EVICTION", "true").lower() == "true":
        return known_sources
    if datetime.now().day != 1:
        return known_sources
    builtin_set = set(DEFAULT_SOURCES)
    quality = state.get("source_quality", {})
    scores = []
    for src in known_sources:
        if src in builtin_set:
            continue
        q = quality.get(src, {"total": 1, "success": 0})
        score = q["success"] / max(1, q["total"])
        scores.append((score, src))
    scores.sort(key=lambda x: x[0])
    if len(scores) <= max_sources:
        return known_sources
    remove_count = len(scores) - max_sources
    to_remove = {src for _, src in scores[:remove_count]}
    new_sources = [src for src in known_sources if src not in to_remove]
    log(f"淘汰 {len(to_remove)} 个低质量源，剩余 {len(new_sources)} 个")
    with open("known_sources.txt", "w", encoding='utf-8') as f:
        for src in new_sources:
            if src not in builtin_set:
                f.write(src + "\n")
    return new_sources

# ===================== 内容去重 =====================

def is_duplicate_content(content_hash, state, current_url):
    if "content_hash_map" not in state:
        state["content_hash_map"] = {}
    if content_hash in state["content_hash_map"]:
        existing_url = state["content_hash_map"][content_hash]
        if existing_url != current_url:
            log(f"内容重复: {current_url} 与 {existing_url} 相同，忽略")
            return True
    else:
        state["content_hash_map"][content_hash] = current_url
    return False

# ===================== 告警 =====================

def send_alert(message, level="info"):
    webhook = os.environ.get("ALERT_WEBHOOK_URL")
    if not webhook:
        return
    try:
        payload = {"text": f"[{level.upper()}] {message}"}
        get_session().post(webhook, json=payload, timeout=5)
    except Exception as e:
        log(f"发送告警失败: {e}", "error")

# ===================== 更新 README =====================

def update_readme(stats):
    readme_path = Path("README.md")
    if not readme_path.exists():
        with open(readme_path, 'w', encoding='utf-8') as f:
            f.write("# AUTO-TVBOX\n\n<!-- AUTO_TVBOX_STATS_START -->\n<!-- AUTO_TVBOX_STATS_END -->")
    with open(readme_path, 'r', encoding='utf-8') as f:
        content = f.read()
    stats_table = f"""<!-- AUTO_TVBOX_STATS_START -->
## 📊 最新运行状态

| 项目 | 数值 |
|------|------|
| 运行日期 | {stats['date']} |
| 已知多仓源 | {stats['total_sources']} |
| 解析单线路 | {stats['parsed_lines']} |
| 当日验证 | {stats['verified_today']} (成功 {stats['verified_success']}, 失败 {stats['verified_fail']}) |
| 最终输出 | {stats['final_lines']} 条 |
| 新备份 jar | {stats['jars_backup']} |
| 新替换线路 | {stats['jars_replaced']} |

🕒 下次自动更新: 每日 04:20 (北京时间)
<!-- AUTO_TVBOX_STATS_END -->"""
    if "<!-- AUTO_TVBOX_STATS_START -->" in content and "<!-- AUTO_TVBOX_STATS_END -->" in content:
        new_content = re.sub(r'<!-- AUTO_TVBOX_STATS_START -->.*?<!-- AUTO_TVBOX_STATS_END -->', stats_table, content, flags=re.DOTALL)
    else:
        new_content = content + "\n\n" + stats_table
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.write(new_content)

# ===================== 主函数 =====================

def main():
    # ===================== 自动创建缺失文件和目录（避免git-add报错） =====================
    # 确保 known_sources.txt 和 community_sources.txt 存在（如果不存在则创建空文件）
    Path("known_sources.txt").touch()
    Path("community_sources.txt").touch()
    # 确保 jars、lines、archives 目录存在，并创建 .gitkeep 文件以便 git 跟踪空目录
    for d in ["jars", "lines", "archives"]:
        path = Path(d)
        path.mkdir(exist_ok=True)
        keep_file = path / ".gitkeep"
        if not keep_file.exists():
            keep_file.touch()

    # 读取环境变量
    gh_token = os.environ.get('GH_TOKEN', '')
    validity_days = int(os.environ.get('VALIDITY_DAYS', 3))
    func_check_days = int(os.environ.get('FUNC_CHECK_DAYS', 7))
    cooling_days = int(os.environ.get('COOLING_DAYS', 7))
    jar_live_check_days = int(os.environ.get('JAR_LIVE_CHECK_DAYS', 3))
    max_output = int(os.environ.get('MAX_OUTPUT_LINES', 200))
    max_verify_per_day = int(os.environ.get('MAX_VERIFY_PER_DAY', 500))
    max_jar_backups = int(os.environ.get('MAX_JAR_BACKUPS_PER_DAY', 50))
    max_sources = int(os.environ.get('MAX_SOURCES', 200))
    max_sources_per_day = int(os.environ.get('MAX_SOURCES_PER_DAY', 50))
    archive_days = int(os.environ.get('ARCHIVE_DAYS', 30))
    alert_failure_ratio = float(os.environ.get('ALERT_FAILURE_RATIO', 0.1))
    concurrency = int(os.environ.get('CONCURRENCY', 5))
    max_content_size = int(os.environ.get('MAX_CONTENT_SIZE', 10485760))

    # 加载状态文件
    state_file = Path("state.json")
    if state_file.exists():
        with open(state_file, 'r', encoding='utf-8') as f:
            state = json.load(f)
    else:
        state = {
            "valid_lines": {},
            "cooling_lines": {},
            "source_quality": {},
            "cache_headers": {},
            "content_hash_map": {},
            "jar_update_record": {},
            "last_search_date": None,
            "last_community_fetch": None,
            "source_last_parse": {}
        }

    # 步骤2：加载已知多仓源（原始地址，不做CDN转换）
    known_sources = load_known_sources()
    known_sources = [normalize_for_request(src) for src in known_sources]

    # 步骤3：社区源采集（每7天）
    new_community = fetch_community_new_sources(state)
    if new_community:
        for src in new_community:
            if src not in known_sources:
                known_sources.append(src)
        with open("known_sources.txt", "a", encoding='utf-8') as f:
            for src in new_community:
                f.write(src + "\n")
        log(f"社区采集新增 {len(new_community)} 个源")

    # 步骤4：GitHub搜索补充（每7天）
    new_github = github_search_new_sources(gh_token, state)
    if new_github:
        for src in new_github:
            if src not in known_sources:
                known_sources.append(src)
        with open("known_sources.txt", "a", encoding='utf-8') as f:
            for src in new_github:
                f.write(src + "\n")
        log(f"GitHub搜索新增 {len(new_github)} 个源")

    # 步骤5：源质量淘汰（每月1日）
    known_sources = evict_low_quality_sources(state, known_sources, max_sources)

    # 步骤5b：源轮询限流（选择最久未解析的源）
    now_ts = time.time()
    source_last_parse = state.get("source_last_parse", {})
    sources_with_priority = [(source_last_parse.get(src, 0), src) for src in known_sources]
    sources_with_priority.sort(key=lambda x: x[0])
    selected_sources = [src for _, src in sources_with_priority[:max_sources_per_day]]
    log(f"今日选择解析 {len(selected_sources)} 个源（共 {len(known_sources)} 个）")

    # 步骤6：解析选中的源，提取单线路URL
    all_line_urls = set()
    for src in selected_sources:
        log(f"解析源: {src}")
        line_urls = extract_single_line_urls(src, state)
        if line_urls:
            all_line_urls.update(line_urls)
            update_source_quality(state, src, True)
        else:
            update_source_quality(state, src, False)
        source_last_parse[src] = now_ts
    state["source_last_parse"] = source_last_parse
    log(f"总计提取到 {len(all_line_urls)} 个唯一单线路URL")

    # 步骤7：构建待验证队列
    to_verify = set()
    for url in all_line_urls:
        if url in state["cooling_lines"]:
            if now_ts >= state["cooling_lines"][url]:
                to_verify.add(url)
        elif url not in state["valid_lines"]:
            to_verify.add(url)
        else:
            info = state["valid_lines"][url]
            if now_ts - info.get("last_live", 0) > validity_days * 86400:
                to_verify.add(url)
            elif now_ts - info.get("last_func", 0) > func_check_days * 86400:
                to_verify.add(url)
    for url, end_ts in list(state["cooling_lines"].items()):
        if now_ts >= end_ts:
            to_verify.add(url)
    if len(to_verify) > max_verify_per_day:
        to_verify = set(list(to_verify)[:max_verify_per_day])
    log(f"待验证队列大小: {len(to_verify)}")

    # 步骤8-9：并发验证
    verified_success = 0
    verified_fail = 0
    jar_backup_count = 0
    replaced_count = 0

    new_valid = {url: info.copy() for url, info in state["valid_lines"].items() if url not in to_verify}

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        future_to_url = {executor.submit(verify_live, url): url for url in to_verify}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                alive, elapsed = future.result()
            except:
                alive, elapsed = False, 0
            if not alive:
                verified_fail += 1
                if url in new_valid:
                    del new_valid[url]
                state["cooling_lines"][url] = now_ts + cooling_days * 86400
                continue
            # 存活成功
            need_func = False
            if url not in new_valid:
                need_func = True
            elif now_ts - new_valid[url].get("last_func", 0) > func_check_days * 86400:
                need_func = True
            if need_func:
                func_ok, jar_url, content_hash = verify_functional(url, state, max_content_size)
                if not func_ok:
                    verified_fail += 1
                    if url in new_valid:
                        del new_valid[url]
                    state["cooling_lines"][url] = now_ts + cooling_days * 86400
                    continue
                if is_duplicate_content(content_hash, state, url):
                    verified_fail += 1
                    continue
                verified_success += 1
                if jar_url and jar_backup_count < max_jar_backups:
                    jar_hash = backup_jar(jar_url, state)
                    if jar_hash:
                        jar_backup_count += 1
                else:
                    jar_hash = None
                new_valid[url] = {
                    "last_live": now_ts,
                    "last_func": now_ts,
                    "jar_url": jar_url,
                    "jar_backup_hash": jar_hash,
                    "content_hash": content_hash,
                    "response_time_avg": elapsed
                }
                if jar_url and "jar_last_check" not in new_valid[url]:
                    new_valid[url]["jar_last_check"] = 0
                if jar_url and now_ts - new_valid[url].get("jar_last_check", 0) > jar_live_check_days * 86400:
                    jar_alive, _ = verify_live(jar_url)
                    new_valid[url]["jar_alive"] = jar_alive
                    new_valid[url]["jar_last_check"] = now_ts
                else:
                    jar_alive = new_valid[url].get("jar_alive", True)
                # 替换决策
                if jar_hash and not jar_alive and "local_line_path" not in new_valid[url]:
                    local_path = create_local_line(url, url, jar_hash, state)
                    if local_path:
                        new_valid[url]["local_line_path"] = local_path
                        replaced_count += 1
                if jar_hash and jar_alive and "local_line_path" in new_valid[url]:
                    revert_local_line(url, state)
                    del new_valid[url]["local_line_path"]
            else:
                verified_success += 1
                if url in new_valid:
                    new_valid[url]["last_live"] = now_ts
                    old_avg = new_valid[url].get("response_time_avg", elapsed)
                    new_valid[url]["response_time_avg"] = old_avg * 0.7 + elapsed * 0.3
                else:
                    new_valid[url] = {"last_live": now_ts, "last_func": 0}

    log(f"验证完成: 成功 {verified_success}, 失败 {verified_fail}")

    # 清理冷却字典
    for url in list(state["cooling_lines"].keys()):
        if now_ts >= state["cooling_lines"][url]:
            del state["cooling_lines"][url]

    state["valid_lines"] = new_valid

    # 清理过期缓存头
    if "cache_headers" in state:
        cutoff = now_ts - 30 * 86400
        for key in list(state["cache_headers"].keys()):
            if state["cache_headers"][key].get("last_accessed", 0) < cutoff:
                del state["cache_headers"][key]

    # 步骤14：输出限流与排序（最终输出时转换为CDN地址）
    line_scores = []
    for url, info in state["valid_lines"].items():
        score = info.get("last_func", 0) * (1 + 0.5 * (1 if info.get("jar_backup_hash") else 0))
        line_scores.append((score, url, info))
    line_scores.sort(key=lambda x: x[0], reverse=True)
    final_lines = line_scores[:max_output]
    multibox = []
    base_cdn = get_local_base_url()
    for idx, (_, url, info) in enumerate(final_lines, 1):
        if "local_line_path" in info and base_cdn:
            line_hash = hashlib.sha256(url.encode()).hexdigest()
            final_url = f"{base_cdn}/lines/{line_hash}.json"
        else:
            final_url = to_jsdelivr_url(url)
        multibox.append({"name": f"线路_{idx:04d}", "url": final_url})
    with open("multibox.json", "w", encoding='utf-8') as f:
        json.dump(multibox, f, indent=2, ensure_ascii=False)
    log(f"生成 multibox.json，包含 {len(multibox)} 条有效单线路（已转换为CDN地址）")

    # 步骤15：版本归档
    archive_dir = Path("archives")
    archive_dir.mkdir(exist_ok=True)
    archive_file = archive_dir / f"multibox_{datetime.now().strftime('%Y-%m-%d')}.json"
    shutil.copy("multibox.json", archive_file)
    for f in archive_dir.glob("multibox_*.json"):
        try:
            date_str = f.stem.split('_')[1]
            file_date = datetime.strptime(date_str, '%Y-%m-%d')
            if (datetime.now() - file_date).days > archive_days:
                f.unlink()
        except:
            pass

    # 步骤16：告警
    total_verify = verified_success + verified_fail
    if total_verify > 50 and verified_fail / total_verify > alert_failure_ratio:
        send_alert(f"今日验证失败率过高: {verified_fail}/{total_verify} ({verified_fail/total_verify:.1%})", "warning")

    # 步骤17：更新README
    stats = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "total_sources": len(known_sources),
        "parsed_lines": len(all_line_urls),
        "verified_today": total_verify,
        "verified_success": verified_success,
        "verified_fail": verified_fail,
        "final_lines": len(multibox),
        "jars_backup": jar_backup_count,
        "jars_replaced": replaced_count
    }
    update_readme(stats)

    # 保存状态
    with open(state_file, "w", encoding='utf-8') as f:
        json.dump(state, f, indent=2)

    log("全部步骤执行完毕")
    send_alert(f"每日更新完成: 有效线路 {len(multibox)} 条", "info")

if __name__ == "__main__":
    main()