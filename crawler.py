import argparse
import re
import time
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

import db

ALLOWED_TLDS = (".edu", ".org", ".gov")
SKIP_EXTENSIONS = (".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".mp4", ".mp3",
                   ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".zip", ".tar", ".gz")
SKIP_PATH_PATTERNS = re.compile(r"/(login|signin|auth|logout|signup|register|account|sso)/", re.I)

STOP_WORDS = frozenset(
    "the a an of for to in on at by is it and or but with from as this that "
    "are was were be been being have has had do does did will would shall should "
    "may might can could not no all any each every some its our your their "
    "what which who whom how when where why about into through during before after "
    "above below between up down out off over under again further then once also "
    "more most very just than too so such only own same here there these those "
    "home page site web contact us help search skip navigation menu main content "
    "new go get one two use".split()
)

USER_AGENT = "EduSpider/1.0 (educational crawler; +https://github.com/eduspider)"
REQUEST_TIMEOUT = 15
MIN_TOPIC_LENGTH = 3

# Per-domain rate limiting
_last_request_time = {}
_robots_cache = {}


def rate_limit(domain):
    now = time.time()
    last = _last_request_time.get(domain, 0)
    wait = 1.0 - (now - last)
    if wait > 0:
        time.sleep(wait)
    _last_request_time[domain] = time.time()


def check_robots(url):
    parsed = urlparse(url)
    domain = parsed.netloc
    if domain in _robots_cache:
        return _robots_cache[domain].can_fetch(USER_AGENT, url)
    robots_url = f"{parsed.scheme}://{domain}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
    except Exception:
        pass  # If we can't read robots.txt, assume allowed
    _robots_cache[domain] = rp
    return rp.can_fetch(USER_AGENT, url)


def normalize_url(url):
    parsed = urlparse(url)
    # Remove fragment, normalize trailing slash on path
    path = parsed.path.rstrip("/") or "/"
    return urlunparse((parsed.scheme, parsed.netloc.lower(), path, parsed.params, parsed.query, ""))


def is_allowed_domain(url):
    domain = urlparse(url).netloc.lower()
    return any(domain.endswith(tld) for tld in ALLOWED_TLDS)


def should_skip_url(url):
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return True
    lower_path = parsed.path.lower()
    if any(lower_path.endswith(ext) for ext in SKIP_EXTENSIONS):
        return True
    if SKIP_PATH_PATTERNS.search(parsed.path):
        return True
    return False


def extract_topics(title, headings):
    raw_texts = []
    if title:
        raw_texts.append(title)
    raw_texts.extend(headings)

    topics = set()
    for text in raw_texts:
        # Clean and split
        text = re.sub(r"[^\w\s-]", " ", text.lower())
        words = text.split()
        # Single meaningful words
        for word in words:
            word = word.strip("-")
            if len(word) >= MIN_TOPIC_LENGTH and word not in STOP_WORDS and not word.isdigit():
                topics.add(word)
        # Bigrams from headings (captures short phrases)
        for i in range(len(words) - 1):
            a, b = words[i].strip("-"), words[i + 1].strip("-")
            if a not in STOP_WORDS and b not in STOP_WORDS and len(a) >= MIN_TOPIC_LENGTH and len(b) >= MIN_TOPIC_LENGTH:
                topics.add(f"{a} {b}")

    return topics


def fetch_page(url):
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
    resp.raise_for_status()
    content_type = resp.headers.get("Content-Type", "")
    if "text/html" not in content_type:
        return None
    return resp.text


def parse_page(html, base_url):
    soup = BeautifulSoup(html, "html.parser")

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    description = ""
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        description = meta_desc["content"].strip()
    if not description:
        first_p = soup.find("p")
        if first_p:
            description = first_p.get_text(strip=True)[:300]

    headings = []
    for tag in soup.find_all(["h1", "h2", "h3"]):
        text = tag.get_text(strip=True)
        if text:
            headings.append(text)

    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(base_url, href)
        links.add(full_url)

    return title, description, headings, links


def crawl(url, max_depth, crawl_id, depth=0, visited=None):
    if visited is None:
        visited = set()

    url = normalize_url(url)

    if url in visited or depth > max_depth:
        return 0

    visited.add(url)

    if db.page_exists(url):
        return 0

    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    if not check_robots(url):
        print(f"  [blocked by robots.txt] {url}")
        return 0

    rate_limit(domain)

    print(f"  [depth {depth}] Fetching: {url}")
    try:
        html = fetch_page(url)
    except Exception as e:
        print(f"  [error] {url}: {e}")
        return 0

    if html is None:
        return 0

    title, description, headings, links = parse_page(html, url)

    page_id = db.save_page(url, title, description, domain, depth, crawl_id)
    if page_id is None:
        return 0  # Already saved by another path

    pages_found = 1

    topics = extract_topics(title, headings)
    for topic_name in topics:
        topic_id = db.get_or_create_topic(topic_name)
        db.link_page_topic(page_id, topic_id)

    if depth < max_depth:
        for link in links:
            link = normalize_url(link)
            if link in visited:
                continue
            if should_skip_url(link):
                continue
            if not is_allowed_domain(link):
                continue
            pages_found += crawl(link, max_depth, crawl_id, depth + 1, visited)

    return pages_found


def main():
    parser = argparse.ArgumentParser(description="EduSpider - crawl .edu/.org/.gov sites")
    parser.add_argument("url", help="Seed URL to start crawling from")
    parser.add_argument("--depth", type=int, default=10, help="Maximum crawl depth (default: 10)")
    args = parser.parse_args()

    seed_url = args.url
    if not seed_url.startswith(("http://", "https://")):
        seed_url = "https://" + seed_url

    if not is_allowed_domain(seed_url):
        print(f"Error: {seed_url} is not a .edu, .org, or .gov domain")
        return

    db.init_db()

    print(f"Starting crawl: {seed_url} (max depth: {args.depth})")
    crawl_id = db.create_crawl(seed_url, args.depth)

    try:
        pages_found = crawl(seed_url, args.depth, crawl_id)
        db.finish_crawl(crawl_id, pages_found, "done")
        print(f"\nCrawl complete. Pages found: {pages_found}")
    except KeyboardInterrupt:
        print("\nCrawl interrupted by user.")
        conn = db.get_conn()
        row = conn.execute("SELECT COUNT(*) as c FROM pages WHERE crawl_id = ?", (crawl_id,)).fetchone()
        conn.close()
        db.finish_crawl(crawl_id, row["c"], "interrupted")
        print(f"Saved {row['c']} pages before stopping.")
    except Exception as e:
        print(f"\nCrawl failed: {e}")
        db.finish_crawl(crawl_id, 0, "error")


if __name__ == "__main__":
    main()
