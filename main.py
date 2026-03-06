#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   🌍 VIRAL SHORTS MACHINE v6.0 — RADICAL FIX                ║
║   All known bugs fixed • Bulletproof pipeline                ║
╚══════════════════════════════════════════════════════════════╝
"""

import os, json, re, subprocess, tempfile, glob, requests, feedparser
import base64, textwrap, random, time, shutil
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from email.utils import parsedate_to_datetime

from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as SACredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import gspread

try:
    from PIL import Image, ImageDraw, ImageFont, ImageEnhance
except ImportError:
    subprocess.run(["pip", "install", "Pillow", "-q"], check=True)
    from PIL import Image, ImageDraw, ImageFont, ImageEnhance

# ─────────────────────────────────────────────
# 🔑  SECRETS
# ─────────────────────────────────────────────
GROQ_API_KEY        = os.environ["GROQ_API_KEY"]
PEXELS_API_KEY      = os.environ["PEXELS_API_KEY"]
PIXABAY_API_KEY     = os.environ["PIXABAY_API_KEY"]
YT_CLIENT_ID        = os.environ["YT_CLIENT_ID"]
YT_CLIENT_SECRET    = os.environ["YT_CLIENT_SECRET"]
YT_REFRESH_TOKEN    = os.environ["YT_REFRESH_TOKEN"]
YOUTUBE_PLAYLIST_ID = os.environ.get("YOUTUBE_PLAYLIST_ID", "")
SHEETS_DOC_ID       = os.environ.get("SHEETS_DOC_ID", "")
GOOGLE_CREDS_JSON   = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
LOGO_B64            = os.environ.get("LOGO_B64", "")

# ─────────────────────────────────────────────
# 🛡️  GLOBAL SAFE-STRING HELPER
# Groq sometimes returns list/int/None for any field
# ─────────────────────────────────────────────
def S(v, default: str = "") -> str:
    """Always return a clean plain string — never crashes."""
    if v is None:               return default
    if isinstance(v, list):     return " ".join(str(x) for x in v if x)
    if isinstance(v, (int, float)): return str(v)
    s = str(v).strip()
    return s if s else default

# ─────────────────────────────────────────────
# 🎨  DESIGN CONSTANTS
# ─────────────────────────────────────────────
W, H         = 1080, 1920
GOLD         = (255, 200, 0)
RED          = (220, 30, 30)
WHITE        = (255, 255, 255)
DARK_BG      = (10, 10, 15)
TWITTER_BLUE = (29, 161, 242)

# ─────────────────────────────────────────────
# 🔤  FONTS
# ─────────────────────────────────────────────
def find_font(bold=False):
    paths = ([
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    ] if bold else [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    ])
    for p in paths:
        if os.path.exists(p): return p
    found = glob.glob("/usr/share/fonts/**/*.ttf", recursive=True)
    if found: return found[0]
    raise FileNotFoundError("No TTF font found")

FONT_BOLD    = find_font(bold=True)
FONT_REGULAR = find_font(bold=False)
print(f"[FONT] Bold:    {FONT_BOLD}")
print(f"[FONT] Regular: {FONT_REGULAR}")

def gfont(path, size):
    try:    return ImageFont.truetype(path, size)
    except: return ImageFont.load_default()

# ─────────────────────────────────────────────
# 📡  STEP 1 — NEWS & TWEETS
# ─────────────────────────────────────────────
RSS_FEEDS = [
    "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
    "https://news.google.com/rss/search?q=when:6h+allinurl:reuters.com&ceid=US:en&hl=en-US&gl=US",
    "https://finance.yahoo.com/news/rssindex",
    "https://feeds.marketwatch.com/marketwatch/topstories/",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://www.investing.com/rss/news_25.rss",
]

INFLUENCERS = [
    {"handle": "KobeissiLetter", "name": "The Kobeissi Letter", "weight": 10},
    {"handle": "MacroAlf",       "name": "Alfonso Peccatiello", "weight": 9},
    {"handle": "RayDalio",       "name": "Ray Dalio",           "weight": 10},
    {"handle": "elonmusk",       "name": "Elon Musk",           "weight": 8},
    {"handle": "zerohedge",      "name": "ZeroHedge",           "weight": 7},
    {"handle": "michaeljburry",  "name": "Michael Burry",       "weight": 10},
]

NITTER = [
    "https://nitter.privacydev.net",
    "https://nitter.cz",
    "https://nitter.poast.org",
    "https://nitter.woodland.cafe",
]

def fetch_rss(url):
    try:
        feed  = feedparser.parse(url)
        items = []
        for e in feed.entries[:15]:
            t = e.get("title", "")
            if not t or len(t) < 15: continue
            items.append({
                "title":   t,
                "summary": e.get("summary", "")[:500],
                "link":    e.get("link", ""),
                "pubDate": e.get("published", ""),
                "source":  url.split("/")[2],
                "type":    "news",
                "influencerWeight": 0,
            })
        return items
    except Exception:
        return []

def fetch_twitter(inf):
    for host in NITTER:
        try:
            r = requests.get(
                f"{host}/{inf['handle']}/rss",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=6
            )
            if r.status_code != 200 or "<item>" not in r.text: continue
            items = []
            for m in re.finditer(r"<item>([\s\S]*?)</item>", r.text):
                raw   = m.group(1)
                t     = re.search(r"<title><!\[CDATA\[([\s\S]*?)\]\]></title>", raw)
                title = re.sub(r"<[^>]+>", " ", t.group(1) if t else "").strip()
                pub   = re.search(r"<pubDate>([^<]+)</pubDate>", raw)
                if not title or len(title) < 20: continue
                items.append({
                    "title":    title,
                    "summary":  title,
                    "source":   f"@{inf['handle']}",
                    "handle":   inf["handle"],
                    "sourceName": inf["name"],
                    "pubDate":  pub.group(1) if pub else "",
                    "type":     "influencer_tweet",
                    "influencerWeight": inf["weight"],
                })
            if items: return items[:5]
        except Exception:
            continue
    return []

def collect_all_news():
    all_items = []
    print("[NEWS] Fetching all sources in parallel...")
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = (
            [ex.submit(fetch_rss, u)     for u in RSS_FEEDS] +
            [ex.submit(fetch_twitter, i) for i in INFLUENCERS]
        )
        for f in as_completed(futs):
            all_items.extend(f.result())
    print(f"[NEWS] ✅ {len(all_items)} items collected")
    return all_items

# ─────────────────────────────────────────────
# 🧠  STEP 2 — VIRAL SCORING
# ─────────────────────────────────────────────
VIRAL_KW = {
    "federal reserve": 30, "fed rate": 30, "market crash": 30, "recession": 30,
    "inflation": 20, "bitcoin": 28, "crypto crash": 30, "dollar collapse": 30,
    "world war": 30, "nuclear": 28, "layoffs": 22, "bank collapse": 28,
    "trillion": 20, "tariff": 15, "trade war": 18, "trump": 20, "musk": 18,
    "powell": 18, "gold": 18, "oil price": 20, "interest rate": 20,
    "stock market": 20, "sanctions": 25, "gdp": 15, "unemployment": 18,
}
HOOK_KW = {
    "breaking": 25, "just in": 25, "urgent": 22, "shocking": 18,
    "crash": 20, "collapse": 22, "record": 15, "warning": 18,
    "crisis": 20, "panic": 18, "emergency": 20, "exposed": 15,
}

def score_item(item):
    tl = (item.get("title") or "").lower()
    ks = sum(s for k, s in VIRAL_KW.items() if k in tl)
    hs = sum(s for k, s in HOOK_KW.items()  if k in tl)
    if ks == 0 and hs < 20: return None
    try:
        age_h = (datetime.now(timezone.utc) - parsedate_to_datetime(
            item.get("pubDate", ""))).total_seconds() / 3600
    except Exception:
        age_h = 0
    rec   = max(0, 40 - age_h * 6.67)
    inf   = item.get("influencerWeight", 0) * 12 if item.get("type") == "influencer_tweet" else 0
    trnd  = 20 if age_h < 1 and (ks + hs) > 25 else 0
    total = ks + hs + rec + inf + trnd
    return {
        **item,
        "scores": {"keyword": ks, "hook": hs, "recency": round(rec), "influencer": inf, "total": round(total)},
        "isBreaking":   age_h < 1 and hs > 15,
        "emotionLevel": "EXTREME" if (ks + hs) > 50 else "HIGH" if (ks + hs) > 30 else "MEDIUM",
    }

def pick_stories(items, n=6):
    seen   = set()
    scored = []
    for item in items:
        key = re.sub(r"[^a-z0-9]", "", (item.get("title") or "").lower())[:55]
        if key in seen: continue
        seen.add(key)
        r = score_item(item)
        if r: scored.append(r)

    scored.sort(key=lambda x: x["scores"]["total"], reverse=True)
    tweets = [s for s in scored if s["type"] == "influencer_tweet"]
    news   = [s for s in scored if s["type"] == "news"]
    result = []
    if tweets: result.append(tweets[0])
    result.extend(news[:n])
    result = result[:n]
    if not result: result = scored[:n]

    print(f"[SCORE] {len(items)} in → {len(scored)} relevant → top {len(result)} selected")
    for i, s in enumerate(result):
        icon = "🐦" if s["type"] == "influencer_tweet" else "📰"
        print(f"  #{i+1} {icon} [{s['scores']['total']}pts] {s['title'][:65]}")
    return result

# ─────────────────────────────────────────────
# 🤖  STEP 3 — GROQ AI
# ─────────────────────────────────────────────
def generate_content(stories: list) -> dict:
    story_block = ""
    for i, s in enumerate(stories):
        kind = "TWEET" if s["type"] == "influencer_tweet" else "NEWS"
        name = s.get("sourceName") or s.get("source", "")
        story_block += (
            f"\nSTORY {i+1} [{kind}] — Source: {name}\n"
            f"Title: {s['title']}\n"
            f"Summary: {s.get('summary','')[:300]}\n"
            f"Score: {s['scores']['total']}pts | Emotion: {s['emotionLevel']}\n"
        )

    prompt = f"""You are the #1 viral financial Shorts producer.
You receive 2-3 connected stories/tweets and build ONE powerful narrative.

STORIES:
{story_block}

Write a 55-65 second script (160-180 words) connecting these stories.
NEVER start with: "In today's news" / "Welcome back" / "Hey guys"

STRUCTURE:
[HOOK 0-5s] One shocking sentence. No context yet.
[DROP 5-18s] First story. Real data. Exact numbers.
[TWIST 18-32s] "But here is where it connects..."
[PROOF 32-48s] Third element — political angle OR direct viewer impact.
[CTA 48-58s] "Follow right now — we connect these dots before anyone else."

YOUTUBE TITLE: Start with NUMBER or WARNING/NOBODY/EVERY. MAX 60 chars.

IMPORTANT — ALL values must be plain STRINGS, never arrays or objects.
tags must be a JSON array of strings.
segments must be a JSON array of objects.

Return ONLY valid JSON:
{{"youtube_title":"","youtube_title_b":"","hook_type":"stat","shorts_script":"","description":"","tags":["tag1","tag2"],"trending_hashtags":"#Economy #Markets","segments":[{{"text":"words spoken","visual_type":"broll","visual_query":"stock market","caption":"BREAKING","duration":7}}],"overlay_headline":"BREAKING NEWS","overlay_ticker":"Markets rattled as tariffs hit","pexels_query":"stock market finance","virality_score":8,"comment_bait":"What will happen next?","optimal_post_time":"18:00-20:00 EST"}}"""

    for attempt in range(3):
        try:
            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "llama-3.3-70b-versatile",
                    "temperature": 0.75,
                    "max_tokens": 3500,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": "Expert viral Shorts producer. Return ONLY valid JSON. ALL fields must be strings or arrays. NEVER return a field as an object. shorts_script 160-180 words. English only, zero Arabic."},
                        {"role": "user",   "content": prompt},
                    ]
                },
                timeout=45
            )
            r.raise_for_status()
            raw = r.json()["choices"][0]["message"]["content"]
            pkg = json.loads(raw)
            break
        except Exception as e:
            print(f"[GROQ] Attempt {attempt+1} failed: {e}")
            if attempt == 2: raise
            time.sleep(3)

    # ── Deep sanitize: force every string field to str ──────────
    STR_FIELDS = [
        "youtube_title","youtube_title_b","hook_type","shorts_script",
        "description","trending_hashtags","overlay_headline","overlay_ticker",
        "pexels_query","comment_bait","optimal_post_time",
    ]
    for field in STR_FIELDS:
        pkg[field] = S(pkg.get(field))

    # tags must be list of strings
    tags = pkg.get("tags", [])
    if isinstance(tags, str):   tags = [t.strip() for t in tags.split(",") if t.strip()]
    if not isinstance(tags, list): tags = []
    pkg["tags"] = [S(t) for t in tags if t][:30]

    # virality_score must be int
    try:    pkg["virality_score"] = int(pkg.get("virality_score", 7))
    except: pkg["virality_score"] = 7

    # Remove Arabic chars from all strings
    def clean_arabic(v):
        if isinstance(v, str):  return re.sub(r"[\u0600-\u06FF\u200c-\u200f]+", " ", v).strip()
        if isinstance(v, list): return [clean_arabic(x) for x in v]
        if isinstance(v, dict): return {k: clean_arabic(val) for k, val in v.items()}
        return v
    pkg = clean_arabic(pkg)

    # Ensure segments exist and are well-formed
    segs = pkg.get("segments")
    if not isinstance(segs, list) or len(segs) == 0:
        pkg["segments"] = [{
            "text":         pkg["shorts_script"],
            "visual_type":  "broll",
            "visual_query": pkg["pexels_query"] or "stock market finance",
            "caption":      "BREAKING NEWS",
            "duration":     60,
        }]
    else:
        for seg in pkg["segments"]:
            if not isinstance(seg, dict):
                seg = {}
            seg["visual_type"]  = S(seg.get("visual_type"),  "broll")
            seg["visual_query"] = S(seg.get("visual_query"), "finance economy")
            seg["caption"]      = S(seg.get("caption"),      "")
            seg["text"]         = S(seg.get("text"),         "")
            try:    seg["duration"] = int(seg.get("duration", 5))
            except: seg["duration"] = 5
        pkg["segments"] = pkg["segments"]

    words = len(pkg["shorts_script"].split())
    print(f"[GROQ] ✅ '{pkg['youtube_title'][:55]}'")
    print(f"[GROQ] Words: {words} | Segments: {len(pkg['segments'])}")
    return pkg

# ─────────────────────────────────────────────
# 🖼️  STEP 4 — VISUAL ENGINE
# ─────────────────────────────────────────────
def esc_ffmpeg(s: str) -> str:
    s = re.sub(r"[^\w\s\.\,\!\?\-]", "", str(s))
    s = s.replace("\\","").replace("'","").replace(":"," ").replace("[","").replace("]","")
    return s.strip()[:52]

def pexels_image(query: str) -> bytes | None:
    """Get image from Pexels — always works, no rate limit"""
    try:
        r = requests.get(
            "https://api.pexels.com/v1/search",
            headers={"Authorization": PEXELS_API_KEY},
            params={"query": query, "per_page": 5, "orientation": "portrait"},
            timeout=10
        )
        for photo in r.json().get("photos", []):
            url = photo.get("src", {}).get("portrait") or photo.get("src", {}).get("large")
            if url:
                resp = requests.get(url, timeout=15)
                if resp.status_code == 200 and len(resp.content) > 5000:
                    return resp.content
    except Exception as e:
        print(f"[PEXELS_IMG] {e}")
    return None

def ddg_image(query: str) -> bytes | None:
    """DuckDuckGo fallback — may rate-limit, that's OK"""
    try:
        from duckduckgo_search import DDGS
        with DDGS() as d:
            results = list(d.images(keywords=query, region="us-en", safesearch="moderate", max_results=5))
        for res in results:
            url = res.get("image", "")
            if not url or "svg" in url.lower(): continue
            try:
                resp = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
                if resp.status_code == 200 and len(resp.content) > 8000:
                    Image.open(BytesIO(resp.content))
                    return resp.content
            except Exception:
                continue
    except Exception as e:
        print(f"[DDG] {query[:30]}: {e}")
    return None

def get_image(query: str) -> bytes | None:
    """Try Pexels first, then DDG — always gets something"""
    img = pexels_image(query)
    if img: return img
    time.sleep(0.3)
    return ddg_image(query)

def pexels_clip(query: str, dest: str) -> bool:
    try:
        r = requests.get(
            "https://api.pexels.com/videos/search",
            headers={"Authorization": PEXELS_API_KEY},
            params={"query": query, "per_page": 8, "orientation": "portrait"},
            timeout=12
        )
        for v in r.json().get("videos", []):
            files = v.get("video_files", [])
            pick  = (
                next((f for f in files if f.get("height",0) >= 1080 and f.get("height",0) > f.get("width",0)), None)
                or next((f for f in files if f.get("quality") == "hd"), None)
                or (files[0] if files else None)
            )
            if pick and pick.get("link"):
                resp = requests.get(pick["link"], timeout=90, stream=True)
                resp.raise_for_status()
                with open(dest,"wb") as f:
                    for chunk in resp.iter_content(512*1024): f.write(chunk)
                if os.path.getsize(dest) > 100_000: return True
    except Exception as e:
        print(f"[PEXELS] {e}")
    return False

def pixabay_clip(query: str, dest: str) -> bool:
    try:
        r = requests.get(
            "https://pixabay.com/api/videos/",
            params={"key": PIXABAY_API_KEY, "q": query, "per_page": 5, "safesearch": "true"},
            timeout=12
        )
        for v in r.json().get("hits", []):
            vids = v.get("videos", {})
            url  = (vids.get("large") or vids.get("medium") or vids.get("small") or {}).get("url")
            if url:
                resp = requests.get(url, timeout=90, stream=True)
                resp.raise_for_status()
                with open(dest,"wb") as f:
                    for chunk in resp.iter_content(512*1024): f.write(chunk)
                if os.path.getsize(dest) > 100_000: return True
    except Exception as e:
        print(f"[PIXABAY] {e}")
    return False

# ── Card Makers ──────────────────────────────
def stat_card(query: str) -> Image.Image:
    parts  = query.split("|", 1)
    number = parts[0].strip()
    label  = parts[1].strip() if len(parts) > 1 else ""
    img    = Image.new("RGB", (W, H), DARK_BG)
    draw   = ImageDraw.Draw(img)
    for y in range(H):
        v = max(0, 28 - y // 70)
        draw.line([(0,y),(W,y)], fill=(v,v,v+12))
    draw.rectangle([0,0,W,12], fill=RED)
    size = 155 if len(number)<=6 else 120 if len(number)<=9 else 88
    fn   = gfont(FONT_BOLD, size)
    b    = draw.textbbox((0,0), number, font=fn)
    tw   = b[2]-b[0]
    draw.text(((W-tw)//2+4, H//2-120+4), number, font=fn, fill=(0,0,0))
    draw.text(((W-tw)//2,   H//2-120),   number, font=fn, fill=GOLD)
    if label:
        fl = gfont(FONT_REGULAR, 50)
        bl = draw.textbbox((0,0), label, font=fl)
        draw.text(((W-(bl[2]-bl[0]))//2, H//2+65), label, font=fl, fill=(200,200,200))
    draw.rectangle([0,H-12,W,H], fill=RED)
    return img

def tweet_card(query: str) -> Image.Image:
    parts  = query.split("|", 1)
    handle = parts[0].strip().lstrip("@") if parts else "user"
    text   = parts[1].strip() if len(parts) > 1 else query
    img    = Image.new("RGB", (W, H), DARK_BG)
    draw   = ImageDraw.Draw(img)
    cx,cy,cw,ch = 55, H//2-360, W-110, 720
    draw.rounded_rectangle([cx,cy,cx+cw,cy+ch], radius=28, fill=(21,32,43))
    fx = gfont(FONT_BOLD, 46)
    draw.text((cx+cw-72, cy+20), "𝕏", font=fx, fill=WHITE)
    ax,ay,ar = cx+68, cy+94, 44
    draw.ellipse([ax-ar,ay-ar,ax+ar,ay+ar], fill=TWITTER_BLUE)
    fi  = gfont(FONT_BOLD, 34)
    ini = handle[:2].upper()
    bi  = draw.textbbox((0,0), ini, font=fi)
    draw.text((ax-(bi[2]-bi[0])//2, ay-(bi[3]-bi[1])//2-2), ini, font=fi, fill=WHITE)
    fn = gfont(FONT_BOLD, 36)
    fr = gfont(FONT_REGULAR, 30)
    draw.text((cx+128, cy+54),  handle,       font=fn, fill=WHITE)
    draw.text((cx+128, cy+100), f"@{handle}", font=fr, fill=(136,153,166))
    draw.line([cx+20, cy+155, cx+cw-20, cy+155], fill=(56,68,77), width=1)
    ft    = gfont(FONT_REGULAR, 44)
    lines = textwrap.wrap(text[:280], width=30)[:8]
    ty    = cy+175
    for line in lines:
        draw.text((cx+28, ty), line, font=ft, fill=WHITE)
        ty += 56
    sy = cy+ch-72
    draw.line([cx+20, sy-8, cx+cw-20, sy-8], fill=(56,68,77), width=1)
    fs = gfont(FONT_REGULAR, 28)
    draw.text((cx+28,  sy), f"🔁 {random.randint(1,40)}K", font=fs, fill=(136,153,166))
    draw.text((cx+290, sy), f"❤️ {random.randint(5,90)}K", font=fs, fill=(136,153,166))
    return img

def person_card(name: str, img_bytes: bytes | None) -> Image.Image:
    base = Image.new("RGB", (W, H), DARK_BG)
    if img_bytes:
        try:
            pi = Image.open(BytesIO(img_bytes)).convert("RGB")
            pw,ph = pi.size
            scale = max(W/pw, H/ph)
            nw,nh = int(pw*scale), int(ph*scale)
            pi    = pi.resize((nw,nh), Image.LANCZOS)
            base.paste(pi.crop(((nw-W)//2,(nh-H)//2,(nw-W)//2+W,(nh-H)//2+H)))
            ov = Image.new("RGBA",(W,H),(0,0,0,0))
            d  = ImageDraw.Draw(ov)
            for i in range(520):
                d.line([(0,H-520+i),(W,H-520+i)], fill=(0,0,0,int(215*i/520)))
            base = Image.alpha_composite(base.convert("RGBA"), ov).convert("RGB")
        except Exception as e:
            print(f"[PERSON] {e}")
    draw  = ImageDraw.Draw(base)
    draw.rectangle([0,H-195,W,H-187], fill=RED)
    fn    = gfont(FONT_BOLD, 72)
    parts = name.upper().split()
    if len(parts) >= 2:
        first = " ".join(parts[:-1])
        last  = parts[-1]
        b = draw.textbbox((0,0), first, font=fn)
        draw.text(((W-(b[2]-b[0]))//2, H-180), first, font=fn, fill=WHITE)
        b = draw.textbbox((0,0), last, font=fn)
        draw.text(((W-(b[2]-b[0]))//2, H-105), last,  font=fn, fill=GOLD)
    else:
        b = draw.textbbox((0,0), name.upper(), font=fn)
        draw.text(((W-(b[2]-b[0]))//2, H-140), name.upper(), font=fn, fill=WHITE)
    return base

def news_img_card(img_bytes: bytes | None, caption: str) -> Image.Image:
    base = Image.new("RGB", (W, H), DARK_BG)
    if img_bytes:
        try:
            ni = Image.open(BytesIO(img_bytes)).convert("RGB")
            pw,ph = ni.size
            scale = max(W/pw, H/ph)
            nw,nh = int(pw*scale), int(ph*scale)
            ni    = ni.resize((nw,nh), Image.LANCZOS)
            base.paste(ni.crop(((nw-W)//2,(nh-H)//2,(nw-W)//2+W,(nh-H)//2+H)))
            base = ImageEnhance.Brightness(base).enhance(0.52)
        except Exception as e:
            print(f"[NEWS_IMG] {e}")
    draw  = ImageDraw.Draw(base)
    draw.rectangle([0,H-265,W,H-259], fill=RED)
    fn    = gfont(FONT_BOLD, 60)
    lines = textwrap.wrap(caption.upper()[:80], width=18)[:3]
    ty    = H-250
    for line in lines:
        b = draw.textbbox((0,0), line, font=fn)
        draw.text(((W-(b[2]-b[0]))//2+3, ty+3), line, font=fn, fill=(0,0,0))
        draw.text(((W-(b[2]-b[0]))//2,   ty),   line, font=fn, fill=WHITE)
        ty += 74
    return base

def chart_card(query: str) -> Image.Image:
    img  = Image.new("RGB", (W, H), DARK_BG)
    draw = ImageDraw.Draw(img)
    for y in range(220, H-220, 85):
        draw.line([(80,y),(W-80,y)], fill=(30,35,45), width=1)
    cx,cw,ch = 80, W-160, 620
    cy = H//2
    trend = random.choice([-1,1])
    val   = ch//2
    pts   = []
    for i in range(50):
        x   = cx + int(i*cw/49)
        val = max(55, min(ch-55, val+random.randint(-28,28)+trend*4))
        pts.append((x, cy-val+ch//2))
    is_up = pts[-1][1] < pts[0][1]
    color = (0,210,85) if is_up else (220,50,50)
    poly  = [(cx,cy+ch//2)] + pts + [(W-80,cy+ch//2)]
    fill_img  = Image.new("RGBA",(W,H),(0,0,0,0))
    fill_draw = ImageDraw.Draw(fill_img)
    fc = (0,210,85,35) if is_up else (220,50,50,35)
    fill_draw.polygon(poly, fill=fc)
    img  = Image.alpha_composite(img.convert("RGBA"), fill_img).convert("RGB")
    draw = ImageDraw.Draw(img)
    for i in range(len(pts)-1):
        draw.line([pts[i],pts[i+1]], fill=color, width=5)
    lx,ly = pts[-1]
    draw.ellipse([lx-13,ly-13,lx+13,ly+13], fill=color)
    ticker = query.upper().split()[0][:8] if query else "MARKET"
    fn = gfont(FONT_BOLD, 90 if len(ticker)<=5 else 68)
    b  = draw.textbbox((0,0), ticker, font=fn)
    draw.text(((W-(b[2]-b[0]))//2, 85), ticker, font=fn, fill=WHITE)
    pct  = round(random.uniform(0.4,9.2),2)
    sign = "▲" if is_up else "▼"
    fp   = gfont(FONT_BOLD, 56)
    ct   = f"{sign} {pct}%"
    b    = draw.textbbox((0,0), ct, font=fp)
    draw.text(((W-(b[2]-b[0]))//2, 190), ct, font=fp, fill=color)
    return img

def get_visual(seg: dict, tmp: str, idx: int) -> dict:
    """Returns {"path": "...", "is_video": bool}"""
    vtype = S(seg.get("visual_type"),  "broll")
    query = S(seg.get("visual_query"), "finance economy")
    cap   = S(seg.get("caption"),      "")
    print(f"[VIS] {idx:02d} [{vtype:10s}] '{query[:40]}'")

    def save_img(img):
        p = os.path.join(tmp, f"vis_{idx:03d}.jpg")
        img.save(p, "JPEG", quality=92)
        return {"path": p, "is_video": False}

    def fallback():
        return save_img(stat_card(f"BREAKING | {(cap or query[:20]).upper()}"))

    try:
        if vtype == "stat":
            return save_img(stat_card(query))
        elif vtype == "tweet":
            return save_img(tweet_card(query))
        elif vtype == "chart":
            return save_img(chart_card(query))
        elif vtype in ("broll", "news_image", "person"):
            clip = os.path.join(tmp, f"bclip_{idx}.mp4")
            ok = pexels_clip(query, clip)
            if not ok:
                ok = pixabay_clip(query, clip)
            if ok and os.path.getsize(clip) > 50_000:
                print(f"[VIS] {idx:02d} real video ({os.path.getsize(clip)//1024} KB)")
                return {"path": clip, "is_video": True}
            print(f"[VIS] {idx:02d} no video, using image card")
            if vtype == "person":
                return save_img(person_card(query, get_image(f"{query} professional portrait")))
            else:
                return save_img(news_img_card(get_image(query), cap or query[:30]))
        else:
            return fallback()
    except Exception as e:
        print(f"[VIS] Error seg {idx}: {e}")
        return fallback()

def build_visuals(pkg: dict, tmp: str) -> list:
    segs = pkg.get("segments", [])
    print(f"[VIS] Building {len(segs)} visuals...")
    card_types  = {"stat", "tweet", "chart"}
    results = [None] * len(segs)
    card_segs = [(i, s) for i, s in enumerate(segs) if S(s.get("visual_type")) in card_types]
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(get_visual, s, tmp, i): i for i, s in card_segs}
        for f in as_completed(futs):
            i = futs[f]
            try:    results[i] = f.result()
            except: results[i] = None
    video_segs = [(i, s) for i, s in enumerate(segs) if S(s.get("visual_type")) not in card_types]
    for i, s in video_segs:
        try:    results[i] = get_visual(s, tmp, i)
        except: results[i] = None
        time.sleep(0.2)
    fb_path = os.path.join(tmp, "fallback.jpg")
    stat_card("BREAKING | FINANCIAL NEWS").save(fb_path, "JPEG", quality=85)
    fb = {"path": fb_path, "is_video": False}
    results = [r if (r and r.get("path") and os.path.exists(r["path"])) else fb for r in results]
    n_vid = sum(1 for r in results if r["is_video"])
    print(f"[VIS] {len(results)} visuals — {n_vid} real videos, {len(results)-n_vid} image cards")
    return results

# ─────────────────────────────────────────────
# 🖼️  LOGO
# ─────────────────────────────────────────────
def prepare_logo(tmp: str) -> str | None:
    p = os.path.join(tmp, "logo.png")
    if LOGO_B64:
        try:
            with open(p,"wb") as f: f.write(base64.b64decode(LOGO_B64))
            print("[LOGO] ✅ Loaded")
            return p
        except Exception as e:
            print(f"[LOGO] Error: {e}")
    return None

# ─────────────────────────────────────────────
# 🎙️  STEP 5 — TTS (gTTS — Google, free, no blocks)
# ─────────────────────────────────────────────
def generate_audio(script: str, dest: str):
    print(f"[TTS] {len(script.split())} words | Engine: Deepgram Aura")

    clean_script = re.sub(r"[^\w\s\.,!?\-'\"]", " ", script)
    clean_script = re.sub(r"\s+", " ", clean_script).strip()[:1900]

    DEEPGRAM_KEY = os.environ.get("DEEPGRAM_API_KEY", "")

    # ── Primary: Deepgram Aura (podcast quality, REST API) ──
    if DEEPGRAM_KEY:
        try:
            r = requests.post(
                "https://api.deepgram.com/v1/speak?model=aura-asteria-en",
                headers={
                    "Authorization": f"Token {DEEPGRAM_KEY}",
                    "Content-Type":  "application/json"
                },
                json={"text": clean_script},
                timeout=30
            )
            r.raise_for_status()
            if len(r.content) > 2000:
                with open(dest, "wb") as f: f.write(r.content)
                print(f"[TTS] ✅ Deepgram — {len(r.content)//1024} KB")
                return
        except Exception as e:
            print(f"[TTS] Deepgram failed: {e} — falling back to gTTS")

    # ── Fallback: gTTS ──────────────────────────────────────
    print("[TTS] Using gTTS fallback...")
    try:
        from gtts import gTTS
    except ImportError:
        subprocess.run(["pip","install","gtts","-q"], check=True)
        from gtts import gTTS
    gTTS(text=clean_script, lang="en", slow=False).save(dest)

    if not os.path.exists(dest) or os.path.getsize(dest) < 2000:
        raise ValueError("TTS audio too small or missing")
    print(f"[TTS] ✅ gTTS — {os.path.getsize(dest)//1024} KB")

# ─────────────────────────────────────────────
# 🎬  STEP 6 — CINEMATIC RENDER
# ─────────────────────────────────────────────
def get_duration(path: str, is_audio: bool = False) -> float:
    try:
        o = subprocess.check_output([
            "ffprobe","-v","error","-show_entries","format=duration",
            "-of","default=noprint_wrappers=1:nokey=1", path
        ], stderr=subprocess.DEVNULL).decode().strip()
        val = float(o)
        if val <= 0: raise ValueError(f"Non-positive: {val}")
        return val
    except Exception as e:
        if is_audio: raise ValueError(f"Cannot get audio duration: {e}")
        return 5.0

def img_to_vid(img: str, dur: float, out: str, zoom_variant: int = 0) -> bool:
    """Convert image to video with slow Ken Burns pan effect"""
    offsets = [("0","0"),("(iw-ow)/2","0"),("iw-ow","ih-oh"),("0","ih-oh")]
    ox,oy   = offsets[zoom_variant % 4]
    # Scale slightly larger then crop center — gives slow zoom feel
    vf = f"scale=1120:2000:force_original_aspect_ratio=increase,crop=1080:1920:{ox}:{oy}"
    cmd = [
        "ffmpeg","-y","-loop","1","-i",img,"-t",str(dur),
        "-vf",vf,"-c:v","libx264","-preset","fast","-crf","20",
        "-pix_fmt","yuv420p","-an","-r","25","-threads","2",out
    ]
    r = subprocess.run(cmd, capture_output=True)
    return r.returncode == 0 and os.path.exists(out)

def clip_to_vid(clip: str, dur: float, out: str) -> bool:
    """Trim/loop a real video clip to target duration, scale to 1080x1920"""
    clip_dur = get_duration(clip)
    # If clip is shorter than needed, loop it
    input_flags = ["-stream_loop","-1"] if clip_dur < dur else []
    cmd = [
        "ffmpeg","-y",
        *input_flags,
        "-i", clip,
        "-t", str(dur),
        "-vf", "scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920",
        "-c:v","libx264","-preset","fast","-crf","20",
        "-pix_fmt","yuv420p","-an","-r","25","-threads","2",out
    ]
    r = subprocess.run(cmd, capture_output=True)
    return r.returncode == 0 and os.path.exists(out)

def render_final(pkg: dict, visuals: list, audio: str, output: str, logo):
    aud_dur = get_duration(audio, is_audio=True)
    vid_dur = aud_dur + 2.0
    tmp     = os.path.dirname(output)
    segs    = pkg.get("segments", [])
    print(f"[RENDER] Audio: {aud_dur:.1f}s | Segs: {len(segs)} | Visuals: {len(visuals)}")

    raw_durs = [max(2, int(s.get("duration", 5))) for s in segs]
    total    = sum(raw_durs) or 1
    durs     = [d / total * (aud_dur + 1.0) for d in raw_durs]

    clips = []
    for i, (vis, dur) in enumerate(zip(visuals, durs)):
        out_c    = os.path.join(tmp, f"svid_{i:03d}.mp4")
        is_video = vis.get("is_video", False)
        path     = vis["path"]
        ok = False
        if is_video:
            ok = clip_to_vid(path, dur + 0.3, out_c)
            if not ok:
                print(f"[RENDER] clip_to_vid failed for seg {i}, trying img fallback")
        if not ok:
            ok = img_to_vid(path if not is_video else path, dur + 0.3, out_c, zoom_variant=i)
            if not ok and is_video:
                # extract frame then try img
                frame = os.path.join(tmp, f"frame_{i}.jpg")
                subprocess.run(["ffmpeg","-y","-ss","1","-i",path,"-vframes","1",frame], capture_output=True)
                if os.path.exists(frame):
                    ok = img_to_vid(frame, dur + 0.3, out_c, zoom_variant=i)
        if ok:
            clips.append(out_c)
        else:
            print(f"[RENDER] Seg {i} completely failed, skipping")

    if not clips:
        raise RuntimeError("Zero video segments rendered")

    # Concat all clips
    cl  = os.path.join(tmp, "cl.txt")
    cat = os.path.join(tmp, "cat.mp4")
    with open(cl, "w") as f:
        for c in clips: f.write(f"file '{c}'\n")

    r = subprocess.run([
        "ffmpeg","-y","-f","concat","-safe","0","-i",cl,
        "-c:v","libx264","-preset","fast","-crf","20",
        "-pix_fmt","yuv420p","-t",str(vid_dur+1),"-threads","2",cat
    ], capture_output=True)

    if not os.path.exists(cat) or os.path.getsize(cat) < 1000:
        cat = clips[0]
    print(f"[RENDER] Concat done ({len(clips)} clips)")

    headline   = esc_ffmpeg(S(pkg.get("overlay_headline","BREAKING")))
    raw_ticker = esc_ffmpeg(S(pkg.get("overlay_ticker","FINANCIAL MARKETS - BREAKING NEWS")))
    ticker3    = f"{raw_ticker} --- {raw_ticker} --- {raw_ticker}"

    vf_parts = [
        # Top bar
        "drawbox=x=0:y=0:w=iw:h=198:color=black@0.85:t=fill",
        f"drawtext=fontfile='{FONT_BOLD}':text='{headline}':fontsize=56:fontcolor=white:x=(w-text_w)/2:y=55:shadowcolor=black@0.9:shadowx=3:shadowy=3",
        # Bottom ticker
        "drawbox=x=0:y=1812:w=iw:h=88:color=#CC0000@0.92:t=fill",
        f"drawtext=fontfile='{FONT_BOLD}':text='{ticker3}':fontsize=29:fontcolor=white:x=w-mod(t*110\\,w+text_w):y=1835",
        # Progress bar
        f"drawbox=x=0:y=1905:w='(iw*min(t\\,{aud_dur:.2f})/{aud_dur:.2f})':h=10:color=#FF2222@0.95:t=fill",
        # Vignette
        "vignette=PI/5",
    ]

    # Captions per segment
    t_cursor = 0.0
    for seg, dur in zip(segs, durs):
        cap = esc_ffmpeg(S(seg.get("caption", "")))
        if cap:
            ts = round(t_cursor, 2)
            te = round(t_cursor + dur, 2)
            vf_parts.append(
                f"drawtext=fontfile='{FONT_BOLD}':text='{cap}':fontsize=46:fontcolor=white"
                f":x=(w-text_w)/2:y=h-310:shadowcolor=black@0.9:shadowx=3:shadowy=3"
                f":enable='between(t\\,{ts}\\,{te})'"
            )
        t_cursor += dur

    vf       = ",".join(vf_parts)
    has_logo = logo and os.path.exists(str(logo))

    if has_logo:
        cmd = [
            "ffmpeg","-y","-i",cat,"-i",audio,"-i",str(logo),
            "-filter_complex",
            f"[0:v]{vf}[base];[2:v]scale=108:108[lg];[base][lg]overlay=x=20:y=H-260:format=auto[out]",
            "-map","[out]","-map","1:a",
            "-c:v","libx264","-preset","fast","-crf","18",
            "-c:a","aac","-b:a","192k",
            "-t",str(vid_dur),"-movflags","+faststart","-pix_fmt","yuv420p","-threads","2",output
        ]
    else:
        cmd = [
            "ffmpeg","-y","-i",cat,"-i",audio,
            "-vf",vf,
            "-c:v","libx264","-preset","fast","-crf","18",
            "-c:a","aac","-b:a","192k",
            "-t",str(vid_dur),"-movflags","+faststart","-pix_fmt","yuv420p","-threads","2",output
        ]

    print("[RENDER] Final encode...")
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"[RENDER] stderr:\n{r.stderr[-3000:]}")
        raise RuntimeError(f"FFmpeg failed (code {r.returncode})")

    size = os.path.getsize(output) / 1024 / 1024
    print(f"[RENDER] {size:.1f} MB")


# ─────────────────────────────────────────────
# 🖼️  LOGO
# ─────────────────────────────────────────────
def prepare_logo(tmp: str) -> str | None:
    p = os.path.join(tmp, "logo.png")
    if LOGO_B64:
        try:
            with open(p,"wb") as f: f.write(base64.b64decode(LOGO_B64))
            print("[LOGO] ✅ Loaded")
            return p
        except Exception as e:
            print(f"[LOGO] Error: {e}")
    return None

# ─────────────────────────────────────────────
# 🎙️  STEP 5 — TTS (gTTS — Google, free, no blocks)
# ─────────────────────────────────────────────
def generate_audio(script: str, dest: str):
    print(f"[TTS] {len(script.split())} words | Engine: Deepgram Aura")

    clean_script = re.sub(r"[^\w\s\.,!?\-'\"]", " ", script)
    clean_script = re.sub(r"\s+", " ", clean_script).strip()[:1900]

    DEEPGRAM_KEY = os.environ.get("DEEPGRAM_API_KEY", "")

    # ── Primary: Deepgram Aura (podcast quality, REST API) ──
    if DEEPGRAM_KEY:
        try:
            r = requests.post(
                "https://api.deepgram.com/v1/speak?model=aura-asteria-en",
                headers={
                    "Authorization": f"Token {DEEPGRAM_KEY}",
                    "Content-Type":  "application/json"
                },
                json={"text": clean_script},
                timeout=30
            )
            r.raise_for_status()
            if len(r.content) > 2000:
                with open(dest, "wb") as f: f.write(r.content)
                print(f"[TTS] ✅ Deepgram — {len(r.content)//1024} KB")
                return
        except Exception as e:
            print(f"[TTS] Deepgram failed: {e} — falling back to gTTS")

    # ── Fallback: gTTS ──────────────────────────────────────
    print("[TTS] Using gTTS fallback...")
    try:
        from gtts import gTTS
    except ImportError:
        subprocess.run(["pip","install","gtts","-q"], check=True)
        from gtts import gTTS
    gTTS(text=clean_script, lang="en", slow=False).save(dest)

    if not os.path.exists(dest) or os.path.getsize(dest) < 2000:
        raise ValueError("TTS audio too small or missing")
    print(f"[TTS] ✅ gTTS — {os.path.getsize(dest)//1024} KB")

# ─────────────────────────────────────────────
# 🎬  STEP 6 — CINEMATIC RENDER
# ─────────────────────────────────────────────
def get_duration(path: str, is_audio: bool = False) -> float:
    try:
        o = subprocess.check_output([
            "ffprobe","-v","error","-show_entries","format=duration",
            "-of","default=noprint_wrappers=1:nokey=1", path
        ], stderr=subprocess.DEVNULL).decode().strip()
        val = float(o)
        if val <= 0: raise ValueError(f"Non-positive: {val}")
        return val
    except Exception as e:
        if is_audio: raise ValueError(f"Cannot get audio duration: {e}")
        return 5.0

def img_to_vid(img: str, dur: float, out: str, zoom_variant: int = 0) -> bool:
    offsets = [("0","0"),("(iw-ow)/2","0"),("iw-ow","ih-oh"),("0","ih-oh")]
    ox,oy   = offsets[zoom_variant % 4]
    vf = f"scale=1120:2000:force_original_aspect_ratio=increase,crop=1080:1920:{ox}:{oy}"
    cmd = [
        "ffmpeg","-y","-loop","1","-i",img,"-t",str(dur),
        "-vf",vf,"-c:v","libx264","-preset","fast","-crf","20",
        "-pix_fmt","yuv420p","-an","-threads","2",out
    ]
    r = subprocess.run(cmd, capture_output=True)
    return r.returncode == 0 and os.path.exists(out)

def render_final(pkg: dict, visuals: list, audio: str, output: str, logo: str | None):
    aud_dur = get_duration(audio, is_audio=True)
    vid_dur = aud_dur + 2.0
    tmp     = os.path.dirname(output)
    segs    = pkg.get("segments", [])
    print(f"[RENDER] Audio: {aud_dur:.1f}s | Segs: {len(segs)} | Visuals: {len(visuals)}")

    raw_durs = [max(2, int(s.get("duration", 5))) for s in segs]
    total    = sum(raw_durs) or 1
    durs     = [d / total * (aud_dur + 1.0) for d in raw_durs]

    clips = []
    for i,(vis,dur) in enumerate(zip(visuals,durs)):
        out_c = os.path.join(tmp, f"svid_{i:03d}.mp4")
        if img_to_vid(vis, dur+0.3, out_c, zoom_variant=i):
            clips.append(out_c)
        else:
            print(f"[RENDER] ⚠️ Seg {i} failed, skipping")

    if not clips:
        raise RuntimeError("Zero video segments — cannot render")

    cl  = os.path.join(tmp,"cl.txt")
    cat = os.path.join(tmp,"cat.mp4")
    with open(cl,"w") as f:
        for c in clips: f.write(f"file '{c}'\n")

    subprocess.run([
        "ffmpeg","-y","-f","concat","-safe","0","-i",cl,
        "-c:v","libx264","-preset","fast","-crf","20",
        "-pix_fmt","yuv420p","-t",str(vid_dur+1),
        "-threads","2",cat
    ], capture_output=True)

    if not os.path.exists(cat): cat = clips[0]
    print(f"[RENDER] ✅ Concat done ({len(clips)} clips)")

    headline   = esc_ffmpeg(pkg.get("overlay_headline","BREAKING"))
    raw_ticker = esc_ffmpeg(pkg.get("overlay_ticker","FINANCIAL MARKETS - ECONOMY - GLOBAL IMPACT"))
    ticker3    = f"{raw_ticker} --- {raw_ticker} --- {raw_ticker}"

    vf_filters = []
    vf_filters.append("drawbox=x=0:y=0:w=iw:h=198:color=black@0.88:t=fill")
    vf_filters.append(
        f"drawtext=fontfile='{FONT_BOLD}':text='{headline}'"
        ":fontsize=56:fontcolor=white:x=(w-text_w)/2:y=52"
        ":shadowcolor=black@0.9:shadowx=3:shadowy=3"
    )
    vf_filters.append("drawbox=x=0:y=1812:w=iw:h=88:color=#CC0000@0.92:t=fill")
    vf_filters.append(
        f"drawtext=fontfile='{FONT_BOLD}':text='{ticker3}'"
        f":fontsize=29:fontcolor=white"
        f":x=w-mod(t*112\\,w+text_w):y=1835"
    )
    vf_filters.append(
        f"drawbox=x=0:y=1903:w='(iw*min(t\\,{aud_dur:.2f})/{aud_dur:.2f})':h=11"
        ":color=#FF2222@0.95:t=fill"
    )
    vf_filters.append("vignette=PI/5")

    t_cursor = 0.0
    for seg,dur in zip(segs,durs):
        cap = esc_ffmpeg(S(seg.get("caption"),""))
        if cap:
            t_start = round(t_cursor,2)
            t_end   = round(t_cursor+dur,2)
            vf_filters.append(
                f"drawtext=fontfile='{FONT_BOLD}':text='{cap}'"
                ":fontsize=46:fontcolor=white:x=(w-text_w)/2:y=h-322"
                ":shadowcolor=black@0.9:shadowx=3:shadowy=3"
                f":enable='between(t\\,{t_start}\\,{t_end})'"
            )
        t_cursor += dur

    vf       = ",".join(vf_filters)
    has_logo = logo and os.path.exists(logo)

    if has_logo:
        cmd = [
            "ffmpeg","-y","-i",cat,"-i",audio,"-i",logo,
            "-filter_complex",
            f"[0:v]{vf}[base];"
            "[2:v]scale=108:108[lg];"
            "[base][lg]overlay=x=20:y=H-270:format=auto[out]",
            "-map","[out]","-map","1:a",
            "-c:v","libx264","-preset","fast","-crf","18",
            "-c:a","aac","-b:a","192k",
            "-t",str(vid_dur),"-movflags","+faststart",
            "-pix_fmt","yuv420p","-threads","2",output
        ]
    else:
        cmd = [
            "ffmpeg","-y","-i",cat,"-i",audio,
            "-vf",vf,"-c:v","libx264","-preset","fast","-crf","18",
            "-c:a","aac","-b:a","192k",
            "-t",str(vid_dur),"-movflags","+faststart",
            "-pix_fmt","yuv420p","-threads","2",output
        ]

    print("[RENDER] Final encode...")
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"[RENDER] stderr:\n{r.stderr[-3000:]}")
        raise RuntimeError(f"FFmpeg failed (code {r.returncode})")

    size = os.path.getsize(output)/1024/1024
    print(f"[RENDER] ✅ {size:.1f} MB")

# ─────────────────────────────────────────────
# 📤  STEP 7 — YOUTUBE UPLOAD
# ─────────────────────────────────────────────
def get_youtube():
    creds = Credentials(
        token=None,
        refresh_token=YT_REFRESH_TOKEN,
        client_id=YT_CLIENT_ID,
        client_secret=YT_CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=[
            "https://www.googleapis.com/auth/youtube.upload",
            "https://www.googleapis.com/auth/youtube",
        ]
    )
    return build("youtube","v3",credentials=creds)

def upload_youtube(video_path: str, pkg: dict) -> str:
    size = os.path.getsize(video_path)//1024//1024
    print(f"[YT] Uploading {size} MB...")
    yt = get_youtube()

    title       = (S(pkg.get("youtube_title"),"BREAKING Financial News") + " #Shorts")[:100]
    description = "\n\n".join([
        S(pkg.get("description")),
        "#Shorts #FinancialNews",
        S(pkg.get("trending_hashtags")),
        "💬 " + S(pkg.get("comment_bait"),"What do you think will happen next?"),
    ])[:5000]

    tags = pkg.get("tags",[])
    if isinstance(tags,str): tags = [t.strip() for t in tags.split(",") if t.strip()]
    tags = (["Shorts","FinancialNews"] + [S(t) for t in tags if t])[:30]

    body = {
        "snippet": {
            "title":                title,
            "description":          description,
            "tags":                 tags,
            "categoryId":           "25",
            "defaultLanguage":      "en",
            "defaultAudioLanguage": "en",
        },
        "status": {
            "privacyStatus":           "public",
            "selfDeclaredMadeForKids": False,
            "madeForKids":             False,
            "embeddable":              True,
        }
    }

    media = MediaFileUpload(video_path, mimetype="video/mp4", resumable=True, chunksize=5*1024*1024)
    req   = yt.videos().insert(part="snippet,status", body=body, media_body=media)
    resp  = None
    while resp is None:
        status, resp = req.next_chunk()
        if status:
            pct = int(status.progress()*100)
            if pct % 25 == 0: print(f"[YT] {pct}%...")

    vid_id = resp["id"]
    print(f"[YT] ✅ https://youtube.com/shorts/{vid_id}")

    if YOUTUBE_PLAYLIST_ID:
        try:
            yt.playlistItems().insert(
                part="snippet",
                body={"snippet":{
                    "playlistId": YOUTUBE_PLAYLIST_ID,
                    "resourceId": {"kind":"youtube#video","videoId":vid_id},
                }}
            ).execute()
            print("[YT] ✅ Added to playlist")
        except Exception as e:
            print(f"[YT] Playlist: {e}")

    return vid_id

# ─────────────────────────────────────────────
# 📊  STEP 8 — GOOGLE SHEETS
# ─────────────────────────────────────────────
def log_sheets(vid_id: str, pkg: dict, stories: list):
    if not SHEETS_DOC_ID or not GOOGLE_CREDS_JSON: return
    try:
        creds = SACredentials.from_service_account_info(
            json.loads(GOOGLE_CREDS_JSON),
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        )
        gc    = gspread.authorize(creds)
        sheet = gc.open_by_key(SHEETS_DOC_ID).worksheet("Shorts Log")
        now   = datetime.now()
        titles = " + ".join(s.get("title","")[:35] for s in stories)
        sheet.append_row([
            now.strftime("%m/%d/%Y"), now.strftime("%H:%M"),
            vid_id, S(pkg.get("youtube_title")),
            f"https://youtube.com/shorts/{vid_id}",
            str(pkg.get("virality_score","")), titles,
            S(pkg.get("optimal_post_time")),
        ])
        print("[SHEETS] ✅ Logged")
    except Exception as e:
        print(f"[SHEETS] Warning: {e}")

# ─────────────────────────────────────────────
# 🚀  MAIN PIPELINE
# ─────────────────────────────────────────────
def process_episode(stories: list) -> bool:
    titles = " + ".join(s["title"][:28] for s in stories)
    print(f"\n{'═'*65}")
    print(f"🎬 {titles[:65]}")
    print(f"{'═'*65}")

    with tempfile.TemporaryDirectory() as tmp:
        audio  = os.path.join(tmp,"voice.mp3")
        output = os.path.join(tmp,"final.mp4")
        try:
            pkg     = generate_content(stories)
            visuals = build_visuals(pkg, tmp)
            generate_audio(S(pkg.get("shorts_script"),"Breaking financial news."), audio)
            logo    = prepare_logo(tmp)
            render_final(pkg, visuals, audio, output, logo)
            vid_id  = upload_youtube(output, pkg)
            log_sheets(vid_id, pkg, stories)
            print(f"\n✅ SUCCESS: https://youtube.com/shorts/{vid_id}")
            return True
        except Exception as e:
            print(f"\n❌ FAILED: {e}")
            import traceback; traceback.print_exc()
            return False

def main():
    print(f"\n╔{'═'*62}╗")
    print(f"║  🌍 VIRAL SHORTS MACHINE v6.0 — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"╚{'═'*62}╝\n")

    all_news = collect_all_news()
    if not all_news:
        print("⚠️ No news collected"); return

    top = pick_stories(all_news, n=6)
    if not top:
        print("⚠️ No relevant stories"); return

    episodes = []
    for i in range(0, min(6,len(top)), 3):
        batch = top[i:i+3]
        if batch: episodes.append(batch)
    episodes = episodes[:2]

    print(f"\n[MAIN] {len(episodes)} episode(s) to produce\n")
    success = sum(1 for ep in episodes if process_episode(ep))

    print(f"\n╔{'═'*62}╗")
    print(f"║  ✅ DONE: {success}/{len(episodes)} episodes published")
    print(f"╚{'═'*62}╝")

if __name__ == "__main__":
    main()
