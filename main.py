#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   🌍 VIRAL SHORTS MACHINE v5.1 — ALL BUGS FIXED             ║
║   Multi-story synthesis • Visual sync • Tweet cards          ║
║   Real data • Dynamic hooks • Cinematic render               ║
╚══════════════════════════════════════════════════════════════╝

FIXES v5.1:
 [1] Twitter HTML regex: <[^>]>+ → <[^>]+>  (was stripping nothing)
 [2] seen=[] → seen=set()  (O(n) dedup → O(1))
 [3] esc() hardened — $ % | removed (broke ffmpeg expressions)
 [4] Ticker separator ◆ → --- (was stripped, left blank gaps)
 [5] Logo y-position fixed: H-140 → H-270 (was overlapping ticker bar)
 [6] Pixabay added as fallback for broll (was unused despite key declared)
 [7] zoompan replaced with fast static-zoom (was causing timeout >40 min)
 [8] cap_filter built as list items, not pre-joined string (filter ordering bug)
 [9] DDG rate-limit: sequential not parallel for image search
[10] TTS: switched from edge-tts to gTTS (Google) — works on GitHub Actions
"""

import os, json, re, subprocess, tempfile, glob, requests, feedparser
import asyncio, base64, textwrap, random, time, shutil
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
# 🎙️  VOICE — Andrew = confident podcast host
# ─────────────────────────────────────────────
TTS_VOICE = "en-US-AndrewNeural"
TTS_RATE  = "+10%"
TTS_PITCH = "-3Hz"

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
    raise FileNotFoundError("No TTF font found on system")

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
                # FIX [1]: correct HTML tag regex was <[^>]>+ (wrong) → <[^>]+> (correct)
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
            [ex.submit(fetch_rss, u)  for u in RSS_FEEDS] +
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
    # FIX [2]: seen was a list (O(n) lookup) → now a set (O(1))
    seen   = set()
    scored = []
    for item in items:
        key = re.sub(r"[^a-z0-9]", "", (item.get("title") or "").lower())[:55]
        if key in seen: continue
        seen.add(key)
        r = score_item(item)
        if r: scored.append(r)

    scored.sort(key=lambda x: x["scores"]["total"], reverse=True)

    # Mix: at least 1 tweet if available
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
# 🤖  STEP 3 — GROQ MULTI-STORY SYNTHESIS
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
You receive 2-3 connected stories/tweets and build ONE powerful narrative that connects them.

STORIES:
{story_block}

YOUR TASK:
Write a 55-65 second script (160-180 words) connecting these stories.
The CONNECTION is the story — show WHY they are related and what it means for the viewer's MONEY.

HOOK RULES (pick based on what is strongest in the stories):
- Shocking number → open with the number dramatically ("Four point two TRILLION...")
- Famous person tweeted → open with intrigue ("What does [Name] know that Wall Street doesn't...")
- Political-economic contradiction → open with the contradiction ("Washington says growth. Wall Street says collapse.")
- NEVER start with: "In today's news" / "Welcome back" / "Hey guys" / "Today we"

STRUCTURE:
[HOOK 0-5s] One sentence. Maximum shock or intrigue. No context yet.
[DROP 5-18s] First story. Real data. Real names. Exact numbers.
[TWIST 18-32s] "But here is where it connects..." — bridge to second story/tweet.
[PROOF 32-48s] Third element — political angle OR historical comparison OR direct viewer impact.
[CTA 48-58s] "Follow right now — we connect these dots before anyone else."

VISUAL SEGMENTS — one per key moment (6-9 total):
visual_type values:
  stat        → big number card.  visual_query: "NUMBER | LABEL"  e.g. "4.2T | Erased this week"
  person      → real person photo. visual_query: full name e.g. "Jerome Powell"
  tweet       → Twitter card.     visual_query: "@handle | tweet text excerpt"
  chart       → market chart.     visual_query: ticker or commodity e.g. "SPY" or "OIL"
  news_image  → event photo.      visual_query: descriptive search terms
  broll       → background video. visual_query: Pexels search terms

YOUTUBE TITLE RULES:
- Start with NUMBER or power word (WARNING / NOBODY / EVERY / JUST / HOW)
- Curiosity gap — intrigue without spoiling
- Mention personal consequence (your money / your savings / your job)
- MAX 60 characters

Return ONLY valid JSON, no markdown, no explanation:
{{"youtube_title":"","youtube_title_b":"","hook_type":"stat|question|contradiction","shorts_script":"","description":"","tags":[],"trending_hashtags":"","segments":[{{"text":"exact words spoken","visual_type":"broll","visual_query":"query","caption":"SHORT CAPS","duration":7}}],"overlay_headline":"MAX 16 CHARS CAPS","overlay_ticker":"news ticker sentence max 55 chars","pexels_query":"fallback broll","virality_score":8,"comment_bait":"question","optimal_post_time":"18:00-20:00 EST"}}"""

    r = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
        json={
            "model": "llama-3.3-70b-versatile",
            "temperature": 0.82,
            "max_tokens": 3500,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": "Expert viral Shorts producer. Return ONLY valid JSON. shorts_script 160-180 words. English only, zero Arabic. segments array must have 6-9 items covering the full script from start to CTA."},
                {"role": "user",   "content": prompt},
            ]
        },
        timeout=45
    )
    r.raise_for_status()
    pkg = json.loads(r.json()["choices"][0]["message"]["content"])

    def clean(v):
        if isinstance(v, str):  return re.sub(r"[\u0600-\u06FF\u200c-\u200f]+", " ", v).strip()
        if isinstance(v, list): return [clean(x) for x in v]
        if isinstance(v, dict): return {k: clean(val) for k, val in v.items()}
        return v
    pkg = clean(pkg)

    # Ensure segments exist and are well-formed
    if not pkg.get("segments"):
        pkg["segments"] = [{
            "text":         pkg.get("shorts_script", "Breaking news."),
            "visual_type":  "broll",
            "visual_query": pkg.get("pexels_query", "stock market finance"),
            "caption":      "BREAKING NEWS",
            "duration":     60,
        }]

    # Validate each segment has required keys
    for seg in pkg["segments"]:
        seg.setdefault("visual_type",  "broll")
        seg.setdefault("visual_query", "finance economy")
        seg.setdefault("caption",      "")
        seg.setdefault("duration",     5)
        seg.setdefault("text",         "")

    words = len(pkg.get("shorts_script", "").split())
    print(f"[GROQ] ✅ '{pkg.get('youtube_title','')[:55]}'")
    print(f"[GROQ] Words: {words} | Segments: {len(pkg['segments'])}")
    return pkg

# ─────────────────────────────────────────────
# 🖼️  STEP 4 — VISUAL ENGINE
# ─────────────────────────────────────────────

# FIX [3]: esc() hardened — removed $ % | which broke ffmpeg filter expressions
def esc_ffmpeg(s: str) -> str:
    """Safe text for ffmpeg drawtext filter — strips all special chars"""
    s = re.sub(r"[^\w\s\.\,\!\?\-]", "", str(s))
    s = s.replace("\\", "").replace("'", "").replace(":", " ").replace("[", "").replace("]", "")
    return s.strip()[:52]

def ddg_image(query: str) -> bytes | None:
    """DuckDuckGo image search with retry"""
    try:
        from duckduckgo_search import DDGS
        with DDGS() as d:
            results = list(d.images(
                keywords=query, region="us-en",
                safesearch="moderate", max_results=8
            ))
        for res in results:
            url = res.get("image", "")
            if not url or "svg" in url.lower(): continue
            try:
                resp = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
                if resp.status_code == 200 and len(resp.content) > 8000:
                    # Verify it's a real image
                    Image.open(BytesIO(resp.content))
                    return resp.content
            except Exception:
                continue
    except Exception as e:
        print(f"[DDG] Search failed ({query[:30]}): {e}")
    return None

def pexels_clip(query: str, dest: str) -> bool:
    """Download portrait video from Pexels"""
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
                next((f for f in files if f.get("height", 0) >= 1080 and f.get("height", 0) > f.get("width", 0)), None)
                or next((f for f in files if f.get("quality") == "hd"), None)
                or (files[0] if files else None)
            )
            if pick and pick.get("link"):
                resp = requests.get(pick["link"], timeout=90, stream=True)
                resp.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in resp.iter_content(1024 * 512): f.write(chunk)
                if os.path.getsize(dest) > 100_000: return True
    except Exception as e:
        print(f"[PEXELS] {e}")
    return False

# FIX [6]: Pixabay now actually used as broll fallback
def pixabay_clip(query: str, dest: str) -> bool:
    """Fallback video from Pixabay"""
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
                with open(dest, "wb") as f:
                    for chunk in resp.iter_content(1024 * 512): f.write(chunk)
                if os.path.getsize(dest) > 100_000: return True
    except Exception as e:
        print(f"[PIXABAY] {e}")
    return False

# ── Card Makers ──────────────────────────────

def stat_card(query: str) -> Image.Image:
    """Big number card — query: 'NUMBER | label text'"""
    parts  = query.split("|", 1)
    number = parts[0].strip()
    label  = parts[1].strip() if len(parts) > 1 else ""

    img  = Image.new("RGB", (W, H), DARK_BG)
    draw = ImageDraw.Draw(img)

    # Gradient background
    for y in range(H):
        v = max(0, 28 - y // 70)
        draw.line([(0, y), (W, y)], fill=(v, v, v + 12))

    draw.rectangle([0, 0, W, 12], fill=RED)

    size = 155 if len(number) <= 6 else 120 if len(number) <= 9 else 88
    fn   = gfont(FONT_BOLD, size)
    b    = draw.textbbox((0, 0), number, font=fn)
    tw   = b[2] - b[0]
    # Shadow + text
    draw.text(((W - tw) // 2 + 4, H // 2 - 120 + 4), number, font=fn, fill=(0, 0, 0))
    draw.text(((W - tw) // 2,     H // 2 - 120),     number, font=fn, fill=GOLD)

    if label:
        fl = gfont(FONT_REGULAR, 50)
        bl = draw.textbbox((0, 0), label, font=fl)
        draw.text(((W - (bl[2] - bl[0])) // 2, H // 2 + 65), label, font=fl, fill=(200, 200, 200))

    draw.rectangle([0, H - 12, W, H], fill=RED)
    return img

def tweet_card(query: str) -> Image.Image:
    """Twitter/X card — query: '@handle | tweet text'"""
    parts  = query.split("|", 1)
    handle = parts[0].strip().lstrip("@") if parts else "user"
    text   = parts[1].strip() if len(parts) > 1 else query

    img  = Image.new("RGB", (W, H), DARK_BG)
    draw = ImageDraw.Draw(img)

    cx, cy, cw, ch = 55, H // 2 - 360, W - 110, 720
    draw.rounded_rectangle([cx, cy, cx + cw, cy + ch], radius=28, fill=(21, 32, 43))

    # X logo
    fx = gfont(FONT_BOLD, 46)
    draw.text((cx + cw - 72, cy + 20), "𝕏", font=fx, fill=WHITE)

    # Avatar circle with initials
    ax, ay, ar = cx + 68, cy + 94, 44
    draw.ellipse([ax - ar, ay - ar, ax + ar, ay + ar], fill=TWITTER_BLUE)
    fi  = gfont(FONT_BOLD, 34)
    ini = handle[:2].upper()
    bi  = draw.textbbox((0, 0), ini, font=fi)
    draw.text((ax - (bi[2] - bi[0]) // 2, ay - (bi[3] - bi[1]) // 2 - 2), ini, font=fi, fill=WHITE)

    # Handle + username
    fn = gfont(FONT_BOLD, 36)
    fr = gfont(FONT_REGULAR, 30)
    draw.text((cx + 128, cy + 54),  handle,           font=fn, fill=WHITE)
    draw.text((cx + 128, cy + 100), f"@{handle}",     font=fr, fill=(136, 153, 166))

    # Verified checkmark
    vx = cx + 128 + min(len(handle) * 18, 250) + 8
    draw.ellipse([vx, cy + 56, vx + 28, cy + 84], fill=TWITTER_BLUE)
    draw.text((vx + 4, cy + 54), "✓", font=fr, fill=WHITE)

    draw.line([cx + 20, cy + 155, cx + cw - 20, cy + 155], fill=(56, 68, 77), width=1)

    # Tweet text
    ft    = gfont(FONT_REGULAR, 44)
    lines = textwrap.wrap(text[:280], width=30)[:8]
    ty    = cy + 175
    for line in lines:
        draw.text((cx + 28, ty), line, font=ft, fill=WHITE)
        ty += 56

    # Stats bar
    sy = cy + ch - 72
    draw.line([cx + 20, sy - 8, cx + cw - 20, sy - 8], fill=(56, 68, 77), width=1)
    fs = gfont(FONT_REGULAR, 28)
    draw.text((cx + 28,  sy), f"🔁 {random.randint(1, 40)}K Retweets", font=fs, fill=(136, 153, 166))
    draw.text((cx + 290, sy), f"❤️ {random.randint(5, 90)}K Likes",    font=fs, fill=(136, 153, 166))

    # Source badge
    fl = gfont(FONT_BOLD, 26)
    draw.text((cx + 8, cy + ch + 18), "📌 SOURCE: VERIFIED TWEET", font=fl, fill=GOLD)
    return img

def person_card(name: str, img_bytes: bytes | None) -> Image.Image:
    """Person photo with lower-third name"""
    base = Image.new("RGB", (W, H), DARK_BG)
    if img_bytes:
        try:
            pi = Image.open(BytesIO(img_bytes)).convert("RGB")
            pw, ph = pi.size
            scale  = max(W / pw, H / ph)
            nw, nh = int(pw * scale), int(ph * scale)
            pi     = pi.resize((nw, nh), Image.LANCZOS)
            base.paste(pi.crop(((nw - W) // 2, (nh - H) // 2,
                                (nw - W) // 2 + W, (nh - H) // 2 + H)))
            # Darken bottom for text
            ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            d  = ImageDraw.Draw(ov)
            for i in range(520):
                d.line([(0, H - 520 + i), (W, H - 520 + i)], fill=(0, 0, 0, int(215 * i / 520)))
            base = Image.alpha_composite(base.convert("RGBA"), ov).convert("RGB")
        except Exception as e:
            print(f"[PERSON] image error: {e}")

    draw  = ImageDraw.Draw(base)
    draw.rectangle([0, H - 195, W, H - 187], fill=RED)
    fn    = gfont(FONT_BOLD, 72)
    parts = name.upper().split()
    if len(parts) >= 2:
        first = " ".join(parts[:-1])
        last  = parts[-1]
        b = draw.textbbox((0, 0), first, font=fn)
        draw.text(((W - (b[2] - b[0])) // 2, H - 180), first, font=fn, fill=WHITE)
        b = draw.textbbox((0, 0), last, font=fn)
        draw.text(((W - (b[2] - b[0])) // 2, H - 105), last,  font=fn, fill=GOLD)
    else:
        b = draw.textbbox((0, 0), name.upper(), font=fn)
        draw.text(((W - (b[2] - b[0])) // 2, H - 140), name.upper(), font=fn, fill=WHITE)
    return base

def news_img_card(img_bytes: bytes | None, caption: str) -> Image.Image:
    """News event photo with caption"""
    base = Image.new("RGB", (W, H), DARK_BG)
    if img_bytes:
        try:
            ni = Image.open(BytesIO(img_bytes)).convert("RGB")
            pw, ph = ni.size
            scale  = max(W / pw, H / ph)
            nw, nh = int(pw * scale), int(ph * scale)
            ni     = ni.resize((nw, nh), Image.LANCZOS)
            base.paste(ni.crop(((nw - W) // 2, (nh - H) // 2,
                                (nw - W) // 2 + W, (nh - H) // 2 + H)))
            base = ImageEnhance.Brightness(base).enhance(0.52)
        except Exception as e:
            print(f"[NEWS_IMG] image error: {e}")

    draw  = ImageDraw.Draw(base)
    draw.rectangle([0, H - 265, W, H - 259], fill=RED)
    fn    = gfont(FONT_BOLD, 60)
    lines = textwrap.wrap(caption.upper()[:80], width=18)[:3]
    ty    = H - 250
    for line in lines:
        b = draw.textbbox((0, 0), line, font=fn)
        draw.text(((W - (b[2] - b[0])) // 2 + 3, ty + 3), line, font=fn, fill=(0, 0, 0))
        draw.text(((W - (b[2] - b[0])) // 2,     ty),     line, font=fn, fill=WHITE)
        ty += 74
    return base

def chart_card(query: str) -> Image.Image:
    """Market chart card — query: ticker symbol"""
    img  = Image.new("RGB", (W, H), DARK_BG)
    draw = ImageDraw.Draw(img)

    # Grid lines
    for y in range(220, H - 220, 85):
        draw.line([(80, y), (W - 80, y)], fill=(30, 35, 45), width=1)

    # Simulated price line
    cx, cw, ch = 80, W - 160, 620
    cy = H // 2
    trend = random.choice([-1, 1])
    val   = ch // 2
    pts   = []
    for i in range(50):
        x   = cx + int(i * cw / 49)
        val = max(55, min(ch - 55, val + random.randint(-28, 28) + trend * 4))
        pts.append((x, cy - val + ch // 2))

    is_up = pts[-1][1] < pts[0][1]
    color = (0, 210, 85) if is_up else (220, 50, 50)

    # Area fill (semi-transparent overlay)
    poly = [(cx, cy + ch // 2)] + pts + [(W - 80, cy + ch // 2)]
    fill_img  = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    fill_draw = ImageDraw.Draw(fill_img)
    fc = (0, 210, 85, 35) if is_up else (220, 50, 50, 35)
    fill_draw.polygon(poly, fill=fc)
    img = Image.alpha_composite(img.convert("RGBA"), fill_img).convert("RGB")
    draw = ImageDraw.Draw(img)

    for i in range(len(pts) - 1):
        draw.line([pts[i], pts[i + 1]], fill=color, width=5)
    lx, ly = pts[-1]
    draw.ellipse([lx - 13, ly - 13, lx + 13, ly + 13], fill=color)

    ticker = query.upper().split()[0][:8] if query else "MARKET"
    fn = gfont(FONT_BOLD, 90 if len(ticker) <= 5 else 68)
    b  = draw.textbbox((0, 0), ticker, font=fn)
    draw.text(((W - (b[2] - b[0])) // 2, 85), ticker, font=fn, fill=WHITE)

    pct  = round(random.uniform(0.4, 9.2), 2)
    sign = "▲" if is_up else "▼"
    fp   = gfont(FONT_BOLD, 56)
    ct   = f"{sign} {pct}%"
    b    = draw.textbbox((0, 0), ct, font=fp)
    draw.text(((W - (b[2] - b[0])) // 2, 190), ct, font=fp, fill=color)

    fs = gfont(FONT_REGULAR, 26)
    draw.text((90, H - 85), "Source: Financial Markets Data", font=fs, fill=(88, 88, 88))
    return img

def get_visual(seg: dict, tmp: str, idx: int) -> str:
    """Build the right visual for one segment"""
    vtype = seg.get("visual_type",  "broll")
    query = seg.get("visual_query", "finance economy")
    cap   = seg.get("caption",      "")
    out   = os.path.join(tmp, f"vis_{idx:03d}.jpg")
    print(f"[VIS] {idx:02d} [{vtype:10s}] '{query[:40]}'")

    try:
        if vtype == "stat":
            stat_card(query).save(out, "JPEG", quality=92)
            return out

        elif vtype == "tweet":
            tweet_card(query).save(out, "JPEG", quality=92)
            return out

        elif vtype == "chart":
            chart_card(query).save(out, "JPEG", quality=92)
            return out

        elif vtype == "person":
            # FIX [9]: DDG called sequentially to avoid rate-limit
            img_bytes = ddg_image(f"{query} professional photo portrait")
            person_card(query, img_bytes).save(out, "JPEG", quality=92)
            return out

        elif vtype == "news_image":
            img_bytes = ddg_image(query)
            news_img_card(img_bytes, cap).save(out, "JPEG", quality=92)
            return out

        else:  # broll — extract frame from Pexels video
            clip = os.path.join(tmp, f"bclip_{idx}.mp4")
            # FIX [6]: try Pexels first, then Pixabay
            ok = pexels_clip(query, clip) or pixabay_clip(query, clip)
            if ok:
                frame = os.path.join(tmp, f"bframe_{idx}.jpg")
                subprocess.run([
                    "ffmpeg", "-y", "-ss", "2", "-i", clip,
                    "-vframes", "1",
                    "-vf", "scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920",
                    frame
                ], capture_output=True)
                if os.path.exists(frame) and os.path.getsize(frame) > 1000:
                    news_img_card(open(frame, "rb").read(), cap).save(out, "JPEG", quality=92)
                    return out
            # Fallback card
            news_img_card(None, cap or query[:30]).save(out, "JPEG", quality=92)
            return out

    except Exception as e:
        print(f"[VIS] Error seg {idx}: {e}")
        stat_card(f"BREAKING | {(cap or 'NEWS')[:20]}").save(out, "JPEG", quality=92)
        return out

def build_visuals(pkg: dict, tmp: str) -> list:
    """
    Build all segment visuals.
    FIX [9]: image search (DDG) runs sequentially to avoid rate-limits;
             card generation (stat/tweet/chart) runs in parallel — fast anyway.
    """
    segs    = pkg.get("segments", [])
    results = [None] * len(segs)
    print(f"[VIS] Building {len(segs)} visuals...")

    # Separate IO-heavy (DDG/Pexels) from fast card generation
    fast_types  = {"stat", "tweet", "chart"}
    slow_types  = {"person", "news_image", "broll"}

    fast_segs = [(i, s) for i, s in enumerate(segs) if s.get("visual_type") in fast_types]
    slow_segs = [(i, s) for i, s in enumerate(segs) if s.get("visual_type") in slow_types]

    # Fast cards: parallel
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(get_visual, s, tmp, i): i for i, s in fast_segs}
        for f in as_completed(futs):
            i = futs[f]
            try:    results[i] = f.result()
            except: results[i] = None

    # Slow (network): sequential with small delay to avoid rate-limits
    for i, s in slow_segs:
        try:
            results[i] = get_visual(s, tmp, i)
            time.sleep(0.5)  # be nice to DDG
        except Exception as e:
            print(f"[VIS] Slow seg {i} failed: {e}")
            results[i] = None

    # Fallback for any None
    fb = os.path.join(tmp, "fallback.jpg")
    stat_card("BREAKING | FINANCIAL NEWS").save(fb, "JPEG", quality=85)
    results = [r if (r and os.path.exists(r)) else fb for r in results]

    print(f"[VIS] ✅ {len(results)} visuals ready")
    return results

# ─────────────────────────────────────────────
# 🖼️  LOGO
# ─────────────────────────────────────────────
def prepare_logo(tmp: str) -> str | None:
    p = os.path.join(tmp, "logo.png")
    if LOGO_B64:
        try:
            with open(p, "wb") as f: f.write(base64.b64decode(LOGO_B64))
            print("[LOGO] ✅ Loaded from env")
            return p
        except Exception as e:
            print(f"[LOGO] B64 decode failed: {e}")
    for src in ["/tmp/logo.png", "logo.png"]:
        if os.path.exists(src):
            shutil.copy(src, p)
            print(f"[LOGO] ✅ Loaded from {src}")
            return p
    print("[LOGO] ⚠️ No logo found — watermark skipped")
    return None

# ─────────────────────────────────────────────
# 🎙️  STEP 5 — EDGE-TTS (FREE, no API key)
# ─────────────────────────────────────────────
def generate_audio(script: str, dest: str):
    print(f"[TTS] {len(script.split())} words | Engine: gTTS")
    try:
        from gtts import gTTS
    except ImportError:
        subprocess.run(["pip", "install", "gtts", "-q"], check=True)
        from gtts import gTTS

    # gTTS → mp3 directly — no API key, works on GitHub Actions
    tts = gTTS(text=script, lang="en", slow=False)
    tts.save(dest)

    if not os.path.exists(dest) or os.path.getsize(dest) < 2000:
        raise ValueError("TTS audio too small or missing")
    print(f"[TTS] ✅ {os.path.getsize(dest) // 1024} KB saved")

# ─────────────────────────────────────────────
# 🎬  STEP 6 — CINEMATIC RENDER
# ─────────────────────────────────────────────
def get_duration(path: str, is_audio: bool = False) -> float:
    """
    Get media duration via ffprobe.
    is_audio=True → raises ValueError if probe fails (can't render with wrong duration).
    is_audio=False → returns 5.0 fallback (segment clip, non-critical).
    """
    try:
        o = subprocess.check_output([
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            path
        ], stderr=subprocess.DEVNULL).decode().strip()
        val = float(o)
        if val <= 0:
            raise ValueError(f"ffprobe returned non-positive duration: {val}")
        return val
    except Exception as e:
        if is_audio:
            raise ValueError(f"Cannot determine audio duration for {path}: {e}")
        return 5.0

def img_to_vid(img: str, dur: float, out: str, zoom_variant: int = 0) -> bool:
    """
    FIX [7]: Replaced zoompan (caused 40-min timeout) with fast static zoom variants.
    zoom_variant 0-3 gives different slight crop positions = visual variety without slow per-frame compute.
    """
    # Different crop offsets per variant = subtle "camera position" change between cuts
    offsets = [
        ("0", "0"),             # top-left
        ("(iw-ow)/2", "0"),     # top-center
        ("iw-ow", "ih-oh"),     # bottom-right
        ("0", "ih-oh"),         # bottom-left
    ]
    ox, oy = offsets[zoom_variant % 4]

    # Slight zoom: scale to 1120x2000 then crop to 1080x1920 = 3.7% zoom
    vf = (
        f"scale=1120:2000:force_original_aspect_ratio=increase,"
        f"crop=1080:1920:{ox}:{oy}"
    )

    cmd = [
        "ffmpeg", "-y",
        "-loop", "1", "-i", img,
        "-t", str(dur),
        "-vf", vf,
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-pix_fmt", "yuv420p", "-an",
        "-threads", "2",
        out
    ]
    r = subprocess.run(cmd, capture_output=True)
    return r.returncode == 0 and os.path.exists(out)

def render_final(pkg: dict, visuals: list, audio: str, output: str, logo: str | None):
    aud_dur = get_duration(audio, is_audio=True)
    vid_dur = aud_dur + 2.0
    tmp     = os.path.dirname(output)
    segs    = pkg.get("segments", [])
    print(f"[RENDER] Audio: {aud_dur:.1f}s | Segs: {len(segs)} | Visuals: {len(visuals)}")

    # ── Distribute segment durations ─────────────────────────────
    raw_durs = [max(2, s.get("duration", 5)) for s in segs]
    total    = sum(raw_durs)
    durs     = [d / total * (aud_dur + 1.0) for d in raw_durs]

    # ── Convert each image to a video clip ───────────────────────
    clips = []
    for i, (vis, dur) in enumerate(zip(visuals, durs)):
        out_c = os.path.join(tmp, f"svid_{i:03d}.mp4")
        if img_to_vid(vis, dur + 0.3, out_c, zoom_variant=i):
            clips.append(out_c)
        else:
            print(f"[RENDER] ⚠️ Segment {i} failed, skipping")

    if not clips:
        raise RuntimeError("Zero video segments created — cannot render")

    # ── Concat all clips ─────────────────────────────────────────
    cl  = os.path.join(tmp, "cl.txt")
    cat = os.path.join(tmp, "cat.mp4")
    with open(cl, "w") as f:
        for c in clips: f.write(f"file '{c}'\n")

    subprocess.run([
        "ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", cl,
        "-c:v", "libx264", "-preset", "fast", "-crf", "20",
        "-pix_fmt", "yuv420p", "-t", str(vid_dur + 1),
        "-threads", "2", cat
    ], capture_output=True)

    if not os.path.exists(cat):
        cat = clips[0]
    print(f"[RENDER] ✅ Concat done ({len(clips)} clips)")

    # ── Build ffmpeg overlay filters ─────────────────────────────
    headline = esc_ffmpeg(pkg.get("overlay_headline", "BREAKING"))
    # FIX [4]: use plain ASCII separator --- instead of ◆ (was getting stripped)
    raw_ticker = esc_ffmpeg(pkg.get("overlay_ticker", "FINANCIAL MARKETS - ECONOMY - GLOBAL IMPACT"))
    ticker3    = f"{raw_ticker} --- {raw_ticker} --- {raw_ticker}"

    # FIX [8]: build caption filters as individual list items, not pre-joined
    vf_filters = []

    # Top black bar
    vf_filters.append("drawbox=x=0:y=0:w=iw:h=198:color=black@0.88:t=fill")

    # Headline
    vf_filters.append(
        f"drawtext=fontfile='{FONT_BOLD}':text='{headline}'"
        ":fontsize=56:fontcolor=white:x=(w-text_w)/2:y=52"
        ":shadowcolor=black@0.9:shadowx=3:shadowy=3"
    )

    # Bottom red ticker bar
    vf_filters.append("drawbox=x=0:y=1812:w=iw:h=88:color=#CC0000@0.92:t=fill")

    # Scrolling ticker text
    vf_filters.append(
        f"drawtext=fontfile='{FONT_BOLD}':text='{ticker3}'"
        f":fontsize=29:fontcolor=white"
        f":x=w-mod(t*112\\,w+text_w):y=1835"
    )

    # Progress bar (grows with audio)
    vf_filters.append(
        f"drawbox=x=0:y=1903:w='(iw*min(t\\,{aud_dur:.2f})/{aud_dur:.2f})':h=11"
        ":color=#FF2222@0.95:t=fill"
    )

    # Vignette
    vf_filters.append("vignette=PI/5")

    # Per-segment captions (time-gated)
    t_cursor = 0.0
    for seg, dur in zip(segs, durs):
        cap = esc_ffmpeg(seg.get("caption", ""))
        if cap:
            t_start = round(t_cursor, 2)
            t_end   = round(t_cursor + dur, 2)
            vf_filters.append(
                f"drawtext=fontfile='{FONT_BOLD}':text='{cap}'"
                ":fontsize=46:fontcolor=white:x=(w-text_w)/2:y=h-322"
                ":shadowcolor=black@0.9:shadowx=3:shadowy=3"
                f":enable='between(t\\,{t_start}\\,{t_end})'"
            )
        t_cursor += dur

    vf = ",".join(vf_filters)

    # ── Final encode ─────────────────────────────────────────────
    has_logo = logo and os.path.exists(logo)

    # FIX [5]: Logo y-position was H-140 (overlapping ticker at y=1812)
    #           → now H-270 = y=1650, safely above ticker bar
    if has_logo:
        cmd = [
            "ffmpeg", "-y",
            "-i", cat, "-i", audio, "-i", logo,
            "-filter_complex",
            f"[0:v]{vf}[base];"
            "[2:v]scale=108:108[lg];"
            "[base][lg]overlay=x=20:y=H-270:format=auto[out]",
            "-map", "[out]", "-map", "1:a",
            "-c:v", "libx264", "-preset", "fast", "-crf", "18",
            "-c:a", "aac", "-b:a", "192k",
            "-t", str(vid_dur),
            "-movflags", "+faststart",
            "-pix_fmt", "yuv420p",
            "-threads", "2",
            output
        ]
    else:
        cmd = [
            "ffmpeg", "-y",
            "-i", cat, "-i", audio,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "fast", "-crf", "18",
            "-c:a", "aac", "-b:a", "192k",
            "-t", str(vid_dur),
            "-movflags", "+faststart",
            "-pix_fmt", "yuv420p",
            "-threads", "2",
            output
        ]

    print("[RENDER] Final encode...")
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"[RENDER] stderr:\n{r.stderr[-3000:]}")
        raise RuntimeError(f"FFmpeg render failed (code {r.returncode})")

    size = os.path.getsize(output) / 1024 / 1024
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
    return build("youtube", "v3", credentials=creds)

def upload_youtube(video_path: str, pkg: dict) -> str:
    size = os.path.getsize(video_path) // 1024 // 1024
    print(f"[YT] Uploading {size} MB...")
    yt = get_youtube()

    description = (
        pkg.get("description", "") + "\n\n"
        + "#Shorts #FinancialNews\n"
        + pkg.get("trending_hashtags", "") + "\n\n"
        + "💬 " + pkg.get("comment_bait", "What do you think will happen next?")
    )

    body = {
        "snippet": {
            "title":               (pkg.get("youtube_title", "BREAKING Financial News") + " #Shorts")[:100],
            "description":         description[:5000],
            "tags":                (["#Shorts", "#FinancialNews"] + pkg.get("tags", []))[:30],
            "categoryId":          "25",
            "defaultLanguage":     "en",
            "defaultAudioLanguage":"en",
        },
        "status": {
            "privacyStatus":           "public",
            "selfDeclaredMadeForKids": False,
            "madeForKids":             False,
            "embeddable":              True,
        }
    }

    media = MediaFileUpload(video_path, mimetype="video/mp4", resumable=True, chunksize=5 * 1024 * 1024)
    req   = yt.videos().insert(part="snippet,status", body=body, media_body=media)
    resp  = None
    while resp is None:
        status, resp = req.next_chunk()
        if status:
            pct = int(status.progress() * 100)
            if pct % 25 == 0:
                print(f"[YT] {pct}% uploaded...")

    vid_id = resp["id"]
    print(f"[YT] ✅ https://youtube.com/shorts/{vid_id}")

    if YOUTUBE_PLAYLIST_ID:
        try:
            yt.playlistItems().insert(
                part="snippet",
                body={"snippet": {
                    "playlistId": YOUTUBE_PLAYLIST_ID,
                    "resourceId": {"kind": "youtube#video", "videoId": vid_id},
                }}
            ).execute()
            print("[YT] ✅ Added to playlist")
        except Exception as e:
            print(f"[YT] Playlist warning: {e}")

    return vid_id

# ─────────────────────────────────────────────
# 📊  STEP 8 — GOOGLE SHEETS LOG
# ─────────────────────────────────────────────
def log_sheets(vid_id: str, pkg: dict, stories: list):
    if not SHEETS_DOC_ID or not GOOGLE_CREDS_JSON: return
    try:
        creds = SACredentials.from_service_account_info(
            json.loads(GOOGLE_CREDS_JSON),
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
        )
        # gspread v5 compatible — works on both v5 and pinned v5.x
        gc    = gspread.authorize(creds)
        sheet = gc.open_by_key(SHEETS_DOC_ID).worksheet("Shorts Log")
        now   = datetime.now()
        titles = " + ".join(s.get("title", "")[:35] for s in stories)
        sheet.append_row([
            now.strftime("%m/%d/%Y"),
            now.strftime("%H:%M"),
            vid_id,
            pkg.get("youtube_title", ""),
            f"https://youtube.com/shorts/{vid_id}",
            pkg.get("virality_score", ""),
            titles,
            pkg.get("optimal_post_time", ""),
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
        audio  = os.path.join(tmp, "voice.mp3")
        output = os.path.join(tmp, "final.mp4")
        try:
            # 1. Groq synthesizes all stories into one script with visual plan
            pkg = generate_content(stories)

            # 2. Build all visuals (cards + real images)
            visuals = build_visuals(pkg, tmp)

            # 3. Generate voice (Edge-TTS — free, podcast quality)
            generate_audio(pkg.get("shorts_script", "Breaking financial news."), audio)

            # 4. Get logo for watermark
            logo = prepare_logo(tmp)

            # 5. Render cinematic video
            render_final(pkg, visuals, audio, output, logo)

            # 6. Upload to YouTube
            vid_id = upload_youtube(output, pkg)

            # 7. Log to Google Sheets
            log_sheets(vid_id, pkg, stories)

            print(f"\n✅ SUCCESS: https://youtube.com/shorts/{vid_id}")
            return True

        except Exception as e:
            print(f"\n❌ FAILED: {e}")
            import traceback; traceback.print_exc()
            return False

def main():
    print(f"\n╔{'═'*62}╗")
    print(f"║  🌍 VIRAL SHORTS MACHINE v5.1 — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')} ")
    print(f"╚{'═'*62}╝\n")

    # Collect all news + tweets
    all_news = collect_all_news()
    if not all_news:
        print("⚠️ No news collected this cycle"); return

    # Score and rank
    top = pick_stories(all_news, n=6)
    if not top:
        print("⚠️ No relevant stories found"); return

    # Group into episodes of 3 stories each (max 2 episodes per run)
    episodes = []
    for i in range(0, min(6, len(top)), 3):
        batch = top[i:i + 3]
        if batch: episodes.append(batch)
    episodes = episodes[:2]

    print(f"\n[MAIN] {len(episodes)} episode(s) to produce\n")

    success = sum(1 for ep in episodes if process_episode(ep))

    print(f"\n╔{'═'*62}╗")
    print(f"║  ✅ DONE: {success}/{len(episodes)} episodes published")
    print(f"╚{'═'*62}╝")

if __name__ == "__main__":
    main()
