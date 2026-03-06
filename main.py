#!/usr/bin/env python3
"""
🌍 VIRAL SHORTS MACHINE — Python Edition v3.0
ffmpeg-based rendering (no Creatomate) — works 100% inside GitHub Actions
"""

import os, json, re, subprocess, tempfile, glob, requests, feedparser
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as SACredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import gspread

# ─────────────────────────────────────────────
# 🔑 API KEYS
# ─────────────────────────────────────────────
GROQ_API_KEY        = os.environ["GROQ_API_KEY"]
DEEPGRAM_API_KEY    = os.environ["DEEPGRAM_API_KEY"]
PEXELS_API_KEY      = os.environ["PEXELS_API_KEY"]
PIXABAY_API_KEY     = os.environ["PIXABAY_API_KEY"]
YOUTUBE_PLAYLIST_ID = os.environ.get("YOUTUBE_PLAYLIST_ID", "")
SHEETS_DOC_ID       = os.environ.get("SHEETS_DOC_ID", "")

# YouTube OAuth tokens
YT_CLIENT_ID     = os.environ["YT_CLIENT_ID"]
YT_CLIENT_SECRET = os.environ["YT_CLIENT_SECRET"]
YT_REFRESH_TOKEN = os.environ["YT_REFRESH_TOKEN"]

# Google Service Account (Sheets only)
GOOGLE_CREDS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")

# ─────────────────────────────────────────────
# 🔤 Font Detection — لا hardcode، يكتشف تلقائياً
# ─────────────────────────────────────────────

def find_font(bold=False):
    """
    يبحث عن أي خط TTF متاح على النظام.
    يفضل DejaVu، ثم Liberation، ثم أي خط آخر.
    """
    candidates_bold = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    ]
    candidates_regular = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
    ]
    candidates = candidates_bold if bold else candidates_regular
    for path in candidates:
        if os.path.exists(path):
            return path
    # fallback: أي خط TTF على النظام
    results = glob.glob("/usr/share/fonts/**/*.ttf", recursive=True)
    if results:
        return results[0]
    raise FileNotFoundError("No TTF font found on system. Run: sudo apt-get install -y fonts-dejavu")

FONT_BOLD    = find_font(bold=True)
FONT_REGULAR = find_font(bold=False)
print(f"[FONT] Bold:    {FONT_BOLD}")
print(f"[FONT] Regular: {FONT_REGULAR}")

# ─────────────────────────────────────────────
# 📡 STEP 1: جمع الأخبار
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
    {"handle": "LynAldenContact","name": "Lyn Alden",           "weight": 9},
    {"handle": "RayDalio",       "name": "Ray Dalio",           "weight": 10},
    {"handle": "elonmusk",       "name": "Elon Musk",           "weight": 8},
    {"handle": "zerohedge",      "name": "ZeroHedge",           "weight": 7},
    {"handle": "jsblokland",     "name": "Jeroen Blokland",     "weight": 8},
    {"handle": "michaeljburry",  "name": "Michael Burry",       "weight": 10},
]

NITTER = [
    "https://nitter.privacydev.net",
    "https://nitter.cz",
    "https://nitter.poast.org",
]

def fetch_rss(url):
    try:
        feed  = feedparser.parse(url)
        items = []
        for entry in feed.entries[:15]:
            title   = entry.get("title", "")
            summary = entry.get("summary", "")[:400]
            link    = entry.get("link", "")
            pub     = entry.get("published", "")
            if not title or len(title) < 15:
                continue
            items.append({
                "title": title, "summary": summary,
                "link": link, "pubDate": pub,
                "source": url.split("/")[2],
                "type": "news", "influencerWeight": 0,
            })
        return items
    except Exception as e:
        print(f"[RSS] Error {url[:40]}: {e}")
        return []

def fetch_twitter(inf):
    for host in NITTER:
        try:
            r = requests.get(
                f"{host}/{inf['handle']}/rss",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=5
            )
            if r.status_code != 200 or "<item>" not in r.text:
                continue
            items = []
            for m in re.finditer(r"<item>([\s\S]*?)</item>", r.text):
                raw   = m.group(1)
                t     = re.search(r"<title><!\[CDATA\[([\s\S]*?)\]\]></title>", raw)
                title = t.group(1) if t else ""
                title = re.sub(r"<[^>]+>", " ", title).strip()
                pub   = re.search(r"<pubDate>([^<]+)</pubDate>", raw)
                pub   = pub.group(1) if pub else ""
                if not title or len(title) < 15:
                    continue
                items.append({
                    "title": title, "summary": "",
                    "source": f"@{inf['handle']}",
                    "sourceName": inf["name"],
                    "pubDate": pub,
                    "type": "influencer_tweet",
                    "influencerWeight": inf["weight"],
                })
            if items:
                return items
        except Exception:
            continue
    return []

def collect_all_news():
    all_items = []
    print("[NEWS] Fetching all sources in parallel...")
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(fetch_rss, url) for url in RSS_FEEDS]
        futures += [ex.submit(fetch_twitter, inf) for inf in INFLUENCERS]
        for f in as_completed(futures):
            all_items.extend(f.result())
    print(f"[NEWS] ✅ {len(all_items)} items collected")
    return all_items

# ─────────────────────────────────────────────
# 🧠 STEP 2: Viral Score Engine
# ─────────────────────────────────────────────

VIRAL_KW = {
    "federal reserve":30,"fed rate":30,"market crash":30,"recession":30,
    "hyperinflation":30,"economic collapse":30,"bitcoin":28,"crypto crash":30,
    "dollar collapse":30,"world war":30,"nuclear":28,"sanctions":25,
    "inflation":20,"interest rate":20,"stock market":20,"oil price":20,
    "gold":18,"china":20,"russia":20,"gdp":15,"unemployment":18,"layoffs":22,
    "bank collapse":28,"banking crisis":28,"trillion":20,"billion":12,
    "record high":18,"record low":18,"tariff":15,"trade war":18,
    "opec":15,"energy crisis":18,
}
HOOK_KW = {
    "breaking":25,"just in":25,"urgent":22,"alert":20,"shocking":18,
    "crash":20,"surge":15,"plunge":18,"collapse":22,"soar":12,
    "record":15,"warning":18,"crisis":20,"panic":18,"massive":12,
    "historic":15,"emergency":20,"revealed":12,"exposed":15,
}

def score_item(item):
    tl = (item.get("title") or "").lower()
    ks = sum(s for k, s in VIRAL_KW.items() if k in tl)
    hs = sum(s for k, s in HOOK_KW.items()  if k in tl)
    if ks == 0 and hs < 20:
        return None
    try:
        from email.utils import parsedate_to_datetime
        age_h = (datetime.now(timezone.utc) - parsedate_to_datetime(item.get("pubDate",""))).total_seconds() / 3600
    except Exception:
        age_h = 0
    rec   = max(0, 40 - age_h * 6.67)
    inf   = item.get("influencerWeight", 0) * 12 if item.get("type") == "influencer_tweet" else 0
    trnd  = 20 if age_h < 1 and (ks+hs) > 25 else 0
    total = ks + hs + rec + inf + trnd
    return {
        **item,
        "scores": {"keyword":ks,"hook":hs,"recency":round(rec),"influencer":inf,"total":round(total)},
        "isBreaking":   age_h < 1 and hs > 15,
        "emotionLevel": "EXTREME" if (ks+hs)>50 else "HIGH" if (ks+hs)>30 else "MEDIUM",
    }

def pick_top_stories(items, n=3):
    seen, scored = set(), []
    for item in items:
        key = re.sub(r"[^a-z0-9]", "", (item.get("title") or "").lower())[:55]
        if key in seen:
            continue
        seen.add(key)
        r = score_item(item)
        if r:
            scored.append(r)
    scored.sort(key=lambda x: x["scores"]["total"], reverse=True)
    top = scored[:n]
    print(f"[SCORE] {len(items)} in → {len(scored)} relevant → top {len(top)} selected")
    for i, s in enumerate(top):
        print(f"  #{i+1} [{s['scores']['total']}pts] {s['title'][:70]}")
    return top

# ─────────────────────────────────────────────
# 🤖 STEP 3: Groq AI
# ─────────────────────────────────────────────

def generate_content(news):
    prompt = f"""Create a viral YouTube Shorts content package for this financial news.
NEWS TYPE: {"TOP ECONOMIST: "+news.get("sourceName","") if news.get("type")=="influencer_tweet" else "Breaking Financial News"}
HEADLINE: {news["title"]}
SOURCE: {news.get("source","Financial Wire")}
SUMMARY: {news.get("summary","")[:300]}
VIRAL SCORE: {news["scores"]["total"]}
EMOTION: {news["emotionLevel"]}

RULES:
1. YouTube title: max 70 chars
2. Script: EXACTLY 170-185 words, end with: Follow right now — we break these stories first.
3. English only
4. trending_hashtags: space-separated like #Economy #Markets
5. overlay_headline: max 40 chars, plain text only, no special characters
6. overlay_subtext: max 60 chars, plain text only, no special characters
7. overlay_ticker: max 40 chars, plain text only, no special characters

Return ONLY JSON:
{{"youtube_title":"","youtube_title_b":"","hook_line":"","open_loop":"","shorts_script":"","description":"","tags":[],"overlay_headline":"","overlay_subtext":"","overlay_ticker":"","pexels_query":"","pixabay_query":"","category":"markets","virality_prediction":8,"algorithm_hook_0_3s":"","watch_through_strategy":"","comment_bait":"","share_trigger":"","community_post":"","optimal_post_time":"18:00-20:00 EST","trending_hashtags":""}}"""

    r = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
        json={
            "model": "llama-3.3-70b-versatile",
            "temperature": 0.8, "max_tokens": 2800,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": "Return ONLY valid JSON. shorts_script MUST be 170-185 words. English only."},
                {"role": "user",   "content": prompt}
            ]
        },
        timeout=30
    )
    r.raise_for_status()
    pkg = json.loads(r.json()["choices"][0]["message"]["content"])
    def clean(v):
        if isinstance(v, str): return re.sub(r"[\u0600-\u06FF]+", "", v).strip()
        if isinstance(v, list): return [clean(x) for x in v]
        return v
    pkg = {k: clean(v) for k, v in pkg.items()}
    print(f"[GROQ] ✅ {pkg.get('youtube_title','')[:60]}")
    print(f"[GROQ] words: {len(pkg.get('shorts_script','').split())}")
    return pkg

# ─────────────────────────────────────────────
# 🎬 STEP 4: B-Roll Video
# ─────────────────────────────────────────────

def download_video(pq, xq, dest_path):
    url = None
    try:
        r = requests.get(
            "https://api.pexels.com/videos/search",
            headers={"Authorization": PEXELS_API_KEY},
            params={"query": pq, "per_page": 8, "orientation": "portrait"},
            timeout=10
        )
        for v in r.json().get("videos", []):
            files = v.get("video_files", [])
            pick  = next((f for f in files if f.get("height",0) > f.get("width",0) and f.get("height",0) >= 1080), None) \
                 or next((f for f in files if f.get("quality") == "hd"), None) \
                 or (files[0] if files else None)
            if pick and pick.get("link"):
                url = pick["link"]
                print(f"[VIDEO] ✅ Pexels")
                break
    except Exception as e:
        print(f"[VIDEO] Pexels error: {e}")

    if not url:
        try:
            r = requests.get(
                "https://pixabay.com/api/videos/",
                params={"key": PIXABAY_API_KEY, "q": xq, "per_page": 5, "safesearch": "true"},
                timeout=10
            )
            for v in r.json().get("hits", []):
                vids = v.get("videos", {})
                u    = (vids.get("large") or vids.get("medium") or vids.get("small") or {}).get("url")
                if u:
                    url = u
                    print(f"[VIDEO] ✅ Pixabay")
                    break
        except Exception as e:
            print(f"[VIDEO] Pixabay error: {e}")

    if not url:
        url = "https://videos.pexels.com/video-files/3191528/3191528-uhd_2160_4096_25fps.mp4"
        print("[VIDEO] Using fallback")

    print("[VIDEO] Downloading...")
    r = requests.get(url, timeout=120, stream=True)
    r.raise_for_status()
    with open(dest_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
    print(f"[VIDEO] ✅ Saved ({os.path.getsize(dest_path)//1024//1024} MB)")

# ─────────────────────────────────────────────
# 🎙️ STEP 5: Deepgram TTS
# ─────────────────────────────────────────────

def generate_audio(script, dest_path):
    print(f"[TTS] Generating audio ({len(script.split())} words)...")
    r = requests.post(
        "https://api.deepgram.com/v1/speak?model=aura-asteria-en",
        headers={"Authorization": f"Token {DEEPGRAM_API_KEY}", "Content-Type": "application/json"},
        json={"text": script[:1900]},
        timeout=30
    )
    r.raise_for_status()
    if len(r.content) < 1000:
        raise ValueError(f"Audio too small: {len(r.content)} bytes")
    with open(dest_path, "wb") as f:
        f.write(r.content)
    print(f"[TTS] ✅ {len(r.content)//1024} KB saved")

# ─────────────────────────────────────────────
# 🎬 STEP 6: ffmpeg — دمج كل شيء محلياً
# ─────────────────────────────────────────────

def esc_ffmpeg(s):
    """تنظيف النص لاستخدامه في drawtext بأمان تام"""
    # احتفظ بالحروف الآمنة فقط
    s = re.sub(r"[^\w\s\$\%\.\,\!\?\-\+]", "", s)
    # escape الأحرف الخاصة في ffmpeg
    s = s.replace("\\", "\\\\")
    s = s.replace("'",  "")
    s = s.replace(":",  "\\:")
    return s.strip()

def render_video_ffmpeg(pkg, video_path, audio_path, output_path):
    headline = esc_ffmpeg(pkg.get("overlay_headline", "BREAKING NEWS")[:40])
    subtext  = esc_ffmpeg(pkg.get("overlay_subtext",  "Markets Update")[:60])
    ticker   = esc_ffmpeg(pkg.get("overlay_ticker",   "FINANCIAL NEWS")[:40])

    # احسب مدة الصوت
    try:
        out = subprocess.check_output([
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            audio_path
        ], stderr=subprocess.DEVNULL).decode().strip()
        audio_duration = float(out)
    except Exception:
        audio_duration = 60.0

    video_duration = audio_duration + 2.0
    print(f"[FFMPEG] Audio: {audio_duration:.1f}s → Video: {video_duration:.1f}s")

    vf = (
        # تحويل لـ portrait 1080x1920
        "scale=1080:1920:force_original_aspect_ratio=increase,"
        "crop=1080:1920,"
        # شريط أسود علوي
        "drawbox=x=0:y=0:w=1080:h=185:color=black@0.82:t=fill,"
        # headline
        f"drawtext=fontfile='{FONT_BOLD}':text='{headline}'"
        ":fontsize=52:fontcolor=white:x=(w-text_w)/2:y=55:line_spacing=4,"
        # subtext
        f"drawtext=fontfile='{FONT_REGULAR}':text='{subtext}'"
        ":fontsize=28:fontcolor=#FFD700:x=(w-text_w)/2:y=130:line_spacing=4,"
        # شريط أحمر سفلي
        "drawbox=x=0:y=1800:w=1080:h=120:color=red@0.88:t=fill,"
        # ticker
        f"drawtext=fontfile='{FONT_BOLD}':text='{ticker}'"
        ":fontsize=30:fontcolor=white:x=(w-text_w)/2:y=1843:line_spacing=4"
    )

    cmd = [
        "ffmpeg", "-y",
        "-stream_loop", "-1",       # كرر الفيديو إن كان أقصر من الصوت
        "-i", video_path,
        "-i", audio_path,
        "-vf", vf,
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-t", str(video_duration),
        "-shortest",
        "-movflags", "+faststart",
        "-pix_fmt", "yuv420p",
        output_path
    ]

    print("[FFMPEG] Rendering...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[FFMPEG] stderr:\n{result.stderr[-2000:]}")
        raise RuntimeError(f"ffmpeg failed with code {result.returncode}")

    print(f"[FFMPEG] ✅ Done — {os.path.getsize(output_path)/1024/1024:.1f} MB")

# ─────────────────────────────────────────────
# 📤 STEP 7: Upload to YouTube (OAuth2)
# ─────────────────────────────────────────────

def get_youtube_service():
    creds = Credentials(
        token=None,
        refresh_token=YT_REFRESH_TOKEN,
        client_id=YT_CLIENT_ID,
        client_secret=YT_CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=[
            "https://www.googleapis.com/auth/youtube.upload",
            "https://www.googleapis.com/auth/youtube"
        ]
    )
    return build("youtube", "v3", credentials=creds)

def upload_youtube(video_path, pkg, news):
    print(f"[YOUTUBE] Uploading {os.path.getsize(video_path)//1024//1024} MB...")
    youtube = get_youtube_service()
    body = {
        "snippet": {
            "title":       pkg.get("youtube_title", "BREAKING Financial News")[:100],
            "description": pkg.get("description", "") + "\n\n" + pkg.get("trending_hashtags", ""),
            "tags":        pkg.get("tags", []),
            "categoryId":  "25",
        },
        "status": {
            "privacyStatus":           "public",
            "selfDeclaredMadeForKids": False,
        }
    }
    media    = MediaFileUpload(video_path, mimetype="video/mp4", resumable=True)
    req      = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
    response = None
    while response is None:
        status, response = req.next_chunk()
        if status:
            print(f"[YOUTUBE] Uploading {int(status.progress()*100)}%")

    video_id = response["id"]
    print(f"[YOUTUBE] ✅ https://youtube.com/shorts/{video_id}")

    if YOUTUBE_PLAYLIST_ID:
        youtube.playlistItems().insert(
            part="snippet",
            body={"snippet": {
                "playlistId": YOUTUBE_PLAYLIST_ID,
                "resourceId": {"kind": "youtube#video", "videoId": video_id}
            }}
        ).execute()
        print("[YOUTUBE] ✅ Added to playlist")

    return video_id

# ─────────────────────────────────────────────
# 📊 STEP 8: Google Sheets
# ─────────────────────────────────────────────

def log_to_sheets(video_id, pkg, news):
    if not SHEETS_DOC_ID or not GOOGLE_CREDS_JSON:
        return
    try:
        creds = SACredentials.from_service_account_info(
            json.loads(GOOGLE_CREDS_JSON),
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc    = gspread.authorize(creds)
        sheet = gc.open_by_key(SHEETS_DOC_ID).worksheet("Shorts Log")
        now   = datetime.now()
        sheet.append_row([
            now.strftime("%m/%d/%Y"), now.strftime("%I:%M:%S %p"),
            video_id, pkg.get("youtube_title",""), pkg.get("youtube_title_b",""),
            pkg.get("category",""), pkg.get("virality_prediction",""),
            news.get("title",""), news.get("source",""), str(news.get("isBreaking",False)),
            str(news.get("scores",{}).get("total",0)),
            f"https://youtube.com/shorts/{video_id}",
            "github_actions","","Live","PENDING","PENDING","PENDING",
            pkg.get("community_post",""), pkg.get("hook_line",""),
            pkg.get("open_loop",""), pkg.get("comment_bait",""),
            pkg.get("share_trigger",""), news.get("emotionLevel",""),
            pkg.get("watch_through_strategy",""), pkg.get("algorithm_hook_0_3s",""),
            pkg.get("optimal_post_time",""),
        ])
        print("[SHEETS] ✅ Logged")
    except Exception as e:
        print(f"[SHEETS] Warning: {e}")

# ─────────────────────────────────────────────
# 🚀 MAIN
# ─────────────────────────────────────────────

def process_story(news):
    print(f"\n{'='*60}")
    print(f"🎬 {news['title'][:70]}")
    print(f"{'='*60}")

    with tempfile.TemporaryDirectory() as tmp:
        video_raw  = os.path.join(tmp, "broll.mp4")
        audio_file = os.path.join(tmp, "voice.mp3")
        output     = os.path.join(tmp, "final.mp4")

        try:
            pkg = generate_content(news)
            download_video(
                pkg.get("pexels_query", "stock market"),
                pkg.get("pixabay_query", "economy"),
                video_raw
            )
            generate_audio(pkg.get("shorts_script", "Breaking financial news."), audio_file)
            render_video_ffmpeg(pkg, video_raw, audio_file, output)
            video_id = upload_youtube(output, pkg, news)
            log_to_sheets(video_id, pkg, news)
            print(f"\n✅ SUCCESS: https://youtube.com/shorts/{video_id}")
            return True
        except Exception as e:
            print(f"\n❌ FAILED: {e}")
            import traceback; traceback.print_exc()
            return False

def main():
    print(f"\n🌍 VIRAL SHORTS MACHINE v3.0 — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print("="*60)
    all_news    = collect_all_news()
    top_stories = pick_top_stories(all_news, n=3)
    if not top_stories:
        print("⚠️ No relevant stories found this cycle")
        return
    success = sum(1 for news in top_stories if process_story(news))
    print(f"\n{'='*60}")
    print(f"✅ DONE: {success}/{len(top_stories)} shorts published")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
