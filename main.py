import os
import time
import json
import math
import asyncio
import datetime
import tempfile
import shutil
import subprocess
import urllib.request
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import yt_dlp
from pyrogram import Client, filters

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
API_ID   = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
_CHAT_RAW = os.environ["CHAT_ID"]
CHAT_ID   = int(_CHAT_RAW) if _CHAT_RAW.lstrip("-").isdigit() else _CHAT_RAW
QUALITY   = os.environ.get("QUALITY", "1080").strip().lower()
REPORT_SCHEDULE = os.environ.get("REPORT_SCHEDULE", "none").strip().lower()

URLS_FILE      = Path("urls.txt")
PROCESSED_FILE = Path("processed.txt")
DOWNLOADS_DIR  = Path("downloads")
HISTORY_FILE   = Path("download_history.json")

MAX_UPLOAD_BYTES = 1_900 * 1024 * 1024  # 1.9 GB Telegram bot limit
MAX_RETRIES      = 3
BAR_LENGTH       = 12
DAILY_LIMIT      = int(os.environ.get("DAILY_LIMIT", "100"))
DAILY_COUNT_FILE = Path("daily_count.json")
USER_AGENT       = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

# ── Global shared state ───────────────────────────────────────────────────────
_g = {
    "downloading": None,   # state dict of current download
    "uploading":   None,   # state dict of current upload
    "queue":       [],     # remaining URLs
    "session": {
        "start":       None,
        "ok":          0,
        "fail":        0,
        "bytes":       0,
        "errors":      [],      # list of (url, error_str)
        "batch_size":  0,       # initial URL count for batch-done detection
        "batch_done":  False,
    },
}
_upload_q: asyncio.Queue = None   # set in main()
_dl_queue:  asyncio.Queue = None   # set in main(), also used by URL handler


# ── Persistence helpers ───────────────────────────────────────────────────────

def load_urls() -> list[str]:
    if not URLS_FILE.exists():
        print(f"[ERROR] {URLS_FILE} not found.")
        return []
    return [
        l.strip() for l in URLS_FILE.read_text().splitlines()
        if l.strip() and not l.startswith("#")
    ]

def load_processed() -> set[str]:
    if not PROCESSED_FILE.exists():
        return set()
    return set(PROCESSED_FILE.read_text().splitlines())

def mark_processed(url: str):
    with open(PROCESSED_FILE, "a") as f:
        f.write(url + "\n")

def load_history() -> list[dict]:
    if not HISTORY_FILE.exists():
        return []
    try:
        return json.loads(HISTORY_FILE.read_text())
    except Exception:
        return []

def save_history(entry: dict):
    history = load_history()
    history.append(entry)
    HISTORY_FILE.write_text(json.dumps(history, indent=2))


def _today_str() -> str:
    return datetime.date.today().isoformat()

def get_daily_count() -> int:
    if not DAILY_COUNT_FILE.exists():
        return 0
    try:
        data = json.loads(DAILY_COUNT_FILE.read_text())
        return data.get(_today_str(), 0)
    except Exception:
        return 0

def increment_daily_count():
    data: dict = {}
    if DAILY_COUNT_FILE.exists():
        try:
            data = json.loads(DAILY_COUNT_FILE.read_text())
        except Exception:
            data = {}
    today = _today_str()
    data[today] = data.get(today, 0) + 1
    # Prune old days (keep only last 7)
    keys = sorted(data.keys())
    for old_key in keys[:-7]:
        del data[old_key]
    DAILY_COUNT_FILE.write_text(json.dumps(data, indent=2))


# ── Format helpers ────────────────────────────────────────────────────────────

def fmt_size(b: float) -> str:
    for unit, thresh in [("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)]:
        if b >= thresh:
            return f"{b/thresh:.1f} {unit}"
    return f"{b:.0f} B"

def fmt_speed(bps: float) -> str:
    if bps <= 0:
        return "—"
    return f"{bps/1024**2:.1f} MB/s" if bps >= 1024**2 else f"{bps/1024:.1f} KB/s"

def fmt_eta(sec: float) -> str:
    if sec <= 0:
        return "—"
    m, s = divmod(int(sec), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

def progress_bar(pct: float) -> str:
    filled = int(pct / 100 * BAR_LENGTH)
    return "▰" * filled + "▱" * (BAR_LENGTH - filled)

def quality_label() -> str:
    return "best" if QUALITY == "best" else f"{QUALITY}p"

def build_format(q: str) -> str:
    if q == "best":
        return "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best[ext=mp4]/best"
    try:
        h = int(q)
        return (
            f"bestvideo[height<={h}][ext=mp4]+bestaudio[ext=m4a]"
            f"/bestvideo[height<={h}]+bestaudio"
            f"/best[height<={h}][ext=mp4]/best[height<={h}]"
        )
    except ValueError:
        return "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best[ext=mp4]/best"


# ── Thumbnail ─────────────────────────────────────────────────────────────────

def fetch_thumbnail(thumb_url: str, output_dir: Path) -> Path | None:
    if not thumb_url:
        return None
    try:
        raw = output_dir / "thumb_raw"
        urllib.request.urlretrieve(thumb_url, str(raw))
        jpg = output_dir / "thumb.jpg"
        subprocess.run(
            ["ffmpeg", "-y", "-i", str(raw), str(jpg)],
            capture_output=True,
        )
        return jpg if jpg.exists() and jpg.stat().st_size > 0 else None
    except Exception as e:
        print(f"[WARN] Thumbnail failed: {e}")
        return None


# ── Video splitting ───────────────────────────────────────────────────────────

def get_duration(filepath: Path) -> float:
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_format", str(filepath)],
            capture_output=True, text=True,
        )
        return float(json.loads(out.stdout).get("format", {}).get("duration", 0))
    except Exception:
        return 0.0

def split_video(filepath: Path, tmp_dir: Path) -> list[Path]:
    size = filepath.stat().st_size
    if size <= MAX_UPLOAD_BYTES:
        return [filepath]
    duration = get_duration(filepath)
    if duration <= 0:
        print(f"[WARN] Cannot determine duration — uploading as-is (may fail if > 2 GB).")
        return [filepath]
    n = math.ceil(size / MAX_UPLOAD_BYTES)
    part_dur = duration / n
    parts = []
    for i in range(n):
        out = tmp_dir / f"part_{i+1:03d}_of_{n:03d}.mp4"
        subprocess.run([
            "ffmpeg", "-y",
            "-ss", f"{i * part_dur:.3f}",
            "-i", str(filepath),
            "-t", f"{part_dur:.3f}",
            "-c", "copy",
            "-avoid_negative_ts", "make_zero",
            str(out),
        ], capture_output=True)
        if out.exists() and out.stat().st_size > 0:
            parts.append(out)
    return parts or [filepath]


# ── Progress hooks ────────────────────────────────────────────────────────────

def make_download_hook(state: dict):
    def hook(d: dict):
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            done  = d.get("downloaded_bytes", 0)
            state.update(
                phase="downloading",
                dl_percent=(done / total * 100) if total else 0,
                dl_downloaded=done,
                dl_total=total,
                dl_speed=d.get("speed") or 0,
                dl_eta=d.get("eta") or 0,
            )
        elif d["status"] == "finished":
            state["phase"] = "merging"
    return hook

def make_upload_hook(state: dict):
    state.update(ul_last_bytes=0, ul_last_time=time.monotonic())
    def cb(current: int, total: int):
        now = time.monotonic()
        elapsed = now - state["ul_last_time"]
        if elapsed >= 0.5:
            state["ul_speed"] = (current - state["ul_last_bytes"]) / elapsed
            state.update(ul_last_bytes=current, ul_last_time=now)
        state.update(
            phase="uploading",
            ul_percent=(current / total * 100) if total else 0,
            ul_current=current,
            ul_total=total,
        )
    return cb


# ── Progress message builder ──────────────────────────────────────────────────

DIVIDER = "─" * 22

def build_progress_text(state: dict) -> str:
    phase = state.get("phase", "starting")
    title = state.get("title", "")
    url   = state.get("url", "")
    t     = f"🎬 **{title}**\n" if title else ""
    u     = f"🔗 `{url}`"

    if phase == "starting":
        return f"⏳ **Fetching info...**\n{DIVIDER}\n{t}{u}"

    if phase == "retry":
        n    = state.get("retry_num", 1)
        wait = state.get("retry_wait", 0)
        return (
            f"⚠️ **Retry {n}/{MAX_RETRIES} — waiting {wait}s...**\n"
            f"{DIVIDER}\n{t}{u}"
        )

    if phase == "downloading":
        pct   = state.get("dl_percent", 0)
        speed = state.get("dl_speed", 0)
        done  = state.get("dl_downloaded", 0)
        total = state.get("dl_total", 0)
        eta   = state.get("dl_eta", 0)
        bar   = progress_bar(pct)
        size  = f"{fmt_size(done)} / {fmt_size(total)}" if total else "—"
        return (
            f"⬇️ **Downloading** · `{quality_label()}`\n{DIVIDER}\n"
            f"{t}{u}\n\n"
            f"`{bar}` **{pct:.1f}%**\n"
            f"📦 {size}\n"
            f"⚡ {fmt_speed(speed)}\n"
            f"⏱ ETA: {fmt_eta(eta)}"
        )

    if phase == "merging":
        return f"🔄 **Merging formats...**\n{DIVIDER}\n{t}{u}"

    if phase == "uploading":
        pct   = state.get("ul_percent", 0)
        speed = state.get("ul_speed", 0)
        curr  = state.get("ul_current", 0)
        total = state.get("ul_total", 0)
        bar   = progress_bar(pct)
        size  = f"{fmt_size(curr)} / {fmt_size(total)}" if total else "—"
        pnum  = state.get("part_num", 1)
        ptot  = state.get("part_total", 1)
        part  = f" · Part {pnum}/{ptot}" if ptot > 1 else ""
        return (
            f"📤 **Uploading to Telegram**{part}\n{DIVIDER}\n"
            f"{t}{u}\n\n"
            f"`{bar}` **{pct:.1f}%**\n"
            f"📦 {size}\n"
            f"⚡ {fmt_speed(speed)}"
        )

    return f"⏳ **Working...**\n{DIVIDER}\n{t}{u}"


# ── Live message updater ──────────────────────────────────────────────────────

async def live_updater(msg, state: dict, stop: asyncio.Event):
    last = ""
    while not stop.is_set():
        try:
            text = build_progress_text(state)
            if text != last:
                await msg.edit_text(text)
                last = text
        except Exception:
            pass
        await asyncio.sleep(4)


# ── Download worker (with retry + backoff) ────────────────────────────────────

async def download_worker(app: Client, dl_queue: asyncio.Queue):
    while True:
        url = await dl_queue.get()

        try:
            if url in _g["queue"]:
                _g["queue"].remove(url)
        except ValueError:
            pass

        state = {"url": url, "phase": "starting", "title": ""}
        _g["downloading"] = state

        msg  = await app.send_message(CHAT_ID, build_progress_text(state))
        stop = asyncio.Event()
        updater = asyncio.create_task(live_updater(msg, state, stop))

        DOWNLOADS_DIR.mkdir(exist_ok=True)
        tmp_dir = Path(tempfile.mkdtemp(dir=DOWNLOADS_DIR))

        # ── Daily limit gate ──────────────────────────────────────────────
        daily_count = get_daily_count()
        if daily_count >= DAILY_LIMIT:
            err = f"Daily download limit reached ({DAILY_LIMIT}/day). Try again tomorrow."
            print(f"[LIMIT] {err}")
            _g["session"]["fail"] += 1
            _g["session"]["errors"].append((url, err))
            stop.set()
            await updater
            shutil.rmtree(tmp_dir, ignore_errors=True)
            try:
                await msg.edit_text(
                    f"🚫 **Daily Limit Reached**\n{DIVIDER}\n"
                    f"📊 Downloaded today: **{daily_count}/{DAILY_LIMIT}**\n"
                    f"⏳ Limit resets at midnight. Try again tomorrow."
                )
            except Exception:
                pass
            _g["downloading"] = None
            continue

        ydl_opts = {
            "outtmpl": str(tmp_dir / "%(title)s.%(ext)s"),
            "format": build_format(QUALITY),
            "merge_output_format": "mp4",
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "progress_hooks": [make_download_hook(state)],
            "extractor_retries": 3,
            "http_headers": {
                "User-Agent": USER_AGENT,
                "Accept-Language": "en-US,en;q=0.9",
            },
            "external_downloader": "aria2c",
            "external_downloader_args": {
                "aria2c": [
                    "--max-connection-per-server=16",
                    "--split=16",
                    "--min-split-size=1M",
                    "--console-log-level=warn",
                    "--quiet=true",
                    "--allow-overwrite=true",
                ]
            },
        }

        def probe_url(url: str) -> dict | None:
         try:
            with yt_dlp.YoutubeDL({
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "skip_download": True,
            }) as ydl:
                return ydl.extract_info(url, download=False)
         except Exception as e:
          print(f"[PROBE FAIL] {url} -> {e}")
          return None
        

        def _do_download():
            info = probe_url(url)
            if not info:
                raise Exception("Metadata extraction failed (probe)")
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info  = ydl.extract_info(url, download=True)
                fname = Path(ydl.prepare_filename(info))
                return (
                    fname,
                    info.get("title", fname.stem),
                    info.get("thumbnail", ""),
                    info.get("width") or 0,
                    info.get("height") or 0,
                    int(info.get("duration") or 0),
                )

        success = False
        for attempt in range(MAX_RETRIES):
            try:
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor(max_workers=1) as pool:
                    filepath, title, thumb_url, w, h, dur = \
                        await loop.run_in_executor(pool, _do_download)
                success = True
                break
            except Exception as exc:
                if attempt < MAX_RETRIES - 1:
                    wait = 2 ** attempt
                    state.update(phase="retry", retry_num=attempt + 1, retry_wait=wait)
                    print(f"[RETRY {attempt+1}/{MAX_RETRIES}] {url}: {exc} — wait {wait}s")
                    await asyncio.sleep(wait)
                else:
                    err_str = str(exc)
                    print(f"[FAIL] {url}: {err_str}")
                    _g["session"]["fail"] += 1
                    _g["session"]["errors"].append((url, err_str))
                    stop.set()
                    await updater
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                    try:
                        await msg.edit_text(
                            f"❌ **Download Failed**\n{DIVIDER}\n"
                            f"🔗 `{url}`\n\n"
                            f"⚠️ `{err_str[:250]}`\n"
                            f"🔄 Retried {MAX_RETRIES}× — giving up"
                        )
                    except Exception:
                        pass

        if not success:
            _g["downloading"] = None
            continue

        state["title"] = title

        # Resolve filepath
        if not filepath.exists():
            filepath = filepath.with_suffix(".mp4")
        if not filepath.exists():
            for f in tmp_dir.iterdir():
                if f.suffix in (".mp4", ".mkv", ".webm", ".avi"):
                    filepath = f
                    break

        if not filepath or not filepath.exists():
            err = "Downloaded file not found on disk."
            _g["session"]["fail"] += 1
            _g["session"]["errors"].append((url, err))
            stop.set()
            await updater
            shutil.rmtree(tmp_dir, ignore_errors=True)
            try:
                await msg.edit_text(f"❌ **Error**\n{DIVIDER}\n🔗 `{url}`\n\n⚠️ {err}")
            except Exception:
                pass
            _g["downloading"] = None
            continue

        # Pass to upload worker (do NOT stop updater yet)
        await _upload_q.put({
            "url":      url,
            "filepath": filepath,
            "tmp_dir":  tmp_dir,
            "title":    title,
            "thumb_url": thumb_url,
            "width":    w,
            "height":   h,
            "duration": dur,
            "msg":      msg,
            "state":    state,
            "stop":     stop,
            "updater":  updater,
        })
        _g["downloading"] = None


# ── Upload worker ─────────────────────────────────────────────────────────────

async def upload_worker(app: Client):
    while True:
        item = await _upload_q.get()

        url      = item["url"]
        filepath = item["filepath"]
        tmp_dir  = item["tmp_dir"]
        title    = item["title"]
        thumb_url = item["thumb_url"]
        w, h, dur = item["width"], item["height"], item["duration"]
        msg     = item["msg"]
        state   = item["state"]
        stop    = item["stop"]
        updater = item["updater"]

        _g["uploading"] = state
        delete_msg = True

        try:
            thumb  = fetch_thumbnail(thumb_url, tmp_dir)
            parts  = split_video(filepath, tmp_dir)
            n_parts = len(parts)

            if n_parts > 1:
                print(f"[INFO] Splitting into {n_parts} parts: {title}")

            for i, part in enumerate(parts, 1):
                part_size = part.stat().st_size
                state.update(
                    phase="uploading",
                    ul_percent=0,
                    ul_current=0,
                    ul_total=part_size,
                    ul_speed=0,
                    part_num=i,
                    part_total=n_parts,
                )

                part_label = f" — Part {i}/{n_parts}" if n_parts > 1 else ""
                caption = (
                    f"🎬 **{title}**{part_label}\n\n"
                    f"🔗 `{url}`\n"
                    f"📦 {fmt_size(part_size)}  •  🎞 {quality_label()}"
                    + (f"  •  ⏱ {fmt_eta(dur)}" if dur and n_parts == 1 else "")
                )

                await app.send_video(
                    chat_id=CHAT_ID,
                    video=str(part),
                    thumb=str(thumb) if thumb and i == 1 else None,
                    caption=caption,
                    width=w or None,
                    height=h or None,
                    duration=dur or None,
                    supports_streaming=True,
                    progress=make_upload_hook(state),
                )

            total_size = sum(p.stat().st_size for p in parts)
            _g["session"]["ok"]    += 1
            _g["session"]["bytes"] += total_size
            mark_processed(url)
            increment_daily_count()
            save_history({
                "date":     datetime.datetime.utcnow().isoformat(),
                "title":    title,
                "url":      url,
                "size":     total_size,
                "quality":  QUALITY,
                "duration": dur,
                "parts":    n_parts,
            })
            print(f"[OK] {title}")

            # Trigger summary when initial batch finishes
            sess = _g["session"]
            total_done = sess["ok"] + sess["fail"]
            if not sess["batch_done"] and total_done >= sess["batch_size"] and not _g["queue"]:
                sess["batch_done"] = True
                asyncio.create_task(send_summary(app))

        except Exception as exc:
            err_str = str(exc)
            print(f"[ERROR] Upload failed: {err_str}")
            _g["session"]["fail"] += 1
            _g["session"]["errors"].append((url, err_str))
            delete_msg = False
            try:
                await msg.edit_text(
                    f"❌ **Upload Failed**\n{DIVIDER}\n"
                    f"🎬 **{title}**\n"
                    f"🔗 `{url}`\n\n"
                    f"⚠️ `{err_str[:250]}`"
                )
            except Exception:
                pass

            sess = _g["session"]
            total_done = sess["ok"] + sess["fail"]
            if not sess["batch_done"] and total_done >= sess["batch_size"] and not _g["queue"]:
                sess["batch_done"] = True
                asyncio.create_task(send_summary(app))

        finally:
            stop.set()
            await updater
            shutil.rmtree(tmp_dir, ignore_errors=True)
            _g["uploading"] = None
            if delete_msg:
                try:
                    await msg.delete()
                except Exception:
                    pass


# ── Command / URL handlers ────────────────────────────────────────────────────

async def handle_url(app: Client, message):
    url = (message.text or "").strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        await message.reply("⚠️ Please send a valid URL starting with `http://` or `https://`.")
        return

    processed = load_processed()
    if url in processed:
        await message.reply(f"⚠️ This URL has already been downloaded.\n🔗 `{url}`")
        return

    if url in _g["queue"]:
        pos = _g["queue"].index(url) + 1
        await message.reply(f"ℹ️ Already in queue at position #{pos}.\n🔗 `{url}`")
        return

    _g["queue"].append(url)
    _g["session"]["batch_size"] += 1
    _g["session"]["batch_done"] = False
    await _dl_queue.put(url)

    pos = len(_g["queue"])
    active = _g["downloading"] is not None or _g["uploading"] is not None
    status = "⬇️ Downloading now!" if pos == 1 and not active else f"📋 Position in queue: #{pos}"
    await message.reply(
        f"✅ **Added to queue!**\n{DIVIDER}\n"
        f"🔗 `{url}`\n\n"
        f"{status}"
    )


async def handle_status(_, message):
    dl   = _g["downloading"]
    ul   = _g["uploading"]
    sess = _g["session"]
    elapsed = time.monotonic() - sess["start"] if sess["start"] else 0

    lines = [f"📊 **Bot Status**\n{DIVIDER}"]

    if dl:
        t   = dl.get("title") or "fetching info..."
        pct = dl.get("dl_percent", 0)
        lines.append(f"⬇️ **Downloading:** {t} ({pct:.1f}%)")
    else:
        lines.append("⬇️ No active download")

    if ul:
        t   = ul.get("title", "")
        pct = ul.get("ul_percent", 0)
        spd = ul.get("ul_speed", 0)
        lines.append(f"📤 **Uploading:** {t} ({pct:.1f}% · {fmt_speed(spd)})")
    else:
        lines.append("📤 No active upload")

    daily = get_daily_count()
    lines += [
        f"\n📋 Queue: **{len(_g['queue'])}** remaining",
        f"✅ Done: **{sess['ok']}**   ❌ Failed: **{sess['fail']}**",
        f"📦 Uploaded: **{fmt_size(sess['bytes'])}**",
        f"⏱ Running: **{fmt_eta(elapsed)}**",
        f"📅 Today: **{daily}/{DAILY_LIMIT}** downloads",
    ]
    await message.reply("\n".join(lines))


async def handle_queue(_, message):
    q = _g["queue"]
    if not q:
        await message.reply(f"📋 Queue is empty.\n{DIVIDER}\n✅ All URLs have been processed.")
        return
    lines = [f"📋 **Download Queue — {len(q)} pending**\n{DIVIDER}"]
    for i, url in enumerate(q[:20], 1):
        lines.append(f"`{i}.` {url}")
    if len(q) > 20:
        lines.append(f"\n_...and {len(q) - 20} more_")
    await message.reply("\n".join(lines))


async def handle_summary_cmd(_, message):
    await send_summary(None, reply_to=message)


# ── Session summary ───────────────────────────────────────────────────────────

async def send_summary(app: Client | None, reply_to=None):
    sess    = _g["session"]
    elapsed = time.monotonic() - sess["start"] if sess["start"] else 0
    icon    = "✅" if not sess["fail"] else "⚠️"
    lines   = [
        f"{icon} **Session Summary**\n{DIVIDER}",
        f"📥 Processed: **{sess['ok']}** video(s)",
    ]
    if sess["fail"]:
        lines.append(f"❌ Failed: **{sess['fail']}** video(s)")
    lines += [
        f"📦 Total uploaded: **{fmt_size(sess['bytes'])}**",
        f"⏱ Running: **{fmt_eta(elapsed)}**",
        f"📋 Queue remaining: **{len(_g['queue'])}**",
    ]
    if sess["errors"]:
        lines.append(f"\n⚠️ **Errors:**")
        for url, err in sess["errors"][:5]:
            short_url = url[:60] + ("…" if len(url) > 60 else "")
            lines.append(f"• `{short_url}`\n  ↳ _{err[:100]}_")
    text = "\n".join(lines)
    try:
        if reply_to:
            await reply_to.reply(text)
        elif app:
            await app.send_message(CHAT_ID, text)
    except Exception as e:
        print(f"[ERROR] Summary: {e}")


# ── Period report ─────────────────────────────────────────────────────────────

async def send_period_report(app: Client, since: datetime.datetime, label: str):
    history = load_history()
    items   = [
        h for h in history
        if datetime.datetime.fromisoformat(h["date"]) >= since
    ]
    if not items:
        return
    total_size = sum(h.get("size", 0) for h in items)
    date_str   = datetime.datetime.utcnow().strftime("%b %d, %Y")
    lines = [
        f"📅 **{label} Report — {date_str}**\n{DIVIDER}",
        f"📥 {len(items)} video(s) downloaded",
        f"📦 Total: **{fmt_size(total_size)}**",
        "",
    ]
    for h in items[-15:]:
        ql  = f"{h['quality']}p" if h.get("quality") != "best" else "best"
        sz  = fmt_size(h.get("size", 0))
        lines.append(f"📌 **{h['title']}** — {ql} — {sz}")
    try:
        await app.send_message(CHAT_ID, "\n".join(lines))
    except Exception as e:
        print(f"[ERROR] Report: {e}")


async def report_scheduler(app: Client):
    if REPORT_SCHEDULE == "none":
        return
    while True:
        now = datetime.datetime.utcnow()
        if REPORT_SCHEDULE == "daily":
            nxt = (now + datetime.timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0)
            since = now - datetime.timedelta(days=1)
            label = "Daily"
        elif REPORT_SCHEDULE == "weekly":
            days = (6 - now.weekday()) % 7 or 7
            nxt  = (now + datetime.timedelta(days=days)).replace(
                hour=0, minute=0, second=0, microsecond=0)
            since = now - datetime.timedelta(weeks=1)
            label = "Weekly"
        else:
            return
        await asyncio.sleep(max((nxt - now).total_seconds(), 1))
        await send_period_report(app, since, label)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    global _upload_q, _dl_queue
    _upload_q = asyncio.Queue()
    _dl_queue  = asyncio.Queue()

    urls      = load_urls()
    processed = load_processed()
    pending   = [u for u in urls if u not in processed]

    _g["queue"]                  = pending.copy()
    _g["session"]["start"]       = time.monotonic()
    _g["session"]["batch_size"]  = len(pending)

    if not pending:
        print("[INFO] No pending URLs in urls.txt — waiting for URLs via Telegram.")
    else:
        print(f"[INFO] {len(pending)} URL(s) queued. Quality: {QUALITY}")
        for url in pending:
            await _dl_queue.put(url)

    app = Client(
        name="tg_uploader_bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
    )

    @app.on_message(filters.command("status"))
    async def _status(client, message):
        await handle_status(client, message)

    @app.on_message(filters.command("queue"))
    async def _queue_cmd(client, message):
        await handle_queue(client, message)

    @app.on_message(filters.command("summary"))
    async def _summary(client, message):
        await handle_summary_cmd(client, message)

    @app.on_message(filters.private & filters.text & ~filters.command(["status", "queue", "summary"]))
    async def _url_msg(client, message):
        await handle_url(client, message)
        
    keep_alive = asyncio.Event() 

    async with app:
        print("[INFO] Bot started. Send any URL to the bot to queue a download.")
        await asyncio.gather(
            download_worker(app, _dl_queue),
            upload_worker(app),
            report_scheduler(app),
            keep_alive.wait(),
        )


if __name__ == "__main__":
    asyncio.run(main())
