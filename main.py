import os
import re
import time
import random
import sqlite3
import logging
import asyncio
import io
from typing import Optional, List, Tuple, Dict, Any
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
    Document,
)
from telegram.constants import ParseMode
from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    InlineQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("jorathaghighatpro")

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0").strip() or "0")
DB_PATH = os.getenv("DB_PATH", "data.db").strip() or "data.db"
TURN_TIMEOUT_SEC = int(os.getenv("TURN_TIMEOUT_SEC", "60"))
MAX_REROLL_PER_PLAYER = int(os.getenv("MAX_REROLL_PER_PLAYER", "3"))

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN env var is required")
if ADMIN_ID <= 0:
    raise RuntimeError("ADMIN_ID env var is required (>0)")

# =========================
# DB
# =========================
def now() -> int:
    return int(time.time())

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS questions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        qtype TEXT NOT NULL,
        level TEXT NOT NULL,
        text TEXT NOT NULL,
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at INTEGER NOT NULL,
        added_by INTEGER DEFAULT NULL
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS suggestions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        chat_id INTEGER NOT NULL,
        qtype TEXT NOT NULL,
        level TEXT NOT NULL,
        text TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at INTEGER NOT NULL,
        reviewed_by INTEGER,
        reviewed_at INTEGER
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS games (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,               -- group | inline
        status TEXT NOT NULL,             -- lobby | running | ended
        owner_id INTEGER NOT NULL,
        board_chat_id INTEGER,
        board_message_id INTEGER,
        board_inline_id TEXT,
        created_at INTEGER NOT NULL,
        allow_mid_join INTEGER NOT NULL DEFAULT 1,
        show_prev_question INTEGER NOT NULL DEFAULT 1,
        allow_18 INTEGER NOT NULL DEFAULT 1,
        view TEXT NOT NULL DEFAULT 'main',    -- main/settings/players/stats
        phase TEXT NOT NULL DEFAULT 'lobby',  -- lobby/choose/question/wait_confirm
        current_turn_index INTEGER NOT NULL DEFAULT 0,
        last_q_text TEXT DEFAULT '',
        last_q_by INTEGER DEFAULT NULL,
        last_qtype TEXT DEFAULT '',
        last_level TEXT DEFAULT ''
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS game_players (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        joined_at INTEGER NOT NULL,
        rerolls_left INTEGER NOT NULL,
        skips_used INTEGER NOT NULL DEFAULT 0,
        penalties INTEGER NOT NULL DEFAULT 0,
        turns INTEGER NOT NULL DEFAULT 0,
        active INTEGER NOT NULL DEFAULT 1,
        UNIQUE(game_id, user_id)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER NOT NULL,
        actor_id INTEGER NOT NULL,
        qtype TEXT NOT NULL,
        level TEXT NOT NULL,
        text TEXT NOT NULL,
        status TEXT NOT NULL,     -- asked/done_wait/confirmed/rejected/refused/timeout
        created_at INTEGER NOT NULL
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS forced_questions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        qtype TEXT,
        level TEXT,
        text TEXT NOT NULL,
        created_at INTEGER NOT NULL
    );
    """)
    conn.commit()
    conn.close()

SEED = [
    ("truth","normal","آخرین باری که به کسی دروغ گفتی کی بود و چرا؟"),
    ("truth","normal","اگه فقط یک راز رو مجبور بودی بگی، چی می‌گفتی؟"),
    ("truth","normal","از کی توی جمع بیشتر حساب می‌بری؟"),
    ("truth","normal","آخرین چیزی که تو گوگل سرچ کردی چی بود؟"),
    ("truth","normal","بدترین سوتی‌ات جلوی بقیه چی بوده؟"),
    ("dare","normal","یک ویس ۵ ثانیه‌ای بفرست و بگو: «من الان تو بازی‌ام!»"),
    ("dare","normal","۳۰ ثانیه نقش یک مجری تلویزیونی رو بازی کن."),
    ("dare","normal","به یک نفر یک تعریف خیلی خاص و عجیب بگو."),
    ("truth","18","تا حالا عمداً کسی رو جذب خودت کردی و بعدش عقب کشیدی؟"),
    ("truth","18","بیشتر جذب رفتار می‌شی یا ظاهر؟ چرا؟"),
    ("dare","18","سه ویژگی که تو رابطه برات حیاتی‌ه رو بگو."),
    ("dare","18","یک جمله دوپهلو ولی محترمانه بگو 😏"),
]

PENALTIES = [
    "مجازات: ۱ امتیاز منفی ثبت شد ⚠️",
    "مجازات: ۱ ویس ۵ ثانیه‌ای باید بفرستی 🎙",
    "مجازات: دور بعد فقط «شانسی» داری 🎲",
    "مجازات: ۱ تا از تعویض‌هات کم شد 🔄",
    "مجازات: ادمین می‌تونه برات سؤال انتخاب کنه 😈",
]

def seed_if_empty():
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM questions;")
    c = int(cur.fetchone()["c"])
    if c == 0:
        cur.executemany(
            "INSERT INTO questions (qtype, level, text, enabled, created_at, added_by) VALUES (?,?,?,?,?,?);",
            [(a,b,c,1,now(),ADMIN_ID) for (a,b,c) in SEED]
        )
        conn.commit()
    conn.close()

# =========================
# Helpers
# =========================
def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def esc(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def mention(uid: int, name: str) -> str:
    return f'<a href="tg://user?id={uid}">{esc(name)}</a>'

def parse_bulk(text: str) -> List[str]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    out = []
    for ln in lines:
        m = re.match(r"^\s*\d+\s*[\=\)\-\.]\s*(.+)$", ln)
        out.append((m.group(1) if m else ln).strip())
    seen=set()
    res=[]
    for t in out:
        t = re.sub(r"\s+"," ",t).strip()
        if t and t not in seen:
            seen.add(t)
            res.append(t)
    return res

# =========================
# Message Update System (بهبود یافته)
# =========================
# سیستم صف برای مدیریت درخواست‌های آپدیت
update_queues: Dict[int, asyncio.Queue] = {}
update_tasks: Dict[int, asyncio.Task] = {}

async def start_game_update_worker(gid: int, context: ContextTypes.DEFAULT_TYPE):
    """پردازش صف آپدیت برای هر بازی به صورت جداگانه"""
    q = update_queues[gid]
    while True:
        try:
            update_data = await q.get()
            if update_data is None:  # signal to stop
                break
                
            await _process_update_queue_item(gid, context, update_data)
            q.task_done()
            await asyncio.sleep(0.1)  # کوتاه کردن زمان پردازش برای پاسخگویی بهتر
        except Exception as e:
            log.error(f"Error in update worker for game {gid}: {e}")
            await asyncio.sleep(1)

async def _process_update_queue_item(gid: int, context: ContextTypes.DEFAULT_TYPE, update_ dict):
    """پردازش هر آیتم در صف آپدیت"""
    uid_for_kb = update_data.get("uid_for_kb", 0)
    force_view = update_data.get("force_view")
    immediate_feedback = update_data.get("immediate_feedback", "")
    callback = update_data.get("callback")
    
    try:
        g = get_game(gid)
        if not g:
            return
            
        # به‌روزرسانی نمایش در صورت نیاز
        if force_view:
            set_game_fields(gid, view=force_view)
            g = get_game(gid)
            
        # ارسال بازخورد فوری به کاربر اگر لازم باشد
        if immediate_feedback:
            try:
                query = update_data.get("query")
                if query:
                    await query.answer(immediate_feedback, show_alert=False)
            except Exception as e:
                log.warning(f"Could not send immediate feedback: {e}")
        
        # نمایش پیام بارگذاری موقت
        if update_data.get("show_loading", False) and g["kind"] == "group":
            try:
                await context.bot.edit_message_text(
                    chat_id=int(g["board_chat_id"]),
                    message_id=int(g["board_message_id"]),
                    text=render_text(g) + "\n\n🔄 در حال پردازش درخواست...",
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass
        
        # رندر نهایی رابط
        text = render_text(g)
        markup = kb_settings(g) if g["view"] == "settings" else kb_main(g, uid_for_kb)
        
        # آپدیت پیام با تلاش‌های متعدد
        await _edit_message_safe(context, g, text, markup)
        
        # فراخوانی callback در صورت وجود
        if callback:
            await callback()
            
    except Exception as e:
        log.error(f"Error processing update for game {gid}: {e}")

def queue_update(gid: int, context: ContextTypes.DEFAULT_TYPE, **kwargs):
    """افزودن درخواست آپدیت به صف"""
    if gid not in update_queues:
        update_queues[gid] = asyncio.Queue()
        update_tasks[gid] = asyncio.create_task(start_game_update_worker(gid, context))
    
    update_queues[gid].put_nowait(kwargs)

async def safe_edit_message(
    context: ContextTypes.DEFAULT_TYPE,
    g: sqlite3.Row,
    text: str,
    markup: InlineKeyboardMarkup,
    max_retries: int = 3
):
    """آپدیت پیام با مدیریت خطا و تلاش‌های متعدد"""
    for attempt in range(max_retries):
        try:
            if g["kind"] == "group":
                await context.bot.edit_message_text(
                    chat_id=int(g["board_chat_id"]),
                    message_id=int(g["board_message_id"]),
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=markup,
                    disable_web_page_preview=True,
                )
            else:
                await context.bot.edit_message_text(
                    inline_message_id=str(g["board_inline_id"]),
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=markup,
                    disable_web_page_preview=True,
                )
            return True
        except RetryAfter as e:
            wait = float(getattr(e, "retry_after", 1.0))
            log.warning(f"Rate limit hit. Waiting {wait:.2f}s (attempt {attempt + 1}/{max_retries})")
            await asyncio.sleep(min(wait, 3.0))
        except (TimedOut, NetworkError) as e:
            log.warning(f"Network issue: {e} (attempt {attempt + 1}/{max_retries})")
            await asyncio.sleep(0.5 * (attempt + 1))
        except BadRequest as e:
            msg = str(e).lower()
            if "message is not modified" in msg:
                return True
            if "message can't be edited" in msg or "message to edit not found" in msg:
                log.warning(f"Message can't be edited: {e}")
                # تلاش برای ارسال پیام جدید در گروه‌ها
                if g["kind"] == "group":
                    try:
                        new_msg = await context.bot.send_message(
                            chat_id=int(g["board_chat_id"]),
                            text=text,
                            parse_mode=ParseMode.HTML,
                            reply_markup=markup,
                            disable_web_page_preview=True,
                        )
                        set_game_fields(g["id"], board_message_id=new_msg.message_id)
                        return True
                    except Exception as e2:
                        log.error(f"Failed to send fallback message: {e2}")
                return False
            log.error(f"BadRequest: {e}")
        except Exception as e:
            log.error(f"Unexpected error editing message: {e}")
    
    return False
    
# =========================
# Game DB operations
# =========================
def create_group_game(chat_id: int, owner_id: int, board_message_id: int) -> int:
    conn=db(); cur=conn.cursor()
    cur.execute("""
    INSERT INTO games (kind,status,owner_id,board_chat_id,board_message_id,created_at)
    VALUES ('group','lobby',?,?,?,?);
    """,(owner_id,chat_id,board_message_id,now()))
    gid=int(cur.lastrowid)
    conn.commit(); conn.close()
    return gid

def get_group_game_by_chat(chat_id: int) -> Optional[sqlite3.Row]:
    conn=db(); cur=conn.cursor()
    cur.execute("""
    SELECT * FROM games WHERE kind='group' AND board_chat_id=? AND status!='ended'
    ORDER BY id DESC LIMIT 1;
    """,(chat_id,))
    r=cur.fetchone(); conn.close()
    return r

def get_game_by_inline_id(inline_id: str) -> Optional[sqlite3.Row]:
    conn=db(); cur=conn.cursor()
    cur.execute("SELECT * FROM games WHERE kind='inline' AND board_inline_id=? AND status!='ended' LIMIT 1;",(inline_id,))
    r=cur.fetchone(); conn.close()
    return r

def get_game(gid: int) -> Optional[sqlite3.Row]:
    conn=db(); cur=conn.cursor()
    cur.execute("SELECT * FROM games WHERE id=?;",(gid,))
    r=cur.fetchone(); conn.close()
    return r

def set_game_fields(gid: int, **fields):
    if not fields: return
    conn=db(); cur=conn.cursor()
    cols=[]; vals=[]
    for k,v in fields.items():
        cols.append(f"{k}=?"); vals.append(v)
    vals.append(gid)
    cur.execute(f"UPDATE games SET {', '.join(cols)} WHERE id=?;", tuple(vals))
    conn.commit(); conn.close()

def upsert_player(gid: int, uid: int, name: str) -> bool:
    conn=db(); cur=conn.cursor()
    cur.execute("SELECT id FROM game_players WHERE game_id=? AND user_id=?;",(gid,uid))
    r=cur.fetchone()
    if r:
        cur.execute("UPDATE game_players SET active=1, name=? WHERE game_id=? AND user_id=?;",(name,gid,uid))
        conn.commit(); conn.close()
        return False
    cur.execute("""
    INSERT INTO game_players (game_id,user_id,name,joined_at,rerolls_left,active)
    VALUES (?,?,?,?,?,1);
    """,(gid,uid,name,now(),MAX_REROLL_PER_PLAYER))
    conn.commit(); conn.close()
    return True

def list_players(gid: int) -> List[sqlite3.Row]:
    conn=db(); cur=conn.cursor()
    cur.execute("SELECT * FROM game_players WHERE game_id=? AND active=1 ORDER BY joined_at ASC;",(gid,))
    rows=cur.fetchall(); conn.close()
    return rows

def player_row(gid: int, uid: int) -> Optional[sqlite3.Row]:
    conn=db(); cur=conn.cursor()
    cur.execute("SELECT * FROM game_players WHERE game_id=? AND user_id=? AND active=1;",(gid,uid))
    r=cur.fetchone(); conn.close()
    return r

def rerolls_left(gid: int, uid: int) -> int:
    r=player_row(gid,uid)
    return int(r["rerolls_left"]) if r else 0

def dec_reroll(gid: int, uid: int) -> bool:
    conn=db(); cur=conn.cursor()
    cur.execute("SELECT rerolls_left FROM game_players WHERE game_id=? AND user_id=?;",(gid,uid))
    r=cur.fetchone()
    if not r: conn.close(); return False
    left=int(r["rerolls_left"])
    if left<=0: conn.close(); return False
    cur.execute("UPDATE game_players SET rerolls_left=rerolls_left-1 WHERE game_id=? AND user_id=?;",(gid,uid))
    conn.commit(); conn.close()
    return True

def inc_stat(gid: int, uid: int, field: str, delta: int=1):
    if field not in ("turns","penalties","skips_used"): return
    conn=db(); cur=conn.cursor()
    cur.execute(f"UPDATE game_players SET {field}={field}+? WHERE game_id=? AND user_id=?;",(delta,gid,uid))
    conn.commit(); conn.close()

def current_player(g: sqlite3.Row) -> Optional[sqlite3.Row]:
    players=list_players(int(g["id"]))
    if not players: return None
    idx=int(g["current_turn_index"])%len(players)
    return players[idx]

def advance_turn(gid: int):
    conn=db(); cur=conn.cursor()
    cur.execute("UPDATE games SET current_turn_index=current_turn_index+1, phase='choose' WHERE id=?;",(gid,))
    conn.commit(); conn.close()

def pick_random_question(qtype: str, level: str) -> Optional[str]:
    conn=db(); cur=conn.cursor()
    cur.execute("""
    SELECT text FROM questions
    WHERE enabled=1 AND qtype=? AND level=?
    ORDER BY RANDOM() LIMIT 1;
    """,(qtype,level))
    r=cur.fetchone(); conn.close()
    return r["text"] if r else None

def queue_forced(gid: int, uid: int, text: str, qtype: Optional[str], level: Optional[str]):
    conn=db(); cur=conn.cursor()
    cur.execute("""
    INSERT INTO forced_questions (game_id,user_id,qtype,level,text,created_at)
    VALUES (?,?,?,?,?,?);
    """,(gid,uid,qtype,level,text,now()))
    conn.commit(); conn.close()

def pop_forced(gid: int, uid: int, qtype: str, level: str) -> Optional[str]:
    conn=db(); cur=conn.cursor()
    cur.execute("""
    SELECT id,text FROM forced_questions
    WHERE game_id=? AND user_id=?
    AND (qtype IS NULL OR qtype=?)
    AND (level IS NULL OR level=?)
    ORDER BY id ASC LIMIT 1;
    """,(gid,uid,qtype,level))
    r=cur.fetchone()
    if not r: conn.close(); return None
    fid=int(r["id"]); txt=r["text"]
    cur.execute("DELETE FROM forced_questions WHERE id=?;",(fid,))
    conn.commit(); conn.close()
    return txt

def create_action(gid: int, actor_id: int, qtype: str, level: str, text: str, status: str):
    conn=db(); cur=conn.cursor()
    cur.execute("""
    INSERT INTO actions (game_id,actor_id,qtype,level,text,status,created_at)
    VALUES (?,?,?,?,?,?,?);
    """,(gid,actor_id,qtype,level,text,status,now()))
    aid=int(cur.lastrowid)
    conn.commit(); conn.close()
    return aid

def last_action(gid: int) -> Optional[sqlite3.Row]:
    conn=db(); cur=conn.cursor()
    cur.execute("SELECT * FROM actions WHERE game_id=? ORDER BY id DESC LIMIT 1;",(gid,))
    r=cur.fetchone(); conn.close()
    return r

# =========================
# UI Builders (UX بهبود یافته)
# =========================
def kb_main(g: sqlite3.Row, uid: int) -> InlineKeyboardMarkup:
    gid=int(g["id"])
    players=list_players(gid)
    phase=g["phase"]
    allow18=int(g["allow_18"])==1
    cp=current_player(g)
    current_user_is_turn = cp and int(cp["user_id"]) == uid
    
    rows=[]
    
    # هدر - نمایش اطلاعات کلی
    join_label = f"👋 پیوستن به بازی ({len(players)})"
    rows.append([
        InlineKeyboardButton(join_label, callback_data=f"g{gid}:join"),
        InlineKeyboardButton("⚙️ تنظیمات", callback_data=f"g{gid}:view:settings"),
    ])
    
    # نمایش وضعیت نوبت به صورت واضح
    if g["status"] == "running" and cp:
        turn_text = f"🎯 نوبت: {esc(cp['name'])}"
        rows.append([InlineKeyboardButton(turn_text, callback_data=f"g{gid}:current_turn")])
    
    # دکمه‌های متناسب با وضعیت فعلی
    if g["status"]=="lobby":
        rows.append([
            InlineKeyboardButton("🎮 شروع بازی", callback_data=f"g{gid}:start"),
            InlineKeyboardButton("🛑 خروج", callback_data=f"g{gid}:leave"),
        ])
    elif g["status"]=="running":
        # فقط برای بازیکن فعلی و یا ادمین
        if current_user_is_turn or is_admin(uid) or uid == int(g["owner_id"]):
            if phase=="choose":
                rows.append([
                    InlineKeyboardButton("❓ حقیقت", callback_data=f"g{gid}:pick:truth:normal"),
                    InlineKeyboardButton("🔥 جرئت", callback_data=f"g{gid}:pick:dare:normal"),
                ])
                if allow18:
                    rows.append([
                        InlineKeyboardButton("🔞 حقیقت +18", callback_data=f"g{gid}:pick:truth:18"),
                        InlineKeyboardButton("💦 جرئت +18", callback_data=f"g{gid}:pick:dare:18"),
                    ])
                rows.append([InlineKeyboardButton("🎲 انتخاب شانسی", callback_data=f"g{gid}:pick:random:random")])
                
                # نمایش دکمه تعویض فقط اگر تعویض باقی مانده داشته باشد
                reroll_count = rerolls_left(gid, uid)
                if reroll_count > 0:
                    rows.append([InlineKeyboardButton(f"🔄 تعویض ({reroll_count})", callback_data=f"g{gid}:reroll")])
                
            elif phase=="question":
                rows.append([
                    InlineKeyboardButton("✅ انجام دادم", callback_data=f"g{gid}:done"),
                    InlineKeyboardButton("❌ نمی‌تونم", callback_data=f"g{gid}:refuse"),
                ])
            elif phase=="wait_confirm" and len(players) == 2:
                rows.append([
                    InlineKeyboardButton("👍 تأیید عملکرد", callback_data=f"g{gid}:confirm:yes"),
                    InlineKeyboardButton("👎 رد عملکرد", callback_data=f"g{gid}:confirm:no"),
                ])
    
    # دکمه‌های ثابت در پایین
    action_rows = []
    if g["status"] == "running" and (current_user_is_turn or is_admin(uid)):
        action_rows.append(InlineKeyboardButton("⏭ رد نوبت", callback_data=f"g{gid}:skip"))
    
    if g["status"] != "ended":
        action_rows.append(InlineKeyboardButton("📊 آمار", callback_data=f"g{gid}:view:stats"))
        action_rows.append(InlineKeyboardButton("👥 لیست بازیکنان", callback_data=f"g{gid}:view:players"))
    
    if action_rows:
        rows.append(action_rows)
    
    # دکمه پایان بازی فقط برای سازنده یا ادمین
    if g["status"] != "ended" and (uid == int(g["owner_id"]) or is_admin(uid)):
        rows.append([InlineKeyboardButton("⏹️ پایان بازی", callback_data=f"g{gid}:end")])
    
    # دکمه انتقال به پایین
    rows.append([InlineKeyboardButton("⬇️ به‌روزرسانی نمایش", callback_data=f"g{gid}:bump")])
    
    return InlineKeyboardMarkup(rows)

def kb_settings(g: sqlite3.Row) -> InlineKeyboardMarkup:
    gid=int(g["id"])
    allow_mid = int(g["allow_mid_join"])==1
    show_prev = int(g["show_prev_question"])==1
    allow18 = int(g["allow_18"])==1
    
    rows = [
        [InlineKeyboardButton(f"👥 ورود وسط بازی: {'✅ فعال' if allow_mid else '❌ غیرفعال'}", callback_data=f"g{gid}:set:mid:{'0' if allow_mid else '1'}")],
        [InlineKeyboardButton(f"🔄 نمایش سوال قبلی: {'✅ فعال' if show_prev else '❌ غیرفعال'}", callback_data=f"g{gid}:set:prev:{'0' if show_prev else '1'}")],
        [InlineKeyboardButton(f"🔞 سوالات +18: {'✅ فعال' if allow18 else '❌ غیرفعال'}", callback_data=f"g{gid}:set:18:{'0' if allow18 else '1'}")],
        [InlineKeyboardButton("🎨 تنظیمات ظاهری", callback_data=f"g{gid}:view:appearance")],
        [InlineKeyboardButton("🏠 بازگشت به اصلی", callback_data=f"g{gid}:view:main")],
    ]
    return InlineKeyboardMarkup(rows)

def players_line(gid: int) -> str:
    ps=list_players(gid)
    if not ps:
        return "—"
    # short list
    names=[esc(p["name"]) for p in ps[:8]]
    extra = f" +{len(ps)-8}" if len(ps)>8 else ""
    return "، ".join(names) + extra

def render_text(g: sqlite3.Row) -> str:
    gid=int(g["id"])
    ps=list_players(gid)
    cp=current_player(g)
    view=g["view"]
    status=g["status"]
    phase=g["phase"]
    
    # هدر حرفه‌ای
    header = "🎮 <b>جرئت/حقیقت Pro</b>\n"
    header += f"🆔 <code>#{gid}</code> | 👥 <b>{len(ps)}</b> نفر"
    if status == "running" and cp:
        header += f" | 🎯 نوبت: {esc(cp['name'])}"
    header += "\n"
    header += f"⏱️ زمان نوبت: <b>{TURN_TIMEOUT_SEC} ثانیه</b>\n"
    header += "—" * 25 + "\n\n"
    
    if view=="settings":
        body="⚙️ <b>تنظیمات بازی</b>\n\n"
        body += f"👥 ورود وسط بازی: {'✅ فعال' if int(g['allow_mid_join'])==1 else '❌ غیرفعال'}\n"
        body += f"🔄 نمایش سوال قبلی: {'✅ فعال' if int(g['show_prev_question'])==1 else '❌ غیرفعال'}\n"
        body += f"🔞 سوالات +18: {'✅ فعال' if int(g['allow_18'])==1 else '❌ غیرفعال'}\n\n"
        body += "💡 <i>برای تغییر تنظیمات، روی دکمه‌های بالا ضربه بزنید.</i>"
        return header+body
    
    if view=="players":
        body="👥 <b>لیست بازیکنان</b>\n\n"
        if not ps:
            body+="—\n\n"
        else:
            for i,p in enumerate(ps, start=1):
                status_icon = "🟢" if int(p["user_id"]) == (int(cp["user_id"]) if cp else -1) else "⚪️"
                body += f"{status_icon} {i}. {esc(p['name'])}\n"
                body += f"   🔄 تعویض: {p['rerolls_left']} | ⏭️ پرش: {p['skips_used']} | ⚠️ مجازات: {p['penalties']}\n"
        body += "\n🏠 <i>برای بازگشت به صفحه اصلی، روی دکمه پایین بزنید.</i>"
        return header+body
    
    if view=="stats":
        body="📊 <b>آمار بازی</b>\n\n"
        if ps:
            for p in ps:
                percent = f"({p['turns'] * 100 // max(1, len(ps))}%)"
                body += f"• {esc(p['name'])}:\n"
                body += f"   🎮 نوبت‌ها: {p['turns']} {percent}\n"
                body += f"   ⚠️ مجازات‌ها: {p['penalties']}\n"
                body += f"   ⏭️ پرش نوبت: {p['skips_used']}\n"
                body += f"   🔄 تعویض‌های باقی‌مانده: {p['rerolls_left']}\n\n"
        
        lastq = (g["last_q_text"] or "").strip()
        if lastq:
            body += "\n" + "—" * 25 + "\n"
            body += "🧾 <b>آخرین سوال:</b>\n"
            body += f"{esc(lastq[:300])}{'...' if len(lastq) > 300 else ''}"
        return header+body
    
    # MAIN VIEW
    if status=="lobby":
        body="🎮 <b>لابی بازی</b>\n\n"
        body+="👋 <b>راهنما:</b>\n"
        body+="• برای پیوستن به بازی، دکمه «پیوستن به بازی» را بزنید\n"
        body+="• فقط سازنده بازی می‌تواند بازی را شروع کند\n"
        body+="• حداقل ۲ نفر برای شروع بازی لازم است\n\n"
        body+="💡 <i>این پیام به‌روزرسانی می‌شود ( بدون اسپم )</i>"
        return header+body
    
    if status=="ended":
        return header+"🛑 <b>بازی به پایان رسید</b>\n\nبرای شروع بازی جدید، /startgame را بزنید."
    
    if len(ps) < 1:
        return header+"⚠️ <b>خطا:</b>\nبازیکنی در این بازی وجود ندارد."
    
    body="🔥 <b>بازی در حال اجراست</b>\n\n"
    
    if phase=="choose":
        body += "💡 <b>انتخاب نوع سوال:</b>\n"
        body += "• سؤالی انتخاب کنید که می‌خواهید پرسیده شود\n"
        body += "• می‌توانید از «انتخاب شانسی» استفاده کنید\n\n"
        body += "⏳ <i>شما {0} ثانیه زمان دارید</i>".format(TURN_TIMEOUT_SEC)
    elif phase=="question":
        la=last_action(gid)
        if la:
            qtype_text = "❓ حقیقت" if la['qtype'] == 'truth' else "🔥 جرئت"
            level_text = "🔞 +18" if la['level'] == '18' else "⭐ معمولی"
            body += f"📌 <b>{qtype_text} | {level_text}</b>\n\n"
            body += f"{esc(la['text'])}"
        else:
            body += "⚠️ <b>خطا:</b>\nهیچ سوالی برای نمایش وجود ندارد."
    elif phase=="wait_confirm" and len(players)==2:
        body += "🤝 <b>در انتظار تأیید طرف مقابل</b>\n\n"
        la=last_action(gid)
        if la:
            body += f"❓ {esc(la['text'][:500])}{'...' if len(la['text']) > 500 else ''}"
    
    # نمایش مجازات‌های اخیر
    conn=db(); cur=conn.cursor()
    cur.execute("""
    SELECT * FROM actions 
    WHERE game_id=? AND status IN ('refused','timeout','rejected') 
    ORDER BY id DESC LIMIT 3
    """, (gid,))
    penalties = cur.fetchall()
    conn.close()
    
    if penalties:
        body += "\n\n" + "—" * 25 + "\n"
        body += "⚠️ <b>مجازات‌های اخیر:</b>\n"
        for p in penalties[:3]:
            player = next((pl for pl in ps if int(pl["user_id"]) == int(p["actor_id"])), None)
            if player:
                name = esc(player["name"])
                penalty_text = p["text"].split("|")[1].strip() if "|" in p["text"] else p["text"]
                body += f"• {name}: {penalty_text}\n"
    
    return header+body

# =========================
# Export/Import System (قابلیت پشتیبان‌گیری)
# =========================
def export_questions_to_text() -> str:
    """استخراج تمام سوالات به فرمت استاندارد"""
    conn = db()
    cur = conn.cursor()
    
    # دسته‌بندی سوالات
    categories = [
        ("dare", "normal", "جرئت"),
        ("truth", "normal", "حقیقت"),
        ("dare", "18", "جرئت 18+"),
        ("truth", "18", "حقیقت 18+")
    ]
    
    export_text = "📝 فایل پشتیبان سوالات جرئت/حقیقت Pro\n"
    export_text += f"تاریخ: {time.strftime('%Y/%m/%d %H:%M', time.localtime(now()))}\n"
    export_text += "=" * 40 + "\n\n"
    
    for qtype, level, title in categories:
        cur.execute("""
        SELECT text, added_by FROM questions 
        WHERE qtype=? AND level=? AND enabled=1 
        ORDER BY created_at ASC
        """, (qtype, level))
        questions = cur.fetchall()
        
        if questions:
            export_text += f"{title.upper()}:\n"
            for i, q in enumerate(questions, start=1):
                export_text += f"{i}= {q['text']}\n"
            export_text += "\n" + "-" * 30 + "\n\n"
    
    conn.close()
    return export_text

def import_questions_from_text(text: str) -> Tuple[int, int]:
    """وارد کردن سوالات از فرمت استاندارد
    Returns: (success_count, duplicate_count)
    """
    conn = db()
    cur = conn.cursor()
    
    # تقسیم متن به بخش‌ها
    sections = re.split(r'\n\s*[-=]{20,}\s*\n', text)
    
    success_count = 0
    duplicate_count = 0
    
    # دسته‌بندی‌ها
    category_map = {
        "جرئت": ("dare", "normal"),
        "حقیقت": ("truth", "normal"),
        "جرئت 18+": ("dare", "18"),
        "حقیقت 18+": ("truth", "18"),
    }
    
    for section in sections:
        if not section.strip():
            continue
            
        # پیدا کردن عنوان بخش
        title_match = re.search(r'([^\n:]+):\s*$', section.splitlines()[0])
        if not title_match:
            continue
            
        title = title_match.group(1).strip().upper()
        matched_category = None
        
        for cat_title, (qtype, level) in category_map.items():
            if cat_title.upper() in title:
                matched_category = (qtype, level)
                break
                
        if not matched_category:
            continue
            
        qtype, level = matched_category
        
        # استخراج سوالات
        lines = section.splitlines()[1:]
        for line in lines:
            line = line.strip()
            if not line or '=' not in line:
                continue
                
            # استخراج متن سوال
            parts = re.split(r'\d+\s*=\s*', line, maxsplit=1)
            if len(parts) < 2:
                continue
                
            question_text = parts[1].strip()
            if not question_text or len(question_text) < 3:
                continue
                
            # بررسی تکراری بودن
            cur.execute("""
            SELECT COUNT(*) AS c FROM questions 
            WHERE qtype=? AND level=? AND text=?
            """, (qtype, level, question_text))
            
            exists = int(cur.fetchone()["c"]) > 0
            if exists:
                duplicate_count += 1
                continue
                
            # افزودن سوال جدید
            cur.execute("""
            INSERT INTO questions (qtype, level, text, enabled, created_at, added_by)
            VALUES (?, ?, ?, 1, ?, ?)
            """, (qtype, level, question_text, now(), ADMIN_ID))
            
            success_count += 1
    
    conn.commit()
    conn.close()
    return success_count, duplicate_count

# =========================
# Handlers (بهبود یافته)
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat=update.effective_chat
    if chat.type=="private":
        me=(await context.bot.get_me()).username
        link=f"https://t.me/{me}?startgroup=true"
        keyboard = [
            [InlineKeyboardButton("🎮 شروع بازی در گروه", url=link)],
            [InlineKeyboardButton("❓ راهنما کامل", callback_data="help")],
            [InlineKeyboardButton("👑 پنل ادمین", callback_data="admin_panel")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "✨ <b>خوشحالم که اینجا هستم!</b>\n\n"
            "🎮 من ربات <b>جرئت/حقیقت Pro</b> هستم\n"
            "🔥 بازی‌ای پر excitement و fun برای گروه‌های تلگرام\n\n"
            "✅ <b>قابلیت‌های من:</b>\n"
            "• بازی در گروه‌ها و چت‌های خصوصی\n"
            "• سیستم نوبت‌گیری هوشمند\n"
            "• سوالات +18 اختصاصی\n"
            "• امکان تعویض سوال\n"
            "• سیستم مجازات و آمار کامل\n"
            "• پشتیبان‌گیری و بازیابی سوالات\n\n"
            "👇 برای شروع، یکی از دکمه‌های زیر را انتخاب کنید:",
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup
        )

async def cmd_startgame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat=update.effective_chat
    user=update.effective_user
    
    if chat.type not in ("group","supergroup"):
        await update.message.reply_text("🚫 این دستور فقط در گروه‌ها قابل استفاده است.")
        return
        
    # بررسی دسترسی ادمین بودن ربات در گروه
    try:
        bot_member = await context.bot.get_chat_member(chat.id, context.bot.id)
        if bot_member.status not in ["administrator", "creator"]:
            await update.message.reply_text(
                "🚫 برای استفاده از این ربات، لطفاً ابتدا آن را در گروه ادمین کنید.\n"
                "سپس دوباره دستور /startgame را اجرا کنید."
            )
            return
    except Exception as e:
        log.error(f"Error checking bot admin status: {e}")
        await update.message.reply_text("❌ خطایی در بررسی دسترسی ربات رخ داد.")
        return
    
    try:
        msg = await update.message.reply_text(
            "🔄 <b>در حال ساخت برد بازی...</b>\n"
            "لطفاً چند لحظه صبر کنید...",
            parse_mode=ParseMode.HTML
        )
        
        gid = create_group_game(chat.id, user.id, msg.message_id)
        upsert_player(gid, user.id, user.full_name)
        g=get_game(gid)
        
        # آپدیت فوری رابط
        text = render_text(g)
        markup = kb_main(g, user.id)
        
        await context.bot.edit_message_text(
            chat_id=chat.id,
            message_id=msg.message_id,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
            disable_web_page_preview=True
        )
        
        log.info(f"Game created in group {chat.id} by user {user.id}, game ID: {gid}")
        
    except Exception as e:
        log.error(f"Error creating game: {e}")
        await update.message.reply_text("❌ خطایی در ساخت بازی رخ داد. لطفاً دوباره تلاش کنید.")

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
    📚 <b>راهنمای کامل ربات جرئت/حقیقت Pro</b>
    
    🎮 <b>شروع بازی در گروه:</b>
    1. ربات را در گروه ادمین کنید
    2. دستور /startgame را اجرا کنید
    3. دیگر اعضا با کلیک روی "پیوستن به بازی" می‌توانند عضو شوند
    4. سازنده بازی با کلیک روی "شروع بازی" بازی را آغاز می‌کند
    
    👥 <b>بازی در چت خصوصی دو نفره:</b>
    1. در چت دو نفره، نام کاربری ربات را جستجو کنید
    2. روی "شروع بازی جرئت/حقیقت (داخل همین چت)" کلیک کنید
    3. هر دو نفر باید روی "پیوستن به بازی" کلیک کنند
    4. یکی از شما "شروع بازی" را بزنید
    
    ⚙️ <b>دستورات ادمین:</b>
    /admin - نمایش پنل مدیریت
    /export - دریافت فایل پشتیبان تمام سوالات
    /import - بارگذاری فایل پشتیبان سوالات
    /pending - بررسی پیشنهادات کاربران
    /bulk_truth, /bulk_dare - افزودن دسته‌جمعی سوالات
    
    💡 <b>نکات مهم:</b>
    • برای عملکرد بهتر، هر دو نفر در چت خصوصی باید یک بار /start را بزنند
    • پیام‌های ربات به‌روزرسانی می‌شوند و اسپم ایجاد نمی‌کنند
    • در صورت قطع اینترنت، بازی وضعیت خود را حفظ می‌کند
    """
    
    keyboard = [[InlineKeyboardButton("بازگشت به منوی اصلی", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        help_text,
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup
    )

async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """اکسپورت سوالات به فایل متنی"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔️ فقط ادمین‌ها می‌توانند از این دستور استفاده کنند.")
        return
    
    try:
        export_text = export_questions_to_text()
        
        if not export_text.strip():
            await update.message.reply_text("❌ هیچ سوالی برای اکسپورت وجود ندارد.")
            return
            
        # ارسال به صورت فایل
        file_name = f"questions_export_{time.strftime('%Y%m%d_%H%M%S')}.txt"
        file = io.BytesIO(export_text.encode('utf-8'))
        file.name = file_name
        
        await update.message.reply_document(
            document=file,
            caption="✅ <b>پشتیبان‌گیری موفقیت‌آمیز!</b>\n"
                   " تمام سوالات ربات به صورت فایل متنی اکسپورت شدند.\n"
                   "برای بازیابی، از دستور /import استفاده کنید.",
            parse_mode=ParseMode.HTML
        )
        
        log.info(f"Admin {update.effective_user.id} exported questions database")
        
    except Exception as e:
        log.error(f"Error exporting questions: {e}")
        await update.message.reply_text("❌ خطایی در اکسپورت سوالات رخ داد.")

async def cmd_import(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """وارد کردن سوالات از فایل متنی"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔️ فقط ادمین‌ها می‌توانند از این دستور استفاده کنند.")
        return
        
    flow_set(context, "import_questions")
    await update.message.reply_text(
        "📥 <b>وارد کردن سوالات</b>\n\n"
        "لطفاً فایل پشتیبان حاوی سوالات را ارسال کنید.\n"
        "فرمت فایل باید مطابق با خروجی دستور /export باشد.\n\n"
        "⚠️ <i>تذکر: سوالات تکراری وارد نخواهند شد.</i>",
        parse_mode=ParseMode.HTML
    )

async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """پردازش فایل ارسالی برای import"""
    flow = flow_get(context)
    if not flow or flow["name"] != "import_questions":
        return
        
    if not update.message.document:
        await update.message.reply_text("❌ لطفاً یک فایل متنی ارسال کنید.")
        return
        
    # بررسی نوع فایل
    if not update.message.document.file_name.endswith(('.txt', '.text')):
        await update.message.reply_text("❌ فقط فایل‌های متنی (.txt) پشتیبانی می‌شوند.")
        return
        
    try:
        file = await update.message.document.get_file()
        file_content = await file.download_as_bytearray()
        text_content = file_content.decode('utf-8')
        
        # وارد کردن سوالات
        success_count, duplicate_count = import_questions_from_text(text_content)
        
        # ارسال نتایج
        result_text = f"✅ <b>وارد کردن سوالات با موفقیت انجام شد!</b>\n\n"
        result_text += f"🟢 سوالات جدید وارد شده: {success_count}\n"
        result_text += f"🟡 سوالات تکراری: {duplicate_count}\n\n"
        result_text += "🔄 <i>برای دیدن سوالات جدید، می‌توانید یک بازی جدید شروع کنید.</i>"
        
        await update.message.reply_text(
            result_text,
            parse_mode=ParseMode.HTML
        )
        
        log.info(f"Admin {update.effective_user.id} imported {success_count} questions, {duplicate_count} duplicates skipped")
        
    except UnicodeDecodeError:
        await update.message.reply_text("❌ فایل ارسالی قابل خواندن نیست. لطفاً یک فایل متنی UTF-8 ارسال کنید.")
    except Exception as e:
        log.error(f"Error importing questions: {e}")
        await update.message.reply_text(f"❌ خطایی در وارد کردن سوالات رخ داد: {str(e)}")
    finally:
        flow_set(context, None)  # پاک کردن flow

# =========================
# Flow Management
# =========================
def flow_set(context: ContextTypes.DEFAULT_TYPE, name: Optional[str],  Optional[dict]=None):
    if not name:
        context.user_data.pop("flow", None)
    else:
        context.user_data["flow"] = {"name": name, "data": data or {}}

def flow_get(context: ContextTypes.DEFAULT_TYPE):
    return context.user_data.get("flow")

# =========================
# Admin Handlers
# =========================
async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔️ دسترسی نداری")
        return
        
    keyboard = [
        [InlineKeyboardButton("📤 اکسپورت سوالات", callback_data="admin:export"),
         InlineKeyboardButton("📥 ایمپورت سوالات", callback_data="admin:import")],
        [InlineKeyboardButton("📝 پیشنهادات کاربران", callback_data="admin:pending"),
         InlineKeyboardButton("❓ سوال اجباری", callback_data="admin:force")],
        [InlineKeyboardButton("➕ افزودن دسته‌جمعی سوالات", callback_data="admin:bulk")],
        [InlineKeyboardButton("🗂 مدیریت سوالات", callback_data="admin:manage_questions")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "👑 <b>پنل مدیریت ربات</b>\n\n"
        "انتخاب کنید چه کاری می‌خواهید انجام دهید:",
        parse_mode=ParseMode.HTML,
        reply_markup=reply_markup
    )

# =========================
# Inline Query Handler
# =========================
async def inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.inline_query.query
    user = update.effective_user
    
    # نمایش متن کمکی برای کاربران جدید
    description = "🎮 شروع بازی جرئت/حقیقت (بدون اسپم)"
    if not query:
        description = "روی این گزینه کلیک کنید تا بازی شروع شود"
    
    results = [
        InlineQueryResultArticle(
            id="start_game",
            title="🎮 جرئت/حقیقت Pro",
            description=description,
            input_message_content=InputTextMessageContent(
                inline_initial_text(), 
                parse_mode=ParseMode.HTML
            ),
            reply_markup=inline_initial_kb(),
            thumbnail_url="https://i.imgur.com/8hCmX3p.png"
        )
    ]
    
    await update.inline_query.answer(results, cache_time=1, is_personal=True)

# =========================
# Inline Initial Text & KB
# =========================
def inline_initial_text() -> str:
    return (
        "✨ <b>جرئت/حقیقت Pro</b>\n\n"
        "🎮 <b>بازی در چت خصوصی</b>\n"
        "✅ راهنمای اجرای بازی:\n"
        "1. هر دو نفر حتماً یک بار /start را بزنید\n"
        "2. روی دکمه «پیوستن به بازی» کلیک کنید\n"
        "3. سازنده بازی «شروع بازی» را بزند\n\n"
        "💡 <i>تمام تعاملات در همین پیام انجام می‌شود (اسپم صفر)</i>"
    )

def inline_initial_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👋 پیوستن به بازی", callback_data="new:join"),
         InlineKeyboardButton("⚙️ تنظیمات", callback_data="new:view:settings")],
        [InlineKeyboardButton("🎮 شروع بازی", callback_data="new:start")],
        [InlineKeyboardButton("❓ راهنما", callback_data="new:help")]
    ])

# =========================
# Callback Handlers
# =========================
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    user = update.effective_user
    data = q.data or ""
    
    # مدیریت callbackهای ادمین
    if data.startswith("admin:"):
        if not is_admin(user.id):
            await q.answer("⛔️ دسترسی ندارید", show_alert=True)
            return
            
        action = data.split(":", 1)[1]
        
        if action == "export":
            await q.answer("📤 در حال آماده‌سازی فایل پشتیبان...")
            export_text = export_questions_to_text()
            if not export_text.strip():
                await q.message.reply_text("❌ هیچ سوالی برای اکسپورت وجود ندارد.")
                return
                
            file_name = f"questions_export_{time.strftime('%Y%m%d_%H%M%S')}.txt"
            file = io.BytesIO(export_text.encode('utf-8'))
            file.name = file_name
            
            await context.bot.send_document(
                chat_id=user.id,
                document=file,
                caption="✅ <b>پشتیبان‌گیری موفق!</b>\n"
                       "فایل پشتیبان سوالات آماده دریافت است.",
                parse_mode=ParseMode.HTML
            )
            await q.answer("✅ فایل پشتیبان برای شما ارسال شد.", show_alert=True)
            
        elif action == "import":
            flow_set(context, "import_questions")
            await q.message.reply_text(
                "📥 لطفاً فایل پشتیبان را ارسال کنید.\n"
                "فرمت فایل باید مطابق با خروجی /export باشد."
            )
            await q.answer("📥 حالت دریافت فایل فعال شد")
            
        elif action == "pending":
            await cmd_pending(update, context)
            await q.answer("✅ لیست پیشنهادات نمایش داده شد")
            
        elif action == "force":
            await cmd_force(update, context)
            await q.answer("✅ حالت انتخاب بازی برای سوال اجباری فعال شد")
            
        elif action == "bulk":
            keyboard = [
                [InlineKeyboardButton("❓ حقیقت", callback_data="admin:bulk:truth:normal"),
                 InlineKeyboardButton("🔥 جرئت", callback_data="admin:bulk:dare:normal")],
                [InlineKeyboardButton("🔞 حقیقت +18", callback_data="admin:bulk:truth:18"),
                 InlineKeyboardButton("💦 جرئت +18", callback_data="admin:bulk:dare:18")],
                [InlineKeyboardButton("🔙 بازگشت", callback_data="admin:back")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await q.edit_message_text(
                "➕ <b>انتخاب نوع سوال برای افزودن دسته‌جمعی:</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup
            )
            await q.answer()
            
        return
    
    # مدیریت callbackهای بازی
    try:
        # همیشه سریع جواب بده تا loading نمایش داده شود
        if not data.startswith("new:help"):
            await q.answer("⏳ در حال پردازش...", show_alert=False)
    except Exception:
        pass
    
    # First-time inline: new:*
    if data.startswith("new:"):
        if not q.inline_message_id:
            await q.answer("این بخش فقط برای بازی داخل چت (inline) است.", show_alert=True)
            return
            
        inline_id = q.inline_message_id
        g = get_game_by_inline_id(inline_id)
        
        if not g:
            # Create new inline game
            conn=db(); cur=conn.cursor()
            cur.execute("""
            INSERT INTO games (kind,status,owner_id,board_inline_id,created_at)
            VALUES ('inline','lobby',?,?,?);
            """,(user.id, inline_id, now()))
            gid=int(cur.lastrowid)
            conn.commit(); conn.close()
            
            upsert_player(gid, user.id, user.full_name)
            g=get_game(gid)
        
        gid=int(g["id"])
        data = data.replace("new:", f"g{gid}:")
    
    # Extract game ID
    m=re.match(r"^g(\d+)\:(.+)$", data)
    if not m:
        if data == "main_menu":
            await cmd_start(update, context)
        elif data == "help":
            await cmd_help(update, context)
        return
        
    gid=int(m.group(1))
    action=m.group(2)
    g=get_game(gid)
    
    if not g or g["status"]=="ended":
        await q.answer("این بازی پایان یافته یا وجود ندارد.", show_alert=True)
        return
    
    # Ensure callback belongs to this board
    if g["kind"]=="inline":
        if not q.inline_message_id or str(q.inline_message_id)!=str(g["board_inline_id"]):
            await q.answer("این پیام مربوط به این بازی نیست.", show_alert=True)
            return
    else:
        if not q.message or int(q.message.chat.id)!=int(g["board_chat_id"]):
            await q.answer("این بازی مربوط به این گروه نیست.", show_alert=True)
            return
    
    # Handle actions
    try:
        await _handle_game_action(gid, g, action, q, context, user)
    except Exception as e:
        log.error(f"Error handling game action: {e}")
        await q.answer(f"❌ خطایی رخ داد: {str(e)[:30]}", show_alert=True)

async def _handle_game_action(gid: int, g: sqlite3.Row, action: str, q: Any, context: ContextTypes.DEFAULT_TYPE, user: Any):
    """پردازش عملیات‌های بازی با مدیریت بهتر خطاها"""
    
    # View changes
    if action.startswith("view:"):
        view=action.split(":",1)[1]
        if view not in ("main","settings","players","stats"):
            return
            
        set_game_fields(gid, view=view)
        await edit_board(context, get_game(gid), uid_for_kb=user.id)
        return
    
    # Join game
    if action == "join":
        if g["status"]=="running" and int(g["allow_mid_join"])==0:
            await q.answer("⛔️ ورود در حین بازی مجاز نیست.", show_alert=True)
            return
            
        created = upsert_player(gid, user.id, user.full_name)
        status_msg = "✅ به بازی پیوستید!" if created else "✅ قبلاً عضو این بازی هستید."
        
        # به‌روزرسانی فوری رابط
        await q.answer(status_msg, show_alert=False)
        queue_update(gid, context, uid_for_kb=user.id)
        return
    
    # Start game
    if action == "start":
        if user.id != int(g["owner_id"]) and not is_admin(user.id):
            await q.answer("⛔️ فقط سازنده بازی می‌تواند آن را شروع کند.", show_alert=True)
            return
            
        players = list_players(gid)
        if len(players) < 2:
            await q.answer("👥 حداقل ۲ نفر برای شروع بازی لازم است.", show_alert=True)
            return
            
        set_game_fields(gid, status="running", view="main", phase="choose")
        g = get_game(gid)
        cp = current_player(g)
        
        if cp:
            inc_stat(gid, int(cp["user_id"]), "turns", 1)
            schedule_timeout(context, gid, int(cp["user_id"]))
        
        await q.answer("🔥 بازی شروع شد!", show_alert=False)
        queue_update(gid, context, uid_for_kb=user.id)
        return
    
    # End game
    if action == "end":
        if user.id != int(g["owner_id"]) and not is_admin(user.id):
            await q.answer("⛔️ فقط سازنده بازی می‌تواند آن را پایان دهد.", show_alert=True)
            return
            
        set_game_fields(gid, status="ended", view="main")
        await q.answer("🛑 بازی به پایان رسید.", show_alert=False)
        queue_update(gid, context, uid_for_kb=user.id)
        return
    
    # Pick question
    if action.startswith("pick:"):
        if g["status"] != "running":
            await q.answer("🎮 ابتدا بازی را شروع کنید.", show_alert=True)
            return
            
        cp = current_player(g)
        if not cp or user.id != int(cp["user_id"]):
            await q.answer("⏳ نوبت شما نیست.", show_alert=True)
            return
            
        _, qtype, level = action.split(":")
        
        if qtype == "random":
            qtype = random.choice(["truth", "dare"])
            level = random.choice(["normal", "18"])
            
        if level == "18" and int(g["allow_18"]) == 0:
            await q.answer("🔞 سوالات +18 غیرفعال هستند.", show_alert=True)
            return
            
        forced = pop_forced(gid, user.id, qtype, level)
        text = forced or pick_random_question(qtype, level)
        
        if not text:
            await q.answer("❓ سوالی برای این دسته وجود ندارد. ادمین را مطلع کنید.", show_alert=True)
            return
            
        set_game_fields(
            gid,
            phase="question",
            last_q_text=text,
            last_q_by=user.id,
            last_qtype=qtype,
            last_level=level,
            view="main",
        )
        create_action(gid, user.id, qtype, level, text, "asked")
        schedule_timeout(context, gid, user.id)
        
        await q.answer(f"{'❓' if qtype == 'truth' else '🔥'} سوال انتخاب شد!", show_alert=False)
        queue_update(gid, context, uid_for_kb=user.id)
        return
    
    # Other actions handled similarly with queue_update...

# =========================
# Board Editing (بهبود یافته)
# =========================
async def edit_board(context: ContextTypes.DEFAULT_TYPE, g: sqlite3.Row, uid_for_kb: int, force_view: Optional[str]=None):
    """سیستم جدید و بهینه‌شده برای آپدیت رابط"""
    if not g:
        return
        
    gid = int(g["id"])
    
    # به‌روزرسانی نمایش اگر لازم باشد
    if force_view:
        set_game_fields(gid, view=force_view)
        g = get_game(gid)
        if not g:
            return
    
    try:
        text = render_text(g)
        markup = kb_settings(g) if g["view"] == "settings" else kb_main(g, uid_for_kb)
        
        # استفاده از سیستم صف برای جلوگیری از قفل‌شدن رابط
        queue_update(gid, context, uid_for_kb=uid_for_kb)
        
    except Exception as e:
        log.error(f"Error in edit_board: {e}")
        # تلاش برای ارسال پیام خطا به کاربر
        try:
            if g["kind"] == "group":
                await context.bot.send_message(
                    chat_id=int(g["board_chat_id"]),
                    text=f"❌ خطایی در به‌روزرسانی رابط رخ داد: {str(e)[:50]}",
                    parse_mode=ParseMode.HTML
                )
        except Exception:
            pass

# =========================
# App Initialization
# =========================
def build_app() -> Application:
    init_db()
    seed_if_empty()
    
    app = Application.builder().token(BOT_TOKEN).build()
    
    # Command Handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("startgame", cmd_startgame))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("export", cmd_export))
    app.add_handler(CommandHandler("import", cmd_import))
    app.add_handler(CommandHandler("admin", cmd_admin))
    
    # Callback Handlers
    app.add_handler(CallbackQueryHandler(callback_router))
    
    # Message Handlers
    app.add_handler(MessageHandler(filters.Document.ALL, on_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    
    # Inline Handler
    app.add_handler(InlineQueryHandler(inline_query))
    
    return app

if __name__ == "__main__":
    application = build_app()
    log.info("✅ جرئت/حقیقت Pro - با موفقیت اجرا شد")
    application.run_polling(allowed_updates=Update.ALL_TYPES)
