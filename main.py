import os
import re
import time
import random
import sqlite3
import logging
import asyncio
from typing import Optional, List, Tuple

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InlineQueryResultArticle,
    InputTextMessageContent,
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
        created_at INTEGER NOT NULL
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
            "INSERT INTO questions (qtype, level, text, enabled, created_at) VALUES (?,?,?,?,?);",
            [(a,b,c,1,now()) for (a,b,c) in SEED]
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
# LOCKS (برای حذف لگ/هنگ ادیت)
# =========================
def game_lock(app: Application, gid: int) -> asyncio.Lock:
    locks = app.bot_data.setdefault("game_locks", {})
    if gid not in locks:
        locks[gid] = asyncio.Lock()
    return locks[gid]

# =========================
# UI Builders
# =========================
def kb_main(g: sqlite3.Row, uid: int) -> InlineKeyboardMarkup:
    gid=int(g["id"])
    players=list_players(gid)
    phase=g["phase"]
    allow18=int(g["allow_18"])==1

    rows=[]
    join_label = f"✋ منم میخوام بازی کنم ({len(players)})"
    rows.append([
        InlineKeyboardButton(join_label, callback_data=f"g{gid}:join"),
        InlineKeyboardButton("⚙️ تنظیمات", callback_data=f"g{gid}:view:settings"),
    ])

    # Start only useful in lobby; show for all, but only owner can execute (toast)
    if g["status"]=="lobby":
        rows.append([InlineKeyboardButton("🎮 شروع بازی", callback_data=f"g{gid}:start")])

    rows.append([
        InlineKeyboardButton("👥 بازیکنان", callback_data=f"g{gid}:view:players"),
        InlineKeyboardButton("📊 آمار", callback_data=f"g{gid}:view:stats"),
    ])
    rows.append([
        InlineKeyboardButton("⏭ رد کردن نوبت", callback_data=f"g{gid}:skip"),
        InlineKeyboardButton("❌ پایان بازی", callback_data=f"g{gid}:end"),
    ])

    if g["status"]=="running":
        can_reroll = rerolls_left(gid, uid)>0
        if phase=="choose":
            rows.append([
                InlineKeyboardButton("👀 حقیقت", callback_data=f"g{gid}:pick:truth:normal"),
                InlineKeyboardButton("😅 جرأت", callback_data=f"g{gid}:pick:dare:normal"),
            ])
            if allow18:
                rows.append([
                    InlineKeyboardButton("🔥 حقیقت +18", callback_data=f"g{gid}:pick:truth:18"),
                    InlineKeyboardButton("💦 جرأت +18", callback_data=f"g{gid}:pick:dare:18"),
                ])
            rows.append([InlineKeyboardButton("🎲 انتخاب شانسی", callback_data=f"g{gid}:pick:random:random")])
            if can_reroll:
                rows.append([InlineKeyboardButton(f"🔄 تعویض (باقی: {rerolls_left(gid, uid)})", callback_data=f"g{gid}:reroll")])
            if int(g["show_prev_question"])==1 and (g["last_q_text"] or ""):
                rows.append([InlineKeyboardButton("❓ سوال قبلی", callback_data=f"g{gid}:prev")])

        elif phase=="question":
            rows.append([
                InlineKeyboardButton("✅ انجام دادم/جواب دادم", callback_data=f"g{gid}:done"),
                InlineKeyboardButton("❌ انجام ندادم", callback_data=f"g{gid}:refuse"),
            ])
        elif phase=="wait_confirm":
            rows.append([
                InlineKeyboardButton("👍 تأیید", callback_data=f"g{gid}:confirm:yes"),
                InlineKeyboardButton("👎 رد", callback_data=f"g{gid}:confirm:no"),
            ])

    rows.append([InlineKeyboardButton("⬇️ انتقال به پایین", callback_data=f"g{gid}:bump")])
    return InlineKeyboardMarkup(rows)

def kb_settings(g: sqlite3.Row) -> InlineKeyboardMarkup:
    gid=int(g["id"])
    allow_mid = int(g["allow_mid_join"])==1
    show_prev = int(g["show_prev_question"])==1
    allow18 = int(g["allow_18"])==1
    rows=[
        [InlineKeyboardButton(f"➕ ورود وسط بازی: {'فعال✅' if allow_mid else 'خاموش❌'}", callback_data=f"g{gid}:set:mid:{'0' if allow_mid else '1'}")],
        [InlineKeyboardButton(f"❓ سوال قبلی: {'فعال✅' if show_prev else 'خاموش❌'}", callback_data=f"g{gid}:set:prev:{'0' if show_prev else '1'}")],
        [InlineKeyboardButton(f"🔞 سوالات +18: {'فعال✅' if allow18 else 'خاموش❌'}", callback_data=f"g{gid}:set:18:{'0' if allow18 else '1'}")],
        [InlineKeyboardButton("🏠 پایه", callback_data=f"g{gid}:view:main")],
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

    header = "😈 <b>جرأت/حقیقت Pro</b>\n"
    header += f"🆔 <code>{gid}</code> | 🧑‍🤝‍🧑 <b>{len(ps)}</b> نفر | ⏱ <b>{TURN_TIMEOUT_SEC}s</b>\n"
    header += f"👥 بازیکنان: {players_line(gid)}\n"
    header += "— — — — —\n"

    if view=="settings":
        body="⚙️ <b>تنظیمات بازی</b>\n"
        body += f"➕ ورود وسط بازی: {'فعال✅' if int(g['allow_mid_join'])==1 else 'خاموش❌'}\n"
        body += f"❓ سوال قبلی: {'فعال✅' if int(g['show_prev_question'])==1 else 'خاموش❌'}\n"
        body += f"🔞 سوالات +18: {'فعال✅' if int(g['allow_18'])==1 else 'خاموش❌'}\n"
        body += "\n🏠 برای برگشت «پایه» رو بزن."
        return header+body

    if view=="players":
        body="👥 <b>بازیکنان</b>\n"
        if not ps:
            body+="—\n"
        else:
            for i,p in enumerate(ps, start=1):
                body += f"{i}) {mention(int(p['user_id']), p['name'])} | 🔄{p['rerolls_left']} | ⏭{p['skips_used']} | ⚠️{p['penalties']}\n"
        body += "\n🏠 برای برگشت «پایه» رو بزن."
        return header+body

    if view=="stats":
        body="📊 <b>آمار بازی</b>\n"
        if ps:
            for p in ps:
                body += f"• {mention(int(p['user_id']), p['name'])}: نوبت {p['turns']} | مجازات {p['penalties']} | رد نوبت {p['skips_used']} | تعویض {p['rerolls_left']}\n"
        lastq = (g["last_q_text"] or "").strip()
        if lastq:
            body += "\n🧾 <b>آخرین سوال:</b>\n"
            body += f"{esc(lastq[:600])}\n"
        body += "\n🏠 برای برگشت «پایه» رو بزن."
        return header+body

    # MAIN
    if status=="lobby":
        body="🎮 <b>لابی</b>\n"
        body+="• هرکی می‌خواد بازی کنه «منم میخوام بازی کنم» رو بزنه.\n"
        body+="• فقط سازنده بازی می‌تونه «شروع بازی» رو بزنه.\n"
        body+="\n📌 این پیام آپدیت میشه (اسپم صفر)."
        return header+body

    if status=="ended":
        return header+"🛑 <b>بازی تمام شد</b>\nبرای شروع دوباره، یک بازی جدید بساز."

    if not cp:
        return header+"❌ بازیکنی نیست."

    body="🔥 <b>بازی شروع شد</b>\n"
    body += f"👤 نوبت: {mention(int(cp['user_id']), cp['name'])}\n"
    body += f"🎛 وضعیت: <b>{'انتخاب' if phase=='choose' else 'سوال' if phase=='question' else 'تأیید'}</b>\n\n"

    if phase=="choose":
        body += "❓ <b>نوع سوالاتو انتخاب کن</b>"
        return header+body

    if phase=="question":
        la=last_action(gid)
        if la:
            body += f"📌 <b>{'حقیقت' if la['qtype']=='truth' else 'جرأت'}</b> | سطح: <b>{'18+' if la['level']=='18' else 'معمولی'}</b>\n\n"
            body += f"❓ {esc(la['text'][:900])}"
        else:
            body += "❓ سوالی ثبت نشده."
        return header+body

    if phase=="wait_confirm":
        la=last_action(gid)
        body += "⏳ منتظر تایید طرف مقابل…\n\n"
        if la:
            body += f"❓ {esc(la['text'][:700])}"
        return header+body

    return header+body

# =========================
# Robust edit with retry + lock
# =========================
async def _edit_message_safe(context: ContextTypes.DEFAULT_TYPE, g: sqlite3.Row, text: str, markup: InlineKeyboardMarkup):
    # Telegram rate limits / network hiccups => retry
    for attempt in range(4):
        try:
            if g["kind"]=="group":
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
            return  # success
        except RetryAfter as e:
            wait = float(getattr(e, "retry_after", 1.0))
            log.warning("RetryAfter %.2fs (attempt %d)", wait, attempt+1)
            await asyncio.sleep(min(wait, 3.0))
        except (TimedOut, NetworkError) as e:
            log.warning("Network/Timeout %s (attempt %d)", e, attempt+1)
            await asyncio.sleep(0.25 * (attempt+1))
        except BadRequest as e:
            msg = str(e).lower()
            if "message is not modified" in msg:
                return
            # inline sometimes: "message can't be edited"
            log.error("BadRequest edit: %s", e)
            raise
    raise RuntimeError("Failed to edit message after retries")

async def edit_board(context: ContextTypes.DEFAULT_TYPE, g: sqlite3.Row, uid_for_kb: int, force_view: Optional[str]=None):
    gid=int(g["id"])
    lock = game_lock(context.application, gid)

    async with lock:
        if force_view:
            set_game_fields(gid, view=force_view)
        g = get_game(gid)
        if not g:
            return

        text = render_text(g)
        markup = kb_settings(g) if g["view"]=="settings" else kb_main(g, uid_for_kb)

        try:
            await _edit_message_safe(context, g, text, markup)
        except BadRequest:
            # group fallback: create new board if old isn't editable anymore
            if g["kind"]=="group":
                try:
                    msg = await context.bot.send_message(
                        chat_id=int(g["board_chat_id"]),
                        text=text,
                        parse_mode=ParseMode.HTML,
                        reply_markup=markup,
                        disable_web_page_preview=True,
                    )
                    set_game_fields(gid, board_message_id=msg.message_id)
                except Exception as e:
                    log.error("Group fallback send failed: %s", e)

# =========================
# TIMEOUT Job
# =========================
def schedule_timeout(context: ContextTypes.DEFAULT_TYPE, gid: int, actor_id: int):
    key=f"timeout:{gid}"
    job=context.application.bot_data.get(key)
    if job:
        try: job.schedule_removal()
        except Exception: pass
    context.application.bot_data[key]=context.job_queue.run_once(
        timeout_job, when=TURN_TIMEOUT_SEC, data={"gid":gid,"actor":actor_id}, name=key
    )

async def timeout_job(context: ContextTypes.DEFAULT_TYPE):
    data=context.job.data or {}
    gid=int(data.get("gid",0))
    actor=int(data.get("actor",0))
    g=get_game(gid)
    if not g or g["status"]!="running":
        return
    cp=current_player(g)
    if not cp or int(cp["user_id"])!=actor:
        return

    penalty=random.choice(PENALTIES)
    inc_stat(gid, actor, "penalties", 1)
    if rerolls_left(gid, actor)>0 and random.random()<0.5:
        dec_reroll(gid, actor)

    create_action(gid, actor, "timeout", "normal", f"TIMEOUT | {penalty}", "timeout")
    advance_turn(gid)
    set_game_fields(gid, view="main", phase="choose")

    g=get_game(gid)
    if g:
        new_cp=current_player(g)
        if new_cp:
            inc_stat(gid, int(new_cp["user_id"]), "turns", 1)
            schedule_timeout(context, gid, int(new_cp["user_id"]))
        await edit_board(context, g, uid_for_kb=actor)

# =========================
# INLINE: initial message
# =========================
def inline_initial_text() -> str:
    return (
        "😈 <b>جرأت/حقیقت Pro</b>\n"
        "✅ برای اینکه دکمه‌ها تو پی‌وی کار کنه، هر دو نفر یک‌بار /start بات رو بزنن.\n\n"
        "🎮 شروع بازی دو نفره داخل همین چت:\n"
        "1) هر دو «✋ منم میخوام بازی کنم»\n"
        "2) فقط سازنده «🎮 شروع بازی»\n\n"
        "📌 این پیام آپدیت میشه (اسپم صفر)."
    )

def inline_initial_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✋ منم میخوام بازی کنم", callback_data="new:join"),
         InlineKeyboardButton("⚙️ تنظیمات", callback_data="new:view:settings")],
        [InlineKeyboardButton("🎮 شروع بازی", callback_data="new:start")],
    ])

# =========================
# Handlers
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat=update.effective_chat
    if chat.type=="private":
        me=(await context.bot.get_me()).username
        link=f"https://t.me/{me}?startgroup=true"
        await update.message.reply_text(
            "🎲 جرأت/حقیقت Pro\n\n"
            "✅ بازی در پی‌وی دو نفره (داخل همان چت):\n"
            f"داخل چت دونفره بنویس: @{me}\n"
            "و «شروع بازی» رو انتخاب کن.\n\n"
            "✅ بازی در گروه:\n"
            "/startgame\n\n"
            f"📤 لینک اضافه‌کردن به گروه:\n{link}",
            disable_web_page_preview=True,
        )

async def cmd_startgame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat=update.effective_chat
    user=update.effective_user
    if chat.type not in ("group","supergroup"):
        await update.message.reply_text("این دستور مخصوص گروهه.")
        return
    msg = await update.message.reply_text("⏳ در حال ساخت برد بازی…")
    gid = create_group_game(chat.id, user.id, msg.message_id)
    upsert_player(gid, user.id, user.full_name)
    g=get_game(gid)
    await edit_board(context, g, uid_for_kb=user.id)

async def inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = InlineQueryResultArticle(
        id="start_game",
        title="🎮 شروع بازی جرأت/حقیقت (داخل همین چت)",
        description="یک پیام ثابت میاد و هی آپدیت میشه (کم‌اسپم)",
        input_message_content=InputTextMessageContent(inline_initial_text(), parse_mode=ParseMode.HTML),
        reply_markup=inline_initial_kb(),
    )
    await update.inline_query.answer([result], cache_time=0, is_personal=True)

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query
    user=update.effective_user
    data=q.data or ""

    # همیشه سریع جواب بده تا “loading…” نمونه
    try:
        await q.answer("✅", show_alert=False)
    except Exception:
        pass

    # First-time inline: new:*
    if data.startswith("new:"):
        if not q.inline_message_id:
            await q.answer("این بخش فقط برای بازی داخل چت (inline) است.", show_alert=True)
            return
        inline_id=q.inline_message_id
        g=get_game_by_inline_id(inline_id)
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

    m=re.match(r"^g(\d+)\:(.+)$", data)
    if not m:
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

    # view change
    if action.startswith("view:"):
        view=action.split(":",1)[1]
        if view not in ("main","settings","players","stats"):
            return
        set_game_fields(gid, view=view)
        await edit_board(context, get_game(gid), uid_for_kb=user.id)
        return

    # settings toggles
    if action.startswith("set:"):
        if user.id!=int(g["owner_id"]) and not is_admin(user.id):
            await q.answer("فقط سازنده می‌تونه تنظیمات رو عوض کنه.", show_alert=False)
            return
        _, key, val = action.split(":")
        if key=="mid":
            set_game_fields(gid, allow_mid_join=int(val))
        elif key=="prev":
            set_game_fields(gid, show_prev_question=int(val))
        elif key=="18":
            set_game_fields(gid, allow_18=int(val))
        set_game_fields(gid, view="settings")
        await edit_board(context, get_game(gid), uid_for_kb=user.id)
        return

    # join
    if action=="join":
        if g["status"]=="running" and int(g["allow_mid_join"])==0:
            await q.answer("ورود وسط بازی خاموشه.", show_alert=False)
            return
        created = upsert_player(gid, user.id, user.full_name)
        await q.answer("✅ عضو شدی" if created else "✅ قبلاً عضو بودی", show_alert=False)
        await edit_board(context, get_game(gid), uid_for_kb=user.id)
        return

    # start (ONLY OWNER)
    if action=="start":
        if user.id!=int(g["owner_id"]) and not is_admin(user.id):
            await q.answer("⛔ فقط سازنده می‌تونه شروع کنه.", show_alert=False)
            return
        players=list_players(gid)
        if len(players)<2:
            await q.answer("حداقل ۲ نفر باید Join کنن.", show_alert=False)
            return
        set_game_fields(gid, status="running", view="main", phase="choose")
        g=get_game(gid)
        cp=current_player(g)
        if cp:
            inc_stat(gid, int(cp["user_id"]), "turns", 1)
            schedule_timeout(context, gid, int(cp["user_id"]))
        await q.answer("🔥 بازی شروع شد", show_alert=False)
        await edit_board(context, g, uid_for_kb=user.id)
        return

    # end
    if action=="end":
        if user.id!=int(g["owner_id"]) and not is_admin(user.id):
            await q.answer("⛔ فقط سازنده می‌تونه پایان بده.", show_alert=False)
            return
        set_game_fields(gid, status="ended", view="main")
        await edit_board(context, get_game(gid), uid_for_kb=user.id)
        return

    # bump
    if action=="bump":
        if g["kind"]=="group":
            try:
                try:
                    await context.bot.edit_message_reply_markup(
                        chat_id=int(g["board_chat_id"]),
                        message_id=int(g["board_message_id"]),
                        reply_markup=None,
                    )
                except Exception:
                    pass
                g=get_game(gid)
                msg=await context.bot.send_message(
                    chat_id=int(g["board_chat_id"]),
                    text=render_text(g),
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb_settings(g) if g["view"]=="settings" else kb_main(g, user.id),
                    disable_web_page_preview=True,
                )
                set_game_fields(gid, board_message_id=msg.message_id)
                await q.answer("✅ منتقل شد", show_alert=False)
            except Exception:
                await q.answer("نتونستم منتقل کنم.", show_alert=False)
        return

    # prev question (toast)
    if action=="prev":
        lastq=(g["last_q_text"] or "").strip()
        if not lastq:
            await q.answer("سوال قبلی نداریم.", show_alert=False)
        else:
            show = lastq if len(lastq)<=180 else lastq[:180]+"…"
            await q.answer(show, show_alert=True)
        return

    # skip
    if action=="skip":
        if g["status"]!="running":
            await q.answer("بازی شروع نشده.", show_alert=False)
            return
        cp=current_player(g)
        if not cp:
            return
        if user.id not in (int(g["owner_id"]), int(cp["user_id"])) and not is_admin(user.id):
            await q.answer("⛔ اجازه رد نوبت نداری.", show_alert=False)
            return
        inc_stat(gid, int(cp["user_id"]), "skips_used", 1)
        advance_turn(gid)
        set_game_fields(gid, phase="choose", view="main")
        g=get_game(gid)
        new_cp=current_player(g)
        if new_cp:
            inc_stat(gid, int(new_cp["user_id"]), "turns", 1)
            schedule_timeout(context, gid, int(new_cp["user_id"]))
        await edit_board(context, g, uid_for_kb=user.id)
        return

    # reroll (only current player)
    if action=="reroll":
        if g["status"]!="running":
            return
        cp=current_player(g)
        if not cp or user.id!=int(cp["user_id"]):
            await q.answer("الان نوبت تو نیست.", show_alert=False)
            return
        if rerolls_left(gid, user.id)<=0:
            await q.answer("تعویضت تموم شده.", show_alert=False)
            return
        dec_reroll(gid, user.id)
        schedule_timeout(context, gid, user.id)
        await edit_board(context, get_game(gid), uid_for_kb=user.id)
        return

    # pick question
    if action.startswith("pick:"):
        if g["status"]!="running":
            return
        cp=current_player(g)
        if not cp or user.id!=int(cp["user_id"]):
            await q.answer("الان نوبت تو نیست.", show_alert=False)
            return

        _, qtype, level = action.split(":")
        if qtype=="random":
            qtype=random.choice(["truth","dare"])
            level=random.choice(["normal","18"])
        if level=="18" and int(g["allow_18"])==0:
            await q.answer("+18 خاموشه.", show_alert=False)
            return

        forced = pop_forced(gid, user.id, qtype, level)
        text = forced or pick_random_question(qtype, level)
        if not text:
            await q.answer("سوال نداریم. با Bulk اضافه کن.", show_alert=True)
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
        await edit_board(context, get_game(gid), uid_for_kb=user.id)
        return

    # refuse
    if action=="refuse":
        if g["status"]!="running":
            return
        cp=current_player(g)
        if not cp or user.id!=int(cp["user_id"]):
            await q.answer("الان نوبت تو نیست.", show_alert=False)
            return
        penalty=random.choice(PENALTIES)
        inc_stat(gid, user.id, "penalties", 1)
        if rerolls_left(gid, user.id)>0 and random.random()<0.7:
            dec_reroll(gid, user.id)
        create_action(gid, user.id, "refuse", "normal", penalty, "refused")

        advance_turn(gid)
        set_game_fields(gid, phase="choose", view="main")
        g=get_game(gid)
        new_cp=current_player(g)
        if new_cp:
            inc_stat(gid, int(new_cp["user_id"]), "turns", 1)
            schedule_timeout(context, gid, int(new_cp["user_id"]))
        await edit_board(context, g, uid_for_kb=user.id)
        return

    # done
    if action=="done":
        if g["status"]!="running":
            return
        cp=current_player(g)
        if not cp or user.id!=int(cp["user_id"]):
            await q.answer("الان نوبت تو نیست.", show_alert=False)
            return

        players=list_players(gid)
        # inline 2-player: need confirm
        if g["kind"]=="inline" and len(players)==2:
            set_game_fields(gid, phase="wait_confirm", view="main")
            la=last_action(gid)
            if la:
                conn=db(); cur=conn.cursor()
                cur.execute("UPDATE actions SET status='done_wait' WHERE id=?;",(int(la["id"]),))
                conn.commit(); conn.close()
            schedule_timeout(context, gid, user.id)
            await edit_board(context, get_game(gid), uid_for_kb=user.id)
            return

        # others: self report
        la=last_action(gid)
        if la:
            conn=db(); cur=conn.cursor()
            cur.execute("UPDATE actions SET status='confirmed' WHERE id=?;",(int(la["id"]),))
            conn.commit(); conn.close()

        advance_turn(gid)
        set_game_fields(gid, phase="choose", view="main")
        g=get_game(gid)
        new_cp=current_player(g)
        if new_cp:
            inc_stat(gid, int(new_cp["user_id"]), "turns", 1)
            schedule_timeout(context, gid, int(new_cp["user_id"]))
        await edit_board(context, g, uid_for_kb=user.id)
        return

    # confirm (2-player)
    if action.startswith("confirm:"):
        if g["status"]!="running":
            return
        players=list_players(gid)
        if len(players)!=2:
            await q.answer("این تایید فقط برای دو نفره‌ست.", show_alert=False)
            return
        cp=current_player(g)
        actor=int(cp["user_id"])
        counterpart = [p for p in players if int(p["user_id"])!=actor][0]
        if user.id != int(counterpart["user_id"]):
            await q.answer("فقط طرف مقابل می‌تونه تایید کنه.", show_alert=False)
            return

        decision = action.split(":")[1]
        la=last_action(gid)
        if la:
            conn=db(); cur=conn.cursor()
            cur.execute("UPDATE actions SET status=? WHERE id=?;", ("confirmed" if decision=="yes" else "rejected", int(la["id"])))
            conn.commit(); conn.close()

        if decision=="no":
            penalty=random.choice(PENALTIES)
            inc_stat(gid, actor, "penalties", 1)
            if rerolls_left(gid, actor)>0 and random.random()<0.7:
                dec_reroll(gid, actor)
            create_action(gid, actor, "reject", "normal", penalty, "rejected")
            await q.answer("👎 رد شد + مجازات", show_alert=False)
        else:
            await q.answer("👍 تایید شد", show_alert=False)

        advance_turn(gid)
        set_game_fields(gid, phase="choose", view="main")
        g=get_game(gid)
        new_cp=current_player(g)
        if new_cp:
            inc_stat(gid, int(new_cp["user_id"]), "turns", 1)
            schedule_timeout(context, gid, int(new_cp["user_id"]))
        await edit_board(context, g, uid_for_kb=user.id)
        return

# =========================
# Admin / Suggestions (همون قبلی، فقط نگه داشتیم)
# =========================
def flow_set(context: ContextTypes.DEFAULT_TYPE, name: Optional[str], data: Optional[dict]=None):
    if not name:
        context.user_data.pop("flow", None)
    else:
        context.user_data["flow"]={"name":name,"data":data or {}}

def flow_get(context: ContextTypes.DEFAULT_TYPE):
    return context.user_data.get("flow")

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔️ دسترسی نداری")
        return
    await update.message.reply_text(
        "👑 پنل ادمین\n"
        "/bulk_truth  یا /bulk_dare  یا /bulk_truth18  یا /bulk_dare18\n"
        "/pending  (پیشنهادها)\n"
        "/force  (سؤال مخفی برای بازیکن)\n"
    )

async def cmd_bulk(update: Update, context: ContextTypes.DEFAULT_TYPE, qtype: str, level: str):
    if not is_admin(update.effective_user.id):
        return
    flow_set(context,"bulk",{"qtype":qtype,"level":level})
    await update.message.reply_text(
        f"➕ Bulk Add برای {qtype}/{level}\n"
        "چند سوال رو یکجا بفرست:\n"
        "1= ...\n2= ...\n3= ...\n"
        "یا هر خط یک سوال."
    )

async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    conn=db(); cur=conn.cursor()
    cur.execute("SELECT * FROM suggestions WHERE status='pending' ORDER BY id ASC LIMIT 10;")
    rows=cur.fetchall(); conn.close()
    if not rows:
        await update.message.reply_text("✅ چیزی در صف نیست.")
        return
    for r in rows:
        kb=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ تایید", callback_data=f"adm:ap:{r['id']}"),
            InlineKeyboardButton("❌ رد", callback_data=f"adm:rj:{r['id']}"),
        ]])
        await update.message.reply_text(
            f"پیشنهاد #{r['id']}\n"
            f"از {r['user_id']} | {r['qtype']}/{r['level']}\n\n"
            f"{r['text']}",
            reply_markup=kb
        )

async def cmd_force(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    conn=db(); cur=conn.cursor()
    cur.execute("SELECT id,kind,status FROM games WHERE status='running' ORDER BY id DESC LIMIT 10;")
    rows=cur.fetchall(); conn.close()
    if not rows:
        await update.message.reply_text("هیچ بازی running نیست.")
        return
    kb=[]
    for r in rows:
        kb.append([InlineKeyboardButton(f"#{r['id']} ({r['kind']})", callback_data=f"adm:fg:{r['id']}")])
    await update.message.reply_text("یک بازی رو انتخاب کن:", reply_markup=InlineKeyboardMarkup(kb))

async def admin_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query
    await q.answer()
    if not is_admin(update.effective_user.id):
        await q.answer("⛔️", show_alert=True)
        return
    data=q.data or ""
    m=re.match(r"^adm\:(ap|rj)\:(\d+)$", data)
    if m:
        act=m.group(1); sid=int(m.group(2))
        conn=db(); cur=conn.cursor()
        cur.execute("SELECT * FROM suggestions WHERE id=?;",(sid,))
        s=cur.fetchone()
        if not s:
            conn.close()
            return
        if act=="ap":
            cur.execute("UPDATE suggestions SET status='approved', reviewed_by=?, reviewed_at=? WHERE id=?;",(ADMIN_ID,now(),sid))
            cur.execute("INSERT INTO questions (qtype,level,text,enabled,created_at) VALUES (?,?,?,?,?);",(s["qtype"],s["level"],s["text"],1,now()))
        else:
            cur.execute("UPDATE suggestions SET status='rejected', reviewed_by=?, reviewed_at=? WHERE id=?;",(ADMIN_ID,now(),sid))
        conn.commit(); conn.close()
        await q.message.reply_text("✅ انجام شد.")
        return

    m=re.match(r"^adm\:fg\:(\d+)$", data)
    if m:
        gid=int(m.group(1))
        ps=list_players(gid)
        if not ps:
            await q.message.reply_text("بازیکنی ندارد.")
            return
        kb=[]
        for p in ps:
            kb.append([InlineKeyboardButton(p["name"], callback_data=f"adm:fp:{gid}:{p['user_id']}")])
        await q.message.reply_text("بازیکن رو انتخاب کن:", reply_markup=InlineKeyboardMarkup(kb))
        return

    m=re.match(r"^adm\:fp\:(\d+)\:(\d+)$", data)
    if m:
        gid=int(m.group(1)); uid=int(m.group(2))
        flow_set(context,"force_text",{"gid":gid,"uid":uid})
        await q.message.reply_text("متن سؤال سفارشی رو بفرست (همینجا):")
        return

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    flow=flow_get(context)
    if not flow:
        return

    if flow["name"]=="bulk":
        if not is_admin(update.effective_user.id):
            flow_set(context,None); return
        qtype=flow["data"]["qtype"]; level=flow["data"]["level"]
        items=parse_bulk(update.message.text or "")
        if not items:
            await update.message.reply_text("هیچی دریافت نشد.")
            return
        conn=db(); cur=conn.cursor()
        cur.executemany(
            "INSERT INTO questions (qtype,level,text,enabled,created_at) VALUES (?,?,?,?,?);",
            [(qtype,level,t,1,now()) for t in items]
        )
        conn.commit(); conn.close()
        flow_set(context,None)
        await update.message.reply_text(f"✅ {len(items)} سؤال اضافه شد.")
        return

    if flow["name"]=="force_text":
        if not is_admin(update.effective_user.id):
            flow_set(context,None); return
        gid=int(flow["data"]["gid"]); uid=int(flow["data"]["uid"])
        txt=(update.message.text or "").strip()
        if not txt:
            await update.message.reply_text("متن خالیه.")
            return
        queue_forced(gid, uid, txt, qtype=None, level=None)
        flow_set(context,None)
        await update.message.reply_text("✅ سؤال مخفی صف شد (لو نمی‌رود).")
        return

# =========================
# App
# =========================
def build_app() -> Application:
    init_db()
    seed_if_empty()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("startgame", cmd_startgame))

    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("pending", cmd_pending))
    app.add_handler(CommandHandler("force", cmd_force))

    app.add_handler(CommandHandler("bulk_truth", lambda u,c: cmd_bulk(u,c,"truth","normal")))
    app.add_handler(CommandHandler("bulk_dare", lambda u,c: cmd_bulk(u,c,"dare","normal")))
    app.add_handler(CommandHandler("bulk_truth18", lambda u,c: cmd_bulk(u,c,"truth","18")))
    app.add_handler(CommandHandler("bulk_dare18", lambda u,c: cmd_bulk(u,c,"dare","18")))

    app.add_handler(InlineQueryHandler(inline_query))

    app.add_handler(CallbackQueryHandler(admin_cb, pattern=r"^adm:"))
    app.add_handler(CallbackQueryHandler(callback_router))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    return app

if __name__ == "__main__":
    application = build_app()
    log.info("Bot is running (polling)...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)