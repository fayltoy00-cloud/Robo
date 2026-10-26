"""
Robocontest nazorat boti (aiogram 3 + requests)

O'rnatish:   pip install aiogram requests
Ishga tushirish:   BOT_TOKEN=123:ABC python robocontest_bot.py

Ishlashi:
  1. Ustoz guruhda:  /task https://robocontest.uz/tasks/A0002
     -> bot xabar tashlaydi, tagida "Ro'yxatdan o'tish" tugmasi bor.
  2. O'quvchi tugmani bosadi -> botning shaxsiy chatiga o'tadi va
     robocontest profil havolasi yoki username'ini yuboradi.
     (Guruhda ham bo'ladi:  /reg shxr)
  3. Bot har INTERVAL soniyada barcha o'quvchilarning
     https://robocontest.uz/profile/<user> sahifasini parallel tekshiradi
     va guruhdagi o'sha xabarni tahrirlaydi:
        ✅ ishlagan   🟡 urinib ko'rgan (xato)   ❌ ishlamagan
  4. /stop A0002 -> shu masalani kuzatishni to'xtatadi.
"""
import asyncio
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from html import escape
from html.parser import HTMLParser

import requests
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ChatType, ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

BOT_TOKEN = os.getenv("BOT_TOKEN", "8566592437:AAEYGrsCMEgReS1mMHd5ZvXUvRISdTcZ1aA")
INTERVAL = 60            # tekshirish oralig'i (soniya)
MAX_WORKERS = 20         # parallel so'rovlar soni
DB_FILE = "robocontest_db.json"
BASE = "https://robocontest.uz"
HEADERS = {"User-Agent": "Mozilla/5.0 (RobocontestBot)"}

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("rbbot")

pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
dp = Dispatcher()
pending: dict[int, str] = {}   # tg_user_id -> chat_id (ro'yxatdan o'tish kutilmoqda)
db_lock = asyncio.Lock()

# ---------------------------------------------------------------- saqlash (Firebase Realtime DB)
FIREBASE_URL = os.getenv("FIREBASE_URL", "https://robo-915f4-default-rtdb.firebaseio.com").rstrip("/")
FB_NODE = "robocontest"
FB_URL = f"{FIREBASE_URL}/{FB_NODE}.json"
_fb_exec = ThreadPoolExecutor(max_workers=1)     # yozishlar navbat bilan, bot qotib qolmasin
_fb_latest = {"data": None}


def _normalize(db) -> dict:
    """Firebase bo'sh {} va [] larni saqlamaydi, shuning uchun qaytarib to'ldiramiz."""
    db = db if isinstance(db, dict) else {}
    db.setdefault("chats", {})
    for c in db["chats"].values():
        c.setdefault("users", {})
        tasks = c.get("tasks") or []
        if isinstance(tasks, dict):              # Firebase ba'zan ro'yxatni lug'at qilib qaytaradi
            tasks = [v for _, v in sorted(tasks.items(), key=lambda kv: int(kv[0]))]
        c["tasks"] = [t for t in tasks if t]
    return db


def load_db() -> dict:
    local = None
    if os.path.exists(DB_FILE):
        with open(DB_FILE, encoding="utf-8") as f:
            local = json.load(f)
    try:
        r = requests.get(FB_URL, timeout=20)
        r.raise_for_status()
        remote = r.json()
    except Exception as e:
        log.warning("Firebase'dan o'qib bo'lmadi, lokal fayl ishlatiladi: %s", e)
        return _normalize(local)
    if remote is None and local:                 # Firebase bo'sh -> eski bazani bir marta ko'chiramiz
        log.info("Lokal baza Firebase'ga ko'chirilmoqda…")
        rr = requests.put(FB_URL, data=json.dumps(local, ensure_ascii=False).encode("utf-8"),
                          headers={"Content-Type": "application/json"}, timeout=30)
        rr.raise_for_status()
        return _normalize(local)
    return _normalize(remote)


def _fb_send():
    data = _fb_latest["data"]
    _fb_latest["data"] = None
    if data is None:
        return
    try:
        r = requests.put(FB_URL, data=data.encode("utf-8"),
                         headers={"Content-Type": "application/json"}, timeout=30)
        r.raise_for_status()
    except Exception as e:
        log.warning("Firebase'ga yozib bo'lmadi: %s", e)


def save_db():
    data = json.dumps(DB, ensure_ascii=False)    # event loop ichida, DB o'zgarmay turganda
    with open(DB_FILE, "w", encoding="utf-8") as f:   # lokal zaxira nusxa
        f.write(data)
    _fb_latest["data"] = data
    _fb_exec.submit(_fb_send)


DB = load_db()


def chat_data(chat_id) -> dict:
    return DB["chats"].setdefault(str(chat_id), {"users": {}, "tasks": []})


# ---------------------------------------------------------------- robocontest
class LinkParser(HTMLParser):
    """Sahifadagi barcha <a> larni (class, href, matn) ko'rinishida yig'adi."""

    def __init__(self):
        super().__init__()
        self.links = []
        self._cur = None

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            a = dict(attrs)
            self._cur = {"cls": a.get("class") or "", "href": a.get("href") or "", "text": ""}

    def handle_data(self, data):
        if self._cur is not None:
            self._cur["text"] += data

    def handle_endtag(self, tag):
        if tag == "a" and self._cur is not None:
            self._cur["text"] = self._cur["text"].strip()
            self.links.append(self._cur)
            self._cur = None


def check_user(rb_user: str, code: str) -> str:
    """
    Qaytaradi: solved | tried | none | unknown | nouser | error
    Rang bo'yicha:  emerald = ishlangan, amber = urinish (xato), muted = ishlanmagan
    """
    try:
        r = requests.get(f"{BASE}/profile/{rb_user}", headers=HEADERS, timeout=15)
    except requests.RequestException:
        return "error"
    if r.status_code == 404:
        return "nouser"
    if r.status_code != 200:
        return "error"
    st = _status_from_json(r.text, code)   # asosiy usul: sahifadagi JSON
    if st:
        return st
    p = LinkParser()
    p.feed(r.text)
    for a in p.links:
        if a["text"].upper() == code.upper():
            cls = a["cls"]
            if "emerald" in cls:
                return "solved"
            if "amber" in cls:
                return "tried"
            return "none"
    return "unknown"   # profil sahifasida bu masala kodi topilmadi


async def check_all(users: dict, code: str) -> dict:
    loop = asyncio.get_running_loop()
    ids = list(users)
    res = await asyncio.gather(
        *[loop.run_in_executor(pool, check_user, users[i]["rb"], code) for i in ids]
    )
    return dict(zip(ids, res))


# ---------------------------------------------------------------- xabar matni
ICON = {"solved": "✅", "tried": "🟡", "none": "❌", "unknown": "❔", "nouser": "🚫", "error": "⚠️"}
ORDER = {"solved": 0, "tried": 1, "none": 2, "unknown": 3, "error": 4, "nouser": 5}


def render(task: dict, users: dict, results: dict) -> str:
    code = task["code"]
    rows = sorted(users, key=lambda i: (ORDER.get(results.get(i, "error"), 9), users[i]["name"].lower()))
    solved = sum(1 for i in users if results.get(i) == "solved")
    lines = [
        f"📝 <b>Masala:</b> {BASE}/tasks/{escape(code)}",
        f"👥 Ro'yxatda: {len(users)} | ✅ Ishladi: {solved}",
        "",
    ]
    if not users:
        lines.append("Hali hech kim ro'yxatdan o'tmagan. Pastdagi tugmani bosing 👇")
    for n, i in enumerate(rows, 1):
        st = results.get(i, "error")
        u = users[i]
        lines.append(f"{n}. {ICON[st]} {escape(u['name'])} (<code>{escape(u['rb'])}</code>)")
    lines += ["", "✅ ishlagan  🟡 urinib ko'rgan  ❌ ishlamagan  ❔ topilmadi"]
    return "\n".join(lines)


def reg_keyboard(bot_username: str, chat_id) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📥 Ro'yxatdan o'tish",
                             url=f"https://t.me/{bot_username}?start=reg_{chat_id}")
    ]])


async def refresh_task(bot: Bot, chat_id: str, task: dict):
    users = chat_data(chat_id)["users"]
    results = await check_all(users, task["code"]) if users else {}
    text = render(task, users, results)
    if text == task.get("last"):
        return
    me = await bot.get_me()
    try:
        await bot.edit_message_text(
            text, chat_id=int(chat_id), message_id=task["msg_id"],
            reply_markup=reg_keyboard(me.username, chat_id, task["code"]),
            disable_web_page_preview=True,
        )
        task["last"] = text
    except TelegramBadRequest as e:
        if "not modified" in str(e):
            task["last"] = text
        else:
            log.warning("edit xato: %s", e)


async def refresh_chat(bot: Bot, chat_id: str):
    for t in chat_data(chat_id)["tasks"]:
        if t.get("active"):
            await refresh_task(bot, chat_id, t)
    async with db_lock:
        save_db()


async def poller(bot: Bot):
    while True:
        try:
            for chat_id in list(DB["chats"]):
                await refresh_chat(bot, chat_id)
        except Exception:
            log.exception("poller xatosi")
        await asyncio.sleep(INTERVAL)


# ---------------------------------------------------------------- yordamchilar
async def is_admin(bot: Bot, chat_id, user_id) -> bool:
    m = await bot.get_chat_member(chat_id, user_id)
    return m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)


def parse_rb_user(text: str) -> str | None:
    m = re.search(r"profile/([^/?#\s]+)", text)
    name = m.group(1) if m else text.strip()
    return name if re.fullmatch(r"[\w.\-]{1,64}", name) else None


async def register(bot: Bot, chat_id: str, tg_user, text: str) -> str:
    rb = parse_rb_user(text)
    if not rb:
        return "❗ Profil havolasi yoki username noto'g'ri."
    loop = asyncio.get_running_loop()
    st = await loop.run_in_executor(pool, check_user, rb, "-")
    if st == "nouser":
        return f"🚫 Robocontest'da «{escape(rb)}» degan profil topilmadi."
    if st == "error":
        return "⚠️ Robocontest bilan bog'lanib bo'lmadi, birozdan keyin urinib ko'ring."
    async with db_lock:
        chat_data(chat_id)["users"][str(tg_user.id)] = {"name": tg_user.full_name, "rb": rb}
        save_db()
    asyncio.create_task(refresh_chat(bot, chat_id))
    return f"✅ Ro'yxatga olindi: <code>{escape(rb)}</code>"


# ---------------------------------------------------------------- handlerlar
@dp.message(Command("task"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_task(msg: Message, command: CommandObject, bot: Bot):
    if not await is_admin(bot, msg.chat.id, msg.from_user.id):
        return await msg.reply("Faqat adminlar masala qo'sha oladi.")
    m = re.search(r"robocontest\.uz/tasks/([A-Za-z0-9_\-]+)", command.args or "")
    if not m:
        return await msg.reply("Ishlatish: /task https://robocontest.uz/tasks/A0002")
    code = m.group(1)
    me = await bot.get_me()
    task = {"code": code, "active": True}
    users = chat_data(msg.chat.id)["users"]
    sent = await msg.answer(
        render(task, users, {}),
        reply_markup=reg_keyboard(me.username, msg.chat.id, code),
        disable_web_page_preview=True,
    )
    task["msg_id"] = sent.message_id
    async with db_lock:
        chat_data(msg.chat.id)["tasks"].append(task)
        save_db()
    asyncio.create_task(refresh_task(bot, str(msg.chat.id), task))


@dp.message(Command("stop"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_stop(msg: Message, command: CommandObject, bot: Bot):
    if not await is_admin(bot, msg.chat.id, msg.from_user.id):
        return
    code = (command.args or "").strip().upper()
    n = 0
    async with db_lock:
        for t in chat_data(msg.chat.id)["tasks"]:
            if t["active"] and t["code"].upper() == code:
                t["active"] = False
                n += 1
        save_db()
    await msg.reply(f"To'xtatildi: {n} ta." if n else "Bunday faol masala topilmadi.")


@dp.message(Command("reg"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_reg(msg: Message, command: CommandObject, bot: Bot):
    if not command.args:
        return await msg.reply("Ishlatish: /reg username  yoki  /reg https://robocontest.uz/profile/username")
    await msg.reply(await register(bot, str(msg.chat.id), msg.from_user, command.args))


@dp.message(CommandStart(), F.chat.type == ChatType.PRIVATE)
async def cmd_start(msg: Message, command: CommandObject):
    arg = command.args or ""
    if arg.startswith("reg_"):
        pending[msg.from_user.id] = arg[4:]
        return await msg.answer(
            "Robocontest profilingiz havolasini yoki username'ingizni yuboring.\n"
            "Masalan: <code>https://robocontest.uz/profile/shxr</code> yoki <code>shxr</code>"
        )
    await msg.answer("Ro'yxatdan o'tish uchun guruhdagi xabar tagidagi tugmani bosing.")


@dp.message(F.chat.type == ChatType.PRIVATE, F.text)
async def private_text(msg: Message, bot: Bot):
    chat_id = pending.get(msg.from_user.id)
    if not chat_id:
        return
    reply = await register(bot, chat_id, msg.from_user, msg.text)
    if reply.startswith("✅"):
        pending.pop(msg.from_user.id, None)
    await msg.answer(reply)


# ======================================================================
# /gorobo — o'quvchilar guruhda o'zlari test (masalalar to'plami) tuzadi
#   1) qiyinlik  2) savollar soni  3) bo'lim uchun ovoz berish
#   Tanlangan bo'limda masala yetmasa, qolgani oddiy (bo'limsiz) ro'yxatdan olinadi.
# ======================================================================
import random
from aiogram.types import CallbackQuery

DIFFICULTIES = ["easy", "medium", "hard", "expert"]   # saytdagi ?difficulty= qiymatlari
COUNTS = [3, 5, 7, 10]
VOTE_SECONDS = 90
CATS_PER_PAGE = 8
MAX_PAGE = 29

# DIQQAT: id = shu ro'yxatdagi tartib raqami deb olingan (1, 2, 3 ...).
# Saytda ?category=<id> boshqacha bo'lsa, faqat shu ro'yxatni to'g'rilang.
CATEGORY_NAMES = [
    "Oson", "Uzun sonlar arifmetikasi", "Dinamik dasturlash", "Sonlar nazariyasi", "Binar daraxt",
    "Binar qidiruv", "Matematika", "Perebor", "Tartiblash", "Heap Sort", "Ochko'z algoritm",
    "Massivlar", "Rekursiya", "Graflar nazariyasi", "String", "Hash Table", "Depth-First Search",
    "Database", "Breadth-First Search", "Tree", "Matrix", "Two Pointers", "Bit Manipulation",
    "Stack", "Design", "Heap (Priority Queue)", "Simulation", "Backtracking", "Prefix Sum",
    "Counting", "Sliding Window", "Linked List", "Union Find", "Ordered Set", "Monotonic Stack",
    "Trie", "Binary Search Tree", "Divide and Conquer", "Bitmask", "Queue", "Geometry",
    "Memoization", "Segment Tree", "Topological Sort", "Game Theory", "Hash Function",
    "Binary Indexed Tree", "Interactive", "String Matching", "Rolling Hash", "Shortest Path",
    "Data Stream", "Combinatorics", "Randomized", "Monotonic Queue", "Iterator", "Merge Sort",
    "Concurrency", "Doubly-Linked List", "Brainteaser", "Probability and Statistics",
    "Quickselect", "Bucket Sort", "Suffix Array", "Minimum Spanning Tree", "Counting Sort",
    "Shell", "Line Sweep", "Reservoir Sampling", "Eulerian Circuit",
    "Strongly Connected Component", "Radix Sort", "Rejection Sampling", "Biconnected Component",
]
CATEGORIES = {i: n for i, n in enumerate(CATEGORY_NAMES, 1)}

sessions: dict[str, dict] = {}   # "chat_id:msg_id" -> test tuzish sessiyasi


import time

_UA_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "uz,en;q=0.8,ru;q=0.6",
}
_CODE_RE = re.compile(r"^[A-Za-z]{1,3}\d{2,5}[A-Za-z]?$")
_TASK_URL_RE = re.compile(r"\\?/tasks\\?/([A-Za-z0-9_\-]+)")
_fetch_pool = ThreadPoolExecutor(max_workers=8)   # sahifalarni parallel olish uchun
FETCH_ERR = {"last": None}                        # oxirgi xato sababi (foydalanuvchiga ko'rsatiladi)


def codes_from_html(text: str) -> list:
    """Sahifadan masala kodlarini oladi: avval <a> havolalardan, bo'lmasa sahifa ichidagi JSON dan."""
    p = LinkParser()
    p.feed(text)

    def anchors(strict: bool) -> list:
        out = []
        for a in p.links:
            m = re.fullmatch(rf"(?:{re.escape(BASE)})?/tasks/([A-Za-z0-9_\-]+)", a["href"])
            if (m and a["text"] == m.group(1) and _CODE_RE.match(m.group(1))
                    and (not strict or "hover:underline" in a["cls"]) and m.group(1) not in out):
                out.append(m.group(1))
        return out

    out = anchors(True) or anchors(False)
    if out:
        return out
    t = _unescape(text) if "&" in text else text
    for m in _TASK_URL_RE.finditer(t):
        c = m.group(1)
        if _CODE_RE.match(c) and c not in out:
            out.append(c)
    return out


def fetch_codes(diff, cat, direction, page):
    """Ro'yxat sahifasidagi masala kodlari. Xato bo'lsa None, bo'sh sahifa bo'lsa []."""
    params = {"direction": direction, "page": page, "sort": "solved_users"}
    if diff:
        params["difficulty"] = diff
    if cat:
        params["category"] = cat
    for attempt in range(2):
        try:
            r = requests.get(f"{BASE}/tasks", params=params, headers=_UA_HEADERS, timeout=20)
        except requests.RequestException as e:
            FETCH_ERR["last"] = f"ulanish xatosi ({type(e).__name__})"
            time.sleep(1)
            continue
        if r.status_code == 200:
            return codes_from_html(r.text)
        FETCH_ERR["last"] = f"HTTP {r.status_code}"
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1 + attempt)
            continue
        return None
    return None


def pick_codes(diff, cat, n, exclude):
    """Tasodifiy sahifalarni parallel olib, har sahifadan tasodifiy masala tanlaydi."""
    got, hi, seen, pages_codes = [], MAX_PAGE, set(), {}
    for _ in range(6):
        if len(got) >= n or hi < 1:
            break
        free = [p for p in range(1, hi + 1) if p not in seen]
        if not free:
            break
        pages = random.sample(free, min(len(free), max(3, n - len(got))))
        seen.update(pages)
        futs = [(p, _fetch_pool.submit(fetch_codes, diff, cat, random.choice(["asc", "desc"]), p))
                for p in pages]
        per = max(1, -(-(n - len(got)) // len(pages)))
        for p, f in futs:
            codes = f.result()
            if codes is None:
                continue
            if not codes:                 # sahifa bo'sh -> oxirgi sahifa kichikroq
                hi = min(hi, p - 1)
                continue
            pages_codes[p] = codes
            cand = [c for c in codes if c not in exclude and c not in got]
            random.shuffle(cand)
            got += cand[:max(0, min(per, n - len(got)))]
    if len(got) < n:                      # sahifalar oz bo'lsa, olingan sahifalardan to'ldiramiz
        rest = list(dict.fromkeys(c for cs in pages_codes.values() for c in cs
                                  if c not in exclude and c not in got))
        random.shuffle(rest)
        got += rest[:n - len(got)]
    return got


def build_set(diff, cat, n, exclude):
    """(kodlar, bo'lim yetmadimi, izohlar)"""
    notes, fallback = [], False
    codes = pick_codes(diff, cat, n, exclude) if cat else []
    if cat and len(codes) < n:
        fallback = True
    if len(codes) < n:                    # oddiy (bo'limsiz) ro'yxatdan to'ldirish
        codes += pick_codes(diff, None, n - len(codes), set(exclude) | set(codes))
    if len(codes) < n:                    # hali ham yetmasa, takrorlarga ruxsat
        more = pick_codes(diff, None, n - len(codes), set(codes))
        if more:
            notes.append("🔁 Masalalar yetmagani uchun ayrimlari avval tanlanganlar bilan takrorlandi.")
        codes += more
    if not codes:                         # qiyinlik filtri ishlamayotgan bo'lishi mumkin
        codes = pick_codes(None, None, n, set(exclude))
        if codes:
            notes.append("⚠️ Qiyinlik bo'yicha masala topilmadi, masalalar aralash qiyinlikdan olindi.")
    if len(codes) < n:                    # ro'yxatdan topilmadi -> tasodifiy kodni o'zi topadi
        before = len(codes)
        codes += random_verified(n - len(codes), set(exclude) | set(codes))
        if len(codes) > before:
            notes.append("🎲 Ro'yxat sahifasidan olib bo'lmadi, masalalar tasodifiy topildi "
                         "(qiyinlik va bo'lim hisobga olinmadi).")
    if 0 < len(codes) < n:
        notes.append(f"ℹ️ Faqat {len(codes)} ta masala topildi.")
    return codes, fallback, notes


def _rand_code() -> str:
    """Saytdagi kod ko'rinishlari: A0001 va M067A."""
    if random.random() < 0.5:
        return f"A{random.randint(1, 1200):04d}"
    return f"M{random.randint(1, 150):03d}{random.choice('ABCDE')}"


def _task_exists(code: str) -> bool:
    try:
        r = requests.get(f"{BASE}/tasks/{code}", headers=_UA_HEADERS, timeout=15)
    except requests.RequestException as e:
        FETCH_ERR["last"] = f"ulanish xatosi ({type(e).__name__})"
        return False
    if r.status_code != 200:
        FETCH_ERR["last"] = f"HTTP {r.status_code}"
        return False
    return code.upper() in r.text.upper()


def random_verified(n: int, exclude: set) -> list:
    """Tasodifiy kodlar tuzadi va saytda borligini parallel tekshiradi (https://robocontest.uz/tasks/M067A)."""
    got = []
    for _ in range(4):
        if len(got) >= n:
            break
        cands = list({_rand_code() for _ in range(max(12, (n - len(got)) * 6))} - set(exclude) - set(got))
        for code, ok in zip(cands, _fetch_pool.map(_task_exists, cands)):
            if ok and len(got) < n:
                got.append(code)
    return got


@dp.message(Command("gotest"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_gotest(msg: Message, command: CommandObject, bot: Bot):
    """/gotest easy [bo'lim] [sahifa] — bot ro'yxat sahifasida nimani ko'rayotganini ko'rsatadi (adminlar)."""
    if not await is_admin(bot, msg.chat.id, msg.from_user.id):
        return
    a = (command.args or "easy").split()
    diff = a[0] if a and a[0] != "-" else None
    cat = int(a[1]) if len(a) > 1 and a[1].isdigit() else None
    page = int(a[2]) if len(a) > 2 and a[2].isdigit() else 1

    def work() -> str:
        params = {"direction": "desc", "page": page, "sort": "solved_users"}
        if diff:
            params["difficulty"] = diff
        if cat:
            params["category"] = cat
        try:
            r = requests.get(f"{BASE}/tasks", params=params, headers=_UA_HEADERS, timeout=20)
        except requests.RequestException as e:
            return f"Ulanish xatosi: {escape(str(e))}"
        codes = codes_from_html(r.text) if r.status_code == 200 else []
        c = r.text
        raw = []   # wer_test.py kabi: sahifadagi barcha /tasks/KOD (\/tasks\/KOD ham)
        for m in _TASK_URL_RE.finditer(c):
            if m.group(1) not in raw:
                raw.append(m.group(1))
        apis = sorted(set(re.findall(r"""["'](/(?:api|_)[^"'\s]{0,80})""", c)))[:8]
        return "\n".join([
            f"HTTP: {r.status_code}",
            f"URL: {escape(r.url)}",
            f"Content-Type: {escape(str(r.headers.get('content-type')))}",
            f"Hajmi: {len(c)} belgi",
            f"Bot topgan masala kodlari: {len(codes)} — {escape(', '.join(codes[:10]))}",
            f"Sahifadagi barcha /tasks/... : {len(raw)} — {escape(', '.join(raw[:15]))}",
            f"&lt;a&gt; havolalar: {len(re.findall(r'<a\s', c))}",
            f"data-page bor: {'ha' if 'data-page=' in c else 'yoq'}",
            f"api izlari: {escape(', '.join(apis)) or 'yoq'}",
            f"\nSahifa boshi:\n<pre>{escape(c[:250])}</pre>",
            f"Sahifa oxiri:\n<pre>{escape(c[-250:])}</pre>",
        ])

    loop = asyncio.get_running_loop()
    await msg.reply(await loop.run_in_executor(pool, work))


def tally(s):
    t = {}
    for c in s["votes"].values():
        t[c] = t.get(c, 0) + 1
    return t


def gor_screen(s):
    if s["stage"] == "diff":
        text = (f"🧪 <b>Test tuzish</b> — {escape(s['owner_name'])}\n"
                "Qiyinlik darajasini tanlang:")
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text=d, callback_data=f"gr:d:{d}") for d in DIFFICULTIES
        ]])
        return text, kb
    if s["stage"] == "count":
        text = (f"🧪 <b>Test tuzish</b> — {escape(s['owner_name'])}\n"
                f"Qiyinlik: <b>{s['diff']}</b>\nNechta masala bo'lsin?")
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text=str(c), callback_data=f"gr:n:{c}") for c in COUNTS
        ]])
        return text, kb
    # stage == "vote"
    t = tally(s)
    top = sorted(t.items(), key=lambda kv: -kv[1])[:5]
    lines = [
        "🗳 <b>Bo'lim uchun ovoz bering</b>",
        f"Qiyinlik: <b>{s['diff']}</b> · Masalalar: <b>{s['n']}</b>",
        f"Ovoz berish {VOTE_SECONDS} soniya davom etadi, ovozni almashtirish mumkin.",
        f"Ovoz berganlar: {len(s['votes'])}",
    ]
    if top:
        lines.append("")
        for c, v in top:
            lines.append(f"• {escape(CATEGORIES.get(c, 'Bo`limsiz (oddiy)'))} — {v}")
    cats = list(CATEGORIES.items())
    pages = (len(cats) + CATS_PER_PAGE - 1) // CATS_PER_PAGE
    page = max(1, min(s["page"], pages))
    chunk = cats[(page - 1) * CATS_PER_PAGE: page * CATS_PER_PAGE]
    btns = [InlineKeyboardButton(
        text=name + (f" ({t[cid]})" if cid in t else ""), callback_data=f"gr:v:{cid}"
    ) for cid, name in chunk]
    rows = [btns[i:i + 2] for i in range(0, len(btns), 2)]
    rows.append([
        InlineKeyboardButton(text="◀️", callback_data=f"gr:p:{max(1, page - 1)}"),
        InlineKeyboardButton(text=f"{page}/{pages}", callback_data=f"gr:p:{page}"),
        InlineKeyboardButton(text="▶️", callback_data=f"gr:p:{min(pages, page + 1)}"),
    ])
    rows.append([
        InlineKeyboardButton(text="🎲 Bo'limsiz (oddiy)" + (f" ({t[0]})" if 0 in t else ""),
                             callback_data="gr:v:0"),
        InlineKeyboardButton(text="✅ Yakunlash", callback_data="gr:f"),
    ])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)


async def gor_show(bot: Bot, s: dict):
    text, kb = gor_screen(s)
    try:
        await bot.edit_message_text(text, chat_id=s["chat_id"], message_id=s["msg_id"],
                                    reply_markup=kb)
    except TelegramBadRequest as e:
        if "not modified" not in str(e):
            log.warning("gorobo edit xato: %s", e)


async def post_tracker(bot: Bot, chat_id, code: str, diff=None):
    """/task bilan bir xil: masala uchun kuzatuv xabari."""
    me = await bot.get_me()
    task = {"code": code, "active": True}
    if diff:
        task["meta"] = {"diff": diff}   # /gorobo da tanlangan qiyinlik (rank uchun)
    users = chat_data(chat_id)["users"]
    sent = await bot.send_message(
        chat_id, render(task, users, {}),
        reply_markup=reg_keyboard(me.username, chat_id, code), disable_web_page_preview=True,
    )
    task["msg_id"] = sent.message_id
    async with db_lock:
        chat_data(chat_id)["tasks"].append(task)
        save_db()
    asyncio.create_task(refresh_task(bot, str(chat_id), task))


async def gor_finish(bot: Bot, key: str):
    s = sessions.get(key)
    if not s or s["done"]:
        return
    s["done"] = True
    chat_id = s["chat_id"]
    t = tally(s)
    cat = None
    if t:
        best = max(t.values())
        cat = random.choice([c for c, v in t.items() if v == best]) or None   # 0 -> bo'limsiz
    try:
        await bot.edit_message_text("⏳ Masalalar tanlanmoqda…", chat_id=chat_id,
                                    message_id=s["msg_id"])
    except TelegramBadRequest:
        pass
    exclude = {x["code"] for x in chat_data(chat_id)["tasks"]}
    loop = asyncio.get_running_loop()
    FETCH_ERR["last"] = None
    codes, fb, notes = await loop.run_in_executor(pool, build_set, s["diff"], cat, s["n"], exclude)
    sessions.pop(key, None)
    if not codes:
        why = FETCH_ERR["last"] or "sahifada masala havolalari topilmadi"
        return await bot.edit_message_text(
            "😕 Masala topilmadi.\n"
            f"Sabab: <b>{escape(why)}</b>\n\n"
            f"Tekshirish uchun admin: <code>/gotest {s['diff']}</code>\n"
            "Keyinroq /gorobo ni qayta bosing.",
            chat_id=chat_id, message_id=s["msg_id"])
    # faqat bitta masala: tanlov xabari o'chiriladi, o'rniga masala havolasi (kuzatuv xabari) tashlanadi
    try:
        await bot.delete_message(chat_id, s["msg_id"])
    except TelegramBadRequest:
        pass
    await post_tracker(bot, chat_id, codes[0], s["diff"])
    extra = list(notes)
    if fb:
        extra.insert(0, "⚠️ Tanlangan bo'limda masala topilmadi, oddiy bo'limdan olindi.")
    if extra:
        await bot.send_message(chat_id, "\n".join(extra))


async def gor_auto(bot: Bot, key: str):
    await asyncio.sleep(VOTE_SECONDS)
    await gor_finish(bot, key)


@dp.message(Command("gorobo"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_gorobo(msg: Message):
    s = {"chat_id": msg.chat.id, "owner": msg.from_user.id, "owner_name": msg.from_user.full_name,
         "stage": "diff", "diff": None, "n": None, "votes": {}, "page": 1, "done": False}
    text, kb = gor_screen(s)
    sent = await msg.answer(text, reply_markup=kb)
    s["msg_id"] = sent.message_id
    sessions[f"{msg.chat.id}:{sent.message_id}"] = s


@dp.callback_query(F.data.startswith("gr:"))
async def gor_cb(cb: CallbackQuery, bot: Bot):
    key = f"{cb.message.chat.id}:{cb.message.message_id}"
    s = sessions.get(key)
    if not s or s["done"]:
        return await cb.answer("Bu test tugagan yoki eskirgan.", show_alert=True)
    parts = cb.data.split(":")
    act = parts[1]
    val = parts[2] if len(parts) > 2 else ""
    if act in ("d", "n", "f") and cb.from_user.id != s["owner"] \
            and not await is_admin(bot, s["chat_id"], cb.from_user.id):
        return await cb.answer("Buni faqat testni boshlagan foydalanuvchi bosa oladi.", show_alert=True)

    if act == "d" and s["stage"] == "diff" and val in DIFFICULTIES:
        s["diff"], s["n"], s["stage"] = val, 1, "vote"    # har doim bitta masala
        asyncio.create_task(gor_auto(bot, key))
        await gor_show(bot, s)
    elif act == "n" and s["stage"] == "count" and val.isdigit() and int(val) in COUNTS:
        s["n"], s["stage"] = int(val), "vote"
        asyncio.create_task(gor_auto(bot, key))
        await gor_show(bot, s)
    elif act == "v" and s["stage"] == "vote" and val.isdigit() \
            and (int(val) == 0 or int(val) in CATEGORIES):
        s["votes"][cb.from_user.id] = int(val)
        await gor_show(bot, s)
        return await cb.answer("Ovoz qabul qilindi ✅")
    elif act == "p" and s["stage"] == "vote" and val.isdigit():
        s["page"] = int(val)
        await gor_show(bot, s)
    elif act == "f" and s["stage"] == "vote":
        await cb.answer()
        return await gor_finish(bot, key)
    await cb.answer()


# ======================================================================
# Faqat bitta guruhda ishlash. Boshqa guruhlardagi xabarlar e'tiborsiz qoldiriladi.
# Shaxsiy chat (ro'yxatdan o'tish uchun) cheklanmaydi.
# ID ni boshqa guruhga o'zgartirish:  ALLOWED_CHAT_ID=-100123... python robocontest_bot.py
# ======================================================================
ALLOWED_CHAT_ID = int(os.getenv("ALLOWED_CHAT_ID", "-1003842307067"))
_GROUP_TYPES = {ChatType.GROUP, ChatType.SUPERGROUP}


@dp.message.outer_middleware()
async def only_my_group_msg(handler, event, data):
    if event.chat.type in _GROUP_TYPES and event.chat.id != ALLOWED_CHAT_ID:
        log.warning("Guruh e'tiborsiz qoldirildi: chat_id=%s (ruxsat berilgan: %s)",
                    event.chat.id, ALLOWED_CHAT_ID)
        return None
    return await handler(event, data)


@dp.callback_query.outer_middleware()
async def only_my_group_cb(handler, event, data):
    m = event.message
    if m is not None and m.chat.type in _GROUP_TYPES and m.chat.id != ALLOWED_CHAT_ID:
        return None
    return await handler(event, data)


# ======================================================================
# Guruhda /start va /help — qisqa yo'riqnoma va ro'yxatdan o'tish tugmasi
# ======================================================================
@dp.message(Command("start", "help"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_group_start(msg: Message, bot: Bot):
    me = await bot.get_me()
    await msg.answer(
        "👋 <b>Robocontest nazorat boti</b>\n\n"
        "📥 Ro'yxatdan o'tish: pastdagi tugma yoki <code>/reg username</code>\n"
        "🧪 <code>/gorobo</code> — o'zingiz test tuzing (qiyinlik, soni, bo'lim ovozi)\n"
        "📝 <code>/task havola</code> — masala qo'shish (faqat adminlar)\n"
        "⛔ <code>/stop KOD</code> — masalani kuzatishni to'xtatish (faqat adminlar)",
        reply_markup=reg_keyboard(me.username, msg.chat.id),
    )


# ======================================================================
# /test username KOD — botning o'zi profil sahifasida nimani ko'rayotganini ko'rsatadi
# (🟡/✅ chiqmasa, sababini aniqlash uchun). Faqat guruh adminlari.
# ======================================================================
@dp.message(Command("test"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_test(msg: Message, command: CommandObject, bot: Bot):
    if not await is_admin(bot, msg.chat.id, msg.from_user.id):
        return
    parts = (command.args or "").split()
    user = parse_rb_user(parts[0]) if len(parts) == 2 else None
    if not user:
        return await msg.reply("Ishlatish: /test username KOD   masalan: /test shxr A0002")
    code = parts[1]

    def work() -> str:
        try:
            r = requests.get(f"{BASE}/profile/{user}", headers=HEADERS, timeout=15)
        except requests.RequestException as e:
            return f"So'rov xatosi: {escape(str(e))}"
        p = LinkParser()
        p.feed(r.text)
        found = [a for a in p.links if a["text"].upper() == code.upper()]
        yes = lambda b: "ha" if b else "yo'q"
        lines = [
            f"HTTP: {r.status_code}",
            f"Hajmi: {len(r.text)} belgi",
            f"Content-Type: {escape(str(r.headers.get('content-type')))}",
            f"&lt;a&gt; soni: {len(p.links)}",
            f"Kod anchor sifatida topildi: {yes(found)}",
            f"Kod xom matnda bor: {yes(code.upper() in r.text.upper())}",
            f"data-page bor: {yes('data-page=' in r.text)}",
            f"Bot natijasi: <b>{check_user(user, code)}</b>",
        ]
        if found:
            lines.append(f"Anchor class oxiri: <code>{escape(found[0]['cls'][-70:])}</code>")
        lines.append(f"\nSahifa boshi:\n<pre>{escape(r.text[:300])}</pre>")
        return "\n".join(lines)

    loop = asyncio.get_running_loop()
    await msg.reply(await loop.run_in_executor(pool, work))


# ======================================================================
# Profil sahifasi ichidagi JSON dan masala holatini o'qish ("solved" / "tried").
# check_user() birinchi shuni sinaydi, topilmasa <a> rang usuliga o'tadi.
# ======================================================================
def _status_from_json(text: str, code: str):
    """solved | tried | none, topilmasa None."""
    _capture_meta(text, code)   # qiyinlik va qabul foizini eslab qoladi (rank uchun)
    from html import unescape
    if "&quot;" in text:
        text = unescape(text)
    pat = r"\\?/tasks\\?/" + re.escape(code) + r"(?![\w\-])"
    for m in re.finditer(pat, text, re.IGNORECASE):
        x = m.start()
        lo, hi = text.rfind("{", 0, x), text.find("}", x)
        parts = []
        if lo != -1 and hi != -1 and hi - lo < 3000:
            parts.append(text[lo:hi + 1])            # shu masalaning JSON obyekti
        parts.append(text[max(0, x - 100): x + 150])  # zaxira: atrofdagi qism
        for part in parts:
            s = re.search(r'"solved"\s*:\s*(true|false)', part)
            if not s:
                continue
            t = re.search(r'"tried"\s*:\s*(true|false)', part)
            if s.group(1) == "true":
                return "solved"
            return "tried" if (t and t.group(1) == "true") else "none"
    return None


# ======================================================================
# Profil rasmi -> yumaloq premium (custom) emoji, ism oldida.
#   Ro'yxatdan o'tganda bot profil rasmini oladi, 100x100 yumaloq PNG qiladi,
#   o'quvchi nomidan bitta custom emoji to'plami yaratadi va ro'yxatda
#   "✅ 🙂 Ism (username)" ko'rinishida ko'rsatadi.
#   DIQQAT: Telegram custom emoji'ni faqat Fragment'dan kollektsion username olgan
#   botlarga yuborishga ruxsat beradi. Bot yubora olmasa, oddiy matn ishlatiladi.
#   Kerak:  pip install pillow   va yangi aiogram (pip install -U aiogram)
# ======================================================================
import io
from html import unescape as _unescape
from aiogram.types import BufferedInputFile, InputSticker

EMOJI_STATE = {"ok": None}   # None: noma'lum | True: ishlaydi | False: bot yubora olmaydi


def get_avatar_url(html_text: str):
    idx = html_text.rfind('_url" content="https')
    if idx == -1:
        return None
    start = html_text.find('content="', idx - 10) + 9
    end = html_text.find('"', start)
    url = _unescape(html_text[start:end])
    return url if url.startswith("http") else None


def make_round_png(img_bytes: bytes):
    try:
        from PIL import Image, ImageChops, ImageDraw
    except ImportError:
        log.warning("Pillow o'rnatilmagan: pip install pillow")
        return None
    try:
        im = Image.open(io.BytesIO(img_bytes)).convert("RGBA")
    except Exception:
        return None
    w, h = im.size
    s = min(w, h)
    x0, y0 = (w - s) // 2, (h - s) // 2
    im = im.crop((x0, y0, x0 + s, y0 + s)).resize((100, 100), Image.LANCZOS)
    mask = Image.new("L", (400, 400), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, 399, 399), fill=255)
    mask = mask.resize((100, 100), Image.LANCZOS)
    im.putalpha(ImageChops.multiply(im.getchannel("A"), mask))
    out = io.BytesIO()
    im.save(out, "PNG")
    return out.getvalue()


def fetch_avatar_png(rb: str):
    try:
        r = requests.get(f"{BASE}/profile/{rb}", headers=HEADERS, timeout=15)
        url = get_avatar_url(r.text) if r.status_code == 200 else None
        if not url:
            return None
        img = requests.get(url, headers=HEADERS, timeout=15)
        return make_round_png(img.content) if img.status_code == 200 else None
    except requests.RequestException:
        return None


async def probe_emoji(bot: Bot, chat_id, eid: str):
    """Bot custom emoji yubora oladimi? Bir marta sinab ko'radi (xabar darhol o'chiriladi)."""
    if EMOJI_STATE["ok"]:
        return
    try:
        m = await bot.send_message(int(chat_id), f'<tg-emoji emoji-id="{eid}">🙂</tg-emoji>')
    except TelegramBadRequest as e:
        if any(k in str(e).upper() for k in ("TOPIC_CLOSED", "TOPIC_DELETED", "THREAD")):
            log.warning("Emoji sinovi mavzu xatosi tufayli o'tmadi (keyin qayta sinaladi): %s", e)
            return                                  # emoji emas, mavzu muammosi: holat noma'lum (None) qoladi
        EMOJI_STATE["ok"] = False
        log.warning("Bot custom emoji yubora olmaydi (Fragment username kerak): %s", e)
        return
    # Telegram ruxsati yo'q botda emojini xatosiz, jimgina olib tashlaydi: javobdagi entity'ga qaraymiz
    shown = any(getattr(e, "type", None) == "custom_emoji" for e in (m.entities or []))
    EMOJI_STATE["ok"] = shown
    if not shown:
        log.warning("Telegram custom emoji'ni jimgina olib tashladi: botda Fragment'dan "
                    "kollektsion username yo'q.")
    try:
        await bot.delete_message(int(chat_id), m.message_id)
    except Exception:
        pass


async def setup_avatar(bot: Bot, chat_id: str, tg_user):
    try:
        u = chat_data(chat_id)["users"].get(str(tg_user.id))
        if not u:
            return
        loop = asyncio.get_running_loop()
        png = await loop.run_in_executor(pool, fetch_avatar_png, u["rb"])
        if not png:
            log.info("Avatar topilmadi: %s", u["rb"])
            return
        me = await bot.get_me()
        name = f"av{tg_user.id}_by_{me.username}"
        try:
            await bot.delete_sticker_set(name)   # eski rasmni yangilash uchun
        except TelegramBadRequest:
            pass
        await bot.create_new_sticker_set(
            user_id=tg_user.id, name=name, title=u["rb"][:64],
            stickers=[InputSticker(sticker=BufferedInputFile(png, "avatar.png"),
                                   format="static", emoji_list=["🙂"])],
            sticker_type="custom_emoji",
        )
        st = await bot.get_sticker_set(name)
        eid = st.stickers[0].custom_emoji_id
        async with db_lock:
            u["emoji_id"] = eid
            save_db()
        await probe_emoji(bot, chat_id, eid)
        asyncio.create_task(refresh_chat(bot, str(chat_id)))
    except Exception:
        log.exception("avatar emoji xatosi")


# register() ni o'rab olamiz: ro'yxatdan o'tgach avatar emoji tayyorlanadi
_orig_register = register


async def register(bot: Bot, chat_id: str, tg_user, text: str) -> str:
    reply = await _orig_register(bot, chat_id, tg_user, text)
    if reply.startswith("✅"):
        asyncio.create_task(setup_avatar(bot, chat_id, tg_user))
    return reply


# render() ni o'rab olamiz: ism oldiga emoji qo'shiladi (faqat bot yubora olsa)
_orig_render = render


def render(task: dict, users: dict, results: dict) -> str:
    text = _orig_render(task, users, results)
    if not EMOJI_STATE["ok"]:
        return text
    se = DB.get("status_emoji") or {}   # /icons orqali yaratilgan holat rasmlari

    def icon(st: str) -> str:
        sid = se.get(st)
        if not sid:
            return ICON[st]
        ids = sid if isinstance(sid, list) else [sid]     # rasm bir necha bo'lakka bo'lingan bo'lishi mumkin
        return "".join(f'<tg-emoji emoji-id="{x}">{ICON[st]}</tg-emoji>' for x in ids)

    for i, u in users.items():
        st = results.get(i, "error")
        tail = f"{escape(u['name'])} (<code>{escape(u['rb'])}</code>)"
        eid = u.get("emoji_id")
        av = f' <tg-emoji emoji-id="{eid}">🙂</tg-emoji>' if eid else ""
        text = text.replace(f"{ICON[st]} {tail}", f"{icon(st)}{av} {tail}")
    # sarlavha va izoh qatoridagi belgilar ham rasm bo'lsin
    for old, st in (("✅ Ishladi", "solved"), ("✅ ishlagan", "solved"),
                    ("🟡 urinib ko'rgan", "tried"), ("❌ ishlamagan", "none")):
        if se.get(st):
            text = text.replace(old, old.replace(ICON[st], icon(st), 1))
    return text


@dp.startup()
async def _emoji_startup(bot: Bot):
    """Bot qayta ishga tushganda saqlangan emoji'larni bir marta sinab ko'radi."""
    ids = [x for v in (DB.get("status_emoji") or {}).values() for x in (v if isinstance(v, list) else [v])]
    for cid, c in DB["chats"].items():
        ids += [u["emoji_id"] for u in c["users"].values() if u.get("emoji_id")]
        if ids:
            await probe_emoji(bot, cid, ids[0])
            return


# ======================================================================
# Holat belgilari (✅ 🟡 ❌) o'rniga o'zingizning rasmlaringiz — custom emoji sifatida.
#   Guruhda bir marta admin  /icons  yozadi: bot rasmlarni yuklab oladi, 100x100 qiladi
#   va 3 ta emojili to'plam yaratadi. Shundan so'ng ro'yxatda rasmlar chiqadi.
#   (Avatar emoji'dagi kabi: bot custom emoji yubora olmasa, ✅ 🟡 ❌ qoladi.)
# ======================================================================
STATUS_IMAGES = {
    "solved": "https://i.ibb.co/gMDws3Jk/image.png",   # ishlangan   (✅ o'rniga)
    "none":   "https://i.ibb.co/dJt6Cw6L/image.png",   # ishlanmagan (❌ o'rniga)
    "tried":  "https://i.ibb.co/3yfCK54H/image.png",   # xato ishlangan (🟡 o'rniga)
}


def make_fit_png(img_bytes: bytes):
    """Rasmni proportsiyasini saqlab 100x100 shaffof PNG ga sig'diradi."""
    try:
        from PIL import Image
    except ImportError:
        log.warning("Pillow o'rnatilmagan: pip install pillow")
        return None
    try:
        im = Image.open(io.BytesIO(img_bytes)).convert("RGBA")
    except Exception:
        return None
    im.thumbnail((100, 100), Image.LANCZOS)
    canvas = Image.new("RGBA", (100, 100), (0, 0, 0, 0))
    canvas.paste(im, ((100 - im.width) // 2, (100 - im.height) // 2), im)
    out = io.BytesIO()
    canvas.save(out, "PNG")
    return out.getvalue()


def fetch_status_pngs() -> dict:
    out = {}
    for key, url in STATUS_IMAGES.items():
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        png = make_fit_png(r.content)
        if not png:
            raise ValueError(f"'{key}' rasmi o'qilmadi ({url})")
        out[key] = png
    return out


@dp.message(Command("icons"), F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def cmd_icons(msg: Message, bot: Bot):
    if not await is_admin(bot, msg.chat.id, msg.from_user.id):
        return
    note = await msg.reply("⏳ Holat rasmlari tayyorlanmoqda…")
    try:
        loop = asyncio.get_running_loop()
        me = await bot.get_me()
        ids = await make_status_set(bot, msg.from_user.id, f"st{msg.from_user.id}_by_{me.username}")
        async with db_lock:
            DB["status_emoji"] = ids
            save_db()
        EMOJI_STATE["ok"] = None   # qayta sinab ko'riladi
        await probe_emoji(bot, msg.chat.id, ids["solved"][0])
    except Exception as e:
        log.exception("/icons xatosi")
        return await note.edit_text(f"❌ Xatolik: {escape(str(e))}")
    if EMOJI_STATE["ok"]:
        await note.edit_text("✅ Holat rasmlari tayyor, ro'yxat yangilanmoqda.")
        asyncio.create_task(refresh_chat(bot, str(msg.chat.id)))
    else:
        await note.edit_text("⚠️ Rasmlar yaratildi, lekin bot custom emoji yubora olmadi "
                             "(botda Fragment'dan kollektsion username kerak). "
                             "Hozircha ✅ 🟡 ❌ belgilari ishlatiladi.")


# ======================================================================
# RANK / BALL tizimi
#   Har bir masala uchun ball masalaning qiyinligi va qabul foiziga bog'liq:
#       ball = asos(qiyinlik) * (1 + (100 - qabul%) / 200)
#   Ishlangan masala  -> +ball.   Urinib, ishlay olmagan (xato) -> -ball * PENALTY_RATIO.
#   Ball har safar masalalarning joriy holatidan qayta hisoblanadi (xato tuzatilsa, jarima olinadi).
#   Har bir daraja (Trainee 3 ... Architect 1) o'z shkalasiga ega.
# ======================================================================
DIFF_BASE = {"easy": 10, "medium": 25, "hard": 50, "expert": 100}
DIFF_DEFAULT = "medium"      # qiyinlik aniqlanmasa shu olinadi
PENALTY_RATIO = 0.3          # xato uchun jarima = ball * 0.3
GIVEUP_RATIO = 0.5           # "Ishlolmadim" tugmasi uchun jarima = ball * 0.5
# (nom, pog'onalar soni, har bir pog'ona shkalasi — shuncha ball)
TIERS = [("Trainee", 3, 100), ("Junior", 5, 200), ("Middle", 5, 400),
         ("Senior", 5, 700), ("Architect", 3, 1200)]
LEVELS = [(f"{n} {k}", w) for n, cnt, w in TIERS for k in range(cnt, 0, -1)]
META: dict = {}              # "A0002" -> {"diff": "Oson", "rate": 13.0}


def norm_diff(label):
    s = (label or "").lower().replace("’", "'").replace("`", "'")
    if not s:
        return None
    if "juda" in s or "expert" in s or "ekspert" in s:
        return "expert"
    if "qiyin" in s or "hard" in s:
        return "hard"
    if "o'rta" in s or "orta" in s or "med" in s:
        return "medium"
    if "oson" in s or "easy" in s:
        return "easy"
    return None


def _dec(s: str) -> str:
    return re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), s)


def _capture_meta(text: str, code: str):
    """Profil sahifasidagi 'A0002 · Nomi (Oson · 13%)' yoki JSON dan qiyinlik va foizni oladi."""
    key = code.upper()
    if key in META or key not in text.upper():
        return
    t = _unescape(text) if "&" in text else text
    for n, m in enumerate(re.finditer(re.escape(code), t, re.IGNORECASE)):
        if n >= 6:
            break
        w = _dec(t[max(0, m.start() - 300): m.start() + 500])
        i = w.upper().find(key)
        a = re.search(r"\(([^()\"]+?)\s*[·•|]\s*(\d+(?:[.,]\d+)?)\s*%\)", w[i:])
        diff = rate = None
        if a:
            diff, rate = a.group(1).strip(), float(a.group(2).replace(",", "."))
        else:
            d = re.search(r'"difficulty[a-z_]*"\s*:\s*"?([^",}]+)', w, re.I)
            r = re.search(r'"(?:acceptance|accepted|success|rate|percent)[a-z_]*"\s*:\s*"?([\d.]+)', w, re.I)
            diff = d.group(1).strip() if d else None
            rate = float(r.group(1)) if r else None
        if diff or rate is not None:
            META[key] = {"diff": diff, "rate": rate}
            return


def task_points(task: dict):
    """(ishlasa +ball, xato qilsa -jarima)"""
    meta = task.get("meta") or {}
    base = DIFF_BASE[norm_diff(meta.get("diff")) or DIFF_DEFAULT]
    rate = meta.get("rate")
    mult = 1 + (100 - min(max(rate, 0), 100)) / 200 if isinstance(rate, (int, float)) else 1.0
    reward = max(1, round(base * mult))
    return reward, max(1, round(reward * PENALTY_RATIO))


def rank_of(points: int):
    """(daraja nomi, shu darajadagi ball, daraja shkalasi yoki None, indeks)"""
    acc = 0
    for idx, (label, w) in enumerate(LEVELS):
        if idx == len(LEVELS) - 1:
            return label, points - acc, None, idx
        if points < acc + w:
            return label, points - acc, w, idx
        acc += w


def bar(cur: int, width: int, n: int = 10) -> str:
    f = max(0, min(n, int(cur / width * n)))
    return "▰" * f + "▱" * (n - f)


_SORDER = {"solved": 2, "tried": 1, "none": 0}


def chat_scores(chat_id, day=None, floor=True) -> dict:
    """uid -> {"pts", "solved", "tried", "rows": [(kod, +-ball)]}; bir kod bir marta hisoblanadi."""
    c = chat_data(chat_id)
    by_code: dict = {}
    for t in c["tasks"]:
        if day and t.get("day") != day:      # faqat shu kunda berilgan masalalar
            continue
        by_code.setdefault(t["code"].upper(), []).append(t)
    scores = {uid: {"pts": 0, "solved": 0, "tried": 0, "gave": 0, "rows": []} for uid in c["users"]}
    for code, tasks in by_code.items():
        best: dict = {}
        for t in tasks:
            for uid, st in (t.get("res") or {}).items():
                if _SORDER.get(st, -1) > _SORDER.get(best.get(uid), -1):
                    best[uid] = st
        reward, pen = task_points(max(tasks, key=lambda t: len(t.get("meta") or {})))
        gave = {u for t in tasks for u in (t.get("gave") or {})}   # "Ishlolmadim" bosganlar
        give_pen = max(pen, round(reward * GIVEUP_RATIO))
        for uid in set(best) | gave:
            st = best.get(uid)
            sc = scores.get(uid)
            if not sc:
                continue
            if st == "solved":
                sc["pts"] += reward
                sc["solved"] += 1
                sc["rows"].append((code, reward))
            elif uid in gave or st == "tried":
                p = give_pen if uid in gave else pen
                sc["pts"] -= p
                sc["tried"] += 1
                if uid in gave:
                    sc["gave"] += 1
                sc["rows"].append((code, -p))
    if floor:
        for sc in scores.values():
            sc["pts"] = max(0, sc["pts"])
    return scores


# render() ni yana o'rab olamiz: natijalarni saqlaydi va ism yoniga daraja yozadi
_emoji_render = render


def _chat_id_of(users: dict):
    for cid, c in DB["chats"].items():
        if c["users"] is users:
            return cid
    return None


def render(task: dict, users: dict, results: dict) -> str:
    if results:
        res = task.setdefault("res", {})
        for uid, st in results.items():
            if st in ("solved", "tried", "none"):
                res[uid] = st
    task.setdefault("day", today_str())      # kunlik hisobot uchun
    meta = task.setdefault("meta", {})
    got = META.get(task["code"].upper()) or {}
    for k in ("diff", "rate"):
        if meta.get(k) is None and got.get(k) is not None:
            meta[k] = got[k]
    text = _emoji_render(task, users, results)
    # sarlavha tagiga: qiyinlik va bu masala necha ball berishi
    reward, pen = task_points(task)
    give_pen = max(pen, round(reward * GIVEUP_RATIO))
    dn = norm_diff(meta.get("diff"))
    shown = {"easy": "Oson", "medium": "O'rtacha", "hard": "Qiyin", "expert": "Expert"}.get(dn) \
        or (str(meta["diff"]) if meta.get("diff") else None)
    if shown:
        rate = meta.get("rate")
        dtxt = f"{escape(shown)}" + (f" · {rate:g}%" if isinstance(rate, (int, float)) else "")
    else:
        dtxt = "qiyinlik aniqlanmadi (o'rtacha deb olindi)"
    head, _, rest = text.partition("\n")
    text = (f"{head}\n🎯 {dtxt} · ishlasa <b>+{reward}</b> · xato <b>−{pen}</b> · "
            f"ishlolmasa <b>−{give_pen}</b> ball\n{rest}")
    cid = _chat_id_of(users)
    if cid is None:
        return text
    sc = chat_scores(cid)
    gave = task.get("gave") or {}
    for uid, u in users.items():
        label, cur, width, _ = rank_of(sc.get(uid, {"pts": 0})["pts"])
        prog = "MAX" if width is None else f"{cur}/{width}"
        old = f"(<code>{escape(u['rb'])}</code>)"
        flag = " 🏳️" if gave.get(uid) and results.get(uid) != "solved" else ""
        text = text.replace(old, f"{old} — {rank_badge(label)}<i>{label} {{{prog}}}</i>{flag}", 1)
    return text


_GROUPS = {ChatType.GROUP, ChatType.SUPERGROUP}


@dp.message(Command("top"), F.chat.type.in_(_GROUPS))
async def cmd_top(msg: Message):
    users = chat_data(msg.chat.id)["users"]
    if not users:
        return await msg.reply("Hali hech kim ro'yxatdan o'tmagan.")
    sc = chat_scores(msg.chat.id)
    rows = sorted(users, key=lambda i: (-sc[i]["pts"], users[i]["name"].lower()))
    medals = ["🥇", "🥈", "🥉"]
    lines = ["🏆 <b>Reyting</b>", ""]
    for n, i in enumerate(rows[:30], 1):
        pts = sc[i]["pts"]
        label, cur, width, _ = rank_of(pts)
        prog = "MAX" if width is None else f"{bar(cur, width)} {cur}/{width}"
        lines.append(f"{medals[n - 1] if n <= 3 else f'{n}.'} {escape(users[i]['name'])} — {rank_badge(label)}<b>{label}</b>")
        lines.append(f"      {prog} · {pts} ball")
    await msg.answer("\n".join(lines))


@dp.message(Command("rank"), F.chat.type.in_(_GROUPS))
async def cmd_rank(msg: Message):
    uid = str(msg.from_user.id)
    if uid not in chat_data(msg.chat.id)["users"]:
        return await msg.reply("Avval ro'yxatdan o'ting: /reg username")
    s = chat_scores(msg.chat.id)[uid]
    label, cur, width, idx = rank_of(s["pts"])
    lines = [f"🎖 <b>{escape(msg.from_user.full_name)}</b> — {rank_badge(label)}<b>{label}</b>", ""]
    if width is None:
        lines.append(f"Shkala: MAX · {s['pts']} ball")
    else:
        lines.append(f"{bar(cur, width)} {cur}/{width}")
        lines.append(f"Jami: {s['pts']} ball · keyingi daraja ({LEVELS[idx + 1][0]}) uchun yana {width - cur} ball")
    lines.append(f"✅ ishlagan: {s['solved']} · 🟡 xato: {s['tried']}")
    rows = sorted(s["rows"], key=lambda r: -abs(r[1]))[:15]
    if rows:
        lines.append("")
        lines += [f"{'+' if p > 0 else '−'}{abs(p)}  {code}" for code, p in rows]
    lines += ["", "<i>Ball: Oson 10, O'rtacha 25, Qiyin 50, Expert 100 × (1 + (100 − qabul%)/200). "
                  f"Xato: −{int(PENALTY_RATIO * 100)}%, «Ishlolmadim»: −{int(GIVEUP_RATIO * 100)}%.</i>"]
    await msg.reply("\n".join(lines))


@dp.message(Command("diff"), F.chat.type.in_(_GROUPS))
async def cmd_diff(msg: Message, command: CommandObject, bot: Bot):
    """/diff KOD easy|medium|hard|expert [qabul%] — qiyinlikni qo'lda belgilash (adminlar)."""
    if not await is_admin(bot, msg.chat.id, msg.from_user.id):
        return
    a = (command.args or "").split()
    if len(a) < 2 or not norm_diff(a[1]):
        return await msg.reply("Ishlatish: /diff A0002 easy 13   (easy|medium|hard|expert, qabul foizi ixtiyoriy)")
    code, d = a[0].upper(), norm_diff(a[1])
    rate = None
    if len(a) > 2:
        try:
            rate = float(a[2].replace("%", "").replace(",", "."))
        except ValueError:
            return await msg.reply("Qabul foizi raqam bo'lishi kerak.")
    n, pts = 0, None
    async with db_lock:
        for t in chat_data(msg.chat.id)["tasks"]:
            if t["code"].upper() == code:
                m = t.setdefault("meta", {})
                m["diff"] = d
                if rate is not None:
                    m["rate"] = rate
                pts = task_points(t)
                n += 1
        save_db()
    if not n:
        return await msg.reply("Bunday masala topilmadi.")
    await msg.reply(f"✅ {code}: {d}" + (f", {rate}%" if rate is not None else "")
                    + f" → ishlasa +{pts[0]}, xato qilsa −{pts[1]} ball")
    asyncio.create_task(refresh_chat(bot, str(msg.chat.id)))


@dp.message(Command("json"), F.chat.type.in_(_GROUPS))
async def cmd_json(msg: Message, command: CommandObject, bot: Bot):
    """/json username KOD — profil sahifasidagi shu masalaning ma'lumotini ko'rsatadi (adminlar)."""
    if not await is_admin(bot, msg.chat.id, msg.from_user.id):
        return
    a = (command.args or "").split()
    user = parse_rb_user(a[0]) if len(a) == 2 else None
    if not user:
        return await msg.reply("Ishlatish: /json shxr A0002")
    code = a[1]

    def work() -> str:
        try:
            r = requests.get(f"{BASE}/profile/{user}", headers=HEADERS, timeout=15)
        except requests.RequestException as e:
            return f"So'rov xatosi: {escape(str(e))}"
        META.pop(code.upper(), None)
        _capture_meta(r.text, code)
        t = _unescape(r.text) if "&" in r.text else r.text
        m = re.search(r"\\?/tasks\\?/" + re.escape(code) + r"(?![\w\-])", t, re.IGNORECASE)
        if not m:
            return "Kod sahifada topilmadi."
        lo, hi = t.rfind("{", 0, m.start()), t.find("}", m.start())
        snippet = t[lo:hi + 1] if lo != -1 and hi != -1 and hi - lo < 1500 else t[max(0, m.start() - 200): m.start() + 300]
        return (f"Aniqlangan: <b>{escape(str(META.get(code.upper())))}</b>\n"
                f"<pre>{escape(_dec(snippet)[:900])}</pre>")

    loop = asyncio.get_running_loop()
    await msg.reply(await loop.run_in_executor(pool, work))


# ======================================================================
# "😞 Ishlolmadim" tugmasi: foydalanuvchi ballidan jarima ayriladi va keyingi masala tashlanadi.
#   Jarima = masala ballining GIVEUP_RATIO (50%) qismi (kamida xato jarimasi qadar).
#   Keyin shu masalani yechsa, jarima olinadi va to'liq ball qo'shiladi.
# ======================================================================
_orig_reg_keyboard = reg_keyboard


def reg_keyboard(bot_username: str, chat_id, code=None) -> InlineKeyboardMarkup:
    kb = _orig_reg_keyboard(bot_username, chat_id)
    if code:
        kb.inline_keyboard.append(
            [InlineKeyboardButton(text="😞 Ishlolmadim", callback_data=f"gu:{code}")])
    return kb


@dp.callback_query(F.data.startswith("gu:"))
async def giveup_cb(cb: CallbackQuery, bot: Bot):
    code = cb.data[3:]
    chat_id = cb.message.chat.id
    uid = str(cb.from_user.id)
    c = chat_data(chat_id)
    if uid not in c["users"]:
        return await cb.answer("Avval ro'yxatdan o'ting: /reg username", show_alert=True)
    tasks = [t for t in c["tasks"] if t["code"].upper() == code.upper()]
    task = next((t for t in tasks if t.get("msg_id") == cb.message.message_id), None) \
        or (tasks[-1] if tasks else None)
    if not task:
        return await cb.answer("Bu masala topilmadi.", show_alert=True)
    if any((t.get("gave") or {}).get(uid) for t in tasks):
        return await cb.answer("Siz bu masalada allaqachon «Ishlolmadim» bosgansiz.", show_alert=True)
    loop = asyncio.get_running_loop()
    st = await loop.run_in_executor(pool, check_user, c["users"][uid]["rb"], code)
    if st == "solved":
        return await cb.answer("Siz bu masalani ishlagansiz ✅", show_alert=True)

    before = chat_scores(chat_id)[uid]["pts"]
    async with db_lock:
        task.setdefault("gave", {})[uid] = True
        save_db()
    after = chat_scores(chat_id)[uid]["pts"]
    lost = before - after
    label, cur, width, _ = rank_of(after)
    prog = "MAX" if width is None else f"{cur}/{width}"
    await cb.answer(f"−{lost} ball → {label} {{{prog}}}. Keyingi masala…")   # alohida xabar yo'q
    meta = task.get("meta") or {}
    diff = norm_diff(meta.get("diff")) or None
    exclude = {t["code"] for t in c["tasks"]}
    FETCH_ERR["last"] = None
    codes, _fb, _notes = await loop.run_in_executor(pool, build_set, diff, None, 1, exclude)
    if not codes:
        await bot.send_message(chat_id, "Keyingi masala topilmadi, keyinroq /gorobo ni bosing.")
    else:
        await post_tracker(bot, chat_id, codes[0], diff)
    asyncio.create_task(refresh_chat(bot, str(chat_id)))


# ======================================================================
# /task bilan qo'shilgan masalalar uchun ham qiyinlik va qabul foizi aniqlanadi:
#   1) profil sahifasidagi "(Oson · 13%)" dan (META),  2) masalaning o'z sahifasidan.
#   Aniqlanmasa o'rtacha (25 asos ball) olinadi; qo'lda:  /diff KOD easy 13
# ======================================================================
def fetch_task_meta(code: str):
    got = META.get(code.upper())
    if got and (got.get("diff") or got.get("rate") is not None):
        return got
    try:
        r = requests.get(f"{BASE}/tasks/{code}", headers=_UA_HEADERS, timeout=15)
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    t = _dec(_unescape(r.text) if "&" in r.text else r.text)
    diff = rate = None
    a = re.search(r"\(([^()\"<>]{2,25}?)\s*[·•|]\s*(\d+(?:[.,]\d+)?)\s*%\)", t)
    if a:
        diff, rate = a.group(1).strip(), float(a.group(2).replace(",", "."))
    else:
        d = re.search(r'"difficulty[a-z_]*"\s*:\s*"?([^",}]+)', t, re.I)
        q = re.search(r'"(?:acceptance|accepted|success|rate|percent)[a-z_]*"\s*:\s*"?([\d.]+)', t, re.I)
        diff = d.group(1).strip() if d else None
        rate = float(q.group(1)) if q else None
    if norm_diff(diff) is None and rate is None:
        return None
    return {"diff": diff, "rate": rate}


_orig_refresh_task = refresh_task


async def refresh_task(bot: Bot, chat_id: str, task: dict):
    meta = task.setdefault("meta", {})
    if (meta.get("diff") is None or meta.get("rate") is None) and task.get("meta_try", 0) < 4:
        task["meta_try"] = task.get("meta_try", 0) + 1
        loop = asyncio.get_running_loop()
        got = await loop.run_in_executor(pool, fetch_task_meta, task["code"])
        for k in ("diff", "rate"):
            if got and meta.get(k) is None and got.get(k) is not None:
                meta[k] = got[k]
    await _orig_refresh_task(bot, chat_id, task)


# ======================================================================
# /avatars — ro'yxatdagi hamma o'quvchining profil rasmini premium emojiga aylantiradi
#   (/reg ni qayta yozishni kutmasdan). Admin yozadi:  /avatars   yoki   /avatars all (hammasini qayta)
#   To'plam egasi sifatida shu buyruqni yozgan admin ishlatiladi.
# ======================================================================
@dp.message(Command("avatars"), F.chat.type.in_(_GROUPS))
async def cmd_avatars(msg: Message, command: CommandObject, bot: Bot):
    if not await is_admin(bot, msg.chat.id, msg.from_user.id):
        return
    users = chat_data(msg.chat.id)["users"]
    redo = (command.args or "").strip().lower() == "all"
    todo = [uid for uid, u in users.items() if redo or not u.get("emoji_id")]
    if not todo:
        return await msg.reply("Hammaning emojisi tayyor. Qayta yaratish uchun: /avatars all")
    note = await msg.reply(f"⏳ {len(todo)} ta profil rasmi emojiga aylantirilmoqda…")
    loop = asyncio.get_running_loop()
    owner = await _emoji_owner(bot) or msg.from_user.id
    ok, errs = 0, []
    for uid in todo:
        u = users[uid]
        png, why = await loop.run_in_executor(pool, fetch_avatar_png_ex, u["rb"])
        if not png:
            errs.append(f"{u['rb']}: {why}")
            continue
        try:
            eid = await add_avatar_to_pack(bot, owner, u, png)
            ok += 1
            if ok == 1:
                await probe_emoji(bot, msg.chat.id, eid)
        except Exception as e:
            log.exception("avatar xatosi: %s", u["rb"])
            errs.append(f"{u['rb']}: {str(e)[:90]}")
    lines = [f"✅ Tayyor: {ok} ta" + (f" · ❌ xato: {len(errs)} ta" if errs else "")]
    lines += [escape(x) for x in errs[:6]]
    if ok and not EMOJI_STATE["ok"]:
        lines.append("⚠️ Emojilar yaratildi, lekin bot ularni xabarga qo'ya olmadi: botda Fragment'dan "
                     "kollektsion username kerak (Telegram Premium bunga yetmaydi).")
    await note.edit_text("\n".join(lines))
    if ok:
        asyncio.create_task(refresh_chat(bot, str(msg.chat.id)))


# ======================================================================
# Profil rasmi: sababini aniq aytadi + brauzer sarlavhasi bilan so'raydi
# ======================================================================
def fetch_avatar_png_ex(rb: str):
    """(png, "") yoki (None, sabab)."""
    try:
        import PIL  # noqa: F401
    except ImportError:
        return None, "Pillow o'rnatilmagan (pip install pillow)"
    try:
        r = requests.get(f"{BASE}/profile/{rb}", headers=_UA_HEADERS, timeout=15)
    except requests.RequestException as e:
        return None, f"profil sahifasi ochilmadi ({type(e).__name__})"
    if r.status_code != 200:
        return None, f"profil sahifasi HTTP {r.status_code}"
    url = get_avatar_url(r.text)
    if not url:
        m = re.search(r'<meta[^>]+(?:og:image|twitter:image)[^>]*content="([^"]+)"', r.text)
        url = _unescape(m.group(1)) if m else None
    if not url:
        return None, "sahifada rasm havolasi topilmadi"
    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("/"):
        url = BASE + url
    try:
        img = requests.get(url, headers=_UA_HEADERS, timeout=20)
    except requests.RequestException as e:
        return None, f"rasm yuklanmadi ({type(e).__name__})"
    if img.status_code != 200:
        return None, f"rasm yuklanmadi (HTTP {img.status_code})"
    png = make_round_png(img.content)
    if not png:
        return None, f"rasm formati o'qilmadi ({img.headers.get('content-type')})"
    return png, ""


def fetch_avatar_png(rb: str):
    return fetch_avatar_png_ex(rb)[0]


# ======================================================================
# AVTOMATIK: bot ishga tushgach (va har 10 daqiqada) holat rasmlari va hamma o'quvchining
# profil rasmi emojisi o'zi yaratiladi. /icons va /avatars qo'lda yozish shart emas.
# To'plam egasi — guruh egasi (yo'q bo'lsa birinchi admin).
# ======================================================================
async def _emoji_owner(bot: Bot):
    admins = await bot.get_chat_administrators(ALLOWED_CHAT_ID)
    for a in admins:
        if a.status == ChatMemberStatus.CREATOR and not a.user.is_bot:
            return a.user.id
    for a in admins:
        if not a.user.is_bot:
            return a.user.id
    return None


async def _make_set(bot: Bot, owner: int, name: str, title: str, items: list) -> list:
    """items: [(png_bytes, emoji_belgi)] -> custom_emoji_id lar ro'yxati."""
    try:
        await bot.delete_sticker_set(name)
    except TelegramBadRequest:
        pass
    await bot.create_new_sticker_set(
        user_id=owner, name=name, title=title[:64],
        stickers=[InputSticker(sticker=BufferedInputFile(png, f"e{i}.png"),
                               format="static", emoji_list=[em]) for i, (png, em) in enumerate(items)],
        sticker_type="custom_emoji",
    )
    st = await bot.get_sticker_set(name)
    return [s.custom_emoji_id for s in st.stickers]


# ======================================================================
# Holat rasmlari STATUS_TILES (3) bo'lakka bo'linadi va matnda yonma-yon yig'iladi (kattaroq ko'rinadi).
#   Rasm avval bo'sh chetlaridan kesiladi, 300x100 maydonga proportsiyani buzmay sig'diriladi,
#   so'ng 3 ta 100x100 bo'lakka kesiladi. Bo'sh (shaffof) bo'laklar tashlab yuboriladi.
#   Eng yaxshi natija uchun rasm keng (taxminan 3:1) bo'lsin; kvadrat rasm bitta bo'lak bo'ladi.
# ======================================================================
STATUS_TILES = 3


def make_tiles_png(img_bytes: bytes, n: int = STATUS_TILES):
    try:
        from PIL import Image
    except ImportError:
        log.warning("Pillow o'rnatilmagan: pip install pillow")
        return None
    try:
        im = Image.open(io.BytesIO(img_bytes)).convert("RGBA")
    except Exception:
        return None
    box = im.getchannel("A").getbbox()
    if box:
        im = im.crop(box)
    W, H = 100 * n, 100
    k = min(W / im.width, H / im.height)
    im = im.resize((max(1, int(im.width * k)), max(1, int(im.height * k))), Image.LANCZOS)
    canvas = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    canvas.paste(im, ((W - im.width) // 2, (H - im.height) // 2), im)
    tiles = []
    for i in range(n):
        t = canvas.crop((i * 100, 0, (i + 1) * 100, 100))
        if t.getchannel("A").getbbox() is None:      # butunlay bo'sh bo'lak
            continue
        out = io.BytesIO()
        t.save(out, "PNG")
        tiles.append(out.getvalue())
    return tiles or None


def fetch_status_tiles() -> dict:
    out = {}
    for key, url in STATUS_IMAGES.items():
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        tiles = make_tiles_png(r.content)
        if not tiles:
            raise ValueError(f"'{key}' rasmi o'qilmadi ({url})")
        out[key] = tiles
    return out


async def make_status_set(bot: Bot, owner: int, name: str) -> dict:
    """{'solved': [id, id, id], 'tried': [...], 'none': [...]}"""
    loop = asyncio.get_running_loop()
    tiles = await loop.run_in_executor(pool, fetch_status_tiles)
    keys = ["solved", "tried", "none"]
    items = [(png, ICON[k]) for k in keys for png in tiles[k]]
    ids = await _make_set(bot, owner, name, "Robocontest holatlari", items)
    out, i = {}, 0
    for k in keys:
        out[k] = ids[i:i + len(tiles[k])]
        i += len(tiles[k])
    return out


# ======================================================================
# Profil rasmlari BITTA umumiy packda (har bir o'quvchiga alohida pack ochilmaydi).
#   Pack to'lsa (AVATAR_PACK_MAX), keyingisi "avatars2_by_bot" ochiladi.
#   Rasm yangilansa eski emoji packdan o'chiriladi, yangisi qo'shiladi.
# ======================================================================
AVATAR_PACK_MAX = 190          # custom emoji packga sig'adigan soni 200 gacha, zaxira bilan
pack_lock = asyncio.Lock()


async def add_avatar_to_pack(bot: Bot, owner: int, u: dict, png: bytes) -> str:
    me = await bot.get_me()
    sticker = lambda: InputSticker(sticker=BufferedInputFile(png, "avatar.png"),
                                   format="static", emoji_list=["🙂"])
    async with pack_lock:
        packs = DB.setdefault("avatar_packs", [])
        owner = DB.get("avatar_owner", owner)
        if u.get("emoji_file"):                              # eski rasmini o'chiramiz
            try:
                await bot.delete_sticker_from_set(u["emoji_file"])
            except TelegramBadRequest:
                pass
        st = None
        if packs:
            try:
                st = await bot.get_sticker_set(packs[-1])
            except TelegramBadRequest:
                st = None
        if st is None or len(st.stickers) >= AVATAR_PACK_MAX:
            name = f"avatars{len(packs) + 1}_by_{me.username}"
            await bot.create_new_sticker_set(user_id=owner, name=name, title="Robocontest profillari",
                                             stickers=[sticker()], sticker_type="custom_emoji")
            packs.append(name)
            DB["avatar_owner"] = owner
        else:
            name = packs[-1]
            await bot.add_sticker_to_set(user_id=owner, name=name, sticker=sticker())
        new = (await bot.get_sticker_set(name)).stickers[-1]   # oxirgi qo'shilgani
        u["emoji_id"], u["emoji_file"] = new.custom_emoji_id, new.file_id
        async with db_lock:
            save_db()
        return new.custom_emoji_id


async def setup_avatar(bot: Bot, chat_id: str, tg_user):
    """Ro'yxatdan o'tganda: profil rasmi umumiy packga qo'shiladi (eski funksiyani almashtiradi)."""
    try:
        u = chat_data(chat_id)["users"].get(str(tg_user.id))
        if not u:
            return
        loop = asyncio.get_running_loop()
        png, why = await loop.run_in_executor(pool, fetch_avatar_png_ex, u["rb"])
        if not png:
            log.warning("Avatar emoji: %s — %s", u["rb"], why)
            return
        owner = await _emoji_owner(bot) or tg_user.id
        eid = await add_avatar_to_pack(bot, owner, u, png)
        await probe_emoji(bot, chat_id, eid)
        asyncio.create_task(refresh_chat(bot, str(chat_id)))
    except Exception:
        log.exception("avatar emoji xatosi")


# ======================================================================
# DARAJA NISHONLARI: Trainee / Junior / Middle / Senior / Architect — har birining o'z nishoni.
#   Nishon daraja nomi oldida turadi:  [nishon] Trainee 3 {14/100}
#   Rasm (500x100, 5 ta 100x100 nishon) kod ichiga joylangan; bot uni 5 ta custom emojiga ajratib,
#   bitta packga qo'shadi (avtomatik). Boshqa nishon kerak bo'lsa RANK_BADGES_B64 ni almashtiring.
# ======================================================================
import base64

TIER_NAMES = [t[0] for t in TIERS]        # Trainee, Junior, Middle, Senior, Architect
RANK_BADGES_B64 = """
@@RANK_BADGES_B64_BLOB@@
"""


def rank_badge_tiles():
    try:
        from PIL import Image
    except ImportError:
        log.warning("Pillow o'rnatilmagan: pip install pillow")
        return None
    im = Image.open(io.BytesIO(base64.b64decode("".join(RANK_BADGES_B64.split())))).convert("RGBA")
    out = []
    for i in range(len(TIER_NAMES)):
        buf = io.BytesIO()
        im.crop((i * 100, 0, (i + 1) * 100, 100)).save(buf, "PNG")
        out.append(buf.getvalue())
    return out


def rank_badge(label: str) -> str:
    """'Trainee 3' -> shu darajaning custom emoji nishoni (ruxsat yo'q bo'lsa bo'sh)."""
    ids = DB.get("rank_emoji")
    if not ids or not EMOJI_STATE["ok"]:
        return ""
    try:
        return f'<tg-emoji emoji-id="{ids[TIER_NAMES.index(label.split()[0])]}">🏅</tg-emoji> '
    except (ValueError, IndexError):
        return ""


async def make_rank_set(bot: Bot, owner: int, name: str) -> list:
    tiles = rank_badge_tiles()
    if not tiles:
        raise ValueError("daraja nishonlari o'qilmadi")
    return await _make_set(bot, owner, name, "Robocontest darajalari", [(t, "🏅") for t in tiles])


async def auto_emoji_once(bot: Bot):
    owner = await _emoji_owner(bot)
    if owner is None:
        return
    me = await bot.get_me()
    loop = asyncio.get_running_loop()
    changed, first_id = False, None
    se = DB.get("status_emoji")
    if not se or any(isinstance(v, str) for v in se.values()):    # yo'q yoki eski (bo'linmagan) ko'rinish
        try:
            ids = await make_status_set(bot, owner, f"st{owner}_by_{me.username}")
            async with db_lock:
                DB["status_emoji"] = ids
                save_db()
            changed, first_id = True, ids["solved"][0]
        except Exception:
            log.exception("holat rasmlari yaratilmadi")
    if not DB.get("rank_emoji"):                         # daraja nishonlari
        try:
            ids = await make_rank_set(bot, owner, f"rk{owner}_by_{me.username}")
            async with db_lock:
                DB["rank_emoji"] = ids
                save_db()
            changed, first_id = True, first_id or ids[0]
        except Exception:
            log.exception("daraja nishonlari yaratilmadi")
    users = chat_data(ALLOWED_CHAT_ID)["users"]
    for uid, u in list(users.items()):                   # profil rasmlari
        if u.get("emoji_id") or u.get("av_try", 0) >= 3:
            continue
        u["av_try"] = u.get("av_try", 0) + 1
        png, why = await loop.run_in_executor(pool, fetch_avatar_png_ex, u["rb"])
        if not png:
            log.warning("Avatar emoji: %s — %s", u["rb"], why)
            continue
        try:
            eid = await add_avatar_to_pack(bot, owner, u, png)
        except Exception:
            log.exception("avatar emoji yaratilmadi: %s", u["rb"])
            continue
        changed, first_id = True, first_id or eid
    if changed:
        EMOJI_STATE["ok"] = None
        await probe_emoji(bot, ALLOWED_CHAT_ID, first_id)
        await refresh_chat(bot, str(ALLOWED_CHAT_ID))
    async with db_lock:
        save_db()


async def auto_emoji(bot: Bot):
    await asyncio.sleep(5)
    while True:
        try:
            await auto_emoji_once(bot)
        except Exception:
            log.exception("auto emoji xatosi")
        await asyncio.sleep(600)


@dp.startup()
async def _auto_emoji_startup(bot: Bot):
    asyncio.create_task(auto_emoji(bot))


# ======================================================================
# KUN OXIRIDA JADVAL (har kuni DAILY_TIME da, Toshkent vaqti) va /kun — istalgan payt.
#   Har bir o'quvchi uchun: nechta masala berilgani, nechtasi ishlangani (✅), xato (🟡),
#   «Ishlolmadim» (🏳️), kun ichida olgan/yo'qotgan ballari, daraja nishoni va jami ball.
#   O'rinlar 🥇🥈🥉 va 4️⃣… emojilari bilan, daraja raqami rim raqamida (Ⅲ).
# ======================================================================
from datetime import datetime, timedelta, timezone

TZ = timezone(timedelta(hours=5))          # Asia/Tashkent
DAILY_TIME = (23, 30)                      # soat, daqiqa


def today_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d")


_KEYCAP = ["0️⃣", "1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣"]
_ROMAN = {"1": "Ⅰ", "2": "Ⅱ", "3": "Ⅲ", "4": "Ⅳ", "5": "Ⅴ"}


def place_emoji(n: int) -> str:
    if n <= 3:
        return ["🥇", "🥈", "🥉"][n - 1]
    return "".join(_KEYCAP[int(d)] for d in str(n))


def roman_label(label: str) -> str:
    """'Trainee 3' -> 'Trainee Ⅲ'"""
    name, _, num = label.partition(" ")
    return f"{name} {_ROMAN.get(num, num)}"


def _chunks(lines: list, limit: int = 3800) -> list:
    out, cur, size = [], [], 0
    for ln in lines:
        if size + len(ln) + 1 > limit and cur:
            out.append("\n".join(cur))
            cur, size = [], 0
        cur.append(ln)
        size += len(ln) + 1
    if cur:
        out.append("\n".join(cur))
    return out


def daily_report(chat_id, day: str):
    """Kunlik hisobot matnlari ro'yxati; masala yoki o'quvchi bo'lmasa None."""
    c = chat_data(chat_id)
    users = c["users"]
    tasks = [t for t in c["tasks"] if t.get("day") == day]
    if not users or not tasks:
        return None
    codes = list(dict.fromkeys(t["code"].upper() for t in tasks))
    dsc = chat_scores(chat_id, day=day, floor=False)    # kun ichidagi o'zgarish
    tot = chat_scores(chat_id)                          # jami ball (daraja uchun)
    order = sorted(users, key=lambda i: (-dsc[i]["pts"], -dsc[i]["solved"], users[i]["name"].lower()))
    gain_all = sum(1 for i in users if dsc[i]["solved"])
    d = datetime.strptime(day, "%Y-%m-%d").strftime("%d.%m.%Y")
    lines = [f"📊 <b>Kunlik natijalar — {d}</b>",
             f"📝 Berilgan masalalar: <b>{len(codes)}</b> ta · 👥 ro'yxatda: <b>{len(users)}</b> · "
             f"kamida bittasini ishlagan: <b>{gain_all}</b>", ""]
    for n, i in enumerate(order, 1):
        s, u = dsc[i], users[i]
        label = rank_of(tot[i]["pts"])[0]
        plus = sum(p for _, p in s["rows"] if p > 0)
        minus = -sum(p for _, p in s["rows"] if p < 0)
        net = s["pts"]
        lines.append(f"{place_emoji(n)} {rank_badge(label)}<b>{escape(u['name'])}</b> "
                     f"(<code>{escape(u['rb'])}</code>) — <i>{roman_label(label)}</i>")
        left = len(codes) - s["solved"] - s["tried"]
        lines.append(f"      📝 berildi {len(codes)} · ✅ {s['solved']} · 🟡 {s['tried'] - s['gave']} · "
                     f"🏳️ {s['gave']} · ❌ {max(0, left)}")
        lines.append(f"      🎯 +{plus} −{minus} = <b>{net:+d}</b> ball · jami {tot[i]['pts']}")
    lines.append("")
    lines.append("📌 <b>Masalalar</b>")
    for code in codes[:12]:
        ts = [t for t in tasks if t["code"].upper() == code]
        solved = sum(1 for i in users if any((t.get("res") or {}).get(i) == "solved" for t in ts))
        lines.append(f"• {BASE}/tasks/{escape(code)} — ✅ {solved}/{len(users)}")
    if len(codes) > 12:
        lines.append(f"… va yana {len(codes) - 12} ta")
    lines.append("")
    lines.append("✅ ishlagan · 🟡 xato · 🏳️ ishlolmadim · ❌ urinmagan")
    return _chunks(lines)


async def send_daily(bot: Bot, chat_id, day: str) -> bool:
    parts = daily_report(chat_id, day)
    if not parts:
        return False
    for p in parts:
        await bot.send_message(chat_id, p, disable_web_page_preview=True)
    return True


@dp.message(Command("kun"), F.chat.type.in_(_GROUPS))
async def cmd_kun(msg: Message, bot: Bot):
    if not await send_daily(bot, msg.chat.id, today_str()):
        await msg.reply("Bugun hali masala berilmagan yoki hech kim ro'yxatdan o'tmagan.")


async def daily_loop(bot: Bot):
    await asyncio.sleep(10)
    while True:
        try:
            now = datetime.now(TZ)
            day = now.strftime("%Y-%m-%d")
            if (now.hour, now.minute) >= DAILY_TIME and DB.get("last_report") != day:
                await refresh_chat(bot, str(ALLOWED_CHAT_ID))     # eng so'nggi natijalar
                await send_daily(bot, ALLOWED_CHAT_ID, day)
                async with db_lock:
                    DB["last_report"] = day
                    save_db()
        except Exception:
            log.exception("kunlik hisobot xatosi")
        await asyncio.sleep(30)


@dp.startup()
async def _daily_startup(bot: Bot):
    asyncio.create_task(daily_loop(bot))


# ======================================================================
# GURUH KO'CHGANI (supergroup) uchun bazadagi eski kalitni yangisiga o'tkazish
# ======================================================================
_OLD_CHAT_ID = "-5592507893"
if _OLD_CHAT_ID in DB["chats"] and str(ALLOWED_CHAT_ID) not in DB["chats"]:
    DB["chats"][str(ALLOWED_CHAT_ID)] = DB["chats"].pop(_OLD_CHAT_ID)
    save_db()


# ======================================================================
# JADVAL (Telegram Rich Message, Bot API 10.1) + "➡️ Keyingisi" ovozi
#   1) Masala xabari endi tartiblangan jadval: № | Holat | O'quvchi | Daraja | Ball
#   2) "➡️ Keyingisi" tugmasi: ro'yxatdagilarning yarmi bossa bot AVTOMATIK:
#        - ishlamaganlarni (🟡/❌/❔) «ishlolmadi» deb belgilaydi (jarima = «Ishlolmadim» bilan bir xil),
#        - rank/ballni qayta hisoblaydi va natija jadvalini yuboradi,
#        - xuddi shu qiyinlikda keyingi masalani tashlaydi.
#   Rich Message ishlamasa, avvalgi (oddiy matn) ko'rinishga o'zi qaytadi.
# ======================================================================
RICH = {"ok": False if os.getenv("RICH_TABLE") == "0" else None}   # None: jadval ko'rinishi (standart) | RICH_TABLE=0 — oddiy matnli ko'rinish
_rich_busy: set = set()      # bir masalani bir vaqtda ikki marta yangilamaslik uchun


def _tg_raw(method: str, payload: dict) -> dict:
    try:
        r = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{method}", json=payload, timeout=30)
        return r.json()
    except (requests.RequestException, ValueError) as e:
        return {"ok": False, "description": f"{type(e).__name__}: {e}"}


def _rich_fail(r: dict):
    log.warning("Rich message xato: %s", r.get("description"))
    if r.get("error_code") == 404:           # metod yo'q -> eski usulga qaytamiz
        RICH["ok"] = False


def _rt_icon(st: str) -> str:
    sid = (DB.get("status_emoji") or {}).get(st)
    if not sid:
        return ICON[st]
    ids = sid if isinstance(sid, list) else [sid]
    return "".join(f'<tg-emoji emoji-id="{x}">{ICON[st]}</tg-emoji>' for x in ids)


def _need_votes(users: dict) -> int:
    return max(1, (len(users) + 1) // 2)      # ro'yxatning yarmi (yuqoriga yaxlitlanadi)


def rich_html(task: dict, users: dict, results: dict, fancy: bool = True) -> str:
    code = task["code"]
    cid = _chat_id_of(users)
    sc = chat_scores(cid) if cid is not None else {}
    meta = task.get("meta") or {}
    reward, pen = task_points(task)
    give_pen = max(pen, round(reward * GIVEUP_RATIO))
    dn = norm_diff(meta.get("diff"))
    shown = {"easy": "Oson", "medium": "O'rtacha", "hard": "Qiyin", "expert": "Expert"}.get(dn) \
        or (str(meta["diff"]) if meta.get("diff") else None)
    if shown:
        rate = meta.get("rate")
        dtxt = escape(shown) + (f" · {rate:g}%" if isinstance(rate, (int, float)) else "")
    else:
        dtxt = "qiyinlik aniqlanmadi (o'rtacha deb olindi)"
    solved = sum(1 for i in users if results.get(i) == "solved")
    gave = task.get("gave") or {}
    votes = task.get("next_votes") or {}
    have = sum(1 for uid in votes if uid in users)
    need = _need_votes(users)

    out = [
        f'<h3>📝 Masala: <a href="{BASE}/tasks/{escape(code)}">{escape(code)}</a></h3>',
        f"<p>🎯 {dtxt} · ishlasa <b>+{reward}</b> · xato <b>−{pen}</b> · ishlolmasa <b>−{give_pen}</b> ball</p>",
        f"<p>👥 Ro'yxatda: <b>{len(users)}</b> | ✅ Ishladi: <b>{solved}</b></p>",
    ]
    if not users:
        out.append("<p>Hali hech kim ro'yxatdan o'tmagan. Pastdagi tugmani bosing 👇</p>")
    else:
        rows = sorted(users, key=lambda i: (ORDER.get(results.get(i, "error"), 9),
                                            -sc.get(i, {"pts": 0})["pts"], users[i]["name"].lower()))
        out.append("<table bordered striped>")
        out.append("<tr><th>№</th><th>Holat</th><th>O'quvchi</th><th>Daraja</th><th>Ball</th></tr>")
        for n, i in enumerate(rows, 1):
            st = results.get(i, "error")
            u = users[i]
            label, cur, width, _ = rank_of(sc.get(i, {"pts": 0})["pts"])
            prog = "MAX" if width is None else f"{cur}/{width}"
            eid = u.get("emoji_id")
            av = f'<tg-emoji emoji-id="{eid}">🙂</tg-emoji> ' if (fancy and eid) else ""
            flag = " 🏳️" if gave.get(i) and st != "solved" else ""
            badge = rank_badge(label) if fancy else ""
            icon = _rt_icon(st) if fancy else ICON[st]
            out.append(
                f"<tr><td>{n}</td><td>{icon}{flag}</td>"
                f"<td>{av}{escape(u['name'])} (<code>{escape(u['rb'])}</code>)</td>"
                f"<td>{badge}{escape(label)}</td><td>{prog}</td></tr>")
        out.append("</table>")
        out.append("<p>✅ ishlagan · 🟡 urinib ko'rgan · ❌ ishlamagan · 🏳️ ishlolmadi · ❔ topilmadi</p>")
        if task.get("advanced"):
            out.append("<p>➡️ <b>Ovoz bilan keyingi masalaga o'tildi.</b></p>")
        else:
            out.append(f"<p>➡️ Keyingisiga ovoz: <b>{have}/{need}</b> — ro'yxatning yarmi rozi bo'lsa "
                       "avtomatik o'tiladi, ishlamaganlar «ishlolmadi» hisoblanadi.</p>")
    return "\n".join(out)


async def rich_send(bot: Bot, chat_id, task: dict, users: dict, results: dict):
    """Yangi rich xabar yuboradi, message_id qaytaradi (xato bo'lsa None)."""
    loop = asyncio.get_running_loop()
    me = await bot.get_me()
    payload = {"chat_id": int(chat_id),
               "reply_markup": reg_keyboard(me.username, chat_id, task["code"]).model_dump(exclude_none=True)}
    for fancy in ([True, False] if EMOJI_STATE["ok"] else [False]):
        payload["rich_message"] = {"html": rich_html(task, users, results, fancy)}
        r = await loop.run_in_executor(pool, _tg_raw, "sendRichMessage", payload)
        if r.get("ok"):
            RICH["ok"] = True
            return r["result"]["message_id"]
        _rich_fail(r)
    return None


async def rich_edit(bot: Bot, chat_id, task: dict, users: dict, results: dict) -> bool:
    loop = asyncio.get_running_loop()
    me = await bot.get_me()
    payload = {"chat_id": int(chat_id), "message_id": task["msg_id"],
               "reply_markup": reg_keyboard(me.username, chat_id, task["code"]).model_dump(exclude_none=True)}
    for fancy in ([True, False] if EMOJI_STATE["ok"] else [False]):
        html = rich_html(task, users, results, fancy)
        payload["rich_message"] = {"html": html}
        r = await loop.run_in_executor(pool, _tg_raw, "editMessageText", payload)
        if r.get("ok") or "not modified" in str(r.get("description", "")):
            task["last_rich"] = html
            RICH["ok"] = True
            return True
        _rich_fail(r)
    return False


# --- tugmalar: "➡️ Keyingisi" qo'shiladi
_prev_reg_keyboard = reg_keyboard


def reg_keyboard(bot_username: str, chat_id, code=None) -> InlineKeyboardMarkup:
    kb = _prev_reg_keyboard(bot_username, chat_id, code)
    if code:
        kb.inline_keyboard.append([InlineKeyboardButton(text="➡️ Keyingisi", callback_data=f"nx:{code}")])
    return kb


# --- yangilash: jadval ko'rinishida
_pre_rich_refresh_task = refresh_task


async def refresh_task(bot: Bot, chat_id: str, task: dict):
    legacy = RICH["ok"] is False or (not task.get("rich") and task.get("day") not in (None, today_str()))
    if legacy:                                            # eski kunlar / rich ishlamasa: avvalgi usul
        return await _pre_rich_refresh_task(bot, chat_id, task)
    if id(task) in _rich_busy:
        return
    _rich_busy.add(id(task))
    try:
        meta = task.setdefault("meta", {})
        if (meta.get("diff") is None or meta.get("rate") is None) and task.get("meta_try", 0) < 4:
            task["meta_try"] = task.get("meta_try", 0) + 1
            loop = asyncio.get_running_loop()
            got = await loop.run_in_executor(pool, fetch_task_meta, task["code"])
            for k in ("diff", "rate"):
                if got and meta.get(k) is None and got.get(k) is not None:
                    meta[k] = got[k]
        users = chat_data(chat_id)["users"]
        results = await check_all(users, task["code"]) if users else {}
        render(task, users, results)                      # task["res"], kun va qiyinlikni saqlaydi (ball uchun)
        if not task.get("rich"):                          # eski oddiy xabar -> bir marta rich xabarga almashtiriladi
            new_id = await rich_send(bot, chat_id, task, users, results)
            if new_id is None:
                return await _pre_rich_refresh_task(bot, chat_id, task)
            old = task.get("msg_id")
            task["msg_id"], task["rich"] = new_id, True
            task["last_rich"] = rich_html(task, users, results, bool(EMOJI_STATE["ok"]))
            try:
                await bot.delete_message(int(chat_id), old)
            except Exception:
                pass
            return
        if rich_html(task, users, results, bool(EMOJI_STATE["ok"])) == task.get("last_rich"):
            return
        await rich_edit(bot, chat_id, task, users, results)
    finally:
        _rich_busy.discard(id(task))


# --- yangi masala xabari ham rich jadval bilan yuboriladi
_pre_rich_post_tracker = post_tracker


async def post_tracker(bot: Bot, chat_id, code: str, diff=None):
    if RICH["ok"] is False:
        return await _pre_rich_post_tracker(bot, chat_id, code, diff)
    task = {"code": code, "active": True}
    if diff:
        task["meta"] = {"diff": diff}
    users = chat_data(chat_id)["users"]
    render(task, users, {})
    mid = await rich_send(bot, chat_id, task, users, {})
    if mid is None:
        return await _pre_rich_post_tracker(bot, chat_id, code, diff)
    task["msg_id"], task["rich"] = mid, True
    async with db_lock:
        chat_data(chat_id)["tasks"].append(task)
        save_db()
    asyncio.create_task(refresh_task(bot, str(chat_id), task))


# --- "➡️ Keyingisi" ovozi
async def _advance_summary(bot: Bot, chat_id, task: dict):
    cid = str(chat_id)
    c = chat_data(cid)
    users = c["users"]
    loop = asyncio.get_running_loop()
    results = await check_all(users, task["code"])
    before = chat_scores(cid)
    res = task.setdefault("res", {})
    gave = task.setdefault("gave", {})
    auto = set()
    for uid, st in results.items():
        if st in ("solved", "tried", "none"):
            res[uid] = st
        if st in ("tried", "none", "unknown") and res.get(uid) != "solved" and not gave.get(uid):
            gave[uid] = True                              # avtomatik «Ishlolmadim»
            auto.add(uid)
    async with db_lock:
        save_db()
    after = chat_scores(cid)

    order = sorted(users, key=lambda i: (-after[i]["pts"], users[i]["name"].lower()))
    rows = []
    for n, i in enumerate(order, 1):
        st = results.get(i)
        if res.get(i) == "solved" or st == "solved":
            stat = "✅ ishladi"
        elif i in auto:
            stat = "🏳️ ishlolmadi (avto)"
        elif gave.get(i):
            stat = "🏳️ ishlolmadi"
        else:
            stat = "⚠️ tekshirilmadi"
        d = after[i]["pts"] - before[i]["pts"]
        lb, la = rank_of(before[i]["pts"])[0], rank_of(after[i]["pts"])[0]
        lv = escape(la) if lb == la else f"{escape(lb)} → {escape(la)}"
        rows.append(f"<tr><td>{n}</td><td>{escape(users[i]['name'])}</td><td>{stat}</td>"
                    f"<td>{d:+d}</td><td>{lv}</td></tr>")
    html = (f"<h3>➡️ Keyingi masalaga o'tildi</h3>"
            f"<p>Ro'yxatning yarmi rozi bo'ldi. <a href=\"{BASE}/tasks/{escape(task['code'])}\">{escape(task['code'])}</a> "
            f"bo'yicha ishlamaganlar avtomatik «ishlolmadi» deb hisoblandi.</p>"
            "<table bordered striped><tr><th>№</th><th>O'quvchi</th><th>Natija</th><th>Ball</th><th>Daraja</th></tr>"
            + "".join(rows) + "</table>")
    r = {"ok": False} if RICH["ok"] is False else await loop.run_in_executor(
        pool, _tg_raw, "sendRichMessage", {"chat_id": int(chat_id), "rich_message": {"html": html}})
    if not r.get("ok"):
        _rich_fail(r)
        lines = [f"➡️ <b>Keyingi masalaga o'tildi</b> ({escape(task['code'])})", ""]
        for n, i in enumerate(order, 1):
            lines.append(f"{n}. {escape(users[i]['name'])} — {after[i]['pts'] - before[i]['pts']:+d} ball · "
                         f"{escape(rank_of(after[i]['pts'])[0])}")
        await bot.send_message(chat_id, "\n".join(lines))


async def _post_next(bot: Bot, chat_id, task: dict) -> bool:
    """Boshqa (avval berilmagan) masalani topib, guruhga tashlaydi. Muvaffaqiyatli bo'lsa True."""
    c = chat_data(str(chat_id))
    loop = asyncio.get_running_loop()
    diff = norm_diff((task.get("meta") or {}).get("diff")) or None
    exclude = set()
    for t in list(c["tasks"]) + [task]:
        exclude.update({t["code"], t["code"].upper()})
    code = None
    for attempt in range(3):
        codes = []
        try:
            FETCH_ERR["last"] = None
            codes, _fb, _notes = await loop.run_in_executor(pool, build_set, diff, None, 1, exclude)
            codes = [x for x in codes if x not in exclude and x.upper() not in exclude]
            if not codes:                                 # ro'yxat sahifasi ishlamasa: tasodifiy kodni o'zi topadi
                codes = await loop.run_in_executor(pool, random_verified, 1, exclude)
        except Exception:
            log.exception("Keyingi masalani qidirishda xato")
        if codes:
            code = codes[0]
            break
        await asyncio.sleep(1.5)
    if not code:                                          # hech narsa topilmadi/tekshirib bo'lmadi -> tasodifiy kod (tekshirilmagan)
        for _ in range(50):
            cand = _rand_code()
            if cand not in exclude and cand.upper() not in exclude:
                code = cand
                log.warning("Masala topilmadi, tekshirilmagan tasodifiy kod berildi: %s (%s)", code, FETCH_ERR.get("last"))
                break
    if not code:
        why =f" ({FETCH_ERR['last']})" if FETCH_ERR.get("last") else ""
        try:
            await bot.send_message(chat_id, f"😕 Keyingi masala topilmadi{why}. Qayta «➡️ Keyingisi» ni bosing yoki /gorobo ishlating.")
        except Exception:
            log.exception("Xabar yuborilmadi")
        return False
    try:
        await post_tracker(bot, chat_id, code, diff)
        return True
    except Exception:
        log.exception("Rich post_tracker xato, oddiy usul ishlatiladi")
    try:
        await _pre_rich_post_tracker(bot, chat_id, code, diff)
        return True
    except Exception:
        log.exception("Oddiy post_tracker ham xato")
    try:
        await bot.send_message(chat_id, f"{BASE}/tasks/{code}")
        return True
    except Exception:
        log.exception("Masala havolasi yuborilmadi")
        return False


async def advance_task(bot: Bot, chat_id, task: dict):
    try:
        await _advance_summary(bot, chat_id, task)        # natijalar jadvali; xato bo'lsa ham keyingisi tashlanadi
    except Exception:
        log.exception("Natijalar jadvali tuzilmadi")
    ok = await _post_next(bot, chat_id, task)
    if not ok:                                            # masala topilmadi -> qayta ovoz berish mumkin bo'lsin
        task["advanced"] = False
        task["next_votes"] = {}
    asyncio.create_task(refresh_task(bot, str(chat_id), task))


@dp.callback_query(F.data.startswith("nx:"))
async def next_cb(cb: CallbackQuery, bot: Bot):
    code = cb.data[3:]
    chat_id = cb.message.chat.id
    uid = str(cb.from_user.id)
    c = chat_data(chat_id)
    if uid not in c["users"]:
        return await cb.answer("Avval ro'yxatdan o'ting: /reg username", show_alert=True)
    tasks = [t for t in c["tasks"] if t["code"].upper() == code.upper()]
    task = next((t for t in tasks if t.get("msg_id") == cb.message.message_id), None) \
        or (tasks[-1] if tasks else None)
    if not task:
        return await cb.answer("Bu masala topilmadi.", show_alert=True)
    if task.get("advanced"):
        return await cb.answer("Keyingi masalaga allaqachon o'tilgan.", show_alert=True)
    votes = task.setdefault("next_votes", {})
    if votes.get(uid):
        votes.pop(uid, None)                              # ovozni qaytarib olish
        msg = "Ovoz qaytarib olindi"
    else:
        votes[uid] = True
        msg = "Ovoz qabul qilindi ✅"
    have = sum(1 for u in votes if u in c["users"])
    need = _need_votes(c["users"])
    if have >= need and not task.get("advanced"):
        task["advanced"] = True                           # (await yo'q) ikki marta o'tib ketmasligi uchun
        await cb.answer(f"Yetarli ovoz ({have}/{need}). Keyingi masalaga o'tilmoqda…")
        return await advance_task(bot, chat_id, task)
    await cb.answer(f"{msg}: {have}/{need}")
    asyncio.create_task(refresh_task(bot, str(chat_id), task))


# ======================================================================
# MAVZULI (forum) GURUH: bot xabarlarini buyruq berilgan mavzuga yuboradi.
#   Aks holda xabar yopiq «General» mavzusiga tushib, TOPIC_CLOSED xatosi chiqadi.
#   Mavzu oxirgi buyruq (/gorobo, /task, /kun ...) yoki tugma bosilgan 
