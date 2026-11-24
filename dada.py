import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from threading import Thread
from urllib.parse import urlparse

from flask import Flask, request
from splusthon import SoroushClient
from splusthon.sessions import StringSession

# ================= تنظیمات اصلی =================
SESSION_FILE = "soroush_session.txt"
SPAM_JOBS_FILE = "spam_jobs.json"
FWD_JOBS_FILE = "forward_jobs.json"
PORT = int(os.environ.get("PORT", 8080))
PANEL_PATH = os.environ.get("PANEL_PATH", "/my-panel")
if not PANEL_PATH.startswith("/"):
    PANEL_PATH = "/" + PANEL_PATH

for _name in ("splusthon", "asyncio"):
    _lg = logging.getLogger(_name)
    _lg.setLevel(logging.CRITICAL)
    _lg.propagate = False

spam_jobs = []
forward_jobs = []
spam_seq = 0
fwd_seq = 0

state = {
    "status": "در حال راه‌اندازی...",
    "ping": "در حال بررسی...",
    "auth_status": "disconnected",  # disconnected | code_sent | connected
    "auth_msg": "شماره تلفن را وارد کنید (مثال: +98912...)",
    "phone": "",
}

client = SoroushClient(StringSession())
group_list = []
bot_loop = None
phone_code_hash = None
login_in_progress = False
client_ready = False
connect_lock = asyncio.Lock()
io_lock = asyncio.Lock()


def now_clock() -> str:
    return datetime.now().strftime("%H:%M:%S")


def is_auth_key_error(err) -> bool:
    s = str(err or "").lower()
    return (
        "authkeyunregistered" in s
        or "key is not registered" in s
        or "auth_key_unregistered" in s
    )


def clear_session():
    if os.path.exists(SESSION_FILE):
        try:
            os.remove(SESSION_FILE)
        except Exception:
            pass


def load_session() -> str:
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            pass
    return ""


def save_session(val: str):
    try:
        with open(SESSION_FILE, "w", encoding="utf-8") as f:
            f.write(val or "")
    except Exception:
        pass


def load_all_jobs():
    global spam_jobs, forward_jobs, spam_seq, fwd_seq

    if os.path.exists(SPAM_JOBS_FILE):
        try:
            with open(SPAM_JOBS_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
                if isinstance(raw, list):
                    spam_jobs = raw
                    for j in spam_jobs:
                        j["idx"] = int(j.get("idx") or 0)
                        j["sent"] = int(j.get("sent") or 0)
                        j["max_count"] = int(j.get("max_count") or 0)
                        j["speed"] = max(0.05, float(j.get("speed") or 1.0))
                        if j.get("running"):
                            j["last_status"] = "بازیابی تسک و ادامه..."
                            j["last_err"] = ""
                    spam_seq = len(spam_jobs)
        except Exception:
            pass

    if os.path.exists(FWD_JOBS_FILE):
        try:
            with open(FWD_JOBS_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
                if isinstance(raw, list):
                    forward_jobs = raw
                    for j in forward_jobs:
                        j["sent"] = int(j.get("sent") or 0)
                        j["max_count"] = int(j.get("max_count") or 0)
                        j["speed"] = max(0.05, float(j.get("speed") or 1.0))
                        if j.get("running"):
                            j["last_status"] = "بازیابی فوروارد و ادامه..."
                            j["last_err"] = ""
                    fwd_seq = len(forward_jobs)
        except Exception:
            pass


def save_spam_jobs():
    try:
        with open(SPAM_JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(spam_jobs, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def save_fwd_jobs():
    try:
        with open(FWD_JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(forward_jobs, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def parse_target(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    if "splus.ir" in s or s.startswith("http://") or s.startswith("https://"):
        url_s = s if s.startswith("http") else "https://" + s
        try:
            parsed = urlparse(url_s)
            parts = [p for p in parsed.path.split("/") if p]
            if parts:
                if len(parts) >= 2 and parts[-1].isdigit():
                    if parts[0] == "c" and len(parts) >= 2 and parts[1].lstrip("-").isdigit():
                        cid = parts[1]
                        return f"-100{cid}" if not cid.startswith("-") else cid
                    return parts[-2]
                return parts[0]
        except Exception:
            pass
    m = re.search(r"(?:https?://)?(?:www\.)?splus\.ir/([^/\s?#]+)", s)
    if m:
        return m.group(1).strip()
    if s.startswith("@"):
        return s[1:].strip()
    return s


def to_peer(target: str):
    t = parse_target(target)
    return int(t) if t.lstrip("-").isdigit() else t


def resolve_name(target: str) -> str:
    t = str(target).strip()
    for g in group_list:
        if str(g.get("id")) == t or str(g.get("name")) == t:
            return g.get("name") or t
    return t


def parse_texts(encoded: str) -> list:
    if not encoded:
        return []
    txt = encoded.replace("_NEWLINE_", "\n")
    try:
        arr = json.loads(txt)
        if isinstance(arr, list):
            items = [str(x).strip() for x in arr if str(x).strip()]
            if items:
                return items
    except Exception:
        pass
    return [txt.strip()] if txt.strip() else []


def _extract_account_name(me) -> str:
    if not me:
        return "اکانت"
    fn = (getattr(me, "first_name", None) or "").strip()
    ln = (getattr(me, "last_name", None) or "").strip()
    un = (getattr(me, "username", None) or "").strip()
    full = f"{fn} {ln}".strip()
    if full:
        return full
    if un:
        return f"@{un}"
    uid = getattr(me, "id", None)
    return str(uid) if uid is not None else "اکانت"


def _extract_dialog_info(d):
    ent = getattr(d, "entity", None)
    did = getattr(d, "id", None) or getattr(ent, "id", None)
    if did is None:
        return None
    name = (
        getattr(d, "name", None)
        or getattr(d, "title", None)
        or getattr(ent, "title", None)
        or getattr(ent, "name", None)
        or getattr(ent, "first_name", None)
        or str(did)
    )
    is_grp = bool(
        getattr(d, "is_group", False)
        or getattr(ent, "megagroup", False)
        or getattr(ent, "gigagroup", False)
    )
    is_chn = bool(getattr(d, "is_channel", False) or getattr(ent, "broadcast", False))
    dtype = "کانال" if is_chn else ("گروه" if is_grp else "چت")
    return {"id": str(did), "name": str(name), "type": dtype, "is_gc": bool(is_grp or is_chn)}


def _extract_msg_text(m) -> str:
    for attr in ("text", "message", "raw_text", "caption", "body", "content"):
        val = getattr(m, attr, None)
        if isinstance(val, str) and val.strip():
            return val.strip()
    if isinstance(m, dict):
        for k in ("text", "message", "raw_text", "caption", "body"):
            if m.get(k):
                return str(m[k]).strip()
    if getattr(m, "media", None):
        return "[پیام مدیا / فایل]"
    return ""


def _extract_msg_id(m, fallback_idx: int) -> int:
    for attr in ("id", "message_id", "msg_id"):
        val = getattr(m, attr, None)
        if val is not None:
            try:
                return int(val)
            except Exception:
                pass
    if isinstance(m, dict):
        for k in ("id", "message_id", "msg_id"):
            if m.get(k) is not None:
                try:
                    return int(m[k])
                except Exception:
                    pass
    return fallback_idx


load_all_jobs()


# ================= مدیریت اتصال پایدار =================
async def _close_client_safely(c):
    if c is None:
        return
    for attr in ("_client", "client", "_session_http", "http_session", "_sender", "_updates"):
        sub = getattr(c, attr, None)
        if sub is not None and hasattr(sub, "close"):
            try:
                res = sub.close()
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                pass
    if hasattr(c, "disconnect"):
        try:
            res = c.disconnect()
            if asyncio.iscoroutine(res):
                await res
        except Exception:
            pass


async def _reset_invalid_session(msg: str = "سشن معتبر نیست؛ شماره و کد جدید وارد کنید."):
    global client, client_ready, group_list
    client_ready = False
    group_list = []
    clear_session()
    await _close_client_safely(client)
    client = SoroushClient(StringSession())
    state["ping"] = "قطع"
    if state.get("auth_status") != "code_sent":
        state["auth_status"] = "disconnected"
    state["auth_msg"] = msg
    state["status"] = msg


def _extract_code_hash(sent_obj, phone: str):
    if sent_obj is not None:
        h = getattr(sent_obj, "phone_code_hash", None)
        if h:
            return str(h)
        if isinstance(sent_obj, dict) and sent_obj.get("phone_code_hash"):
            return str(sent_obj["phone_code_hash"])
    for attr in ("_phone_code_hash", "phone_code_hash", "code_hash"):
        val = getattr(client, attr, None)
        if isinstance(val, dict) and phone in val:
            return str(val[phone])
        if isinstance(val, str) and val:
            return val
    return None


async def ensure_connected(force_rebuild: bool = False) -> bool:
    global client, client_ready
    if login_in_progress:
        return False

    sess = load_session()
    if not sess:
        client_ready = False
        state["ping"] = "قطع"
        if state.get("auth_status") != "code_sent":
            state["auth_status"] = "disconnected"
            state["auth_msg"] = "سشن وجود ندارد؛ شماره را وارد کنید."
        return False

    async with connect_lock:
        if not force_rebuild and client_ready:
            try:
                t0 = time.perf_counter()
                me = await asyncio.wait_for(client.get_me(), timeout=7)
                ms = int((time.perf_counter() - t0) * 1000)
                acc_name = _extract_account_name(me)
                state["ping"] = f"{ms} ms"
                state["auth_status"] = "connected"
                state["auth_msg"] = f"متصل و فعال: {acc_name}"
                state["status"] = f"{acc_name} آنلاین و در حال اجراست."
                return True
            except Exception as e:
                if is_auth_key_error(e):
                    await _reset_invalid_session("سشن معتبر نیست؛ شماره و کد جدید وارد کنید.")
                    return False

        try:
            await _close_client_safely(client)
            client = SoroushClient(StringSession(sess))
            t0 = time.perf_counter()
            await asyncio.wait_for(client.connect(), timeout=16)
            me = await asyncio.wait_for(client.get_me(), timeout=12)
            ms = int((time.perf_counter() - t0) * 1000)
            acc_name = _extract_account_name(me)

            client_ready = True
            state["ping"] = f"{ms} ms"
            state["auth_status"] = "connected"
            state["auth_msg"] = f"متصل و فعال: {acc_name}"
            state["status"] = f"{acc_name} آنلاین و در حال اجراست."
            return True
        except Exception as e:
            if is_auth_key_error(e):
                await _reset_invalid_session("سشن معتبر نیست؛ شماره و کد جدید وارد کنید.")
                return False
            client_ready = False
            state["ping"] = "اختلال لحظه‌ای"
            state["status"] = f"در حال حفظ اتصال و تلاش مجدد: {e}"
            return False


async def _do_send_code(phone: str):
    global client, phone_code_hash, login_in_progress, client_ready
    login_in_progress = True
    client_ready = False
    phone_code_hash = None
    clear_session()
    state["phone"] = phone
    state["auth_status"] = "code_sent"
    state["auth_msg"] = "در حال ارسال کد..."
    try:
        async with connect_lock:
            await _close_client_safely(client)
            client = SoroushClient(StringSession())
            await asyncio.wait_for(client.connect(), timeout=20)

            sent_res = None
            if hasattr(client, "send_code_request"):
                sent_res = await asyncio.wait_for(client.send_code_request(phone), timeout=20)
            elif hasattr(client, "send_code"):
                sent_res = await asyncio.wait_for(client.send_code(phone), timeout=20)
            else:
                sent_res = await asyncio.wait_for(client.sign_in(phone=phone), timeout=20)

            phone_code_hash = _extract_code_hash(sent_res, phone)
        state["auth_status"] = "code_sent"
        state["auth_msg"] = "کد ارسال شد؛ کد را وارد کنید."
    except Exception as e:
        state["auth_status"] = "code_sent"
        state["auth_msg"] = f"خطا در ارسال کد: {e}"


async def _do_verify_code(code: str):
    global phone_code_hash, login_in_progress, client_ready
    state["auth_status"] = "code_sent"
    state["auth_msg"] = "در حال تأیید کد..."
    phone = state.get("phone", "").strip()
    try:
        async with connect_lock:
            if not hasattr(client, "sign_in"):
                raise RuntimeError("متد sign_in در کلاینت موجود نیست.")

            signed = False
            last_exc = None

            if phone_code_hash:
                for call in (
                    lambda: client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash),
                    lambda: client.sign_in(phone, code, phone_code_hash),
                ):
                    try:
                        await asyncio.wait_for(call(), timeout=20)
                        signed = True
                        break
                    except TypeError:
                        continue
                    except Exception as e:
                        last_exc = e

            if not signed:
                try:
                    await asyncio.wait_for(client.sign_in(phone=phone, code=code), timeout=20)
                    signed = True
                except Exception as e:
                    last_exc = e

            if not signed:
                raise RuntimeError(str(last_exc or "ورود ناموفق بود."))

            sess = client.session.save() if getattr(client, "session", None) else ""
            if not sess:
                raise RuntimeError("سشن ذخیره نشد.")

            save_session(sess)
            phone_code_hash = None
            login_in_progress = False
            client_ready = True

            acc_name = "اکانت"
            try:
                t0 = time.perf_counter()
                me = await asyncio.wait_for(client.get_me(), timeout=10)
                ms = int((time.perf_counter() - t0) * 1000)
                acc_name = _extract_account_name(me)
                state["ping"] = f"{ms} ms"
            except Exception:
                pass

            state["auth_status"] = "connected"
            state["auth_msg"] = f"متصل و فعال: {acc_name}"
            state["status"] = f"{acc_name} آنلاین و در حال اجراست."

        await refresh_groups()
    except Exception as e:
        state["auth_status"] = "code_sent"
        state["auth_msg"] = f"خطا در کد: {e}"


async def refresh_groups():
    global group_list
    ok = await ensure_connected(force_rebuild=False)
    if not ok:
        return group_list

    async with connect_lock:
        try:
            raw_dialogs = []
            if hasattr(client, "get_dialogs"):
                res = await asyncio.wait_for(client.get_dialogs(limit=200), timeout=15)
                raw_dialogs = list(res) if res else []
            elif hasattr(client, "iter_dialogs"):
                async for d in client.iter_dialogs(limit=200):
                    raw_dialogs.append(d)

            gc_items = []
            all_items = []
            for d in raw_dialogs:
                info = _extract_dialog_info(d)
                if not info:
                    continue
                item = {"id": info["id"], "name": info["name"], "type": info["type"]}
                all_items.append(item)
                if info["is_gc"]:
                    gc_items.append(item)

            group_list = gc_items if gc_items else all_items
            for j in spam_jobs:
                j["target_name"] = resolve_name(j["target"])
            for j in forward_jobs:
                j["source_name"] = resolve_name(j.get("source", ""))
                j["target_name"] = resolve_name(j.get("target", ""))
            return group_list
        except Exception as e:
            if is_auth_key_error(e):
                await _reset_invalid_session("سشن معتبر نیست؛ شماره و کد جدید وارد کنید.")
            return group_list


async def _do_fetch_last10(source_raw: str):
    ok = await ensure_connected(force_rebuild=False)
    if not ok:
        return {"ok": False, "error": state.get("auth_msg") or "اتصال برقرار نیست.", "messages": []}

    peer = to_peer(source_raw)
    async with connect_lock:
        try:
            raw_msgs = []
            if hasattr(client, "get_messages"):
                res = await asyncio.wait_for(client.get_messages(peer, limit=10), timeout=15)
                raw_msgs = list(res) if res else []
            elif hasattr(client, "iter_messages"):
                async for m in client.iter_messages(peer, limit=10):
                    raw_msgs.append(m)
            else:
                raise RuntimeError("امکان خواندن پیام‌ها در کلاینت وجود ندارد.")

            out = []
            for idx, m in enumerate(raw_msgs):
                mid = _extract_msg_id(m, idx + 1)
                full_text = _extract_msg_text(m) or "[پیام مدیا / بدون متن]"
                one_line = " ".join(full_text.splitlines()).strip()
                summary = (one_line[:75] + "...") if len(one_line) > 75 else one_line
                out.append({
                    "index": idx + 1,
                    "id": mid,
                    "summary": summary,
                    "text": full_text,
                })
            return {"ok": True, "error": "", "messages": out}
        except Exception as e:
            if is_auth_key_error(e):
                await _reset_invalid_session("سشن معتبر نیست؛ شماره و کد جدید وارد کنید.")
            return {"ok": False, "error": str(e), "messages": []}


def run_in_bot_loop(coro):
    if bot_loop and not bot_loop.is_closed():
        asyncio.run_coroutine_threadsafe(coro, bot_loop)


def run_in_bot_loop_wait(coro, timeout=18):
    if not bot_loop or bot_loop.is_closed():
        return None
    fut = asyncio.run_coroutine_threadsafe(coro, bot_loop)
    try:
        return fut.result(timeout=timeout)
    except Exception:
        return None


# ================= رابط کاربری پنل =================
PANEL_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>پنل اسپم و فوروارد</title>
<style>
  body { font-family: Tahoma, sans-serif; background: #111; color: #eee; margin: 0; padding: 12px; }
  .box { max-width: 480px; margin: 0 auto 12px; background: #1b1b1b; border: 1px solid #333; border-radius: 8px; padding: 12px; }
  h3 { margin: 0 0 10px; font-size: 15px; border-bottom: 1px solid #2c2c2c; padding-bottom: 6px; }
  label { display: block; font-size: 12px; color: #aaa; margin: 8px 0 4px; }
  input, textarea, select { width: 100%; box-sizing: border-box; padding: 8px; border-radius: 6px; border: 1px solid #3a3a3a; background: #111; color: #fff; font-family: Tahoma, sans-serif; }
  textarea { min-height: 50px; resize: vertical; }
  button { width: 100%; padding: 9px; margin-top: 8px; border: 0; border-radius: 6px; font-family: Tahoma, sans-serif; font-weight: bold; cursor: pointer; background: #2b6cb0; color: #fff; }
  .btn-green { background: #2f855a; }
  .btn-amber { background: #b7791f; }
  .btn-red { background: #c53030; }
  .mini { width: auto; padding: 5px 10px; font-size: 12px; margin-top: 0; }
  .row { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; margin-top: 6px; }
  .card { background: #121212; border: 1px solid #2e2e2e; border-radius: 6px; padding: 8px; margin-top: 8px; font-size: 12px; line-height: 1.7; }
  .msg-item { background: #121212; border: 1px solid #2e2e2e; border-radius: 6px; padding: 8px; margin-top: 6px; font-size: 12px; cursor: pointer; }
  .msg-item.selected { border-color: #2f855a; background: #132219; }
  .small { font-size: 12px; color: #bbb; margin-top: 6px; word-break: break-word; }
  .groups { display: flex; flex-wrap: wrap; gap: 5px; margin-top: 6px; max-height: 100px; overflow-y: auto; }
  .gbtn { width: auto; margin: 0; padding: 4px 8px; font-size: 11px; background: #252525; border: 1px solid #3d3d3d; }
</style>
</head>
<body>

<div class="box">
  <h3>اطلاعات اجرا و اتصال (پینگ: <span id="ping">-</span>)</h3>
  <div id="info_list"><div class="small">مأموریتی ثبت نشده است.</div></div>
  <div class="small" id="status_txt">-</div>
</div>

<div class="box">
  <h3>ورود با شماره</h3>
  <label>شماره تلفن:</label>
  <input id="phone" type="text" placeholder="+98912..." dir="ltr">
  <button onclick="sendPhone()">ارسال کد</button>
  <div id="code_box" style="display:none; margin-top:8px;">
    <label>کد تأیید:</label>
    <input id="code" type="text" placeholder="12345" dir="ltr">
    <button class="btn-green" onclick="sendCode()">تأیید کد</button>
  </div>
  <div class="small" id="auth_msg">-</div>
</div>

<div class="box">
  <h3>اسپم جدید</h3>
  <button class="btn-amber" onclick="loadGroups()">دریافت لیست گروه‌ها/کانال‌ها</button>
  <div id="groups" class="groups"></div>

  <label>مقصد (لینک چنل/پیام، @یوزرنیم یا آیدی):</label>
  <input id="target" type="text" placeholder="لینک، @username یا آیدی...">

  <label>متن‌های اسپم:</label>
  <div id="texts">
    <div class="row">
      <textarea class="t-inp" style="flex:1" placeholder="متن ۱..."></textarea>
      <button type="button" class="mini btn-red" onclick="delText(this)">X</button>
    </div>
  </div>
  <button type="button" onclick="addText()">افزودن متن دیگر</button>

  <label>سرعت (ثانیه بین هر ارسال):</label>
  <input id="speed" type="number" value="1" step="0.05" min="0.05">

  <label>تعداد ارسال (0 = نامحدود):</label>
  <input id="max_count" type="number" value="0" step="1" min="0">

  <button class="btn-green" onclick="addJob()">شروع اسپم</button>
  <div class="small" id="add_msg"></div>

  <h3 style="margin-top:14px;">لیست اسپم‌ها</h3>
  <div id="jobs_list"><div class="small">هنوز اسپمی ساخته نشده است.</div></div>
</div>

<div class="box">
  <h3>فوروارد جدید (مدیریت مبدأ و مقصد)</h3>
  <button class="btn-amber" onclick="loadGroups()">بروزرسانی لیست گروه‌ها و کانال‌ها</button>

  <label>۱. انتخاب مبدأ:</label>
  <select id="fwd_src_sel" onchange="document.getElementById('fwd_src').value=this.value">
    <option value="">-- انتخاب از لیست یا وارد کردن در کادر زیر --</option>
  </select>
  <input id="fwd_src" type="text" placeholder="آیدی، @username یا لینک مبدأ..." style="margin-top:6px;">

  <button type="button" onclick="loadLast10()">نمایش خلاصه ۱۰ پیام اخیر مبدأ</button>
  <div id="last10_box"><div class="small">پیامی بارگذاری نشده است.</div></div>

  <label>۲. پیام انتخاب‌شده (از ۱۰ پیام اخیر یا آیدی پیام):</label>
  <input id="fwd_msg_id" type="text" placeholder="با کلیک روی پیام بالا پر می‌شود" dir="ltr">
  <div class="small" id="fwd_msg_prev">متن انتخاب‌شده: -</div>

  <label>۳. انتخاب مقصد:</label>
  <select id="fwd_tgt_sel" onchange="document.getElementById('fwd_tgt').value=this.value">
    <option value="">-- انتخاب از لیست یا وارد کردن در کادر زیر --</option>
  </select>
  <input id="fwd_tgt" type="text" placeholder="آیدی، @username یا لینک مقصد..." style="margin-top:6px;">

  <label>۴. سرعت فوروارد (ثانیه):</label>
  <input id="fwd_speed" type="number" value="1" step="0.1" min="0.05">

  <label>۵. تعداد فوروارد (0 = نامحدود):</label>
  <input id="fwd_max" type="number" value="0" step="1" min="0">

  <button class="btn-green" onclick="addFwdJob()">شروع فوروارد</button>
  <div class="small" id="fwd_add_msg"></div>

  <h3 style="margin-top:14px;">لیست فورواردها</h3>
  <div id="fwd_jobs_list"><div class="small">هنوز فورواردی ساخته نشده است.</div></div>
</div>

<script>
let manualCodeBox = false;
let chosenFwdText = '';

function esc(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

function addText(){
  const n = document.querySelectorAll('.t-inp').length + 1;
  const d = document.createElement('div');
  d.className = 'row';
  d.innerHTML = `<textarea class="t-inp" style="flex:1" placeholder="متن ${n}..."></textarea><button type="button" class="mini btn-red" onclick="delText(this)">X</button>`;
  document.getElementById('texts').appendChild(d);
}
function delText(btn){
  const rows = document.querySelectorAll('#texts .row');
  if(rows.length <= 1){ rows[0].querySelector('textarea').value=''; return; }
  btn.parentElement.remove();
}
function getTexts(){
  return Array.from(document.querySelectorAll('.t-inp')).map(x=>x.value.trim()).filter(Boolean);
}

async function sendPhone(){
  const p = document.getElementById('phone').value.trim();
  if(!p) return alert('شماره را وارد کن');
  manualCodeBox = true;
  document.getElementById('code_box').style.display = 'block';
  document.getElementById('auth_msg').innerText = 'در حال ارسال کد...';
  await fetch('/api/send_phone?phone=' + encodeURIComponent(p));
  setTimeout(tick, 600);
}
async function sendCode(){
  const c = document.getElementById('code').value.trim();
  if(!c) return alert('کد را وارد کن');
  document.getElementById('auth_msg').innerText = 'در حال تأیید کد...';
  await fetch('/api/verify_code?code=' + encodeURIComponent(c));
  setTimeout(tick, 800);
}

function applyDialogsToUI(arr){
  const gBox = document.getElementById('groups');
  const sSel = document.getElementById('fwd_src_sel');
  const tSel = document.getElementById('fwd_tgt_sel');
  if(!Array.isArray(arr) || !arr.length){
    gBox.innerHTML = '<span class="small">موردی یافت نشد</span>';
    return;
  }
  gBox.innerHTML = arr.map(g =>
    `<button type="button" class="gbtn" onclick="document.getElementById('target').value='${String(g.id).replace(/'/g,"\\'")}'">${esc(g.name)}</button>`
  ).join('');
  const opts = '<option value="">-- انتخاب کنید --</option>' + arr.map(g =>
    `<option value="${esc(g.id)}">[${esc(g.type || 'چت')}] ${esc(g.name)} (${esc(g.id)})</option>`
  ).join('');
  sSel.innerHTML = opts;
  tSel.innerHTML = opts;
}

async function loadGroups(){
  document.getElementById('groups').innerHTML = '<span class="small">در حال دریافت...</span>';
  const r = await fetch('/api/groups');
  const arr = await r.json();
  applyDialogsToUI(arr);
}

function pickFwdMsg(el, mid, fullText, summary){
  document.querySelectorAll('.msg-item').forEach(x => x.classList.remove('selected'));
  el.classList.add('selected');
  document.getElementById('fwd_msg_id').value = mid;
  chosenFwdText = fullText;
  document.getElementById('fwd_msg_prev').innerText = 'متن انتخاب‌شده: ' + summary;
}

async function loadLast10(){
  const src = document.getElementById('fwd_src').value.trim() || document.getElementById('fwd_src_sel').value.trim();
  const box = document.getElementById('last10_box');
  if(!src){ box.innerHTML = '<div class="small">ابتدا مبدأ را انتخاب یا وارد کنید.</div>'; return; }
  box.innerHTML = '<div class="small">در حال دریافت ۱۰ پیام اخیر...</div>';
  try {
    const r = await fetch('/api/forward/last10?source=' + encodeURIComponent(src));
    const data = await r.json();
    if(!data.ok){ box.innerHTML = `<div class="small" style="color:#feb2b2;">خطا: ${esc(data.error)}</div>`; return; }
    const msgs = data.messages || [];
    if(!msgs.length){ box.innerHTML = '<div class="small">پیامی یافت نشد.</div>'; return; }
    box.innerHTML = msgs.map(m => {
      const b64 = btoa(unescape(encodeURIComponent(m.text || '')));
      const s64 = btoa(unescape(encodeURIComponent(m.summary || '')));
      return `<div class="msg-item" onclick="pickFwdMsg(this, '${esc(m.id)}', decodeURIComponent(escape(atob('${b64}'))), decodeURIComponent(escape(atob('${s64}'))))">
        <div><b>#${m.index}</b> (ID: <span dir="ltr">${esc(m.id)}</span>)</div>
        <div style="margin-top:3px;color:#ccc;">${esc(m.summary)}</div>
      </div>`;
    }).join('');
  } catch(e) {
    box.innerHTML = '<div class="small">خطا در دریافت پیام‌ها.</div>';
  }
}

async function addJob(){
  const t = document.getElementById('target').value.trim();
  const tx = getTexts();
  const sp = document.getElementById('speed').value;
  const mc = document.getElementById('max_count').value;
  if(!t || !tx.length){ document.getElementById('add_msg').innerText = 'مقصد و حداقل یک متن وارد کن.'; return; }
  const enc = encodeURIComponent(JSON.stringify(tx).replace(/\\n/g,'_NEWLINE_'));
  const r = await fetch(`/api/add_job?target=${encodeURIComponent(t)}&texts=${enc}&speed=${encodeURIComponent(sp)}&max_count=${encodeURIComponent(mc)}`);
  const out = await r.json();
  document.getElementById('add_msg').innerText = out.msg || 'ثبت شد.';
  tick();
}

async function addFwdJob(){
  const src = document.getElementById('fwd_src').value.trim() || document.getElementById('fwd_src_sel').value.trim();
  const tgt = document.getElementById('fwd_tgt').value.trim() || document.getElementById('fwd_tgt_sel').value.trim();
  const mid = document.getElementById('fwd_msg_id').value.trim();
  const spd = document.getElementById('fwd_speed').value;
  const mc = document.getElementById('fwd_max').value;
  if(!src || !tgt || !mid){ document.getElementById('fwd_add_msg').innerText = 'مبدا، پیام و مقصد را کامل کنید.'; return; }
  const encTxt = encodeURIComponent((chosenFwdText || '').replace(/\\n/g,'_NEWLINE_'));
  const r = await fetch(`/api/forward/add?source=${encodeURIComponent(src)}&target=${encodeURIComponent(tgt)}&msg_id=${encodeURIComponent(mid)}&msg_text=${encTxt}&speed=${encodeURIComponent(spd)}&max_count=${encodeURIComponent(mc)}`);
  const out = await r.json();
  document.getElementById('fwd_add_msg').innerText = out.msg || 'ثبت شد.';
  tick();
}

async function ctrlJob(id, act){
  await fetch(`/api/${act}_job?id=${encodeURIComponent(id)}`);
  tick();
}

async function ctrlFwd(id, act){
  await fetch(`/api/forward/${act}?id=${encodeURIComponent(id)}`);
  tick();
}

function renderAll(jobs, fwds){
  const info = document.getElementById('info_list');
  const list = document.getElementById('jobs_list');
  const flist = document.getElementById('fwd_jobs_list');

  const spamCards = (jobs || []).map(j => {
    const lim = j.max_count > 0 ? `${j.sent} / ${j.max_count}` : `${j.sent} / ∞`;
    return `<div class="card">
      <b>[اسپم] مقصد:</b> ${esc(j.target_name)} (${esc(j.target)})<br>
      <b>سرعت:</b> ${j.speed}s | <b>ارسال:</b> ${lim} | <b>آخرین ارسال:</b> ${esc(j.last_time||'-')}<br>
      <b>وضعیت:</b> ${j.running ? 'فعال' : 'متوقف'} (${esc(j.last_status||'')})
      ${j.last_err ? `<br><span style="color:#feb2b2" dir="ltr">${esc(j.last_err)}</span>` : ''}
    </div>`;
  });

  const fwdCards = (fwds || []).map(j => {
    const lim = j.max_count > 0 ? `${j.sent} / ${j.max_count}` : `${j.sent} / ∞`;
    return `<div class="card">
      <b>[فوروارد]</b> ${esc(j.source_name||j.source)} ← ${esc(j.target_name||j.target)} (ID: <span dir="ltr">${esc(j.msg_id)}</span>)<br>
      <b>سرعت:</b> ${j.speed}s | <b>ارسال:</b> ${lim} | <b>آخرین:</b> ${esc(j.last_time||'-')}<br>
      <b>وضعیت:</b> ${j.running ? 'فعال' : 'متوقف'} (${esc(j.last_status||'')})
      ${j.last_err ? `<br><span style="color:#feb2b2" dir="ltr">${esc(j.last_err)}</span>` : ''}
    </div>`;
  });

  const allLive = spamCards.concat(fwdCards);
  info.innerHTML = allLive.length ? allLive.join('') : '<div class="small">مأموریتی ثبت نشده است.</div>';

  list.innerHTML = (jobs || []).length ? (jobs || []).map(j => `
    <div class="card">
      <b>${esc(j.target_name)}</b> — سرعت: ${j.speed}s — متن‌ها: ${(j.texts||[]).length}<br>
      <div class="row">
        ${j.running
          ? `<button type="button" class="mini btn-amber" onclick="ctrlJob('${j.id}','pause')">توقف</button>`
          : `<button type="button" class="mini btn-green" onclick="ctrlJob('${j.id}','resume')">ادامه</button>`}
        <button type="button" class="mini btn-red" onclick="ctrlJob('${j.id}','del')">حذف</button>
      </div>
    </div>
  `).join('') : '<div class="small">هنوز اسپمی ساخته نشده است.</div>';

  flist.innerHTML = (fwds || []).length ? (fwds || []).map(j => `
    <div class="card">
      <b>${esc(j.source_name||j.source)}</b> → <b>${esc(j.target_name||j.target)}</b><br>
      <div class="small" style="margin-top:2px;">پیام: ${esc(j.msg_summary || ('ID ' + j.msg_id))} — سرعت: ${j.speed}s</div>
      <div class="row">
        ${j.running
          ? `<button type="button" class="mini btn-amber" onclick="ctrlFwd('${j.id}','pause')">توقف</button>`
          : `<button type="button" class="mini btn-green" onclick="ctrlFwd('${j.id}','resume')">ادامه</button>`}
        <button type="button" class="mini btn-red" onclick="ctrlFwd('${j.id}','del')">حذف</button>
      </div>
    </div>
  `).join('') : '<div class="small">هنوز فورواردی ساخته نشده است.</div>';
}

async function tick(){
  try{
    const r = await fetch('/api/state');
    const d = await r.json();
    document.getElementById('ping').innerText = d.ping;
    document.getElementById('status_txt').innerText = d.status;
    document.getElementById('auth_msg').innerText = d.auth_msg;
    if(d.auth_status === 'connected'){ manualCodeBox = false; }
    const showCode = manualCodeBox || (d.auth_status === 'code_sent');
    document.getElementById('code_box').style.display = showCode ? 'block' : 'none';
    renderAll(d.jobs || [], d.forwards || []);
  }catch(e){}
}

setInterval(tick, 1500);
tick();
</script>
</body>
</html>
"""


# ================= سرور وب =================
logging.getLogger("werkzeug").setLevel(logging.ERROR)
app = Flask(__name__)


def json_resp(obj, code=200):
    return app.response_class(
        response=json.dumps(obj, ensure_ascii=False),
        status=code,
        mimetype="application/json; charset=utf-8",
    )


@app.route(PANEL_PATH)
@app.route("/forward")
def panel_page():
    return PANEL_HTML


@app.route("/")
def root_page():
    return f"OK - Open {PANEL_PATH}", 200


@app.route("/health")
def health_page():
    return "OK", 200


@app.route("/api/send_phone")
def api_send_phone():
    p = (request.args.get("phone") or "").strip()
    if p:
        state["phone"] = p
        state["auth_status"] = "code_sent"
        state["auth_msg"] = "در حال ارسال کد..."
        run_in_bot_loop(_do_send_code(p))
    return "", 200


@app.route("/api/verify_code")
def api_verify_code():
    c = (request.args.get("code") or "").strip()
    if c:
        run_in_bot_loop(_do_verify_code(c))
    return "", 200


@app.route("/api/groups")
def api_groups():
    if client_ready and not group_list:
        run_in_bot_loop_wait(refresh_groups(), timeout=15)
    return json_resp(group_list)


@app.route("/api/add_job")
def api_add_job():
    global spam_seq
    raw_t = (request.args.get("target") or "").strip()
    tgt = parse_target(raw_t)
    txts = parse_texts(request.args.get("texts") or "")
    try:
        spd = max(0.05, float(request.args.get("speed") or "1"))
    except Exception:
        spd = 1.0
    try:
        mc = max(0, int(request.args.get("max_count") or "0"))
    except Exception:
        mc = 0

    if not tgt or not txts:
        return json_resp({"ok": False, "msg": "مقصد یا متن معتبر نیست."})

    spam_seq += 1
    job = {
        "id": f"sp_{spam_seq}_{int(time.time()*1000)%10000}",
        "target": tgt,
        "target_name": resolve_name(tgt),
        "texts": txts,
        "idx": 0,
        "speed": spd,
        "max_count": mc,
        "running": True,
        "sent": 0,
        "last_time": "-",
        "last_err": "",
        "last_status": "در حال شروع",
    }
    spam_jobs.append(job)
    save_spam_jobs()
    state["status"] = f"مأموریت اسپم روی {job['target_name']} ثبت شد."
    return json_resp({"ok": True, "msg": "اسپم اضافه شد."})


@app.route("/api/pause_job")
def api_pause_job():
    jid = (request.args.get("id") or "").strip()
    for j in spam_jobs:
        if j["id"] == jid:
            j["running"] = False
            j["last_status"] = "متوقف"
    save_spam_jobs()
    return "", 200


@app.route("/api/resume_job")
def api_resume_job():
    jid = (request.args.get("id") or "").strip()
    for j in spam_jobs:
        if j["id"] == jid:
            if int(j.get("max_count") or 0) > 0 and int(j.get("sent") or 0) >= int(j["max_count"]):
                j["sent"] = 0
            j["running"] = True
            j["last_err"] = ""
            j["last_status"] = "در حال ادامه..."
    save_spam_jobs()
    return "", 200


@app.route("/api/del_job")
def api_del_job():
    jid = (request.args.get("id") or "").strip()
    spam_jobs[:] = [j for j in spam_jobs if j["id"] != jid]
    save_spam_jobs()
    return "", 200


@app.route("/api/forward/last10")
def api_fwd_last10():
    src = (request.args.get("source") or "").strip()
    res = run_in_bot_loop_wait(_do_fetch_last10(src), timeout=18)
    return json_resp(res if isinstance(res, dict) else {"ok": False, "error": "پاسخ دریافت نشد.", "messages": []})


@app.route("/api/forward/add")
def api_fwd_add():
    global fwd_seq
    src_raw = (request.args.get("source") or "").strip()
    tgt_raw = (request.args.get("target") or "").strip()
    msg_id = (request.args.get("msg_id") or "").strip()
    raw_txt = (request.args.get("msg_text") or "").replace("_NEWLINE_", "\n").strip()

    src = parse_target(src_raw)
    tgt = parse_target(tgt_raw)
    try:
        spd = max(0.05, float(request.args.get("speed") or "1"))
    except Exception:
        spd = 1.0
    try:
        mc = max(0, int(request.args.get("max_count") or "0"))
    except Exception:
        mc = 0

    if not src or not tgt or not msg_id:
        return json_resp({"ok": False, "msg": "مبدا، پیام یا مقصد ناقص است."})

    one_line = " ".join(raw_txt.splitlines()).strip()
    summary = (one_line[:65] + "...") if len(one_line) > 65 else (one_line or f"پیام #{msg_id}")

    fwd_seq += 1
    job = {
        "id": f"fw_{fwd_seq}_{int(time.time()*1000)%10000}",
        "source": src,
        "source_name": resolve_name(src),
        "target": tgt,
        "target_name": resolve_name(tgt),
        "msg_id": msg_id,
        "msg_text": raw_txt,
        "msg_summary": summary,
        "speed": spd,
        "max_count": mc,
        "running": True,
        "sent": 0,
        "last_time": "-",
        "last_err": "",
        "last_status": "در حال شروع",
    }
    forward_jobs.append(job)
    save_fwd_jobs()
    state["status"] = f"مأموریت فوروارد از {job['source_name']} به {job['target_name']} ثبت شد."
    return json_resp({"ok": True, "msg": "فوروارد اضافه شد."})


@app.route("/api/forward/pause")
def api_fwd_pause():
    jid = (request.args.get("id") or "").strip()
    for j in forward_jobs:
        if j["id"] == jid:
            j["running"] = False
            j["last_status"] = "متوقف"
    save_fwd_jobs()
    return "", 200


@app.route("/api/forward/resume")
def api_fwd_resume():
    jid = (request.args.get("id") or "").strip()
    for j in forward_jobs:
        if j["id"] == jid:
            mc = int(j.get("max_count") or 0)
            if mc > 0 and int(j.get("sent") or 0) >= mc:
                j["sent"] = 0
            j["running"] = True
            j["last_err"] = ""
            j["last_status"] = "در حال ادامه..."
    save_fwd_jobs()
    return "", 200


@app.route("/api/forward/del")
def api_fwd_del():
    jid = (request.args.get("id") or "").strip()
    forward_jobs[:] = [j for j in forward_jobs if j["id"] != jid]
    save_fwd_jobs()
    return "", 200


@app.route("/api/state")
def api_state():
    return json_resp({
        "ping": state["ping"],
        "status": state["status"],
        "auth_status": state["auth_status"],
        "auth_msg": state["auth_msg"],
        "jobs": spam_jobs,
        "forwards": forward_jobs,
    })


def run_web():
    app.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True)


# ================= موتور اجرای پیوسته و بازیابی خودکار =================
async def _send_spam_message(peer, msg: str):
    async with io_lock:
        return await asyncio.wait_for(client.send_message(peer, msg), timeout=12)


async def _resolve_fwd_fallback_text(src_peer, msg_id_val, fallback_text: str) -> str:
    txt = (fallback_text or "").strip()
    if txt and txt != "[پیام مدیا / بدون متن]":
        return txt
    if hasattr(client, "get_messages"):
        try:
            res = await asyncio.wait_for(client.get_messages(src_peer, ids=msg_id_val), timeout=10)
            if res:
                first = res[0] if isinstance(res, (list, tuple)) else res
                extracted = _extract_msg_text(first)
                if extracted:
                    return extracted
        except Exception:
            pass
    return txt or f"پیام #{msg_id_val}"


async def _send_forward_message(src_peer, tgt_peer, msg_id_str: str, fallback_text: str):
    msg_id_val = int(msg_id_str) if str(msg_id_str).isdigit() else msg_id_str

    async with io_lock:
        fwd_err = None
        if hasattr(client, "forward_messages"):
            try:
                return await asyncio.wait_for(
                    client.forward_messages(tgt_peer, msg_id_val, from_peer=src_peer),
                    timeout=14,
                )
            except TypeError:
                try:
                    return await asyncio.wait_for(
                        client.forward_messages(tgt_peer, msg_id_val, src_peer),
                        timeout=14,
                    )
                except Exception as e:
                    fwd_err = e
            except Exception as e:
                fwd_err = e

        elif hasattr(client, "forward_message"):
            try:
                return await asyncio.wait_for(
                    client.forward_message(tgt_peer, msg_id_val, from_peer=src_peer),
                    timeout=14,
                )
            except TypeError:
                try:
                    return await asyncio.wait_for(
                        client.forward_message(tgt_peer, msg_id_val, src_peer),
                        timeout=14,
                    )
                except Exception as e:
                    fwd_err = e
            except Exception as e:
                fwd_err = e

        if fwd_err is not None and is_auth_key_error(fwd_err):
            raise fwd_err

        text_to_send = await _resolve_fwd_fallback_text(src_peer, msg_id_val, fallback_text)
        return await asyncio.wait_for(client.send_message(tgt_peer, text_to_send), timeout=14)


async def spam_worker(job: dict):
    global client_ready
    err_streak = 0
    while job in spam_jobs:
        try:
            if not job.get("running"):
                await asyncio.sleep(0.25)
                continue

            mc = int(job.get("max_count") or 0)
            sent = int(job.get("sent") or 0)
            if mc > 0 and sent >= mc:
                job["running"] = False
                job["last_status"] = f"تمام شد ({mc})"
                save_spam_jobs()
                await asyncio.sleep(0.3)
                continue

            if login_in_progress or not client_ready:
                job["last_status"] = "بازیابی اتصال؛ منتظر..."
                await asyncio.sleep(0.6)
                continue

            t0 = time.perf_counter()
            txts = job.get("texts") or ["تست"]
            i = int(job.get("idx") or 0) % len(txts)
            msg = txts[i]
            peer = to_peer(job["target"])

            await _send_spam_message(peer, msg)

            # تنها پس از موفقیت واقعی ارسال، ایندکس و شمارنده را جلو می‌بریم
            job["idx"] = (i + 1) % len(txts)
            job["sent"] = sent + 1
            job["last_time"] = now_clock()
            job["last_err"] = ""
            job["last_status"] = f"ارسال #{job['sent']}"
            err_streak = 0

            if job["sent"] % 10 == 0:
                save_spam_jobs()

            if mc > 0 and job["sent"] >= mc:
                job["running"] = False
                job["last_status"] = f"تمام شد ({mc}/{mc})"
                save_spam_jobs()
                continue

            elapsed = time.perf_counter() - t0
            spd = max(0.05, float(job.get("speed") or 1.0))
            if spd - elapsed > 0:
                await asyncio.sleep(spd - elapsed)

        except Exception as e:
            if is_auth_key_error(e):
                await _reset_invalid_session("سشن معتبر نیست؛ شماره و کد جدید وارد کنید.")
                job["last_err"] = "AuthKeyUnregisteredError"
                job["last_status"] = "نیاز به ورود مجدد"
                await asyncio.sleep(2.0)
                continue

            err_streak += 1
            err_msg = str(e)[:120]
            job["last_err"] = err_msg
            job["last_status"] = f"اختلال کوتاه؛ ادامه همان پیام ({err_streak})"

            sleep_sec = 2.0 if "list index out of range" in err_msg.lower() else 0.6
            if err_streak >= 6:
                client_ready = False
                await ensure_connected(force_rebuild=True)
                err_streak = 0
            await asyncio.sleep(sleep_sec)


async def forward_worker(job: dict):
    global client_ready
    err_streak = 0
    while job in forward_jobs:
        try:
            if not job.get("running"):
                await asyncio.sleep(0.25)
                continue

            mc = int(job.get("max_count") or 0)
            sent = int(job.get("sent") or 0)
            if mc > 0 and sent >= mc:
                job["running"] = False
                job["last_status"] = f"تمام شد ({mc})"
                save_fwd_jobs()
                await asyncio.sleep(0.3)
                continue

            if login_in_progress or not client_ready:
                job["last_status"] = "بازیابی اتصال؛ منتظر..."
                await asyncio.sleep(0.6)
                continue

            t0 = time.perf_counter()
            src_peer = to_peer(job["source"])
            tgt_peer = to_peer(job["target"])

            await _send_forward_message(
                src_peer,
                tgt_peer,
                str(job.get("msg_id") or ""),
                str(job.get("msg_text") or ""),
            )

            job["sent"] = sent + 1
            job["last_time"] = now_clock()
            job["last_err"] = ""
            job["last_status"] = f"فوروارد #{job['sent']}"
            err_streak = 0

            if job["sent"] % 10 == 0:
                save_fwd_jobs()

            if mc > 0 and job["sent"] >= mc:
                job["running"] = False
                job["last_status"] = f"تمام شد ({mc}/{mc})"
                save_fwd_jobs()
                continue

            elapsed = time.perf_counter() - t0
            spd = max(0.05, float(job.get("speed") or 1.0))
            if spd - elapsed > 0:
                await asyncio.sleep(spd - elapsed)

        except Exception as e:
            if is_auth_key_error(e):
                await _reset_invalid_session("سشن معتبر نیست؛ شماره و کد جدید وارد کنید.")
                job["last_err"] = "AuthKeyUnregisteredError"
                job["last_status"] = "نیاز به ورود مجدد"
                await asyncio.sleep(2.0)
                continue

            err_streak += 1
            err_msg = str(e)[:120]
            job["last_err"] = err_msg
            job["last_status"] = f"اختلال کوتاه؛ ادامه فوروارد ({err_streak})"

            sleep_sec = 2.0 if "list index out of range" in err_msg.lower() else 0.7
            if err_streak >= 6:
                client_ready = False
                await ensure_connected(force_rebuild=True)
                err_streak = 0
            await asyncio.sleep(sleep_sec)


async def supervisor_loop():
    sp_tasks = {}
    fw_tasks = {}
    while True:
        try:
            active_sp = {j["id"] for j in spam_jobs}
            for jid in list(sp_tasks.keys()):
                if jid not in active_sp or sp_tasks[jid].done():
                    sp_tasks.pop(jid, None)
            for j in list(spam_jobs):
                if j["id"] not in sp_tasks:
                    sp_tasks[j["id"]] = asyncio.create_task(spam_worker(j))

            active_fw = {j["id"] for j in forward_jobs}
            for jid in list(fw_tasks.keys()):
                if jid not in active_fw or fw_tasks[jid].done():
                    fw_tasks.pop(jid, None)
            for j in list(forward_jobs):
                if j["id"] not in fw_tasks:
                    fw_tasks[j["id"]] = asyncio.create_task(forward_worker(j))
        except Exception:
            pass
        await asyncio.sleep(0.35)


async def heartbeat_loop():
    while True:
        try:
            if not login_in_progress:
                ok = await ensure_connected(force_rebuild=False)
                if ok and not group_list:
                    await refresh_groups()
        except Exception:
            pass
        await asyncio.sleep(6)


async def main_loop():
    global bot_loop
    bot_loop = asyncio.get_running_loop()
    asyncio.create_task(supervisor_loop())
    asyncio.create_task(heartbeat_loop())

    await ensure_connected(force_rebuild=True)
    if client_ready:
        await refresh_groups()

    while True:
        await asyncio.sleep(1)


def start_bot_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        try:
            loop.run_until_complete(main_loop())
        except Exception:
            time.sleep(1)


if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    Thread(target=start_bot_thread, daemon=True).start()
    while True:
        time.sleep(1)
