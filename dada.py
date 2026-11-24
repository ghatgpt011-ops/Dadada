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
ACCOUNTS_FILE = "accounts_store.json"
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

# ساختار اکانت‌ها: [{"id": "acc_1", "phone": "+98...", "name": "...", "session": "...", "ready": False, "ping": "-", "status": "..."}]
accounts = []
active_account_id = ""
acc_seq = 0

# کلاینت‌ها و قفل‌ها به ازای هر اکانت
clients_map = {}      # acc_id -> SoroushClient
groups_map = {}       # acc_id -> list[dict]
io_locks = {}         # acc_id -> asyncio.Lock
connect_locks = {}    # acc_id -> asyncio.Lock
last_keepalive = {}   # acc_id -> monotonic timestamp

spam_jobs = []
forward_jobs = []
spam_seq = 0
fwd_seq = 0

pending_login = {
    "acc_id": "",
    "phone": "",
    "code_hash": None,
    "in_progress": False,
    "auth_status": "idle",  # idle | code_sent | connected
    "auth_msg": "شماره تلفن را وارد کنید (مثال: +98912...)",
}

bot_loop = None
global_status = "در حال راه‌اندازی..."


def now_clock() -> str:
    return datetime.now().strftime("%H:%M:%S")


def is_auth_key_error(err) -> bool:
    s = str(err or "").lower()
    return (
        "authkeyunregistered" in s
        or "key is not registered" in s
        or "auth_key_unregistered" in s
    )


def get_acc(acc_id: str):
    for a in accounts:
        if a.get("id") == acc_id:
            return a
    return None


def get_active_acc():
    if active_account_id:
        a = get_acc(active_account_id)
        if a:
            return a
    return accounts[0] if accounts else None


def ensure_locks(acc_id: str):
    if acc_id not in io_locks:
        io_locks[acc_id] = asyncio.Lock()
    if acc_id not in connect_locks:
        connect_locks[acc_id] = asyncio.Lock()
    if acc_id not in groups_map:
        groups_map[acc_id] = []


def load_json_file(path: str, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return default


def save_json_file(path: str, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def save_accounts():
    clean = []
    for a in accounts:
        clean.append({
            "id": a["id"],
            "phone": a.get("phone", ""),
            "name": a.get("name", a["id"]),
            "session": a.get("session", ""),
        })
    save_json_file(ACCOUNTS_FILE, {"active_id": active_account_id, "accounts": clean})


def save_spam_jobs():
    save_json_file(SPAM_JOBS_FILE, spam_jobs)


def save_fwd_jobs():
    save_json_file(FWD_JOBS_FILE, forward_jobs)


def load_all_data():
    global accounts, active_account_id, acc_seq, spam_jobs, forward_jobs, spam_seq, fwd_seq

    raw_acc = load_json_file(ACCOUNTS_FILE, {})
    if isinstance(raw_acc, dict):
        saved_list = raw_acc.get("accounts") or []
        for idx, item in enumerate(saved_list, start=1):
            aid = str(item.get("id") or f"acc_{idx}")
            acc = {
                "id": aid,
                "phone": str(item.get("phone") or ""),
                "name": str(item.get("name") or item.get("phone") or aid),
                "session": str(item.get("session") or ""),
                "ready": False,
                "ping": "-",
                "status": "در انتظار اتصال...",
            }
            accounts.append(acc)
            ensure_locks(aid)
        acc_seq = len(accounts)
        act = str(raw_acc.get("active_id") or "")
        if act and get_acc(act):
            active_account_id = act
        elif accounts:
            active_account_id = accounts[0]["id"]

    raw_sp = load_json_file(SPAM_JOBS_FILE, [])
    if isinstance(raw_sp, list):
        spam_jobs = raw_sp
        for j in spam_jobs:
            j["idx"] = int(j.get("idx") or 0)
            j["sent"] = int(j.get("sent") or 0)
            j["max_count"] = int(j.get("max_count") or 0)
            j["speed"] = max(0.05, float(j.get("speed") or 1.0))
            if not j.get("account_id") and active_account_id:
                j["account_id"] = active_account_id
            if j.get("running"):
                j["last_status"] = "بازیابی تسک و ادامه..."
                j["last_err"] = ""
        spam_seq = len(spam_jobs)

    raw_fw = load_json_file(FWD_JOBS_FILE, [])
    if isinstance(raw_fw, list):
        forward_jobs = raw_fw
        for j in forward_jobs:
            j["sent"] = int(j.get("sent") or 0)
            j["max_count"] = int(j.get("max_count") or 0)
            j["speed"] = max(0.05, float(j.get("speed") or 1.0))
            if not j.get("account_id") and active_account_id:
                j["account_id"] = active_account_id
            if j.get("running"):
                j["last_status"] = "بازیابی فوروارد و ادامه..."
                j["last_err"] = ""
        fwd_seq = len(forward_jobs)


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


def resolve_name(acc_id: str, target: str) -> str:
    t = str(target).strip()
    for g in groups_map.get(acc_id, []):
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


def _extract_account_name(me, fallback: str = "اکانت") -> str:
    if not me:
        return fallback
    fn = (getattr(me, "first_name", None) or "").strip()
    ln = (getattr(me, "last_name", None) or "").strip()
    un = (getattr(me, "username", None) or "").strip()
    full = f"{fn} {ln}".strip()
    if full:
        return full
    if un:
        return f"@{un}"
    uid = getattr(me, "id", None)
    return str(uid) if uid is not None else fallback


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


load_all_data()


# ================= مدیریت اتصال چنداکانتی =================
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


async def _reset_invalid_account(acc_id: str, msg: str = "سشن معتبر نیست؛ دوباره وارد شوید."):
    acc = get_acc(acc_id)
    if acc:
        acc["ready"] = False
        acc["ping"] = "قطع"
        acc["session"] = ""
        acc["status"] = msg
        save_accounts()
    groups_map[acc_id] = []
    c = clients_map.pop(acc_id, None)
    await _close_client_safely(c)


def _extract_code_hash(sent_obj, phone: str, c):
    if sent_obj is not None:
        h = getattr(sent_obj, "phone_code_hash", None)
        if h:
            return str(h)
        if isinstance(sent_obj, dict) and sent_obj.get("phone_code_hash"):
            return str(sent_obj["phone_code_hash"])
    for attr in ("_phone_code_hash", "phone_code_hash", "code_hash"):
        val = getattr(c, attr, None)
        if isinstance(val, dict) and phone in val:
            return str(val[phone])
        if isinstance(val, str) and val:
            return val
    return None


async def ensure_account_connected(acc_id: str, force_rebuild: bool = False) -> bool:
    acc = get_acc(acc_id)
    if not acc:
        return False
    if pending_login["in_progress"] and pending_login["acc_id"] == acc_id:
        return False

    sess = (acc.get("session") or "").strip()
    if not sess:
        acc["ready"] = False
        acc["ping"] = "-"
        acc["status"] = "سشن ثبت نشده؛ کد ورود را تأیید کنید."
        return False

    ensure_locks(acc_id)
    async with connect_locks[acc_id]:
        c = clients_map.get(acc_id)
        if not force_rebuild and acc.get("ready") and c is not None:
            try:
                t0 = time.perf_counter()
                me = await asyncio.wait_for(c.get_me(), timeout=7)
                ms = int((time.perf_counter() - t0) * 1000)
                acc["name"] = _extract_account_name(me, acc.get("phone") or acc_id)
                acc["ping"] = f"{ms} ms"
                acc["status"] = f"آنلاین ({acc['name']})"
                return True
            except Exception as e:
                if is_auth_key_error(e):
                    await _reset_invalid_account(acc_id, "سشن نامعتبر شد؛ کد جدید بگیرید.")
                    return False

        try:
            await _close_client_safely(c)
            new_c = SoroushClient(StringSession(sess))
            t0 = time.perf_counter()
            await asyncio.wait_for(new_c.connect(), timeout=16)
            me = await asyncio.wait_for(new_c.get_me(), timeout=12)
            ms = int((time.perf_counter() - t0) * 1000)

            clients_map[acc_id] = new_c
            acc["ready"] = True
            acc["name"] = _extract_account_name(me, acc.get("phone") or acc_id)
            acc["ping"] = f"{ms} ms"
            acc["status"] = f"آنلاین ({acc['name']})"
            save_accounts()
            return True
        except Exception as e:
            if is_auth_key_error(e):
                await _reset_invalid_account(acc_id, "سشن نامعتبر شد؛ کد جدید بگیرید.")
                return False
            acc["ready"] = False
            acc["ping"] = "اختلال موقت"
            acc["status"] = f"تلاش مجدد برای اتصال: {str(e)[:80]}"
            return False


async def _do_send_code(phone: str):
    global active_account_id, acc_seq
    pending_login["in_progress"] = True
    pending_login["phone"] = phone
    pending_login["code_hash"] = None
    pending_login["auth_status"] = "code_sent"
    pending_login["auth_msg"] = "در حال ارسال کد..."

    # پیدا کردن یا ساخت اکانت موقت/جدید
    target_acc = next((a for a in accounts if a.get("phone") == phone), None)
    if not target_acc:
        acc_seq += 1
        aid = f"acc_{acc_seq}_{int(time.time()) % 10000}"
        target_acc = {
            "id": aid,
            "phone": phone,
            "name": phone,
            "session": "",
            "ready": False,
            "ping": "-",
            "status": "در انتظار کد تأیید...",
        }
        accounts.append(target_acc)
        ensure_locks(aid)
        active_account_id = aid
        save_accounts()
    else:
        active_account_id = target_acc["id"]
        save_accounts()

    aid = target_acc["id"]
    pending_login["acc_id"] = aid
    ensure_locks(aid)

    try:
        async with connect_locks[aid]:
            old_c = clients_map.pop(aid, None)
            await _close_client_safely(old_c)
            c = SoroushClient(StringSession())
            clients_map[aid] = c
            await asyncio.wait_for(c.connect(), timeout=20)

            if hasattr(c, "send_code_request"):
                sent_res = await asyncio.wait_for(c.send_code_request(phone), timeout=20)
            elif hasattr(c, "send_code"):
                sent_res = await asyncio.wait_for(c.send_code(phone), timeout=20)
            else:
                sent_res = await asyncio.wait_for(c.sign_in(phone=phone), timeout=20)

            pending_login["code_hash"] = _extract_code_hash(sent_res, phone, c)
        pending_login["auth_status"] = "code_sent"
        pending_login["auth_msg"] = f"کد برای {phone} ارسال شد."
    except Exception as e:
        pending_login["auth_status"] = "code_sent"
        pending_login["auth_msg"] = f"خطا در ارسال کد: {e}"


async def _do_verify_code(code: str):
    aid = pending_login.get("acc_id") or (get_active_acc()["id"] if get_active_acc() else "")
    phone = pending_login.get("phone", "").strip()
    acc = get_acc(aid)
    if not acc:
        pending_login["auth_msg"] = "ابتدا شماره را وارد کنید."
        return

    pending_login["auth_status"] = "code_sent"
    pending_login["auth_msg"] = "در حال تأیید کد..."
    ensure_locks(aid)

    try:
        async with connect_locks[aid]:
            c = clients_map.get(aid)
            if not c or not hasattr(c, "sign_in"):
                raise RuntimeError("کلاینت آماده ورود نیست.")

            signed = False
            last_exc = None
            ch = pending_login.get("code_hash")

            if ch:
                for call in (
                    lambda: c.sign_in(phone=phone, code=code, phone_code_hash=ch),
                    lambda: c.sign_in(phone, code, ch),
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
                    await asyncio.wait_for(c.sign_in(phone=phone, code=code), timeout=20)
                    signed = True
                except Exception as e:
                    last_exc = e

            if not signed:
                raise RuntimeError(str(last_exc or "ورود ناموفق بود."))

            sess = c.session.save() if getattr(c, "session", None) else ""
            if not sess:
                raise RuntimeError("سشن ذخیره نشد.")

            acc["session"] = sess
            acc["ready"] = True
            try:
                t0 = time.perf_counter()
                me = await asyncio.wait_for(c.get_me(), timeout=10)
                ms = int((time.perf_counter() - t0) * 1000)
                acc["name"] = _extract_account_name(me, phone)
                acc["ping"] = f"{ms} ms"
            except Exception:
                acc["name"] = phone

            acc["status"] = f"آنلاین ({acc['name']})"
            save_accounts()

        pending_login["in_progress"] = False
        pending_login["code_hash"] = None
        pending_login["auth_status"] = "connected"
        pending_login["auth_msg"] = f"اکانت {acc['name']} با موفقیت متصل شد."
        await refresh_groups(aid)
    except Exception as e:
        pending_login["auth_status"] = "code_sent"
        pending_login["auth_msg"] = f"خطا در کد: {e}"


async def _do_delete_account(acc_id: str):
    global active_account_id
    acc = get_acc(acc_id)
    if not acc:
        return
    c = clients_map.pop(acc_id, None)
    await _close_client_safely(c)
    groups_map.pop(acc_id, None)
    io_locks.pop(acc_id, None)
    connect_locks.pop(acc_id, None)
    last_keepalive.pop(acc_id, None)

    accounts[:] = [a for a in accounts if a.get("id") != acc_id]
    if active_account_id == acc_id:
        active_account_id = accounts[0]["id"] if accounts else ""
    save_accounts()


async def refresh_groups(acc_id: str = ""):
    target_id = acc_id or (get_active_acc()["id"] if get_active_acc() else "")
    if not target_id:
        return []
    ok = await ensure_account_connected(target_id, force_rebuild=False)
    if not ok:
        return groups_map.get(target_id, [])

    ensure_locks(target_id)
    async with connect_locks[target_id]:
        c = clients_map.get(target_id)
        if not c:
            return []
        try:
            raw_dialogs = []
            if hasattr(c, "get_dialogs"):
                res = await asyncio.wait_for(c.get_dialogs(limit=200), timeout=15)
                raw_dialogs = list(res) if res else []
            elif hasattr(c, "iter_dialogs"):
                async for d in c.iter_dialogs(limit=200):
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

            res_list = gc_items if gc_items else all_items
            groups_map[target_id] = res_list

            for j in spam_jobs:
                if j.get("account_id") == target_id:
                    j["target_name"] = resolve_name(target_id, j["target"])
            for j in forward_jobs:
                if j.get("account_id") == target_id:
                    j["source_name"] = resolve_name(target_id, j.get("source", ""))
                    j["target_name"] = resolve_name(target_id, j.get("target", ""))
            return res_list
        except Exception as e:
            if is_auth_key_error(e):
                await _reset_invalid_account(target_id, "سشن نامعتبر شد؛ کد جدید بگیرید.")
            return groups_map.get(target_id, [])


async def _do_fetch_last10(source_raw: str, acc_id: str = ""):
    target_id = acc_id or (get_active_acc()["id"] if get_active_acc() else "")
    if not target_id:
        return {"ok": False, "error": "هیچ اکانتی ثبت نشده است.", "messages": []}

    ok = await ensure_account_connected(target_id, force_rebuild=False)
    if not ok:
        acc = get_acc(target_id)
        return {"ok": False, "error": (acc or {}).get("status") or "اتصال برقرار نیست.", "messages": []}

    peer = to_peer(source_raw)
    ensure_locks(target_id)
    async with connect_locks[target_id]:
        c = clients_map.get(target_id)
        try:
            raw_msgs = []
            if hasattr(c, "get_messages"):
                res = await asyncio.wait_for(c.get_messages(peer, limit=10), timeout=15)
                raw_msgs = list(res) if res else []
            elif hasattr(c, "iter_messages"):
                async for m in c.iter_messages(peer, limit=10):
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
                await _reset_invalid_account(target_id, "سشن نامعتبر شد؛ کد جدید بگیرید.")
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


# ================= رابط کاربری رنگی و مدرن =================
PANEL_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>پنل چنداکانتی سروش</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg1: #0b0f1e;
    --bg2: #17102c;
    --bg3: #0c2233;
    --card: rgba(20, 26, 48, 0.78);
    --border: rgba(139, 92, 246, 0.28);
    --text: #f3f6ff;
    --muted: #9fb0d6;
    --accent1: #7c3aed;
    --accent2: #06b6d4;
    --green: #10b981;
    --amber: #f59e0b;
    --red: #ef4444;
  }
  * { box-sizing: border-box; }
  body {
    font-family: 'Vazirmatn', Tahoma, sans-serif;
    color: var(--text);
    margin: 0;
    padding: 14px;
    min-height: 100vh;
    background:
      radial-gradient(circle at 12% 15%, rgba(124, 58, 237, 0.28), transparent 42%),
      radial-gradient(circle at 88% 20%, rgba(6, 182, 212, 0.24), transparent 45%),
      radial-gradient(circle at 50% 90%, rgba(236, 72, 153, 0.16), transparent 50%),
      linear-gradient(135deg, var(--bg1), var(--bg2), var(--bg3));
    background-attachment: fixed;
  }
  .box {
    max-width: 520px;
    margin: 0 auto 14px;
    background: var(--card);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 14px;
    box-shadow: 0 12px 30px rgba(0, 0, 0, 0.38), inset 0 1px 0 rgba(255,255,255,0.06);
    transition: transform 0.2s ease, border-color 0.2s ease;
  }
  .box:hover { border-color: rgba(34, 211, 238, 0.45); }
  h3 {
    margin: 0 0 12px;
    font-size: 15px;
    font-weight: 800;
    padding-bottom: 8px;
    border-bottom: 1px solid rgba(255,255,255,0.08);
    background: linear-gradient(90deg, #c4b5fd, #67e8f9);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }
  label { display: block; font-size: 12px; color: var(--muted); margin: 9px 0 4px; font-weight: 600; }
  input, textarea, select {
    width: 100%;
    padding: 9px 11px;
    border-radius: 9px;
    border: 1px solid rgba(148, 163, 184, 0.25);
    background: rgba(9, 13, 28, 0.82);
    color: #fff;
    font-family: inherit;
    font-size: 13px;
    transition: border-color 0.2s ease, box-shadow 0.2s ease;
  }
  input:focus, textarea:focus, select:focus {
    outline: none;
    border-color: var(--accent2);
    box-shadow: 0 0 0 3px rgba(6, 182, 212, 0.18);
  }
  textarea { min-height: 54px; resize: vertical; }
  button {
    width: 100%;
    padding: 10px 12px;
    margin-top: 9px;
    border: 0;
    border-radius: 9px;
    font-family: inherit;
    font-weight: 700;
    font-size: 13px;
    cursor: pointer;
    color: #fff;
    background: linear-gradient(135deg, var(--accent1), #2563eb);
    box-shadow: 0 4px 14px rgba(124, 58, 237, 0.35);
    transition: transform 0.15s ease, filter 0.2s ease;
  }
  button:hover { filter: brightness(1.12); transform: translateY(-1px); }
  button:active { transform: translateY(0); }
  .btn-green { background: linear-gradient(135deg, #059669, var(--green)); box-shadow: 0 4px 14px rgba(16, 185, 129, 0.3); }
  .btn-amber { background: linear-gradient(135deg, #d97706, var(--amber)); box-shadow: 0 4px 14px rgba(245, 158, 11, 0.28); }
  .btn-red { background: linear-gradient(135deg, #dc2626, var(--red)); box-shadow: 0 4px 14px rgba(239, 68, 68, 0.28); }
  .btn-cyan { background: linear-gradient(135deg, #0891b2, var(--accent2)); box-shadow: 0 4px 14px rgba(6, 182, 212, 0.28); }
  .mini { width: auto; padding: 5px 10px; font-size: 11px; margin-top: 0; border-radius: 7px; }
  .row { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; margin-top: 6px; }
  .card {
    background: rgba(11, 16, 34, 0.78);
    border: 1px solid rgba(148, 163, 184, 0.18);
    border-radius: 10px;
    padding: 9px 11px;
    margin-top: 8px;
    font-size: 12px;
    line-height: 1.75;
    transition: border-color 0.2s ease;
  }
  .card.active-acc { border-color: var(--green); box-shadow: 0 0 0 1px rgba(16, 185, 129, 0.25); }
  .badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 999px;
    font-size: 10px;
    font-weight: 700;
    background: rgba(16, 185, 129, 0.18);
    color: #6ee7b7;
    border: 1px solid rgba(16, 185, 129, 0.35);
  }
  .badge-off {
    background: rgba(239, 68, 68, 0.18);
    color: #fca5a5;
    border-color: rgba(239, 68, 68, 0.35);
  }
  .msg-item {
    background: rgba(11, 16, 34, 0.78);
    border: 1px solid rgba(148, 163, 184, 0.18);
    border-radius: 9px;
    padding: 8px 10px;
    margin-top: 6px;
    font-size: 12px;
    cursor: pointer;
    transition: 0.18s ease;
  }
  .msg-item:hover { border-color: var(--accent2); }
  .msg-item.selected { border-color: var(--green); background: rgba(16, 185, 129, 0.14); }
  .small { font-size: 12px; color: var(--muted); margin-top: 6px; word-break: break-word; }
  .groups { display: flex; flex-wrap: wrap; gap: 5px; margin-top: 7px; max-height: 105px; overflow-y: auto; }
  .gbtn { width: auto; margin: 0; padding: 4px 9px; font-size: 11px; background: rgba(30, 41, 75, 0.9); border: 1px solid rgba(139, 92, 246, 0.3); box-shadow: none; }
</style>
</head>
<body>

<div class="box">
  <h3>وضعیت زنده و مأموریت‌ها</h3>
  <div id="info_list"><div class="small">مأموریتی ثبت نشده است.</div></div>
  <div class="small" id="status_txt">-</div>
</div>

<div class="box">
  <h3>مدیریت اکانت‌ها (افزودن، انتخاب و حذف)</h3>
  <label>اکانت فعال برای عملیات:</label>
  <select id="active_acc_sel" onchange="selectAccount(this.value)"></select>

  <label>شماره تلفن جدید یا موجود:</label>
  <input id="phone" type="text" placeholder="+98912..." dir="ltr">
  <button class="btn-cyan" onclick="sendPhone()">ارسال کد ورود</button>

  <div id="code_box" style="display:none; margin-top:8px;">
    <label>کد تأیید:</label>
    <input id="code" type="text" placeholder="12345" dir="ltr">
    <button class="btn-green" onclick="sendCode()">تأیید و ثبت اکانت</button>
  </div>
  <div class="small" id="auth_msg">-</div>

  <div id="accounts_list" style="margin-top:10px;"></div>
</div>

<div class="box">
  <h3>ارسال تکراری / اسپم</h3>
  <button class="btn-amber" onclick="loadGroups()">دریافت لیست گروه‌ها و کانال‌ها</button>
  <div id="groups" class="groups"></div>

  <label>مقصد (لینک، @یوزرنیم یا آیدی):</label>
  <input id="target" type="text" placeholder="لینک، @username یا آیدی...">

  <label>متن‌ها:</label>
  <div id="texts">
    <div class="row">
      <textarea class="t-inp" style="flex:1" placeholder="متن ۱..."></textarea>
      <button type="button" class="mini btn-red" onclick="delText(this)">X</button>
    </div>
  </div>
  <button type="button" onclick="addText()">افزودن متن دیگر</button>

  <label>فاصله زمانی (ثانیه):</label>
  <input id="speed" type="number" value="1" step="0.05" min="0.05">

  <label>تعداد ارسال (0 = نامحدود):</label>
  <input id="max_count" type="number" value="0" step="1" min="0">

  <button class="btn-green" onclick="addJob()">شروع عملیات</button>
  <div class="small" id="add_msg"></div>

  <h3 style="margin-top:14px;">لیست مأموریت‌های ارسال</h3>
  <div id="jobs_list"><div class="small">موردی ثبت نشده است.</div></div>
</div>

<div class="box">
  <h3>فوروارد پیام</h3>
  <button class="btn-amber" onclick="loadGroups()">بروزرسانی لیست گروه‌ها و کانال‌ها</button>

  <label>مبدأ:</label>
  <select id="fwd_src_sel" onchange="document.getElementById('fwd_src').value=this.value">
    <option value="">-- انتخاب از لیست --</option>
  </select>
  <input id="fwd_src" type="text" placeholder="آیدی، @username یا لینک مبدأ..." style="margin-top:6px;">

  <button type="button" onclick="loadLast10()">نمایش ۱۰ پیام اخیر مبدأ</button>
  <div id="last10_box"><div class="small">پیامی بارگذاری نشده است.</div></div>

  <label>پیام انتخاب‌شده (آیدی پیام):</label>
  <input id="fwd_msg_id" type="text" placeholder="با کلیک روی پیام بالا پر می‌شود" dir="ltr">
  <div class="small" id="fwd_msg_prev">متن انتخاب‌شده: -</div>

  <label>مقصد:</label>
  <select id="fwd_tgt_sel" onchange="document.getElementById('fwd_tgt').value=this.value">
    <option value="">-- انتخاب از لیست --</option>
  </select>
  <input id="fwd_tgt" type="text" placeholder="آیدی، @username یا لینک مقصد..." style="margin-top:6px;">

  <label>فاصله زمانی (ثانیه):</label>
  <input id="fwd_speed" type="number" value="1" step="0.1" min="0.05">

  <label>تعداد فوروارد (0 = نامحدود):</label>
  <input id="fwd_max" type="number" value="0" step="1" min="0">

  <button class="btn-green" onclick="addFwdJob()">شروع فوروارد</button>
  <div class="small" id="fwd_add_msg"></div>

  <h3 style="margin-top:14px;">لیست فورواردها</h3>
  <div id="fwd_jobs_list"><div class="small">فورواردی ثبت نشده است.</div></div>
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
async function selectAccount(id){
  if(!id) return;
  await fetch('/api/accounts/select?id=' + encodeURIComponent(id));
  setTimeout(tick, 300);
}
async function deleteAccount(id){
  await fetch('/api/accounts/delete?id=' + encodeURIComponent(id));
  setTimeout(tick, 300);
}

function renderAccounts(accs, activeId){
  const sel = document.getElementById('active_acc_sel');
  const box = document.getElementById('accounts_list');
  if(!Array.isArray(accs) || !accs.length){
    sel.innerHTML = '<option value="">اکانتی ثبت نشده است</option>';
    box.innerHTML = '<div class="small">هنوز اکانتی اضافه نشده است.</div>';
    return;
  }
  sel.innerHTML = accs.map(a =>
    `<option value="${esc(a.id)}" ${a.id === activeId ? 'selected' : ''}>${esc(a.name)} (${esc(a.phone || a.id)}) - ${esc(a.ping)}</option>`
  ).join('');

  box.innerHTML = accs.map(a => {
    const isAct = a.id === activeId;
    return `<div class="card ${isAct ? 'active-acc' : ''}">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:6px;">
        <b>${esc(a.name)}</b>
        <span class="badge ${a.ready ? '' : 'badge-off'}">${a.ready ? ('آنلاین · ' + esc(a.ping)) : 'آفلاین'}</span>
      </div>
      <div class="small" style="margin-top:3px;" dir="ltr">${esc(a.phone || '-')}</div>
      <div class="small" style="margin-top:2px;">${esc(a.status || '')}</div>
      <div class="row">
        ${!isAct ? `<button type="button" class="mini btn-cyan" onclick="selectAccount('${esc(a.id)}')">انتخاب فعال</button>` : ''}
        <button type="button" class="mini btn-red" onclick="deleteAccount('${esc(a.id)}')">حذف اکانت</button>
      </div>
    </div>`;
  }).join('');
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
    if(!data.ok){ box.innerHTML = `<div class="small" style="color:#fca5a5;">خطا: ${esc(data.error)}</div>`; return; }
    const msgs = data.messages || [];
    if(!msgs.length){ box.innerHTML = '<div class="small">پیامی یافت نشد.</div>'; return; }
    box.innerHTML = msgs.map(m => {
      const b64 = btoa(unescape(encodeURIComponent(m.text || '')));
      const s64 = btoa(unescape(encodeURIComponent(m.summary || '')));
      return `<div class="msg-item" onclick="pickFwdMsg(this, '${esc(m.id)}', decodeURIComponent(escape(atob('${b64}'))), decodeURIComponent(escape(atob('${s64}'))))">
        <div><b>#${m.index}</b> (ID: <span dir="ltr">${esc(m.id)}</span>)</div>
        <div style="margin-top:3px;color:#cbd5e1;">${esc(m.summary)}</div>
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
      <b>[ارسال]</b> ${esc(j.target_name)} (${esc(j.target)}) · <span style="color:#c4b5fd">${esc(j.account_name || j.account_id || '')}</span><br>
      <b>سرعت:</b> ${j.speed}s | <b>ارسال:</b> ${lim} | <b>آخرین:</b> ${esc(j.last_time||'-')}<br>
      <b>وضعیت:</b> ${j.running ? 'فعال' : 'متوقف'} (${esc(j.last_status||'')})
      ${j.last_err ? `<br><span style="color:#fca5a5" dir="ltr">${esc(j.last_err)}</span>` : ''}
    </div>`;
  });

  const fwdCards = (fwds || []).map(j => {
    const lim = j.max_count > 0 ? `${j.sent} / ${j.max_count}` : `${j.sent} / ∞`;
    return `<div class="card">
      <b>[فوروارد]</b> ${esc(j.source_name||j.source)} ← ${esc(j.target_name||j.target)} · <span style="color:#67e8f9">${esc(j.account_name || j.account_id || '')}</span><br>
      <b>سرعت:</b> ${j.speed}s | <b>ارسال:</b> ${lim} | <b>آخرین:</b> ${esc(j.last_time||'-')}<br>
      <b>وضعیت:</b> ${j.running ? 'فعال' : 'متوقف'} (${esc(j.last_status||'')})
      ${j.last_err ? `<br><span style="color:#fca5a5" dir="ltr">${esc(j.last_err)}</span>` : ''}
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
  `).join('') : '<div class="small">موردی ثبت نشده است.</div>';

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
  `).join('') : '<div class="small">فورواردی ثبت نشده است.</div>';
}

async function tick(){
  try{
    const r = await fetch('/api/state');
    const d = await r.json();
    document.getElementById('status_txt').innerText = d.status || '-';
    document.getElementById('auth_msg').innerText = d.auth_msg || '-';
    if(d.auth_status === 'connected'){ manualCodeBox = false; }
    const showCode = manualCodeBox || (d.auth_status === 'code_sent');
    document.getElementById('code_box').style.display = showCode ? 'block' : 'none';
    renderAccounts(d.accounts || [], d.active_account_id || '');
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
        pending_login["phone"] = p
        pending_login["auth_status"] = "code_sent"
        pending_login["auth_msg"] = "در حال ارسال کد..."
        run_in_bot_loop(_do_send_code(p))
    return "", 200


@app.route("/api/verify_code")
def api_verify_code():
    c = (request.args.get("code") or "").strip()
    if c:
        run_in_bot_loop(_do_verify_code(c))
    return "", 200


@app.route("/api/accounts/select")
def api_select_account():
    global active_account_id
    aid = (request.args.get("id") or "").strip()
    if aid and get_acc(aid):
        active_account_id = aid
        save_accounts()
    return "", 200


@app.route("/api/accounts/delete")
def api_delete_account():
    aid = (request.args.get("id") or "").strip()
    if aid:
        run_in_bot_loop_wait(_do_delete_account(aid), timeout=10)
    return "", 200


@app.route("/api/groups")
def api_groups():
    act = get_active_acc()
    if not act:
        return json_resp([])
    aid = act["id"]
    if act.get("ready") and not groups_map.get(aid):
        run_in_bot_loop_wait(refresh_groups(aid), timeout=15)
    return json_resp(groups_map.get(aid, []))


@app.route("/api/add_job")
def api_add_job():
    global spam_seq
    act = get_active_acc()
    if not act:
        return json_resp({"ok": False, "msg": "ابتدا یک اکانت اضافه و فعال کنید."})

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
        "account_id": act["id"],
        "account_name": act.get("name") or act["id"],
        "target": tgt,
        "target_name": resolve_name(act["id"], tgt),
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
    return json_resp({"ok": True, "msg": "مأموریت ثبت شد."})


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
    act = get_active_acc()
    aid = act["id"] if act else ""
    src = (request.args.get("source") or "").strip()
    res = run_in_bot_loop_wait(_do_fetch_last10(src, aid), timeout=18)
    return json_resp(res if isinstance(res, dict) else {"ok": False, "error": "پاسخ دریافت نشد.", "messages": []})


@app.route("/api/forward/add")
def api_fwd_add():
    global fwd_seq
    act = get_active_acc()
    if not act:
        return json_resp({"ok": False, "msg": "ابتدا یک اکانت اضافه و فعال کنید."})

    src = parse_target((request.args.get("source") or "").strip())
    tgt = parse_target((request.args.get("target") or "").strip())
    msg_id = (request.args.get("msg_id") or "").strip()
    raw_txt = (request.args.get("msg_text") or "").replace("_NEWLINE_", "\n").strip()
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
        "account_id": act["id"],
        "account_name": act.get("name") or act["id"],
        "source": src,
        "source_name": resolve_name(act["id"], src),
        "target": tgt,
        "target_name": resolve_name(act["id"], tgt),
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
    acc_view = [
        {
            "id": a["id"],
            "phone": a.get("phone", ""),
            "name": a.get("name", a["id"]),
            "ready": bool(a.get("ready")),
            "ping": a.get("ping", "-"),
            "status": a.get("status", ""),
        }
        for a in accounts
    ]
    return json_resp({
        "status": global_status,
        "auth_status": pending_login["auth_status"],
        "auth_msg": pending_login["auth_msg"],
        "active_account_id": active_account_id,
        "accounts": acc_view,
        "jobs": spam_jobs,
        "forwards": forward_jobs,
    })


def run_web():
    app.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True)


# ================= موتور اجرای مداوم و نگهداری اتصال =================
async def _send_spam_message(acc_id: str, peer, msg: str):
    c = clients_map.get(acc_id)
    if not c:
        raise RuntimeError("کلاینت اکانت فعال نیست.")
    ensure_locks(acc_id)
    async with io_locks[acc_id]:
        return await asyncio.wait_for(c.send_message(peer, msg), timeout=12)


async def _resolve_fwd_fallback_text(c, src_peer, msg_id_val, fallback_text: str) -> str:
    txt = (fallback_text or "").strip()
    if txt and txt != "[پیام مدیا / بدون متن]":
        return txt
    if hasattr(c, "get_messages"):
        try:
            res = await asyncio.wait_for(c.get_messages(src_peer, ids=msg_id_val), timeout=10)
            if res:
                first = res[0] if isinstance(res, (list, tuple)) else res
                extracted = _extract_msg_text(first)
                if extracted:
                    return extracted
        except Exception:
            pass
    return txt or f"پیام #{msg_id_val}"


async def _send_forward_message(acc_id: str, src_peer, tgt_peer, msg_id_str: str, fallback_text: str):
    c = clients_map.get(acc_id)
    if not c:
        raise RuntimeError("کلاینت اکانت فعال نیست.")
    msg_id_val = int(msg_id_str) if str(msg_id_str).isdigit() else msg_id_str
    ensure_locks(acc_id)

    async with io_locks[acc_id]:
        fwd_err = None
        if hasattr(c, "forward_messages"):
            try:
                return await asyncio.wait_for(
                    c.forward_messages(tgt_peer, msg_id_val, from_peer=src_peer),
                    timeout=14,
                )
            except TypeError:
                try:
                    return await asyncio.wait_for(
                        c.forward_messages(tgt_peer, msg_id_val, src_peer),
                        timeout=14,
                    )
                except Exception as e:
                    fwd_err = e
            except Exception as e:
                fwd_err = e
        elif hasattr(c, "forward_message"):
            try:
                return await asyncio.wait_for(
                    c.forward_message(tgt_peer, msg_id_val, from_peer=src_peer),
                    timeout=14,
                )
            except TypeError:
                try:
                    return await asyncio.wait_for(
                        c.forward_message(tgt_peer, msg_id_val, src_peer),
                        timeout=14,
                    )
                except Exception as e:
                    fwd_err = e
            except Exception as e:
                fwd_err = e

        if fwd_err is not None and is_auth_key_error(fwd_err):
            raise fwd_err

        text_to_send = await _resolve_fwd_fallback_text(c, src_peer, msg_id_val, fallback_text)
        return await asyncio.wait_for(c.send_message(tgt_peer, text_to_send), timeout=14)


async def spam_worker(job: dict):
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

            acc_id = job.get("account_id") or (get_active_acc()["id"] if get_active_acc() else "")
            acc = get_acc(acc_id)
            if not acc or not acc.get("ready"):
                job["last_status"] = "در انتظار اتصال اکانت..."
                await asyncio.sleep(0.7)
                continue

            t0 = time.perf_counter()
            txts = job.get("texts") or ["تست"]
            i = int(job.get("idx") or 0) % len(txts)
            msg = txts[i]
            peer = to_peer(job["target"])

            await _send_spam_message(acc_id, peer, msg)

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
            acc_id = job.get("account_id") or ""
            if is_auth_key_error(e) and acc_id:
                await _reset_invalid_account(acc_id, "سشن نامعتبر شد؛ کد جدید بگیرید.")
                job["last_err"] = "AuthKeyUnregisteredError"
                job["last_status"] = "نیاز به ورود مجدد اکانت"
                await asyncio.sleep(2.0)
                continue

            err_streak += 1
            err_msg = str(e)[:120]
            job["last_err"] = err_msg
            job["last_status"] = f"اختلال کوتاه؛ ادامه همان پیام ({err_streak})"
            if err_streak >= 5 and acc_id:
                acc = get_acc(acc_id)
                if acc:
                    acc["ready"] = False
                await ensure_account_connected(acc_id, force_rebuild=True)
                err_streak = 0
            await asyncio.sleep(0.7)


async def forward_worker(job: dict):
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

            acc_id = job.get("account_id") or (get_active_acc()["id"] if get_active_acc() else "")
            acc = get_acc(acc_id)
            if not acc or not acc.get("ready"):
                job["last_status"] = "در انتظار اتصال اکانت..."
                await asyncio.sleep(0.7)
                continue

            t0 = time.perf_counter()
            await _send_forward_message(
                acc_id,
                to_peer(job["source"]),
                to_peer(job["target"]),
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
            acc_id = job.get("account_id") or ""
            if is_auth_key_error(e) and acc_id:
                await _reset_invalid_account(acc_id, "سشن نامعتبر شد؛ کد جدید بگیرید.")
                job["last_err"] = "AuthKeyUnregisteredError"
                job["last_status"] = "نیاز به ورود مجدد اکانت"
                await asyncio.sleep(2.0)
                continue

            err_streak += 1
            err_msg = str(e)[:120]
            job["last_err"] = err_msg
            job["last_status"] = f"اختلال کوتاه؛ ادامه فوروارد ({err_streak})"
            if err_streak >= 5 and acc_id:
                acc = get_acc(acc_id)
                if acc:
                    acc["ready"] = False
                await ensure_account_connected(acc_id, force_rebuild=True)
                err_streak = 0
            await asyncio.sleep(0.7)


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


async def keepalive_loop():
    global global_status
    while True:
        try:
            now_m = time.monotonic()
            online_count = 0
            for acc in list(accounts):
                aid = acc["id"]
                if pending_login["in_progress"] and pending_login["acc_id"] == aid:
                    continue
                if not (acc.get("session") or "").strip():
                    continue

                last_ka = last_keepalive.get(aid, 0.0)
                need_10m_ping = (now_m - last_ka) >= 600.0

                if not acc.get("ready") or need_10m_ping:
                    ok = await ensure_account_connected(aid, force_rebuild=False)
                    if ok:
                        last_keepalive[aid] = time.monotonic()
                        if not groups_map.get(aid):
                            await refresh_groups(aid)
                if acc.get("ready"):
                    online_count += 1

            running_jobs = sum(1 for j in spam_jobs if j.get("running")) + sum(1 for j in forward_jobs if j.get("running"))
            global_status = f"اکانت‌های آنلاین: {online_count}/{len(accounts)} | تسک‌های فعال: {running_jobs} | آخرین بررسی: {now_clock()}"
        except Exception:
            pass
        await asyncio.sleep(8)


async def main_loop():
    global bot_loop
    bot_loop = asyncio.get_running_loop()
    asyncio.create_task(supervisor_loop())
    asyncio.create_task(keepalive_loop())

    for acc in list(accounts):
        if (acc.get("session") or "").strip():
            ok = await ensure_account_connected(acc["id"], force_rebuild=True)
            if ok:
                last_keepalive[acc["id"]] = time.monotonic()
                await refresh_groups(acc["id"])

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
