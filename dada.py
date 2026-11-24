import asyncio
import json
import logging
import os
import re
import socket
import time
from datetime import datetime
from threading import Thread
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from flask import Flask, request
from splusthon import SoroushClient
from splusthon.sessions import StringSession

# ================= تنظیمات اصلی =================
ACCOUNTS_FILE = "soroush_accounts.json"
LEGACY_SESSION_FILE = "soroush_session.txt"
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

accounts_data = {
    "active_id": "",
    "accounts": [],
}

spam_jobs = []
forward_jobs = []
spam_seq = 0
fwd_seq = 0

state = {
    "status": "در حال راه‌اندازی...",
    "net_status": "در حال بررسی نت...",
    "last_err": "-",
    "ping": "-",
    "last_keepalive": "-",
    "account_name": "بدون اکانت",
    "auth_status": "disconnected",
    "auth_msg": "برای افزودن اکانت جدید، شماره تلفن را وارد کنید (مثال: +98912...)",
    "phone": "",
}

client = None
group_list = []
bot_loop = None
phone_code_hash = None
pending_acc_id = None
login_in_progress = False
client_ready = False

session_lock = asyncio.Lock()
send_lock = asyncio.Lock()

web_thread = None
bot_thread = None


def now_clock() -> str:
    return datetime.now().strftime("%H:%M:%S")


def set_err(msg: str):
    state["last_err"] = str(msg or "-")[:200]
    if msg and msg != "-":
        print(f"[{now_clock()}] ERR: {state['last_err']}", flush=True)


def check_network_access() -> tuple[bool, str]:
    """بررسی واقعی دسترسی شبکه و اتصال به splus.ir"""
    proxy_env = (
        os.environ.get("HTTPS_PROXY")
        or os.environ.get("HTTP_PROXY")
        or os.environ.get("ALL_PROXY")
        or ""
    ).strip()

    try:
        sock = socket.create_connection(("8.8.8.8", 53), timeout=3)
        sock.close()
    except Exception as e:
        info = f"نت قطع است (عدم دسترسی شبکه: {e})"
        if proxy_env:
            info += f" | پراکسی: {proxy_env}"
        return False, info

    try:
        req = Request("https://splus.ir", headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=5) as resp:
            status_code = getattr(resp, "status", 200)
            msg = f"نت وصل است (splus.ir: {status_code})"
            if proxy_env:
                msg += " | با پراکسی"
            return True, msg
    except Exception as e:
        msg = f"شبکه هست ولی اتصال به splus.ir ناموفق است: {e}"
        if proxy_env:
            msg += f" | پراکسی: {proxy_env}"
        return False, msg


def is_auth_key_error(err) -> bool:
    s = str(err or "").lower()
    return (
        "authkeyunregistered" in s
        or "key is not registered" in s
        or "auth_key_unregistered" in s
    )


def get_active_account() -> dict | None:
    aid = accounts_data.get("active_id") or ""
    for a in accounts_data.get("accounts") or []:
        if a.get("id") == aid:
            return a
    if accounts_data.get("accounts"):
        first = accounts_data["accounts"][0]
        accounts_data["active_id"] = first["id"]
        return first
    return None


def save_accounts():
    try:
        with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
            json.dump(accounts_data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def load_accounts():
    global accounts_data
    if os.path.exists(ACCOUNTS_FILE):
        try:
            with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
                if isinstance(raw, dict) and isinstance(raw.get("accounts"), list):
                    accounts_data["active_id"] = str(raw.get("active_id") or "")
                    accounts_data["accounts"] = raw["accounts"]
        except Exception:
            pass

    if not accounts_data["accounts"] and os.path.exists(LEGACY_SESSION_FILE):
        try:
            with open(LEGACY_SESSION_FILE, "r", encoding="utf-8") as f:
                old_sess = f.read().strip()
            if old_sess:
                acc_id = "acc_legacy"
                accounts_data["accounts"].append({
                    "id": acc_id,
                    "name": "اکانت سروش",
                    "phone": "",
                    "session": old_sess,
                    "connected": False,
                })
                accounts_data["active_id"] = acc_id
                save_accounts()
        except Exception:
            pass

    act = get_active_account()
    if act:
        state["account_name"] = act.get("name") or "اکانت سروش"
        state["phone"] = act.get("phone") or ""
    else:
        state["account_name"] = "بدون اکانت"


def get_active_session() -> str:
    act = get_active_account()
    return (act.get("session") or "").strip() if act else ""


def set_active_session(sess: str, acc_name: str = "", phone: str = ""):
    act = get_active_account()
    if not act:
        acc_id = f"acc_{int(time.time()*1000)}"
        act = {
            "id": acc_id,
            "name": acc_name or phone or "اکانت سروش",
            "phone": phone or state.get("phone") or "",
            "session": sess or "",
            "connected": bool(sess),
        }
        accounts_data["accounts"].append(act)
        accounts_data["active_id"] = acc_id
    else:
        act["session"] = sess or ""
        act["connected"] = bool(sess)
        if acc_name:
            act["name"] = acc_name
        if phone:
            act["phone"] = phone

    state["account_name"] = act.get("name") or "اکانت سروش"
    save_accounts()


def clear_active_session():
    act = get_active_account()
    if act:
        act["session"] = ""
        act["connected"] = False
        save_accounts()


def load_jobs():
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
                            j["last_status"] = "دائماً فعال؛ ادامه اسپم"
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
                            j["last_status"] = "دائماً فعال؛ ادامه فوروارد"
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


def _account_label(me) -> str:
    if not me:
        act = get_active_account()
        return (act.get("name") if act else "") or "اکانت سروش"
    fn = (getattr(me, "first_name", None) or "").strip()
    ln = (getattr(me, "last_name", None) or "").strip()
    un = (getattr(me, "username", None) or "").strip()
    full = f"{fn} {ln}".strip()
    if full:
        return full
    if un:
        return f"@{un}"
    uid = getattr(me, "id", None)
    return str(uid) if uid is not None else "اکانت سروش"


def _dialog_item(d):
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


def _msg_text(m) -> str:
    for attr in ("text", "message", "raw_text", "caption", "body", "content"):
        val = getattr(m, attr, None)
        if isinstance(val, str) and val.strip():
            return val.strip()
    if isinstance(m, dict):
        for k in ("text", "message", "raw_text", "caption", "body"):
            if m.get(k):
                return str(m[k]).strip()
    if getattr(m, "media", None):
        return "[پیام مدیا]"
    return ""


def _msg_id(m, idx: int) -> int:
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
    return idx


def _active_work_summary() -> str:
    sp = len([j for j in spam_jobs if j.get("running")])
    fw = len([j for j in forward_jobs if j.get("running")])
    items = []
    if sp:
        items.append(f"{sp} اسپم")
    if fw:
        items.append(f"{fw} فوروارد")
    return " و ".join(items)


load_accounts()
load_jobs()


# ================= هسته اتصال سروش =================
async def _close_client(c):
    if c is None:
        return
    for attr in ("_client", "client", "_session_http", "http_session", "_sender", "_updates"):
        sub = getattr(c, attr, None)
        if sub is not None and hasattr(sub, "close"):
            try:
                res = sub.close()
                if asyncio.iscoroutine(res):
                    await asyncio.wait_for(res, timeout=2)
            except Exception:
                pass
    if hasattr(c, "disconnect"):
        try:
            res = c.disconnect()
            if asyncio.iscoroutine(res):
                await asyncio.wait_for(res, timeout=2)
        except Exception:
            pass


async def _invalidate_session(msg: str = "سشن معتبر نیست؛ شماره و کد جدید وارد کنید."):
    global client, client_ready, group_list
    client_ready = False
    group_list = []
    clear_active_session()
    await _close_client(client)
    client = None
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
    if client is not None:
        for attr in ("_phone_code_hash", "phone_code_hash", "code_hash"):
            val = getattr(client, attr, None)
            if isinstance(val, dict) and phone in val:
                return str(val[phone])
            if isinstance(val, str) and val:
                return val
    return None


async def connect_soroush(force_new: bool = False) -> bool:
    global client, client_ready
    if login_in_progress:
        return False

    net_ok, net_msg = check_network_access()
    state["net_status"] = net_msg

    act = get_active_account()
    sess = get_active_session()
    if not act or not sess:
        client_ready = False
        state["ping"] = "قطع"
        if not act:
            state["account_name"] = "بدون اکانت"
        if state.get("auth_status") != "code_sent":
            state["auth_status"] = "disconnected"
            state["auth_msg"] = "اکانت فعالی با سشن وجود ندارد؛ شماره را وارد کنید."
        return False

    state["account_name"] = act.get("name") or "اکانت سروش"

    if not net_ok:
        client_ready = False
        state["ping"] = "بدون نت"
        state["status"] = net_msg
        return False

    async with session_lock:
        if not force_new and client is not None and client_ready:
            try:
                t0 = time.perf_counter()
                me = await asyncio.wait_for(client.get_me(), timeout=6)
                ms = int((time.perf_counter() - t0) * 1000)
                acc_label = _account_label(me)
                act["name"] = acc_label
                act["connected"] = True
                save_accounts()
                state["account_name"] = acc_label
                state["ping"] = f"{ms} ms"
                state["auth_status"] = "connected"
                state["auth_msg"] = f"متصل به سروش: {acc_label}"
                set_err("-")
                work = _active_work_summary()
                state["status"] = (
                    f"دائماً فعال ({acc_label}) | در حال اجرای {work}"
                    if work
                    else f"دائماً فعال ({acc_label}) | آماده"
                )
                return True
            except Exception as e:
                set_err(f"get_me: {type(e).__name__}: {e}")
                if is_auth_key_error(e):
                    await _invalidate_session("سشن معتبر نیست؛ شماره و کد جدید وارد کنید.")
                    return False
                client_ready = False

        try:
            if force_new or client is None:
                await _close_client(client)
                client = SoroushClient(StringSession(sess))
            t0 = time.perf_counter()
            await asyncio.wait_for(client.connect(), timeout=12)
            me = await asyncio.wait_for(client.get_me(), timeout=8)
            ms = int((time.perf_counter() - t0) * 1000)
            acc_label = _account_label(me)

            act["name"] = acc_label
            act["connected"] = True
            save_accounts()

            client_ready = True
            state["account_name"] = acc_label
            state["ping"] = f"{ms} ms"
            state["auth_status"] = "connected"
            state["auth_msg"] = f"متصل به سروش: {acc_label}"
            set_err("-")
            work = _active_work_summary()
            state["status"] = (
                f"دائماً فعال ({acc_label}) | در حال اجرای {work}"
                if work
                else f"دائماً فعال ({acc_label}) | آماده"
            )
            return True
        except Exception as e:
            err_txt = f"connect: {type(e).__name__}: {e}"
            set_err(err_txt)
            if is_auth_key_error(e):
                await _invalidate_session("سشن معتبر نیست؛ شماره و کد جدید وارد کنید.")
                return False
            client_ready = False
            act["connected"] = False
            state["ping"] = "قطع"
            state["status"] = f"خطا در اتصال سروش: {e}"
            return False


async def _do_send_code(phone: str):
    global client, phone_code_hash, pending_acc_id, login_in_progress, client_ready
    login_in_progress = True
    client_ready = False
    phone_code_hash = None

    net_ok, net_msg = check_network_access()
    state["net_status"] = net_msg

    existing = next((a for a in accounts_data["accounts"] if a.get("phone") == phone), None)
    if existing:
        pending_acc_id = existing["id"]
        accounts_data["active_id"] = pending_acc_id
    else:
        pending_acc_id = f"acc_{int(time.time()*1000)}"
        accounts_data["accounts"].append({
            "id": pending_acc_id,
            "name": phone,
            "phone": phone,
            "session": "",
            "connected": False,
        })
        accounts_data["active_id"] = pending_acc_id
    save_accounts()

    state["phone"] = phone
    state["account_name"] = phone
    state["auth_status"] = "code_sent"
    state["auth_msg"] = "در حال ارسال کد به سروش..."
    try:
        async with session_lock:
            await _close_client(client)
            client = SoroushClient(StringSession())
            await asyncio.wait_for(client.connect(), timeout=12)

            sent_res = None
            if hasattr(client, "send_code_request"):
                sent_res = await asyncio.wait_for(client.send_code_request(phone), timeout=12)
            elif hasattr(client, "send_code"):
                sent_res = await asyncio.wait_for(client.send_code(phone), timeout=12)
            else:
                sent_res = await asyncio.wait_for(client.sign_in(phone=phone), timeout=12)

            phone_code_hash = _extract_code_hash(sent_res, phone)
        state["auth_status"] = "code_sent"
        state["auth_msg"] = "کد ارسال شد؛ کد را وارد کنید."
        set_err("-")
    except Exception as e:
        set_err(f"send_code: {type(e).__name__}: {e}")
        state["auth_status"] = "code_sent"
        state["auth_msg"] = f"خطا در ارسال کد: {e}"


async def _do_verify_code(code: str):
    global phone_code_hash, login_in_progress, client_ready
    state["auth_status"] = "code_sent"
    state["auth_msg"] = "در حال تأیید کد..."
    phone = state.get("phone", "").strip()
    try:
        async with session_lock:
            if client is None or not hasattr(client, "sign_in"):
                raise RuntimeError("ابتدا شماره را ارسال کنید.")

            signed = False
            last_exc = None

            if phone_code_hash:
                for call in (
                    lambda: client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash),
                    lambda: client.sign_in(phone, code, phone_code_hash),
                ):
                    try:
                        await asyncio.wait_for(call(), timeout=12)
                        signed = True
                        break
                    except TypeError:
                        continue
                    except Exception as e:
                        last_exc = e

            if not signed:
                try:
                    await asyncio.wait_for(client.sign_in(phone=phone, code=code), timeout=12)
                    signed = True
                except Exception as e:
                    last_exc = e

            if not signed:
                raise RuntimeError(str(last_exc or "ورود ناموفق بود."))

            sess = client.session.save() if getattr(client, "session", None) else ""
            if not sess:
                raise RuntimeError("سشن ذخیره نشد.")

            acc_label = phone or "اکانت سروش"
            try:
                t0 = time.perf_counter()
                me = await asyncio.wait_for(client.get_me(), timeout=8)
                ms = int((time.perf_counter() - t0) * 1000)
                acc_label = _account_label(me)
                state["ping"] = f"{ms} ms"
            except Exception:
                pass

            set_active_session(sess, acc_name=acc_label, phone=phone)
            phone_code_hash = None
            login_in_progress = False
            client_ready = True

            state["account_name"] = acc_label
            state["auth_status"] = "connected"
            state["auth_msg"] = f"متصل به سروش: {acc_label}"
            state["status"] = f"دائماً فعال ({acc_label}) | آماده"
            set_err("-")

        await refresh_groups()
    except Exception as e:
        set_err(f"verify_code: {type(e).__name__}: {e}")
        state["auth_status"] = "code_sent"
        state["auth_msg"] = f"خطا در کد: {e}"


async def _do_switch_account(acc_id: str):
    global client, client_ready, group_list, login_in_progress
    target = next((a for a in accounts_data["accounts"] if a.get("id") == acc_id), None)
    if not target:
        return
    login_in_progress = False
    client_ready = False
    group_list = []
    await _close_client(client)
    client = None
    accounts_data["active_id"] = acc_id
    save_accounts()
    state["account_name"] = target.get("name") or "اکانت سروش"
    state["phone"] = target.get("phone") or ""
    state["status"] = f"در حال اتصال به اکانت {state['account_name']}..."
    await connect_soroush(force_new=True)
    if client_ready:
        await refresh_groups()


async def _do_delete_account(acc_id: str):
    global client, client_ready, group_list, login_in_progress
    was_active = accounts_data.get("active_id") == acc_id
    accounts_data["accounts"] = [a for a in accounts_data["accounts"] if a.get("id") != acc_id]
    if was_active:
        login_in_progress = False
        client_ready = False
        group_list = []
        await _close_client(client)
        client = None
        next_acc = accounts_data["accounts"][0] if accounts_data["accounts"] else None
        accounts_data["active_id"] = next_acc["id"] if next_acc else ""
        save_accounts()
        if next_acc:
            state["account_name"] = next_acc.get("name") or "اکانت سروش"
            state["phone"] = next_acc.get("phone") or ""
            await connect_soroush(force_new=True)
            if client_ready:
                await refresh_groups()
        else:
            state["account_name"] = "بدون اکانت"
            state["phone"] = ""
            state["ping"] = "قطع"
            state["auth_status"] = "disconnected"
            state["auth_msg"] = "اکانت حذف شد؛ شماره جدید را وارد کنید."
            state["status"] = "هیچ اکانت فعالی ثبت نشده است."
    else:
        save_accounts()


async def refresh_groups():
    global group_list
    if not client_ready or client is None:
        return group_list

    async with session_lock:
        try:
            raw_dialogs = []
            if hasattr(client, "get_dialogs"):
                res = await asyncio.wait_for(client.get_dialogs(limit=200), timeout=10)
                raw_dialogs = list(res) if res else []
            elif hasattr(client, "iter_dialogs"):
                async for d in client.iter_dialogs(limit=200):
                    raw_dialogs.append(d)

            gc_items = []
            all_items = []
            for d in raw_dialogs:
                info = _dialog_item(d)
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
            set_err(f"groups: {type(e).__name__}: {e}")
            if is_auth_key_error(e):
                await _invalidate_session("سشن معتبر نیست؛ شماره و کد جدید وارد کنید.")
            return group_list


async def _do_fetch_last10(source_raw: str):
    if not client_ready or client is None:
        return {
            "ok": False,
            "error": "اتصال به سروش برقرار نیست.",
            "messages": [],
        }

    peer = to_peer(source_raw)
    async with session_lock:
        try:
            raw_msgs = []
            if hasattr(client, "get_messages"):
                res = await asyncio.wait_for(client.get_messages(peer, limit=10), timeout=10)
                raw_msgs = list(res) if res else []
            elif hasattr(client, "iter_messages"):
                async for m in client.iter_messages(peer, limit=10):
                    raw_msgs.append(m)
            else:
                raise RuntimeError("امکان خواندن پیام‌ها وجود ندارد.")

            out = []
            for idx, m in enumerate(raw_msgs):
                mid = _msg_id(m, idx + 1)
                full_text = _msg_text(m) or "[پیام مدیا]"
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
            set_err(f"last10: {type(e).__name__}: {e}")
            if is_auth_key_error(e):
                await _invalidate_session("سشن معتبر نیست؛ شماره و کد جدید وارد کنید.")
            return {"ok": False, "error": str(e), "messages": []}


def run_in_bot_loop(coro):
    if bot_loop and not bot_loop.is_closed():
        try:
            asyncio.run_coroutine_threadsafe(coro, bot_loop)
        except Exception as e:
            set_err(f"loop_submit: {e}")


def run_in_bot_loop_wait(coro, timeout=10):
    if not bot_loop or bot_loop.is_closed():
        return None
    try:
        fut = asyncio.run_coroutine_threadsafe(coro, bot_loop)
        return fut.result(timeout=timeout)
    except Exception as e:
        set_err(f"loop_wait: {e}")
        return None


# ================= رابط کاربری ساده و بی‌رنگ =================
PANEL_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>پنل سروش</title>
<style>
  body { font-family: Tahoma, sans-serif; background: #141414; color: #e2e2e2; margin: 0; padding: 12px; }
  .top-header { max-width: 480px; margin: 0 auto 12px; background: #1f1f1f; border: 1px solid #383838; border-radius: 6px; padding: 10px 12px; }
  .top-acc-title { font-size: 15px; font-weight: bold; color: #ffffff; }
  .top-meta { font-size: 11px; color: #b0b0b0; margin-top: 4px; }
  .box { max-width: 480px; margin: 0 auto 12px; background: #1c1c1c; border: 1px solid #333; border-radius: 6px; padding: 12px; }
  h3 { margin: 0 0 10px; font-size: 14px; border-bottom: 1px solid #2e2e2e; padding-bottom: 6px; color: #f0f0f0; }
  label { display: block; font-size: 12px; color: #b0b0b0; margin: 8px 0 4px; }
  input, textarea, select { width: 100%; box-sizing: border-box; padding: 8px; border-radius: 5px; border: 1px solid #3d3d3d; background: #121212; color: #ececec; font-family: Tahoma, sans-serif; }
  textarea { min-height: 48px; resize: vertical; }
  button { width: 100%; padding: 8px; margin-top: 8px; border: 1px solid #4a4a4a; border-radius: 5px; font-family: Tahoma, sans-serif; font-weight: bold; cursor: pointer; background: #2a2a2a; color: #e8e8e8; }
  button:hover { background: #343434; }
  .mini { width: auto; padding: 4px 10px; font-size: 11px; margin-top: 0; }
  .row { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; margin-top: 6px; }
  .card { background: #141414; border: 1px solid #2d2d2d; border-radius: 5px; padding: 8px; margin-top: 8px; font-size: 12px; line-height: 1.7; }
  .msg-item { background: #141414; border: 1px solid #2d2d2d; border-radius: 5px; padding: 8px; margin-top: 6px; font-size: 12px; cursor: pointer; }
  .msg-item.selected { border-color: #888; background: #242424; }
  .small { font-size: 12px; color: #a8a8a8; margin-top: 6px; word-break: break-word; }
  .groups { display: flex; flex-wrap: wrap; gap: 5px; margin-top: 6px; max-height: 95px; overflow-y: auto; }
  .gbtn { width: auto; margin: 0; padding: 4px 8px; font-size: 11px; background: #232323; border: 1px solid #3c3c3c; color: #dcdcdc; }
  .acc-badge { font-size: 11px; color: #cccccc; border: 1px solid #454545; padding: 1px 6px; border-radius: 4px; }
</style>
</head>
<body>

<div class="top-header">
  <div class="top-acc-title">اکانت: <span id="top_acc_name">بدون اکانت</span></div>
  <div class="top-meta">پینگ: <span id="ping">-</span> | پالس ۱۰دقیقه: <span id="ka">-</span></div>
  <div class="top-meta">نت: <span id="net_st">-</span></div>
  <div class="top-meta">آخرین خطا: <span id="err_st" dir="ltr">-</span></div>
</div>

<div class="box">
  <h3>مدیریت اکانت‌ها</h3>
  <div id="accounts_list"><div class="small">اکانتی ثبت نشده است.</div></div>

  <label>افزودن اکانت جدید (شماره تلفن):</label>
  <input id="phone" type="text" placeholder="+98912..." dir="ltr">
  <button onclick="sendPhone()">افزودن اکانت و ارسال کد</button>
  <div id="code_box" style="display:none; margin-top:8px;">
    <label>کد تأیید:</label>
    <input id="code" type="text" placeholder="12345" dir="ltr">
    <button onclick="sendCode()">تأیید کد و ثبت اکانت</button>
  </div>
  <div class="small" id="auth_msg">-</div>
</div>

<div class="box">
  <h3>وضعیت فعالیت</h3>
  <div id="info_list"><div class="small">موردی ثبت نشده است.</div></div>
  <div class="small" id="status_txt">-</div>
</div>

<div class="box">
  <h3>اسپم</h3>
  <button onclick="loadGroups()">دریافت لیست گروه‌ها و کانال‌ها</button>
  <div id="groups" class="groups"></div>

  <label>مقصد (لینک، @یوزرنیم یا آیدی):</label>
  <input id="target" type="text" placeholder="لینک، @username یا آیدی...">

  <label>متن‌های اسپم:</label>
  <div id="texts">
    <div class="row">
      <textarea class="t-inp" style="flex:1" placeholder="متن ۱..."></textarea>
      <button type="button" class="mini" onclick="delText(this)">X</button>
    </div>
  </div>
  <button type="button" onclick="addText()">افزودن متن دیگر</button>

  <label>سرعت (ثانیه):</label>
  <input id="speed" type="number" value="1" step="0.05" min="0.05">

  <label>تعداد ارسال (0 = نامحدود):</label>
  <input id="max_count" type="number" value="0" step="1" min="0">

  <button onclick="addJob()">شروع اسپم</button>
  <div class="small" id="add_msg"></div>

  <h3 style="margin-top:14px;">لیست اسپم‌ها</h3>
  <div id="jobs_list"><div class="small">اسپمی ثبت نشده است.</div></div>
</div>

<div class="box">
  <h3>فوروارد</h3>
  <button onclick="loadGroups()">دریافت لیست گروه‌ها و کانال‌ها</button>

  <label>۱. انتخاب مبدأ از لیست گروه/کانال:</label>
  <div id="fwd_src_groups" class="groups"></div>
  <input id="fwd_src" type="text" placeholder="آیدی، @username یا لینک مبدأ..." style="margin-top:6px;">

  <button type="button" onclick="loadLast10()">نمایش خلاصه ۱۰ پیام اخیر مبدأ</button>
  <div id="last10_box"><div class="small">پیامی بارگذاری نشده است.</div></div>

  <label>۲. پیام انتخاب‌شده:</label>
  <input id="fwd_msg_id" type="text" placeholder="با کلیک روی پیام بالا پر می‌شود" dir="ltr">
  <div class="small" id="fwd_msg_prev">متن انتخاب‌شده: -</div>

  <label>۳. انتخاب مقصد از لیست گروه/کانال:</label>
  <div id="fwd_tgt_groups" class="groups"></div>
  <input id="fwd_tgt" type="text" placeholder="آیدی، @username یا لینک مقصد..." style="margin-top:6px;">

  <label>۴. سرعت فوروارد (ثانیه):</label>
  <input id="fwd_speed" type="number" value="1" step="0.1" min="0.05">

  <label>۵. تعداد فوروارد (0 = نامحدود):</label>
  <input id="fwd_max" type="number" value="0" step="1" min="0">

  <button onclick="addFwdJob()">شروع فوروارد</button>
  <div class="small" id="fwd_add_msg"></div>

  <h3 style="margin-top:14px;">لیست فورواردها</h3>
  <div id="fwd_jobs_list"><div class="small">فورواردی ثبت نشده است.</div></div>
</div>

<script>
let manualCodeBox = false;
let chosenFwdText = '';

function esc(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function jsEsc(s){ return String(s||'').replace(/\\/g,'\\\\').replace(/'/g,"\\'"); }

function addText(){
  const n = document.querySelectorAll('.t-inp').length + 1;
  const d = document.createElement('div');
  d.className = 'row';
  d.innerHTML = `<textarea class="t-inp" style="flex:1" placeholder="متن ${n}..."></textarea><button type="button" class="mini" onclick="delText(this)">X</button>`;
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
async function switchAcc(id){
  await fetch('/api/accounts/switch?id=' + encodeURIComponent(id));
  setTimeout(tick, 500);
}
async function delAcc(id){
  await fetch('/api/accounts/del?id=' + encodeURIComponent(id));
  setTimeout(tick, 500);
}

function renderAccounts(accs, activeId){
  const box = document.getElementById('accounts_list');
  if(!Array.isArray(accs) || !accs.length){
    box.innerHTML = '<div class="small">اکانتی ثبت نشده است.</div>';
    return;
  }
  box.innerHTML = accs.map(a => {
    const isAct = a.id === activeId;
    const statusTxt = a.connected ? 'متصل' : 'در انتظار اتصال';
    return `<div class="card">
      <div><b>${esc(a.name || a.phone || 'اکانت')}</b> ${a.phone ? `<span dir="ltr">(${esc(a.phone)})</span>` : ''} <span class="acc-badge">${isAct ? 'فعال' : statusTxt}</span></div>
      <div class="row">
        ${!isAct ? `<button type="button" class="mini" onclick="switchAcc('${jsEsc(a.id)}')">انتخاب اکانت</button>` : ''}
        <button type="button" class="mini" onclick="delAcc('${jsEsc(a.id)}')">حذف اکانت</button>
      </div>
    </div>`;
  }).join('');
}

function applyGroups(arr){
  const gBox = document.getElementById('groups');
  const sBox = document.getElementById('fwd_src_groups');
  const tBox = document.getElementById('fwd_tgt_groups');
  if(!Array.isArray(arr) || !arr.length){
    const empty = '<span class="small">لیستی ثبت نشده یا اتصال به سروش قطع است</span>';
    gBox.innerHTML = empty;
    sBox.innerHTML = empty;
    tBox.innerHTML = empty;
    return;
  }
  gBox.innerHTML = arr.map(g =>
    `<button type="button" class="gbtn" onclick="document.getElementById('target').value='${jsEsc(g.id)}'">${esc(g.name)}</button>`
  ).join('');
  sBox.innerHTML = arr.map(g =>
    `<button type="button" class="gbtn" onclick="document.getElementById('fwd_src').value='${jsEsc(g.id)}'">${esc(g.name)}</button>`
  ).join('');
  tBox.innerHTML = arr.map(g =>
    `<button type="button" class="gbtn" onclick="document.getElementById('fwd_tgt').value='${jsEsc(g.id)}'">${esc(g.name)}</button>`
  ).join('');
}

async function loadGroups(){
  document.getElementById('groups').innerHTML = '<span class="small">در حال بررسی...</span>';
  try {
    const r = await fetch('/api/groups');
    const arr = await r.json();
    applyGroups(arr);
  } catch(e) {
    applyGroups([]);
  }
}

function pickFwdMsg(el, mid, fullText, summary){
  document.querySelectorAll('.msg-item').forEach(x => x.classList.remove('selected'));
  el.classList.add('selected');
  document.getElementById('fwd_msg_id').value = mid;
  chosenFwdText = fullText;
  document.getElementById('fwd_msg_prev').innerText = 'متن انتخاب‌شده: ' + summary;
}

async function loadLast10(){
  const src = document.getElementById('fwd_src').value.trim();
  const box = document.getElementById('last10_box');
  if(!src){ box.innerHTML = '<div class="small">ابتدا مبدأ را انتخاب یا وارد کنید.</div>'; return; }
  box.innerHTML = '<div class="small">در حال دریافت ۱۰ پیام اخیر...</div>';
  try {
    const r = await fetch('/api/forward/last10?source=' + encodeURIComponent(src));
    const data = await r.json();
    if(!data.ok){ box.innerHTML = `<div class="small">خطا: ${esc(data.error)}</div>`; return; }
    const msgs = data.messages || [];
    if(!msgs.length){ box.innerHTML = '<div class="small">پیامی یافت نشد.</div>'; return; }
    box.innerHTML = msgs.map(m => {
      const b64 = btoa(unescape(encodeURIComponent(m.text || '')));
      const s64 = btoa(unescape(encodeURIComponent(m.summary || '')));
      return `<div class="msg-item" onclick="pickFwdMsg(this, '${esc(m.id)}', decodeURIComponent(escape(atob('${b64}'))), decodeURIComponent(escape(atob('${s64}'))))">
        <div><b>#${m.index}</b> (ID: <span dir="ltr">${esc(m.id)}</span>)</div>
        <div style="margin-top:3px;">${esc(m.summary)}</div>
      </div>`;
    }).join('');
  } catch(e) {
    box.innerHTML = '<div class="small">ارتباط با سرور یا سروش در دسترس نیست.</div>';
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
  const src = document.getElementById('fwd_src').value.trim();
  const tgt = document.getElementById('fwd_tgt').value.trim();
  const mid = document.getElementById('fwd_msg_id').value.trim();
  const spd = document.getElementById('fwd_speed').value;
  const mc = document.getElementById('fwd_max').value;
  if(!src || !tgt || !mid){ document.getElementById('fwd_add_msg').innerText = 'مبدأ، پیام و مقصد را کامل کنید.'; return; }
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
      <b>[اسپم]</b> ${esc(j.target_name)} (${esc(j.target)})<br>
      سرعت: ${j.speed}s | ارسال: ${lim} | آخرین: ${esc(j.last_time||'-')}<br>
      وضعیت: ${j.running ? 'فعال' : 'متوقف'} (${esc(j.last_status||'')})
      ${j.last_err ? `<br><span dir="ltr">${esc(j.last_err)}</span>` : ''}
    </div>`;
  });

  const fwdCards = (fwds || []).map(j => {
    const lim = j.max_count > 0 ? `${j.sent} / ${j.max_count}` : `${j.sent} / ∞`;
    return `<div class="card">
      <b>[فوروارد]</b> ${esc(j.source_name||j.source)} ← ${esc(j.target_name||j.target)} (ID: <span dir="ltr">${esc(j.msg_id)}</span>)<br>
      سرعت: ${j.speed}s | ارسال: ${lim} | آخرین: ${esc(j.last_time||'-')}<br>
      وضعیت: ${j.running ? 'فعال' : 'متوقف'} (${esc(j.last_status||'')})
      ${j.last_err ? `<br><span dir="ltr">${esc(j.last_err)}</span>` : ''}
    </div>`;
  });

  const allLive = spamCards.concat(fwdCards);
  info.innerHTML = allLive.length ? allLive.join('') : '<div class="small">موردی ثبت نشده است.</div>';

  list.innerHTML = (jobs || []).length ? (jobs || []).map(j => `
    <div class="card">
      <b>${esc(j.target_name)}</b> — سرعت: ${j.speed}s — متن‌ها: ${(j.texts||[]).length}<br>
      <div class="row">
        ${j.running
          ? `<button type="button" class="mini" onclick="ctrlJob('${jsEsc(j.id)}','pause')">توقف</button>`
          : `<button type="button" class="mini" onclick="ctrlJob('${jsEsc(j.id)}','resume')">ادامه</button>`}
        <button type="button" class="mini" onclick="ctrlJob('${jsEsc(j.id)}','del')">حذف</button>
      </div>
    </div>
  `).join('') : '<div class="small">اسپمی ثبت نشده است.</div>';

  flist.innerHTML = (fwds || []).length ? (fwds || []).map(j => `
    <div class="card">
      <b>${esc(j.source_name||j.source)}</b> → <b>${esc(j.target_name||j.target)}</b><br>
      <div class="small" style="margin-top:2px;">پیام: ${esc(j.msg_summary || ('ID ' + j.msg_id))} — سرعت: ${j.speed}s</div>
      <div class="row">
        ${j.running
          ? `<button type="button" class="mini" onclick="ctrlFwd('${jsEsc(j.id)}','pause')">توقف</button>`
          : `<button type="button" class="mini" onclick="ctrlFwd('${jsEsc(j.id)}','resume')">ادامه</button>`}
        <button type="button" class="mini" onclick="ctrlFwd('${jsEsc(j.id)}','del')">حذف</button>
      </div>
    </div>
  `).join('') : '<div class="small">فورواردی ثبت نشده است.</div>';
}

async function tick(){
  try{
    const r = await fetch('/api/state');
    const d = await r.json();
    document.getElementById('top_acc_name').innerText = d.account_name || 'بدون اکانت';
    document.getElementById('ping').innerText = d.ping;
    document.getElementById('ka').innerText = d.last_keepalive || '-';
    document.getElementById('net_st').innerText = d.net_status || '-';
    document.getElementById('err_st').innerText = d.last_err || '-';
    document.getElementById('status_txt').innerText = d.status;
    document.getElementById('auth_msg').innerText = d.auth_msg;
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
        state["phone"] = p
        state["account_name"] = p
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


@app.route("/api/accounts/switch")
def api_switch_account():
    aid = (request.args.get("id") or "").strip()
    if aid:
        run_in_bot_loop(_do_switch_account(aid))
    return "", 200


@app.route("/api/accounts/del")
def api_del_account():
    aid = (request.args.get("id") or "").strip()
    if aid:
        run_in_bot_loop(_do_delete_account(aid))
    return "", 200


@app.route("/api/groups")
def api_groups():
    if client_ready and not group_list:
        run_in_bot_loop(_maybe_refresh_groups())
    return json_resp(group_list)


async def _maybe_refresh_groups():
    if not group_list and client_ready:
        await refresh_groups()


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
    state["status"] = f"دائماً فعال | اسپم روی {job['target_name']} شروع شد."
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
    if not client_ready:
        return json_resp({
            "ok": False,
            "error": "اتصال به سروش قطع است؛ بعد از برقراری اتصال دوباره تلاش کنید.",
            "messages": [],
        })
    res = run_in_bot_loop_wait(_do_fetch_last10(src), timeout=8)
    return json_resp(res if isinstance(res, dict) else {"ok": False, "error": "پاسخ دریافت نشد یا اتصال کند است.", "messages": []})


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
        return json_resp({"ok": False, "msg": "مبدأ، پیام یا مقصد ناقص است."})

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
    state["status"] = f"دائماً فعال | فوروارد از {job['source_name']} به {job['target_name']} شروع شد."
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
    safe_accounts = [
        {
            "id": a.get("id"),
            "name": a.get("name"),
            "phone": a.get("phone"),
            "connected": bool(a.get("connected")),
        }
        for a in (accounts_data.get("accounts") or [])
    ]
    return json_resp({
        "account_name": state["account_name"],
        "active_account_id": accounts_data.get("active_id") or "",
        "accounts": safe_accounts,
        "net_status": state["net_status"],
        "last_err": state["last_err"],
        "ping": state["ping"],
        "last_keepalive": state["last_keepalive"],
        "status": state["status"],
        "auth_status": state["auth_status"],
        "auth_msg": state["auth_msg"],
        "jobs": spam_jobs,
        "forwards": forward_jobs,
    })


def run_web():
    while True:
        try:
            app.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True)
        except Exception as e:
            set_err(f"web: {e}")
            time.sleep(1)


# ================= ارسال اسپم، فوروارد و پالس ۱۰ دقیقه‌ای =================
async def _send_spam_once(peer, msg: str):
    async with send_lock:
        return await asyncio.wait_for(client.send_message(peer, msg), timeout=8)


async def _resolve_fwd_text(src_peer, msg_id_val, fallback_text: str) -> str:
    txt = (fallback_text or "").strip()
    if txt and txt != "[پیام مدیا]":
        return txt
    if hasattr(client, "get_messages"):
        try:
            res = await asyncio.wait_for(client.get_messages(src_peer, ids=msg_id_val), timeout=6)
            if res:
                first = res[0] if isinstance(res, (list, tuple)) else res
                extracted = _msg_text(first)
                if extracted:
                    return extracted
        except Exception:
            pass
    return txt or f"پیام #{msg_id_val}"


async def _send_fwd_once(src_peer, tgt_peer, msg_id_str: str, fallback_text: str):
    msg_id_val = int(msg_id_str) if str(msg_id_str).isdigit() else msg_id_str

    async with send_lock:
        fwd_err = None
        if hasattr(client, "forward_messages"):
            try:
                return await asyncio.wait_for(
                    client.forward_messages(tgt_peer, msg_id_val, from_peer=src_peer),
                    timeout=9,
                )
            except TypeError:
                try:
                    return await asyncio.wait_for(
                        client.forward_messages(tgt_peer, msg_id_val, src_peer),
                        timeout=9,
                    )
                except Exception as e:
                    fwd_err = e
            except Exception as e:
                fwd_err = e

        elif hasattr(client, "forward_message"):
            try:
                return await asyncio.wait_for(
                    client.forward_message(tgt_peer, msg_id_val, from_peer=src_peer),
                    timeout=9,
                )
            except TypeError:
                try:
                    return await asyncio.wait_for(
                        client.forward_message(tgt_peer, msg_id_val, src_peer),
                        timeout=9,
                    )
                except Exception as e:
                    fwd_err = e
            except Exception as e:
                fwd_err = e

        if fwd_err is not None and is_auth_key_error(fwd_err):
            raise fwd_err

        text_to_send = await _resolve_fwd_text(src_peer, msg_id_val, fallback_text)
        return await asyncio.wait_for(client.send_message(tgt_peer, text_to_send), timeout=9)


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

            if login_in_progress or not client_ready or client is None:
                job["last_status"] = "در انتظار وصل شدن مجدد به سروش..."
                await asyncio.sleep(0.6)
                continue

            t0 = time.perf_counter()
            txts = job.get("texts") or ["تست"]
            i = int(job.get("idx") or 0) % len(txts)
            msg = txts[i]
            peer = to_peer(job["target"])

            await _send_spam_once(peer, msg)

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
                await _invalidate_session("سشن معتبر نیست؛ شماره و کد جدید وارد کنید.")
                job["last_err"] = "AuthKeyUnregisteredError"
                job["last_status"] = "نیاز به ورود مجدد"
                await asyncio.sleep(2.0)
                continue

            err_streak += 1
            err_msg = str(e)[:120]
            job["last_err"] = err_msg
            set_err(f"spam: {err_msg}")
            job["last_status"] = f"اختلال کوتاه؛ ادامه همان اسپم ({err_streak})"
            if err_streak >= 3:
                client_ready = False
                err_streak = 0
            await asyncio.sleep(0.7)


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

            if login_in_progress or not client_ready or client is None:
                job["last_status"] = "در انتظار وصل شدن مجدد به سروش..."
                await asyncio.sleep(0.6)
                continue

            t0 = time.perf_counter()
            src_peer = to_peer(job["source"])
            tgt_peer = to_peer(job["target"])

            await _send_fwd_once(
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
                await _invalidate_session("سشن معتبر نیست؛ شماره و کد جدید وارد کنید.")
                job["last_err"] = "AuthKeyUnregisteredError"
                job["last_status"] = "نیاز به ورود مجدد"
                await asyncio.sleep(2.0)
                continue

            err_streak += 1
            err_msg = str(e)[:120]
            job["last_err"] = err_msg
            set_err(f"fwd: {err_msg}")
            job["last_status"] = f"اختلال کوتاه؛ ادامه همان فوروارد ({err_streak})"
            if err_streak >= 3:
                client_ready = False
                err_streak = 0
            await asyncio.sleep(0.8)


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
        except Exception as e:
            set_err(f"supervisor: {e}")
        await asyncio.sleep(0.35)


async def fast_reconnect_loop():
    while True:
        try:
            net_ok, net_msg = check_network_access()
            state["net_status"] = net_msg
            if net_ok and not login_in_progress and get_active_session() and not client_ready:
                await connect_soroush(force_new=True)
                if client_ready and not group_list:
                    await refresh_groups()
        except Exception as e:
            set_err(f"reconnect: {e}")
        await asyncio.sleep(5.0)


async def keepalive_10m_loop():
    while True:
        await asyncio.sleep(600)
        try:
            if login_in_progress or not get_active_session():
                continue
            ok = await connect_soroush(force_new=not client_ready)
            if ok:
                await refresh_groups()
                state["last_keepalive"] = now_clock()
        except Exception as e:
            set_err(f"keepalive: {e}")


async def main_loop():
    global bot_loop
    bot_loop = asyncio.get_running_loop()

    net_ok, net_msg = check_network_access()
    state["net_status"] = net_msg
    print(f"[{now_clock()}] NET: {net_msg}", flush=True)

    asyncio.create_task(supervisor_loop())
    asyncio.create_task(fast_reconnect_loop())
    asyncio.create_task(keepalive_10m_loop())

    if net_ok and get_active_session():
        ok = await connect_soroush(force_new=True)
        if ok:
            await refresh_groups()
            state["last_keepalive"] = now_clock()

    while True:
        await asyncio.sleep(1)


def start_bot_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        try:
            loop.run_until_complete(main_loop())
        except Exception as e:
            set_err(f"bot_loop: {e}")
            time.sleep(1)


def _ensure_threads():
    global web_thread, bot_thread
    if web_thread is None or not web_thread.is_alive():
        web_thread = Thread(target=run_web, daemon=True)
        web_thread.start()
    if bot_thread is None or not bot_thread.is_alive():
        bot_thread = Thread(target=start_bot_thread, daemon=True)
        bot_thread.start()


if __name__ == "__main__":
    print(f"[{now_clock()}] Starting web panel on port {PORT} ({PANEL_PATH})", flush=True)
    _ensure_threads()
    while True:
        _ensure_threads()
        time.sleep(2)
