import os, re, hashlib, json, time, requests
from bs4 import BeautifulSoup

URL = "https://www.vhutein.ru/sveden/vacant/"

BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_IDS_ENV = os.environ.get("CHAT_IDS") or os.environ.get("CHAT_ID")
if not CHAT_IDS_ENV:
    raise RuntimeError("CHAT_IDS or CHAT_ID is required")
CHAT_IDS = [s.strip() for s in str(CHAT_IDS_ENV).split(",") if s.strip()]

STATE_FILE = "vhutein_vacant.state.json"   # тут храним прошлое состояние и последнюю ошибку
TIMEOUT = 25

HEADERS = {
    "User-Agent": "VacancyWatcher/1.1 (+check every 12h; contact: you@example.com)",
    "Accept": "text/html,application/xhtml+xml",
}

# ---------- сетевой слой с ретраями ----------
def get_with_retries(url, headers, timeout, retries=3, backoff=2.0):
    last_exc = None
    for i in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exc = e
            if i < retries - 1:
                time.sleep(backoff ** i)
    raise last_exc

# ---------- парсинг таблицы ----------
def fetch_rows():
    """Возвращает список записей-словарей из строк tr[itemprop="vacant"].
       Если на странице только "Вакантные места отсутствуют" - вернёт пустой список."""
    r = get_with_retries(URL, headers=HEADERS, timeout=TIMEOUT)
    soup = BeautifulSoup(r.text, "html.parser")

    rows = soup.select('tbody tr[itemprop="vacant"]')
    results = []
    for tr in rows:
        text = re.sub(r"\s+", " ", tr.get_text(" ", strip=True)).lower()
        if "вакантные места отсутствуют" in text:
            continue

        get = lambda prop: (tr.find(attrs={"itemprop": prop}).get_text(strip=True)
                            if tr.find(attrs={"itemprop": prop}) else "")
        rec = {
            "eduCode": get("eduCode"),
            "eduName": get("eduName"),
            "eduProf": get("eduProf"),
            "eduLevel": get("eduLevel"),
            "eduCourse": get("eduCourse"),
            "eduForm": get("eduForm"),
            "numberBFVacant": get("numberBFVacant"),
            "numberBRVacant": get("numberBRVacant"),
            "numberBMVacant": get("numberBMVacant"),
            "numberPVacant": get("numberPVacant"),
        }
        # нормализуем числа
        for k in ("numberBFVacant","numberBRVacant","numberBMVacant","numberPVacant"):
            rec[k] = re.sub(r"[^\d]", "", rec[k]) or "0"
        results.append(rec)

    return results

# ---------- состояние / дифф ----------
def load_state():
    if not os.path.exists(STATE_FILE):
        return {"rows": [], "last_error_hash": ""}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def row_key(r):
    """Ключ для сравнения записей."""
    return f"{r.get('eduCode')}|{r.get('eduName')}|{r.get('eduProf')}|{r.get('eduLevel')}|{r.get('eduCourse')}|{r.get('eduForm')}"

def diff_rows(prev_rows, new_rows):
    prev_map = {row_key(r): r for r in prev_rows}
    new_map  = {row_key(r): r for r in new_rows}

    added_keys   = [k for k in new_map.keys()  if k not in prev_map]
    removed_keys = [k for k in prev_map.keys() if k not in new_map]
    common_keys  = [k for k in new_map.keys()  if k in prev_map]

    changed = []
    for k in common_keys:
        a, b = prev_map[k], new_map[k]
        # сравниваем по числовым квотам
        fields = ("numberBFVacant","numberBRVacant","numberBMVacant","numberPVacant")
        if any(a.get(f) != b.get(f) for f in fields):
            changed.append((k, a, b))

    added   = [new_map[k] for k in added_keys]
    removed = [prev_map[k] for k in removed_keys]

    return added, removed, changed

# ---------- отправка в Telegram ----------
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for cid in CHAT_IDS:
        try:
            requests.post(url, data={
                "chat_id": cid,
                "text": msg,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }, timeout=TIMEOUT)
        except Exception as e:
            print(f"send failed for {cid}: {e}")

def esc(s: str) -> str:
    # Экранирование для Markdown
    return s.replace("_", "\\_").replace("*", "\\*").replace("[", "\\[").replace("`", "\\`")

def summarize_rows(rows, limit=8):
    if not rows:
        return "—"
    lines = []
    for r in rows[:limit]:
        title = esc(f"{r.get('eduCode','')} — {r.get('eduName','')}".strip(" —"))
        form  = esc(f"{r.get('eduForm','')}, курс {r.get('eduCourse','')}")
        nums  = f"БФ:{r['numberBFVacant']} БР:{r['numberBRVacant']} БМ:{r['numberBMVacant']} П:{r['numberPVacant']}"
        lines.append(f"• *{title}*\n  {form}\n  {nums}")
    if len(rows) > limit:
        lines.append(f"…и ещё {len(rows) - limit} записей")
    return "\n".join(lines)

def main():
    state = load_state()
    try:
        new_rows = fetch_rows()

        # Eсли файла состояния ещё не было - сразу один раз шлём стартовый пинг
        first_run = (state.get("rows") == [])
        if first_run:
            send_telegram("🔔 Мониторинг запущен. Сообщу при изменениях.")
            # сохраняем пустое состояние, чтобы шаг Persist state создал файл
            save_state(state)

        if not new_rows:
            # если раньше что-то было, а теперь пусто - сообщим;
            if state["rows"]:
                send_telegram(f"ℹ️ На странице снова нет мест.\n{URL}")
                state["rows"] = []
                save_state(state)
            return

        added, removed, changed = diff_rows(state["rows"], new_rows)

        if not state["rows"]:
            state["rows"] = new_rows
            save_state(state)
            msg = (
                "🔔 Мониторинг запущен. Обнаружены следующие вакантные места:\n\n"
                f"{summarize_rows(new_rows)}\n\n{URL}"
            )
            send_telegram(msg)
            return

        # если изменений нет - тишина
        if not added and not removed and not changed:
            return

        # собираем читаемое сообщение
        parts = []
        if added:
            parts.append(f"*Добавлено:* ({len(added)})\n{summarize_rows(added)}")
        if removed:
            parts.append(f"*Удалено:* ({len(removed)})\n{summarize_rows(removed)}")
        if changed:
            lines = []
            for _, old, new in changed[:8]:
                title = esc(f"{new.get('eduCode','')} — {new.get('eduName','')}".strip(" —"))
                nums_old = f"{old['numberBFVacant']}/{old['numberBRVacant']}/{old['numberBMVacant']}/{old['numberPVacant']}"
                nums_new = f"{new['numberBFVacant']}/{new['numberBRVacant']}/{new['numberBMVacant']}/{new['numberPVacant']}"
                lines.append(f"• *{title}*  {nums_old} → *{nums_new}*")
            if len(changed) > 8:
                lines.append(f"…и ещё {len(changed)-8}")
            parts.append("*Изменено:*\n" + "\n".join(lines))

        msg = "✅ Обновления на странице вакантных мест:\n\n" + "\n\n".join(parts) + f"\n\n{URL}"
        send_telegram(msg)

        # обновим состояние
        state["rows"] = new_rows
        # сбросим последний хеш ошибки при успешной проверке
        state["last_error_hash"] = ""
        save_state(state)

    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        h = hashlib.sha256(err.encode("utf-8")).hexdigest()
        # не дублируем одинаковую ошибку
        if state.get("last_error_hash") != h:
            try:
                send_telegram(f"⚠️ Ошибка при проверке: {err}")
            except:
                pass
            state["last_error_hash"] = h
            save_state(state)


if __name__ == "__main__":
    main()
