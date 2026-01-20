from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import pandas as pd
import re
import os
import json
from datetime import datetime

# ==============================
# 🔧 CONFIGURAÇÕES
# ==============================

BASE_URL = "https://app.7k.partners/"
REPORT_PATH_FALLBACK = "/pt/report"  # fallback se o clique no menu falhar

EMAIL = "ronaldhenriquelopes@gmail.com"
SENHA = "RonaldHL2025#"

HEADLESS = False

# Período (DD/MM/YYYY)
DATA_INICIO = "01/12/2025"
DATA_FIM = "19/01/2026"

# ✅ Colunas na ORDEM EXATA solicitada (Time fica em A para data no Sheets)
COLUNAS_ALVO = [
    "Time",
    "Registrations",
    "FTDs",
    "QFTDs, CPA",
    "FTDs Amount",
    "Deposits Amount",
    "RevShare",
    "CPA",
]

# Google Sheets (service account)
SHEET_ID = "1x3PLUE2ubJtMhlxG0eURHDvz5imnq3FUEJuAXcShOjs"
SHEET_TAB = os.getenv("SHEET_TAB", "BET7K")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CREDS_FILE = os.getenv("GOOGLE_CREDS_FILE", "credenciais.json")

# JSON histórico/cache
JSON_DIR = os.getenv("JSON_DIR", "history_7k")
JSON_LATEST = os.path.join(JSON_DIR, "latest.json")


# ==============================
# 🧠 HELPERS
# ==============================

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def safe_click(locator, label="elemento", retries=5, timeout=9000):
    last_err = None
    for _ in range(retries):
        try:
            try:
                locator.wait_for(state="visible", timeout=timeout)
            except Exception:
                pass

            try:
                locator.click(timeout=timeout)
                return
            except Exception:
                locator.click(timeout=timeout, force=True)
                return

        except Exception as e:
            last_err = e
    raise last_err


# ==============================
# ✅ NORMALIZAÇÃO DE DATAS (BR/ISO) + Sheets como SERIAL
# ==============================

def to_datetime_br_or_iso(s: str):
    if s is None:
        return pd.NaT
    s = str(s).strip()
    if not s:
        return pd.NaT

    # yyyy-mm-dd
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")

    # dd/mm/yyyy
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", s):
        return pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")

    # dd-mm-yyyy (extra)
    if re.fullmatch(r"\d{2}-\d{2}-\d{4}", s):
        return pd.to_datetime(s, format="%d-%m-%Y", errors="coerce")

    return pd.to_datetime(s, errors="coerce")


def normalize_time_column(df: pd.DataFrame, col="Time") -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if col not in df.columns:
        return df
    dt = df[col].apply(to_datetime_br_or_iso)
    dt = dt.dt.normalize()
    df[col] = dt
    return df


def parse_number(value: str):
    """
    Converte strings tipo:
    - "1,412" -> 1412
    - "-2,335.21" -> -2335.21
    - "504.68" -> 504.68
    """
    if value is None:
        return None
    s = str(value).strip()
    if s == "" or s.lower() in ("-", "nan", "none"):
        return None

    s = s.replace("$", "").replace("\u00a0", " ").strip()
    s = re.sub(r"[^\d\.,\-]", "", s)

    if s in ("", "-", ",", "."):
        return None

    # padrão EN-US: "," milhar, "." decimal
    if "," in s and "." in s:
        s = s.replace(",", "")
    else:
        # caso só tenha "," e pareça milhar
        if s.count(",") == 1 and s.count(".") == 0:
            right = s.split(",")[1]
            if len(right) == 3:
                s = s.replace(",", "")
        # caso só tenha "." e pareça milhar
        if s.count(".") == 1 and s.count(",") == 0:
            right = s.split(".")[1]
            if len(right) == 3:
                s = s.replace(".", "")

    try:
        return float(s)
    except Exception:
        return None


# ==============================
# Google Sheets
# ==============================

def sheets_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    if not os.path.exists(CREDS_FILE):
        raise FileNotFoundError(
            f"Arquivo de credenciais não encontrado: {CREDS_FILE}\n"
            "Coloque 'credenciais.json' na pasta do script (ou ajuste GOOGLE_CREDS_FILE)."
        )

    creds = service_account.Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def quoted_tab_range(tab: str, rng: str) -> str:
    return f"'{tab}'!{rng}"


def ensure_time_format_in_sheet(sheet_id: str, tab_name: str, pattern: str = "dd/MM/yyyy"):
    service = sheets_service()

    meta = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_id_num = None
    for sh in meta.get("sheets", []):
        props = sh.get("properties", {})
        if props.get("title") == tab_name:
            sheet_id_num = props.get("sheetId")
            break

    if sheet_id_num is None:
        raise RuntimeError(f"Aba '{tab_name}' não encontrada no Sheets.")

    body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id_num,
                        "startRowIndex": 1,      # pula o header
                        "startColumnIndex": 0,   # coluna A
                        "endColumnIndex": 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "DATE",
                                "pattern": pattern
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat"
                }
            }
        ]
    }

    service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body=body
    ).execute()


def read_sheet_as_df(sheet_id: str, tab_name: str) -> pd.DataFrame:
    service = sheets_service()
    resp = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=quoted_tab_range(tab_name, "A:Z"),
    ).execute()

    values = resp.get("values", [])
    if not values:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:] if len(values) > 1 else []
    df = pd.DataFrame(rows, columns=header)

    # ✅ garante colunas e ordem CERTA
    for c in COLUNAS_ALVO:
        if c not in df.columns:
            df[c] = None
    df = df[COLUNAS_ALVO].copy()

    # ✅ normaliza Time
    df = normalize_time_column(df, "Time")

    # ✅ converte numéricos do Sheets para float/int sem “quebrar coluna”
    numeric_cols = [c for c in COLUNAS_ALVO if c != "Time"]
    for c in numeric_cols:
        df[c] = df[c].apply(parse_number)

    # Inteiros “naturais”
    int_cols = ["Registrations", "FTDs", "QFTDs, CPA"]
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    return df


def _safe_values_for_sheets(df: pd.DataFrame):
    out = []
    for row in df.itertuples(index=False, name=None):
        new_row = []
        for v in row:
            if v is None:
                new_row.append("")
            elif isinstance(v, float) and pd.isna(v):
                new_row.append("")
            elif isinstance(v, (pd.Timestamp, datetime)) and pd.isna(v):
                new_row.append("")
            else:
                new_row.append(v)
        out.append(new_row)
    return out


def clear_and_write_sheet(sheet_id: str, tab_name: str, df: pd.DataFrame):
    service = sheets_service()

    # limpa tudo
    service.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=quoted_tab_range(tab_name, "A:Z"),
        body={},
    ).execute()

    df2 = df.copy()

    # ✅ garante colunas e ordem CERTA ANTES de escrever
    for c in COLUNAS_ALVO:
        if c not in df2.columns:
            df2[c] = None
    df2 = df2[COLUNAS_ALVO].copy()

    # ✅ garante Time como datetime
    df2 = normalize_time_column(df2, "Time")

    # ✅ converter Time em serial do Sheets
    if "Time" in df2.columns:
        base = pd.Timestamp("1899-12-30")
        serial = (df2["Time"] - base).dt.days
        df2["Time"] = serial.where(df2["Time"].notna(), None)

    values = [df2.columns.tolist()] + _safe_values_for_sheets(df2)

    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=quoted_tab_range(tab_name, "A1"),
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()

    ensure_time_format_in_sheet(sheet_id, tab_name, pattern="dd/MM/yyyy")


def write_sheet_sorted_dedup(df_new: pd.DataFrame, sheet_id: str, tab_name: str):
    df_old = read_sheet_as_df(sheet_id, tab_name)

    # garante colunas e ordem CERTA no novo também
    for c in COLUNAS_ALVO:
        if c not in df_new.columns:
            df_new[c] = None
    df_new = df_new[COLUNAS_ALVO].copy()

    if df_old is None or df_old.empty:
        combined = df_new.copy()
    else:
        for c in COLUNAS_ALVO:
            if c not in df_old.columns:
                df_old[c] = None
        df_old = df_old[COLUNAS_ALVO].copy()
        combined = pd.concat([df_old, df_new], ignore_index=True)

    combined = normalize_time_column(combined, "Time")

    if "Time" in combined.columns:
        combined["_TimeSort"] = combined["Time"]
        combined = combined.drop_duplicates(subset=["Time"], keep="last")
        combined = combined.sort_values("_TimeSort", ascending=True).drop(columns=["_TimeSort"])

    # garante ordem final
    combined = combined[COLUNAS_ALVO].copy()

    clear_and_write_sheet(sheet_id, tab_name, combined)
    print("✅ Sheets atualizado: deduplicado e ordenado por data (dia após dia).")


# ==============================
# JSON histórico/cache
# ==============================

def dump_json_history(df: pd.DataFrame, meta: dict):
    ensure_dir(JSON_DIR)

    df_json = df.copy() if df is not None else pd.DataFrame()
    if df_json is not None and not df_json.empty and "Time" in df_json.columns:
        df_json = normalize_time_column(df_json, "Time")
        df_json["Time"] = df_json["Time"].dt.strftime("%Y-%m-%d")

    payload = {
        "meta": meta,
        "rows": df_json.to_dict(orient="records") if df_json is not None else [],
    }

    with open(JSON_LATEST, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path_hist = os.path.join(JSON_DIR, f"report_{ts}.json")
    with open(path_hist, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"🧾 JSON salvo: {path_hist}")
    print(f"🧾 JSON cache (latest): {JSON_LATEST}")


# ==============================
# 🎛️ Playwright: Login + Navegação
# ==============================

def goto_report(page):
    print("📄 Indo para Report...")
    page.wait_for_timeout(1200)

    report_link = page.locator("a:has-text('Report')").first
    if report_link.count() > 0:
        safe_click(report_link, "menu Report", retries=6, timeout=12000)
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(1200)
        return

    page.goto(BASE_URL.rstrip("/") + REPORT_PATH_FALLBACK, wait_until="domcontentloaded")
    page.wait_for_timeout(1200)


# ==============================
# 📅 Datepicker (Element Plus / Element UI)
# ==============================

MONTHS_PT = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Março",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro",
}


def parse_ddmmyyyy(date_str: str) -> datetime:
    return datetime.strptime(date_str.strip(), "%d/%m/%Y")


def get_visible_panel(page):
    panel = page.locator(".el-picker-panel.el-date-picker[actualvisible='true']").last
    panel.wait_for(state="visible", timeout=20000)
    return panel


def open_datepicker(date_editor, page):
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass

    safe_click(date_editor, "campo data", retries=3, timeout=8000)
    panel = get_visible_panel(page)
    return panel


def wait_calendar_or_months(panel, timeout=15000):
    start = datetime.now()
    while (datetime.now() - start).total_seconds() * 1000 < timeout:
        try:
            if panel.locator(".el-date-table").first.is_visible():
                return "days"
        except Exception:
            pass
        try:
            if panel.locator(".el-month-table").first.is_visible():
                return "months"
        except Exception:
            pass
        panel.page.wait_for_timeout(120)
    raise PlaywrightTimeoutError("Nem el-date-table nem el-month-table ficaram visíveis no tempo esperado.")


def click_year(panel, year: int):
    year_label = panel.locator(".el-date-picker__header-label").first
    safe_click(year_label, "label ano")

    year_table = panel.locator(".el-year-table").first
    year_table.wait_for(state="visible", timeout=20000)

    year_cell = year_table.locator("td", has_text=str(year)).first
    safe_click(year_cell, f"ano {year}")

    return wait_calendar_or_months(panel, timeout=20000)


def click_month(panel, month: int):
    month_name = MONTHS_PT[month]

    if not panel.locator(".el-month-table").first.is_visible():
        labels = panel.locator(".el-date-picker__header-label")
        month_label = labels.nth(1)
        safe_click(month_label, "label mês")

    month_table = panel.locator(".el-month-table").first
    month_table.wait_for(state="visible", timeout=20000)

    month_td = month_table.locator(f"td[aria-label='{month_name}']").first
    if month_td.count() == 0:
        month_td = month_table.locator("td", has_text=month_name[:3]).first

    safe_click(month_td, f"mês {month_name}")

    panel.locator(".el-date-table").first.wait_for(state="visible", timeout=20000)


def click_day(panel, day: int):
    day_cell = panel.locator(
        "table.el-date-table td:not(.prev-month):not(.next-month):not(.disabled) .el-date-table-cell__text",
        has_text=str(day)
    ).first
    safe_click(day_cell, f"dia {day}")


def set_date_via_calendar(page, date_editor, date_str: str, label: str):
    dt = parse_ddmmyyyy(date_str)

    last_err = None
    for attempt in range(1, 4):
        try:
            panel = open_datepicker(date_editor, page)
            click_year(panel, dt.year)
            click_month(panel, dt.month)
            click_day(panel, dt.day)

            page.wait_for_timeout(350)
            return

        except Exception as e:
            last_err = e
            try:
                page.screenshot(path=f"erro_set_date_{label}_tentativa{attempt}.png", full_page=True)
            except Exception:
                pass
            try:
                page.keyboard.press("Escape")
            except Exception:
                pass
            page.wait_for_timeout(400)

    raise last_err


def apply_period_and_group(page, data_inicio: str, data_fim: str):
    print(f"🗓️ Aplicando período (via calendário): {data_inicio} -> {data_fim}")

    editors = page.locator("div.el-date-editor.el-date-editor--date")
    if editors.count() < 2:
        page.screenshot(path="erro_date_editors.png", full_page=True)
        raise RuntimeError("Não encontrei os 2 campos de data (el-date-editor--date).")

    start_editor = editors.nth(0)
    end_editor = editors.nth(1)

    set_date_via_calendar(page, start_editor, data_inicio, "Start date")
    set_date_via_calendar(page, end_editor, data_fim, "End date")

    print("🧩 Clicando em Group/Agrupar...")
    group_btn = page.locator("button:has-text('Group'), button:has-text('Agrupar')").first
    if group_btn.count() == 0:
        page.screenshot(path="erro_botao_group.png", full_page=True)
        raise RuntimeError("Não encontrei o botão Group/Agrupar.")

    safe_click(group_btn, "botão Group/Agrupar", retries=6, timeout=15000)
    page.wait_for_timeout(2500)


# ==============================
# 📊 Captura da tabela (DIV my_table)
# ==============================

def capture_grid_my_table(page) -> pd.DataFrame:
    print("📊 Capturando tabela (DIV my_table)...")

    root = page.locator("div.my_table").first
    try:
        root.wait_for(state="visible", timeout=35000)
    except PlaywrightTimeoutError:
        page.screenshot(path="erro_my_table_nao_visivel.png", full_page=True)
        raise RuntimeError("Não encontrei o container div.my_table do report.")

    header_row = root.locator("div.table_row").first

    header_texts = []
    for cell in header_row.locator(":scope > div").all():
        t = cell.inner_text().strip()
        if t:
            header_texts.append(t)

    header_texts = [h for h in header_texts if h]
    print("ℹ️ Headers encontrados:", header_texts)

    idx_map = {}
    for col in COLUNAS_ALVO:
        if col in header_texts:
            idx_map[col] = header_texts.index(col)
            continue

        found = None
        for i, h in enumerate(header_texts):
            if h.strip().lower() == col.strip().lower():
                found = i
                break

        if found is None:
            page.screenshot(path="erro_headers_report.png", full_page=True)
            raise RuntimeError(f"Coluna '{col}' não encontrada nos headers: {header_texts}")

        idx_map[col] = found

    rows = root.locator("div.table_row")
    n = rows.count()
    if n <= 1:
        page.screenshot(path="erro_sem_linhas_report.png", full_page=True)
        raise RuntimeError("Tabela encontrada, mas sem linhas de dados.")

    data = []
    for r in range(1, n):
        row = rows.nth(r)
        cols = row.locator(":scope > div")
        if cols.count() == 0:
            continue

        time_val = cols.nth(idx_map["Time"]).inner_text().strip()
        if time_val.strip().lower() == "totals":
            continue

        rec = {}
        for col in COLUNAS_ALVO:
            rec[col] = cols.nth(idx_map[col]).inner_text().strip()
        data.append(rec)

    return pd.DataFrame(data)


# ==============================
# 🚀 CAPTURA PRINCIPAL
# ==============================

def capturar_report_7k():
    if not EMAIL or not SENHA:
        raise RuntimeError("EMAIL/SENHA não definidos.")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)

        context = browser.new_context(
            viewport={"width": 1600, "height": 900},
            locale="pt-BR",
        )
        page = context.new_page()

        print("🌐 Abrindo site...")
        page.goto(BASE_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(1200)

        print("🔐 Fazendo login...")
        try:
            page.wait_for_selector("input[type='password']", timeout=25000)
        except PlaywrightTimeoutError:
            page.screenshot(path="erro_login_sem_password.png", full_page=True)
            raise RuntimeError("Não encontrei o campo de senha na tela de login.")

        email_input = page.locator("input[type='email']").first
        if email_input.count() == 0:
            email_input = page.locator("input[name='email']").first
        if email_input.count() == 0:
            email_input = page.locator("input[type='text']").first

        pass_input = page.locator("input[type='password']").first

        if email_input.count() == 0 or pass_input.count() == 0:
            page.screenshot(path="erro_login_seletores.png", full_page=True)
            raise RuntimeError("Não encontrei campos de login (email/senha). Ajuste os seletores.")

        safe_click(email_input, "campo email")
        email_input.press("Control+A")
        email_input.type(EMAIL, delay=20)

        safe_click(pass_input, "campo senha")
        pass_input.press("Control+A")
        pass_input.type(SENHA, delay=20)

        btn_login = page.locator("button:has-text('Login'), button:has-text('Entrar'), button:has-text('Sign in')").first
        if btn_login.count() > 0:
            safe_click(btn_login, "botão login", retries=6, timeout=15000)
        else:
            pass_input.press("Enter")

        page.wait_for_timeout(1500)
        print("✅ Pós-login URL:", page.url)

        goto_report(page)

        apply_period_and_group(page, DATA_INICIO, DATA_FIM)

        df = capture_grid_my_table(page)

        if df is None or df.empty:
            print("⚠️ Sem dados retornados.")
            dump_json_history(pd.DataFrame(), meta={
                "start": DATA_INICIO, "end": DATA_FIM, "url": page.url, "ts": datetime.now().isoformat()
            })
            context.close()
            browser.close()
            return df

        # ✅ normaliza Time como datetime (ordenação/dedup confiável)
        df = normalize_time_column(df, "Time")

        # ✅ Converte numéricos SEM mexer na coluna errada (cada coluna trata seu próprio valor)
        numeric_cols = [c for c in COLUNAS_ALVO if c != "Time"]
        for c in numeric_cols:
            df[c] = df[c].apply(parse_number)
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

        # Inteiros “naturais”
        int_cols = ["Registrations", "FTDs", "QFTDs, CPA"]
        for c in int_cols:
            if c in df.columns:
                df[c] = df[c].fillna(0).astype(int)

        # ✅ garante ordem FINAL exatamente como COLUNAS_ALVO
        for c in COLUNAS_ALVO:
            if c not in df.columns:
                df[c] = None
        df = df[COLUNAS_ALVO].copy()

        # ✅ dedup e ordena por data (Time datetime)
        df["_TimeSort"] = df["Time"]
        df = df.drop_duplicates(subset=["Time"], keep="last").sort_values("_TimeSort", ascending=True).drop(columns=["_TimeSort"])

        print("✅ Preview:")
        df_preview = df.copy()
        df_preview["Time"] = df_preview["Time"].dt.strftime("%Y-%m-%d")
        print(df_preview.head(20))

        dump_json_history(df, meta={
            "start": DATA_INICIO,
            "end": DATA_FIM,
            "url": page.url,
            "ts": datetime.now().isoformat(),
            "rows": int(len(df)),
        })

        # ✅ escreve no Sheets respeitando a ordem e com Time como data real
        write_sheet_sorted_dedup(df, SHEET_ID, SHEET_TAB)

        context.close()
        browser.close()
        return df


if __name__ == "__main__":
    capturar_report_7k()
