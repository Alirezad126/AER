# aer_multi_dash_mp.py
# Simple, fast, multi-machine Tableau scraper with per-well S3 state and locks.

import os, sys, time, re, shutil, argparse, html, csv, io, platform, json, signal
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from multiprocessing import Process, set_start_method
from typing import List, Optional, Dict, Tuple

# ========= Minimal logging =========
def ts() -> str: return datetime.now().strftime("%H:%M:%S")
def log(msg: str): print(f"[{ts()}] {msg}", flush=True)
def log_s3(msg: str): print(f"[{ts()}][S3] {msg}", flush=True)

# ========= S3 helpers =========
try:
    from s3_merge import (
        s3_read_text, s3_put_text, s3_list_prefix,
        s3_exists, s3_copyto_if_new
    )
except Exception:
    def s3_read_text(key: str) -> Optional[str]: return None
    def s3_put_text(key: str, text: str) -> bool: return False
    def s3_list_prefix(prefix: str) -> List[str]: return []
    def s3_exists(key: str) -> bool: return False
    def s3_copyto_if_new(path: Path, key: str) -> bool: return False

# ========= Locks =========
try:
    from s3_lock import acquire_lock, release_lock, start_lock_heartbeat, purge_expired_locks
except Exception:
    def acquire_lock(lock_id: str) -> bool: return True
    def release_lock(lock_id: str) -> None: pass
    class _HB:
        def stop(self): pass
    def start_lock_heartbeat(lock_id: str): return _HB()
    def purge_expired_locks(): pass

# ========= Config =========
WELLS_FILE = "wells.txt"
OUT_BASE   = Path("Data")
TIMEOUT    = 30
DELAY      = 0.15
WORKERS    = 2

# Tableau settle & retries (simple but effective)
VIZ_SETTLE_SECONDS = 30
EMPTY_SHEETS_RETRIES = 2

# CSV normalization toggles
ADD_UWI_FORMATTED = True
ADD_UWI_SHORT     = True
ADD_PROVENANCE    = True
STRIP_EMPTY_TRAILING_COLS = True

# ========= Dashboards =========
DASHBOARDS: Dict[str, str] = {
    "Well_Summary_Report":
        "https://www2.aer.ca/t/Production/views/PRD_0100_Well_Summary_Report/WellSummaryReport",
    "Well_Gas_Analysis":
        "https://www2.aer.ca/t/Production/views/0125_Well_Gas_Analysis_Data_EXT/WellGasAnalysis",
    "Reservoir_Evaluation":
        "https://www2.aer.ca/t/Production/views/0150_IMB_Well_Reservoir_Eval_EXT/ResourceEvaluation",
}

FLAGS = {
    "Well_Summary_Report": (
        "&%3Aembed=y&%3AshowShareOptions=true&%3Adisplay_count=no&%3AshowVizHome=no&%3Atoolbar=yes"
    ),
    "Reservoir_Evaluation": (
        "&%3AiframeSizedToWindow=true&%3Aembed=y&%3AshowAppBanner=false&%3Adisplay_count=no&%3Adisplay_count=no&%3AshowVizHome=no&%3Atoolbar=yes"
    ),
    "Well_Gas_Analysis": (
        "&%3AiframeSizedToWindow=true&%3Aembed=y&%3AshowAppBanner=false&%3Adisplay_count=no&%3AshowVizHome=no&%3Atoolbar=yes"
    ),
}

FRAGMENTS = {
    "Well_Summary_Report": "#3",
    "Reservoir_Evaluation": "",
    "Well_Gas_Analysis": "",
}

# ========= Small utils =========
def pause(): time.sleep(DELAY)
def sanitize_name(s: str) -> str: return re.sub(r"[^A-Za-z0-9_.-]+", "_", (s or "").strip())
def well_label_from_entry(raw_uwi: str) -> str: return sanitize_name((raw_uwi or "").strip().replace("/", "_"))
WRAPPED_RE = re.compile(r"^([A-Z0-9]{1,2})/(.+)/(\d)$")
def ensure_wrapped(uwi: str) -> str:
    u = (uwi or "").strip(); m = WRAPPED_RE.match(u)
    return u if m else f"00/{u}/0"
def to_short_uwi(uwi: str) -> str:
    m = WRAPPED_RE.match((uwi or "").strip())
    return m.group(2) if m else uwi
def url_for(code: str, base: str, wrapped: str) -> str:
    key = quote("Enter Well Identifier (UWI)", safe=""); val = quote(wrapped, safe="")
    return f"{base}?{key}={val}{FLAGS.get(code,'')}{FRAGMENTS.get(code,'')}"

# ========= Per-well state (single JSON) =========
def state_key(well_label: str) -> str:
    return f"state/wells/{well_label}.json"

def state_load(well_label: str) -> dict:
    txt = s3_read_text(state_key(well_label))
    if not txt:
        return {"well_label": well_label, "uwi_wrapped": "", "dashboards": {}, "updated_at": datetime.now(timezone.utc).isoformat()}
    try:
        obj = json.loads(txt.lstrip("\ufeff"))
    except Exception:
        # corrupt -> return minimal; we won't overwrite unless we hold the lock
        return {"well_label": well_label, "uwi_wrapped": "", "dashboards": {}, "updated_at": datetime.now(timezone.utc).isoformat()}
    obj.setdefault("dashboards", {})
    obj.setdefault("uwi_wrapped", "")
    return obj

def state_save(well_state: dict):
    well_state["updated_at"] = datetime.now(timezone.utc).isoformat()
    s3_put_text(state_key(well_state["well_label"]), json.dumps(well_state, indent=2, sort_keys=True))
    log_s3(f"state saved -> {state_key(well_state['well_label'])}")

def state_ensure_well(raw_uwi: str, wrapped: str, well_label: str):
    st = state_load(well_label)
    st["well_label"] = well_label
    if not st.get("uwi_wrapped"): st["uwi_wrapped"] = wrapped
    st.setdefault("wells_txt_entry", raw_uwi)
    state_save(st)

def state_list_incomplete_sheets(well_state: dict, dash_code: str, all_sheets: List[str]) -> List[str]:
    d = well_state["dashboards"].setdefault(dash_code, {"status": "incomplete", "files": {}, "last_update": ""})
    files = d.setdefault("files", {})
    # ensure every sheet has an entry
    for s in all_sheets:
        files.setdefault(s, {"status": "incomplete", "s3_key": ""})
    # incomplete list
    return [s for s in all_sheets if files.get(s, {}).get("status") != "complete"]

def state_mark_sheet_complete(well_label: str, dash_code: str, sheet: str, s3_key: str):
    st = state_load(well_label)
    d = st["dashboards"].setdefault(dash_code, {"status": "incomplete", "files": {}, "last_update": ""})
    f = d["files"].setdefault(sheet, {"status": "incomplete", "s3_key": ""})
    f["status"] = "complete"; f["s3_key"] = s3_key; d["last_update"] = datetime.now(timezone.utc).isoformat()
    # dashboard completion?
    all_complete = all(meta.get("status") == "complete" for meta in d["files"].values()) if d["files"] else True
    d["status"] = "complete" if all_complete else "incomplete"
    state_save(st)

def state_mark_dashboard_done(well_label: str, dash_code: str):
    st = state_load(well_label)
    d = st["dashboards"].setdefault(dash_code, {"status": "incomplete", "files": {}, "last_update": ""})
    all_complete = all(meta.get("status") == "complete" for meta in d["files"].values()) if d["files"] else True
    d["status"] = "complete" if all_complete else "incomplete"
    d["last_update"] = datetime.now(timezone.utc).isoformat()
    state_save(st)

def can_skip_well_by_state(well_label: str, dashboards: List[str]) -> bool:
    st = state_load(well_label)
    d = st.get("dashboards", {})
    if not d: return False
    for code in dashboards:
        if d.get(code, {}).get("status") != "complete":
            return False
    return True

# ========= Selenium helpers (quiet) =========
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.edge.service import Service as EdgeService

def find_browser_binary():
    system = platform.system().lower()
    candidates = []
    if system == "windows":
        candidates += [
            os.environ.get("CHROME_BIN"), os.environ.get("GOOGLE_CHROME_BIN"),
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        ]
    else:
        candidates += [
            os.environ.get("CHROME_BIN"), os.environ.get("GOOGLE_CHROME_BIN"),
            shutil.which("google-chrome"), shutil.which("google-chrome-stable"),
            shutil.which("chromium-browser"), shutil.which("chromium"), "/snap/bin/chromium", shutil.which("msedge"),
        ]
    for c in candidates:
        if c and os.path.exists(c): return c
    return None

def make_driver(download_dir: Path, headless: bool):
    system = platform.system().lower()
    devnull = "NUL" if system == "windows" else os.devnull
    try:
        from selenium.webdriver.chrome.options import Options as ChromeOptions
        opts = ChromeOptions()
        if headless: opts.add_argument("--headless=new")
        if system != "windows": opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
        else:
            if headless: opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1400,1000")
        opts.add_argument("--log-level=3"); opts.add_argument("--disable-logging"); opts.add_experimental_option("excludeSwitches", ["enable-logging"])
        opts.add_experimental_option("prefs", {
            "download.default_directory": str(download_dir.resolve()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_setting_values.automatic_downloads": 1,
        })
        binpath = find_browser_binary()
        if binpath and os.path.basename(binpath).lower().startswith(("chrome", "chromium")):
            opts.binary_location = binpath
        service = ChromeService(log_output=devnull)
        return webdriver.Chrome(options=opts, service=service)
    except Exception as e:
        try:
            from selenium.webdriver.edge.options import Options as EdgeOptions
            opts = EdgeOptions()
            if headless: opts.add_argument("--headless=new"); opts.add_argument("--disable-gpu")
            opts.add_argument("--window-size=1400,1000")
            opts.add_experimental_option("excludeSwitches", ["enable-logging"])
            opts.add_experimental_option("prefs", {
                "download.default_directory": str(download_dir.resolve()),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "profile.default_content_setting_values.automatic_downloads": 1,
            })
            binpath = find_browser_binary()
            if binpath and os.path.basename(binpath).lower().startswith("msedge"):
                opts.binary_location = binpath
            service = EdgeService(log_output=devnull)
            return webdriver.Edge(options=opts, service=service)
        except Exception as e2:
            raise RuntimeError(f"Cannot start Chrome or Edge. Chrome error: {e}\nEdge error: {e2}")

def enter_viz_context(driver, timeout: int):
    driver.switch_to.default_content()
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-tb-test-id='tb-icons-DownloadBaseIcon']"))
        ); return
    except Exception:
        pass
    for fr in driver.find_elements(By.TAG_NAME, "iframe"):
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(fr)
            WebDriverWait(driver, int(timeout/2)).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-tb-test-id='tb-icons-DownloadBaseIcon']"))
            ); return
        except Exception:
            continue
    raise RuntimeError("Download icon not found (toolbar hidden or layout changed).")

def open_download_flyout(driver, timeout: int):
    icon = WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "[data-tb-test-id='tb-icons-DownloadBaseIcon']"))
    )
    driver.execute_script("arguments[0].closest('button').click();", icon); pause()

def open_crosstab(driver, timeout: int):
    item = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((
            By.XPATH, "//*[@data-tb-test-id='download-flyout-TextMenuItem' and .//span[normalize-space()='Crosstab']]"
        ))
    )
    driver.execute_script("arguments[0].click();", item); pause()

def close_dialog(driver):
    for xp in ["//*[@role='dialog']//button[@aria-label='Close']", "//*[@role='dialog']//button[normalize-space()='Close']"]:
        try: driver.find_element(By.XPATH, xp).click(); pause(); return
        except Exception: pass
    try: driver.switch_to.active_element.send_keys("\ue00c"); pause()
    except Exception: pass

def ensure_csv_format(driver, timeout: int):
    dlg = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, "//*[@role='dialog']")))
    for xp in [".//label[@data-tb-test-id='crosstab-options-dialog-radio-csv-Label']",
               ".//label[normalize-space()='CSV']",
               ".//*[normalize-space()='CSV']"]:
        try: el = dlg.find_element(By.XPATH, xp); driver.execute_script("arguments[0].click();", el); pause(); return
        except Exception: pass

def get_selected_sheet_name(driver) -> Optional[str]:
    dlg = driver.find_element(By.XPATH, "//*[@role='dialog']")
    try: el = dlg.find_element(By.XPATH, ".//*[@role='option' and @aria-selected='true']")
    except Exception: return None
    name = (el.get_attribute("title") or "").strip()
    if not name:
        try: name = el.find_element(By.XPATH, ".//span[contains(@class,'thumbnail-title')]").text.strip()
        except Exception: name = el.text.strip()
    return name or None

def select_sheet_by_name(driver, sheet_name: str):
    dlg = driver.find_element(By.XPATH, "//*[@role='dialog']")
    try:
        el = dlg.find_element(By.XPATH, f".//*[@role='option' and @title={xpath_literal(sheet_name)}]")
    except Exception:
        el = dlg.find_element(By.XPATH, f".//span[contains(@class,'thumbnail-title') and normalize-space()={xpath_literal(sheet_name)}]/ancestor::*[@role='option']")
    driver.execute_script("arguments[0].click();", el); pause()

def click_dialog_export(driver):
    dlg = driver.find_element(By.XPATH, "//*[@role='dialog']")
    for css in ["[data-tb-test-id='export-crosstab-export-Button']"]:
        try: btn = dlg.find_element(By.CSS_SELECTOR, css); driver.execute_script("arguments[0].click();", btn); return
        except Exception: pass
    for xp in [".//button[normalize-space()='Download']", ".//button[@type='submit']"]:
        try: btn = dlg.find_element(By.XPATH, xp); driver.execute_script("arguments[0].click();", btn); return
        except Exception: pass
    raise RuntimeError("Export button not found")

def xpath_literal(s):
    if "'" not in s: return f"'{s}'"
    if '"' not in s: return f'"{s}"'
    parts = s.split("'"); return "concat(" + ", \"'\", ".join([f"'{p}'" for p in parts]) + ")"

# ========= Download watcher & CSV normalization =========
VALID_EXTS = {".csv", ".xlsx"}

def _size_stable(p: Path, dwell=0.8) -> bool:
    try: s1 = p.stat().st_size; time.sleep(dwell); s2 = p.stat().st_size; return s1 == s2
    except FileNotFoundError: return False

def _guess_ext(p: Path) -> str:
    try:
        with open(p, "rb") as f:
            if f.read(4).startswith(b"PK\x03\x04"): return ".xlsx"
    except Exception: pass
    return ".csv"

def wait_for_download_and_move(worker_download_dir: Path, target_dir: Path,
                               well_label: str, sheet_name: str, timeout=180) -> Optional[Path]:
    start = time.time(); deadline = start + timeout
    before = set(p for p in worker_download_dir.glob("*") if p.suffix.lower() in VALID_EXTS)
    candidate = None
    while time.time() < deadline:
        now = set(p for p in worker_download_dir.glob("*") if p.suffix.lower() in VALID_EXTS)
        new_files = list(now - before)
        if new_files:
            new_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            f = new_files[0]
            if _size_stable(f): candidate = f; break
        time.sleep(0.25)
    if candidate is None:
        files = [p for p in worker_download_dir.glob("*") if not p.name.endswith(".crdownload")]
        if not files: return None
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        f = files[0];
        if not _size_stable(f): return None
        candidate = f

    ext = candidate.suffix.lower()
    if ext not in VALID_EXTS: ext = _guess_ext(candidate)

    safe_sheet = sanitize_name(sheet_name)
    target = target_dir / f"{well_label}__{safe_sheet}{ext}"
    cnt = 1
    while target.exists():
        target = target_dir / f"{well_label}__{safe_sheet}_{cnt}{ext}"
        cnt += 1
    try: shutil.move(str(candidate), str(target))
    except Exception:
        shutil.copy2(candidate, target)
        try: candidate.unlink()
        except Exception: pass
    return target

def _detect_delimiter(text: str) -> str:
    CAND = [",", ";", "\t", "|"]
    lines = [ln for ln in text.splitlines() if ln.strip()][:50]
    if not lines: return ","
    best, best_var, best_modal = ",", float("inf"), 0
    for d in CAND:
        cols = [ln.count(d) + 1 for ln in lines]
        modal = max(set(cols), key=cols.count)
        var = sum((c - modal) ** 2 for c in cols)
        if (var < best_var) or (var == best_var and modal > best_modal):
            best, best_var, best_modal = d, var, modal
    return best

def _sniff_text_encoding(path: Path) -> str:
    with open(path, "rb") as fb:
        head = fb.read(4096)
    if head.startswith(b"\xff\xfe"): return "utf-16-le"
    if head.startswith(b"\xfe\xff"): return "utf-16-be"
    if head.startswith(b"\xef\xbb\xbf"): return "utf-8-sig"
    if b"\x00" in head[:100]: return "utf-16-le"
    try: head.decode("utf-8"); return "utf-8"
    except Exception: return "cp1252"

def sniff_csv_dialect_and_newline(raw: str):
    sniffer = csv.Sniffer(); sample = raw[:8192]
    line_term = "\r\n" if "\r\n" in raw else "\n"
    try:
        dia = sniffer.sniff(sample, delimiters=";,|\t,")
        class Detected(csv.Dialect):
            delimiter        = dia.delimiter
            quotechar        = getattr(dia, "quotechar", '"') or '"'
            doublequote      = getattr(dia, "doublequote", True)
            skipinitialspace = getattr(dia, "skipinitialspace", False)
            lineterminator   = line_term
            quoting          = getattr(dia, "quoting", csv.QUOTE_MINIMAL)
        return Detected, line_term
    except Exception:
        d = _detect_delimiter(raw)
        class Fallback(csv.Dialect):
            delimiter        = d
            quotechar        = '"'
            doublequote      = True
            skipinitialspace = False
            lineterminator   = line_term
            quoting          = csv.QUOTE_MINIMAL
        return Fallback, line_term

def _norm_header(h: str) -> str: return re.sub(r"[^a-z0-9]+", "", (h or "").strip().lower())
BASE_FORMATTED_SYNS = {"wellidentifier", "formatteduwi", "welluwiformatted", "enterwellidentifieruwi", "prodstringuwiformatted"}
NUMERIC_UWI_SYNS = {"welluwi","welluwi.","welluwi ","welluwi_","welluwi-"}

def normalize_csv_file(path: Path, well_label: str, wrapped_uwi: str, dashboard: str, sheet: str):
    if path.suffix.lower() != ".csv": return
    enc = _sniff_text_encoding(path)
    raw = path.read_text(encoding=enc, errors="replace")
    dialect, _ = sniff_csv_dialect_and_newline(raw)
    rows = list(csv.reader(io.StringIO(raw), dialect=dialect))
    if not rows: return
    header = [(h or "").strip() for h in rows[0]]
    data   = [list(r) for r in rows[1:]]
    mapped = []; seen_formatted_idx = None
    for i, h in enumerate(header):
        key = _norm_header(h)
        if key in BASE_FORMATTED_SYNS or ("uwi" in key and "formatted" in key) or key == "wellidentifier":
            mapped.append("UWI_Formatted"); seen_formatted_idx = i
        elif (key in NUMERIC_UWI_SYNS) or ("uwi" in key and "formatted" not in key and "identifier" not in key):
            mapped.append("UWI_Numeric")
        else:
            mapped.append(h)
    header = mapped
    for r in data:
        if len(r) < len(header): r += [""] * (len(header) - len(r))
    if ADD_UWI_FORMATTED:
        if seen_formatted_idx is None:
            header.append("UWI_Formatted")
            for r in data: r.append(wrapped_uwi)
        else:
            for r in data:
                if not (r[seen_formatted_idx] or "").strip():
                    r[seen_formatted_idx] = wrapped_uwi
    if ADD_UWI_SHORT and "UWI_Short" not in header:
        header.append("UWI_Short"); short = to_short_uwi(wrapped_uwi)
        for r in data: r.append(short)
    if ADD_PROVENANCE:
        if "Dashboard" not in header:
            header.append("Dashboard");
        for r in data: r.append(dashboard)
        if "Sheet" not in header:
            header.append("Sheet");
        for r in data: r.append(sheet)
    out_rows = [header] + data
    if STRIP_EMPTY_TRAILING_COLS:
        cols = list(zip(*out_rows)); keep_idx = []
        for i, col in enumerate(cols):
            if any((c or "").strip() for c in col[1:]): keep_idx.append(i)
        out_rows = [[r[i] for i in keep_idx] for r in out_rows]
    tmp = path.with_suffix(".csv.tmp")
    with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
        class ExcelDialect(csv.Dialect):
            delimiter=","
            quotechar='"'
            doublequote=True
            skipinitialspace=False
            lineterminator="\r\n"
            quoting=csv.QUOTE_MINIMAL
        w = csv.writer(f, dialect=ExcelDialect)
        width = len(out_rows[0])
        for r in out_rows:
            if len(r) < width: r = r + [""]*(width-len(r))
            elif len(r) > width: r = r[:width]
            w.writerow(r)
    tmp.replace(path)

# ========= Sheets manifest / discovery =========
def manifest_key(well_label: str, dash_code: str) -> str:
    return f"Data/{well_label}/{dash_code}/sheets.txt"

def load_manifest_from_s3(well_label: str, dash_code: str) -> Optional[List[str]]:
    txt = s3_read_text(manifest_key(well_label, dash_code))
    if txt is None: return None
    return [ln.strip() for ln in txt.splitlines() if ln.strip()]

def save_manifest_to_s3(well_label: str, dash_code: str, sheets: List[str]):
    s3_put_text(manifest_key(well_label, dash_code), "\n".join(sheets))
    log_s3(f"uploaded manifest -> {manifest_key(well_label, dash_code)}")

# ========= Scrape dashboard =========
def discover_sheets(driver, timeout: int, dash_dir: Path, well_label: str, dash_code: str,
                    dash_url: str) -> List[str]:
    # First try existing manifest on S3
    s3_sheets = load_manifest_from_s3(well_label, dash_code)
    if s3_sheets is not None and len(s3_sheets) > 0:
        (dash_dir / "sheets.txt").write_text("\n".join(s3_sheets), encoding="utf-8")
        return s3_sheets

    # Open viz and list
    driver.get(dash_url); pause()
    enter_viz_context(driver, timeout); pause()
    time.sleep(VIZ_SETTLE_SECONDS)

    # open crosstab dialog
    open_download_flyout(driver, timeout); pause()
    open_crosstab(driver, timeout); pause()

    # read thumbnails; if empty, refresh & retry
    def list_thumbs() -> List[str]:
        try:
            dlg = driver.find_element(By.XPATH, "//*[@role='dialog']")
            thumbs = dlg.find_elements(By.XPATH, ".//*[starts-with(@data-tb-test-id,'sheet-thumbnail-')]")
            names = []
            for t in thumbs:
                title = (t.get_attribute("title") or "").strip()
                if not title:
                    try: title = t.find_element(By.XPATH, ".//span[contains(@class,'thumbnail-title')]").text.strip()
                    except Exception: title = t.text.strip()
                if title and title not in names: names.append(title)
            return names
        except Exception:
            return []

    sheets = list_thumbs()
    if not sheets:
        close_dialog(driver)
        for _ in range(EMPTY_SHEETS_RETRIES):
            driver.refresh(); time.sleep(2)
            enter_viz_context(driver, timeout); time.sleep(VIZ_SETTLE_SECONDS)
            open_download_flyout(driver, timeout); pause()
            open_crosstab(driver, timeout); pause()
            sheets = list_thumbs()
            if sheets: break

    # Save (even if empty)
    (dash_dir / "sheets.txt").write_text("\n".join(sheets), encoding="utf-8")
    save_manifest_to_s3(well_label, dash_code, sheets)
    close_dialog(driver)
    return sheets

def process_dashboard(driver, worker_tmp_dir: Path, well_root: Path,
                      raw_uwi: str, well_label: str, wrapped: str,
                      dash_code: str, timeout: int):
    dash_dir = well_root / dash_code
    dash_dir.mkdir(parents=True, exist_ok=True)

    dash_url = url_for(dash_code, DASHBOARDS[dash_code], wrapped)
    log(f"      [{dash_code}] -> {dash_url}")

    # discover sheets (use S3 manifest if available)
    sheets = discover_sheets(driver, timeout, dash_dir, well_label, dash_code, dash_url)
    log(f"      [{dash_code}] sheets: {sheets if sheets else '[]'}")

    # build/update state and figure incomplete
    st = state_load(well_label)
    incompletes = state_list_incomplete_sheets(st, dash_code, sheets)
    state_save(st)  # ensure st contains entries

    if not sheets:
        log(f"      [{dash_code}] (no sheets) -> mark complete")
        state_mark_dashboard_done(well_label, dash_code)
        # also write markers for compatibility
        s3_put_text(f"Data/{well_label}/.COMPLETE", "")
        return

    if not incompletes:
        log(f"      [{dash_code}] ✓ already complete via state")
        state_mark_dashboard_done(well_label, dash_code)
        return

    # Open viz/dialog once and export each needed sheet
    driver.get(dash_url); pause()
    enter_viz_context(driver, timeout); pause()
    time.sleep(VIZ_SETTLE_SECONDS)
    open_download_flyout(driver, timeout); pause()
    open_crosstab(driver, timeout); pause()
    ensure_csv_format(driver, timeout)

    for sheet in incompletes:
        safe = sanitize_name(sheet)
        # select proper sheet
        cur = get_selected_sheet_name(driver)
        if (cur or "").strip() != sheet.strip():
            select_sheet_by_name(driver, sheet)
            ensure_csv_format(driver, timeout)
        # export
        click_dialog_export(driver)
        saved = wait_for_download_and_move(worker_tmp_dir, dash_dir, well_label, sheet, timeout=180)
        if not saved:
            log(f"      [{dash_code}] ✗ timeout: {sheet}")
            continue
        # normalize CSV if applicable
        try:
            normalize_csv_file(saved, well_label, wrapped, dash_code, sheet)
        except Exception as e:
            log(f"      [{dash_code}] note: normalize failed for {saved.name}: {e}")

        # upload (never overwrite)
        rel = saved.relative_to(OUT_BASE)
        remote_key = f"Data/{rel.as_posix()}"
        ok = s3_copyto_if_new(saved, remote_key)
        if ok: log_s3(f"uploaded -> {remote_key}")
        else:  log_s3(f"exists -> {remote_key} (skipped)")

        # update state immediately
        state_mark_sheet_complete(well_label, dash_code, sheet, remote_key)

    close_dialog(driver)
    # finalize dashboard status
    state_mark_dashboard_done(well_label, dash_code)

def process_one_well(driver, worker_tmp_dir: Path, out_base: Path, raw_uwi: str,
                     dashboards: List[str], timeout: int):
    raw_uwi = (raw_uwi or "").strip()
    well_label = well_label_from_entry(raw_uwi)
    wrapped = ensure_wrapped(raw_uwi)

    if not acquire_lock(raw_uwi):
        log(f"[lock] SKIP (locked): {raw_uwi}")
        return
    hb = start_lock_heartbeat(raw_uwi)
    try:
        if can_skip_well_by_state(well_label, dashboards):
            log(f"   -> {raw_uwi} (folder: {well_label})")
            log("      [state] ✓ complete for selected dashboards — skipping")
            return

        # ensure state exists and has basics
        state_ensure_well(raw_uwi, wrapped, well_label)

        well_root = out_base / well_label
        well_root.mkdir(parents=True, exist_ok=True)
        log(f"   -> {raw_uwi} (folder: {well_label})")

        for code in dashboards:
            try:
                process_dashboard(driver, worker_tmp_dir, well_root, raw_uwi, well_label, wrapped, code, timeout)
            except Exception as e:
                log(f"      [{code}] ERROR: {e}")
            pause()
    finally:
        try: hb.stop()
        except Exception: pass
        release_lock(raw_uwi)

# ========= Multiprocessing =========
def chunkify(seq: List[str], n: int) -> List[List[str]]:
    n = max(1, n); k, m = divmod(len(seq), n)
    out = []; start = 0
    for i in range(n):
        size = k + (1 if i < m else 0)
        out.append(seq[start:start+size]); start += size
    return out

def load_wells(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.lstrip().startswith("#")]

def worker_main(worker_id: int, wells: List[str], dashboards: List[str],
                headless: bool, timeout: int, delay: float):
    global DELAY
    DELAY = delay
    tmp_dir = OUT_BASE / f"_tmp_worker_{worker_id}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    driver = None
    def cleanup():
        try: driver.quit()
        except Exception: pass

    def _sig_hdlr(sig, frame):
        cleanup(); os._exit(1)

    for s in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if s:
            try: signal.signal(s, _sig_hdlr)
            except Exception: pass

    try:
        driver = make_driver(tmp_dir, headless=headless)
        for uwi in wells:
            process_one_well(driver, tmp_dir, OUT_BASE, uwi, dashboards, timeout)
    finally:
        cleanup()

# ========= CLI =========
def parse_dashboards_spec(spec: Optional[str]) -> List[str]:
    if not spec or spec.strip().lower() == "all": return list(DASHBOARDS.keys())
    wanted = [p.strip() for p in spec.split(",") if p.strip()]
    return [w for w in wanted if w in DASHBOARDS] or list(DASHBOARDS.keys())

def main():
    global OUT_BASE
    parser = argparse.ArgumentParser(description="AER dashboards scraper (simple, per-well S3 state + locks).")
    parser.add_argument("--workers", type=int, default=WORKERS)
    parser.add_argument("--wells", type=str, default=WELLS_FILE)
    parser.add_argument("--dashboards", type=str, default="all")
    parser.add_argument("--out-base", type=str, default=None)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--timeout", type=int, default=TIMEOUT)
    parser.add_argument("--delay", type=float, default=DELAY)
    args = parser.parse_args()

    if args.out_base:
        OUT_BASE = Path(args.out_base)
    OUT_BASE.mkdir(parents=True, exist_ok=True)

    wells = load_wells(args.wells)
    if not wells:
        log("No UWIs in wells.txt"); sys.exit(1)
    dashboards = parse_dashboards_spec(args.dashboards)

    log(f"[info] Dashboards: {dashboards}")
    log(f"[info] OUT_BASE: {OUT_BASE.resolve()}")
    log(f"[info] Headless: {args.headless}, Timeout: {args.timeout}s, Delay: {args.delay}s")

    try: purge_expired_locks()
    except Exception: pass

    groups = chunkify(wells, args.workers)
    procs: List[Process] = []
    for wid, group in enumerate(groups, 1):
        p = Process(target=worker_main, args=(wid, group, dashboards, args.headless, args.timeout, args.delay), daemon=False)
        p.start(); procs.append(p)

    exit_code = 0
    for p in procs:
        p.join()
        if p.exitcode not in (0, None): exit_code = p.exitcode
    sys.exit(exit_code)

if __name__ == "__main__":
    try: set_start_method("spawn")
    except RuntimeError: pass
    main()
