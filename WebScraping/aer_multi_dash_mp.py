# aer_multi_dash_mp.py
# Multi-process AER Tableau scraper (dialect-preserving CSV, per-dash flags, wells.txt label naming)

import os, sys, time, re, shutil, argparse, html, csv, io, platform
from pathlib import Path
from urllib.parse import quote
from multiprocessing import Process, set_start_method
from typing import List, Optional, Dict, Tuple

# ---- Extra S3 helpers (implement in s3_merge, or these stubs fall back safely) ----
try:
    from s3_merge import s3_read_text, s3_put_text, s3_list_prefix
except Exception:
    def s3_read_text(key: str) -> Optional[str]: return None
    def s3_put_text(key: str, text: str) -> bool: return False
    def s3_list_prefix(prefix: str) -> list[str]: return []


# --- optional S3 helpers (no-ops if modules not present) ---
try:
    from s3_lock import acquire_lock, release_lock, start_lock_heartbeat
except Exception:
    def acquire_lock(lock_id: str) -> bool: return True
    def release_lock(lock_id: str) -> None: pass
    class _HB:
        def stop(self): pass
    def start_lock_heartbeat(lock_id: str): return _HB()

try:
    from s3_merge import s3_exists, s3_copyto_if_new
except Exception:
    def s3_exists(key: str) -> bool: return False
    def s3_copyto_if_new(path: Path, key: str) -> bool: return False

def s3_mark_inprogress(well_label: str):
    s3_put_text(f"Data/{well_label}/.INPROGRESS", "")

def s3_mark_complete(well_label: str):
    s3_put_text(f"Data/{well_label}/.COMPLETE", "")
    # Optional: clear INPROGRESS by overwriting to empty (idempotent)
    s3_put_text(f"Data/{well_label}/.INPROGRESS", "")

def s3_mark_incomplete(well_label: str):
    s3_put_text(f"Data/{well_label}/.INCOMPLETE", "")


# ========= DEFAULT CONFIG =========
WELLS_FILE = "wells.txt"
OUT_BASE   = Path(os.environ.get("AER_OUT_BASE", "Data"))
TIMEOUT    = 20
DELAY      = 0.1
WORKERS    = 2

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

# Per-dashboard query flag sets (URL-encoded)
FLAGS = {
    "Well_Summary_Report": (
        "&%3Aembed=y&%3AshowShareOptions=true&%3Adisplay_count=no&%3AshowVizHome=no&%3Atoolbar=yes"
    ),
    "Reservoir_Evaluation": (
        "&%3AiframeSizedToWindow=true&%3Aembed=y&%3AshowAppBanner=false&%3Adisplay_count=no&%3AshowVizHome=no&%3Atoolbar=yes"
    ),
    "Well_Gas_Analysis": (
        "&%3AiframeSizedToWindow=true&%3Aembed=y&%3AshowAppBanner=false&%3Adisplay_count=no&%3AshowVizHome=no&%3Atoolbar=yes"
    ),
}

# Optional URL fragment per dashboard
FRAGMENTS = {
    "Well_Summary_Report": "#3",
    "Reservoir_Evaluation": "",
    "Well_Gas_Analysis": "",
}

# ----------------- small utils -----------------
def pause(): time.sleep(DELAY)

def sanitize_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", (s or "").strip())

def well_label_from_entry(raw_uwi: str) -> str:
    """Filesystem-safe name used for folder & filename prefix, mirrors wells.txt entry."""
    return sanitize_name((raw_uwi or "").strip().replace("/", "_"))

def norm(s: Optional[str]) -> str:
    if s is None: return ""
    s = html.unescape(s)
    return re.sub(r"\s+", " ", s.strip())

def _norm_name(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

# --- UWI handling: support any 1-2 char prefix like 00, W0, AD, AE, etc. ---
WRAPPED_RE = re.compile(r"^([A-Z0-9]{1,2})/(.+)/(\d)$")  # prefix, short, rev

def is_wrapped_any(txt: str) -> bool:
    return bool(WRAPPED_RE.match((txt or "").strip()))

def to_short_uwi(uwi: str) -> str:
    u = (uwi or "").strip()
    m = WRAPPED_RE.match(u)
    return m.group(2) if m else u

def ensure_wrapped(uwi: str) -> str:
    u = (uwi or "").strip()
    if is_wrapped_any(u):
        return u
    short = to_short_uwi(u)
    return f"00/{short}/0"

# --- URL builder with per-dashboard flags/fragment ---
def url_for(dash_code: str, dash_base: str, uwi_wrapped: str) -> str:
    key = quote("Enter Well Identifier (UWI)", safe="")
    val = quote(uwi_wrapped, safe="")
    flags = FLAGS.get(dash_code, "")
    frag  = FRAGMENTS.get(dash_code, "")
    return f"{dash_base}?{key}={val}{flags}{frag}"

def should_keep_sheet(sheet_name: str, allow_list: Optional[List[str]]) -> bool:
    if allow_list is None:
        return True
    s_norm = _norm_name(sheet_name)
    for key in allow_list:
        k_norm = _norm_name(key)
        if k_norm and (k_norm in s_norm or s_norm in k_norm):
            return True
    return False

# --------------- selenium helpers ---------------
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

def find_browser_binary():
    system = platform.system().lower()
    candidates = []
    if system == "windows":
        candidates += [
            os.environ.get("CHROME_BIN"),
            os.environ.get("GOOGLE_CHROME_BIN"),
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        ]
    else:
        candidates += [
            os.environ.get("CHROME_BIN"),
            os.environ.get("GOOGLE_CHROME_BIN"),
            shutil.which("google-chrome"),
            shutil.which("google-chrome-stable"),
            shutil.which("chromium-browser"),
            shutil.which("chromium"),
            "/snap/bin/chromium",
            shutil.which("msedge"),
        ]
    for cand in candidates:
        if cand and os.path.exists(cand):
            return cand
    return None

def make_driver(download_dir: Path, headless: bool):
    system = platform.system().lower()
    try:
        from selenium.webdriver.chrome.options import Options as ChromeOptions
        opts = ChromeOptions()
        if headless: opts.add_argument("--headless=new")
        if system != "windows":
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
        else:
            if headless: opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1400,1000")
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
        return webdriver.Chrome(options=opts)
    except Exception as chrome_err:
        try:
            from selenium.webdriver.edge.options import Options as EdgeOptions
            opts = EdgeOptions()
            if headless:
                opts.add_argument("--headless=new")
                opts.add_argument("--disable-gpu")
            opts.add_argument("--window-size=1400,1000")
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
            return webdriver.Edge(options=opts)
        except Exception as edge_err:
            raise RuntimeError(
                f"Could not start Chrome or Edge WebDriver on {system}.\n"
                f"Chrome error: {chrome_err}\nEdge error: {edge_err}\n"
                "Ensure a browser is installed."
            )

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
    raise RuntimeError("Download icon not found (toolbar hidden or different layout).")

def open_download_flyout(driver, timeout: int):
    icon = WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "[data-tb-test-id='tb-icons-DownloadBaseIcon']"))
    )
    driver.execute_script("arguments[0].closest('button').click();", icon)
    pause()

def open_crosstab(driver, timeout: int):
    item = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((
            By.XPATH, "//*[@data-tb-test-id='download-flyout-TextMenuItem' and .//span[normalize-space()='Crosstab']]"
        ))
    )
    driver.execute_script("arguments[0].click();", item)
    pause()

def close_dialog(driver):
    for xp in [
        "//*[@role='dialog']//button[@aria-label='Close']",
        "//*[@role='dialog']//button[normalize-space()='Close']",
        "//button[@aria-label='Close']",
    ]:
        try:
            driver.find_element(By.XPATH, xp).click()
            pause()
            return
        except Exception:
            pass
    try:
        driver.switch_to.active_element.send_keys("\ue00c")  # ESC
        pause()
    except Exception:
        pass

def _find_reset_dialog(driver, timeout=3):
    try:
        dlg = WebDriverWait(driver, timeout, poll_frequency=0.25).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//*[(@role='dialog' or contains(@class,'dialog')) and "
                "(.//text()[contains(., 'Session Ended by Server')] or "
                ".//text()[contains(., 'reset the view')])]"
            ))
        )
        return dlg
    except TimeoutException:
        return None

def click_no_on_reset_dialog(driver, timeout=3) -> bool:
    dlg = _find_reset_dialog(driver, timeout=timeout)
    if not dlg: return False
    try:
        no_btn = None
        for xp in [
            ".//button[normalize-space()='No']",
            ".//button[@data-tb-test-id='no' or @aria-label='No']",
            ".//button[contains(., 'No')]",
        ]:
            try:
                no_btn = dlg.find_element(By.XPATH, xp); break
            except Exception:
                pass
        if not no_btn:
            btns = dlg.find_elements(By.XPATH, ".//button")
            for b in btns:
                t = (b.text or "").strip().lower()
                if t != "yes":
                    no_btn = b; break
        if no_btn:
            driver.execute_script("arguments[0].click();", no_btn)
            time.sleep(0.5)
            return True
    except StaleElementReferenceException:
        pass
    return False

def guard_session_reset(driver):
    try:
        return click_no_on_reset_dialog(driver, timeout=2)
    except Exception:
        return False

def crosstab_state(driver) -> str:
    dlg = driver.find_element(By.XPATH, "//*[@role='dialog']")
    try:
        dlg.find_element(By.XPATH, ".//*[contains(normalize-space(),'No sheets to select')]")
        return "empty"
    except Exception:
        pass
    thumbs = dlg.find_elements(By.XPATH, ".//*[starts-with(@data-tb-test-id,'sheet-thumbnail-')]")
    if thumbs:
        try:
            btn = dlg.find_element(By.CSS_SELECTOR, "[data-tb-test-id='export-crosstab-export-Button']")
            if btn.is_enabled():
                return "ready"
        except Exception:
            pass
    return "unknown"

def wait_for_crosstab_ready_or_empty(driver, timeout=120) -> str:
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, "//*[@role='dialog']"))
    )
    wait = WebDriverWait(driver, timeout, poll_frequency=0.25)
    def _cond(d):
        st = crosstab_state(d)
        return st if st in ("ready", "empty") else False
    state = wait.until(_cond)
    return state

def open_crosstab_and_wait_state(driver, timeout: int) -> str:
    guard_session_reset(driver)
    open_download_flyout(driver, timeout); pause()
    guard_session_reset(driver)
    open_crosstab(driver, timeout); pause()
    guard_session_reset(driver)
    state = wait_for_crosstab_ready_or_empty(driver, timeout=max(120, timeout))
    pause()
    return state

def ensure_csv_format(driver, timeout: int):
    dlg = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, "//*[@role='dialog']")))
    for xp in [".//label[@data-tb-test-id='crosstab-options-dialog-radio-csv-Label']",
               ".//label[normalize-space()='CSV']",
               ".//*[normalize-space()='CSV']"]:
        try:
            el = dlg.find_element(By.XPATH, xp)
            driver.execute_script("arguments[0].click();", el); pause(); return
        except Exception:
            pass
    try:
        inp = dlg.find_element(By.XPATH, ".//input[@data-tb-test-id='crosstab-options-dialog-radio-csv-RadioButton' or (@type='radio' and translate(@value,'csv','CSV')='CSV')]")
        driver.execute_script("arguments[0].click();", inp); pause()
    except Exception:
        pass

def list_crosstab_sheets(driver, timeout: int) -> List[str]:
    dlg = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, "//*[@role='dialog']")))
    thumbs = dlg.find_elements(By.XPATH, ".//*[starts-with(@data-tb-test-id,'sheet-thumbnail-')]")
    names = []
    for t in thumbs:
        title = (t.get_attribute("title") or "").strip()
        if not title:
            try:
                title = t.find_element(By.XPATH, ".//span[contains(@class,'thumbnail-title')]").text.strip()
            except Exception:
                title = t.text.strip()
        if title and title not in names:
            names.append(title)
    return names

def get_selected_sheet_name(driver) -> Optional[str]:
    dlg = driver.find_element(By.XPATH, "//*[@role='dialog']")
    try:
        el = dlg.find_element(By.XPATH, ".//*[@role='option' and @aria-selected='true']")
    except Exception:
        return None
    name = (el.get_attribute("title") or "").strip()
    if not name:
        try:
            name = el.find_element(By.XPATH, ".//span[contains(@class,'thumbnail-title')]").text.strip()
        except Exception:
            name = el.text.strip()
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
    try:
        btn = dlg.find_element(By.CSS_SELECTOR, "[data-tb-test-id='export-crosstab-export-Button']")
        driver.execute_script("arguments[0].click();", btn); return
    except Exception:
        pass
    for xp in [".//button[normalize-space()='Download']", ".//button[@type='submit']"]:
        try:
            btn = dlg.find_element(By.XPATH, xp)
            driver.execute_script("arguments[0].click();", btn); return
        except Exception:
            pass
    raise RuntimeError("Dialog export Download button not found.")

# --- robust download watcher ---
VALID_EXTS = {".csv", ".xlsx"}

def _size_stable(p: Path, dwell=0.8) -> bool:
    try:
        s1 = p.stat().st_size
        time.sleep(dwell)
        s2 = p.stat().st_size
        return s1 == s2
    except FileNotFoundError:
        return False

def _guess_ext(p: Path) -> str:
    try:
        with open(p, "rb") as f:
            head = f.read(4)
        if head.startswith(b"PK\x03\x04"):
            return ".xlsx"
    except Exception:
        pass
    return ".csv"

def wait_for_download_and_move(worker_download_dir: Path,
                               target_dir: Path,
                               well_label: str,
                               sheet_name: str,
                               timeout=180) -> Optional[Path]:
    start = time.time()
    deadline = start + timeout

    before = set(p for p in worker_download_dir.glob("*")
                 if p.suffix.lower() in VALID_EXTS)

    candidate: Optional[Path] = None
    while time.time() < deadline:
        now = set(p for p in worker_download_dir.glob("*")
                  if p.suffix.lower() in VALID_EXTS)
        new_files = list(now - before)
        if new_files:
            new_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            f = new_files[0]
            if _size_stable(f):
                candidate = f
                break
        time.sleep(0.25)

    if candidate is None:
        files = [p for p in worker_download_dir.glob("*")
                 if not p.name.endswith(".crdownload")]
        if not files:
            return None
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        f = files[0]
        if not _size_stable(f):
            return None
        candidate = f

    ext = candidate.suffix.lower()
    if ext not in VALID_EXTS:
        ext = _guess_ext(candidate)

    safe_sheet = sanitize_name(sheet_name)
    target = target_dir / f"{well_label}__{safe_sheet}{ext}"
    cnt = 1
    while target.exists():
        target = target_dir / f"{well_label}__{safe_sheet}_{cnt}{ext}"
        cnt += 1

    try:
        shutil.move(str(candidate), str(target))
    except Exception:
        shutil.copy2(candidate, target)
        try: candidate.unlink()
        except Exception: pass
    return target


def xpath_literal(s):
    if "'" not in s: return f"'{s}'"
    if '"' not in s: return f'"{s}"'
    parts = s.split("'")
    return "concat(" + ", \"'\", ".join([f"'{p}'" for p in parts]) + ")"

# ---------- CSV normalization ----------
CANDIDATE_DELIMS = [",", ";", "\t", "|"]

def _detect_delimiter(text: str) -> str:
    lines = [ln for ln in text.splitlines() if ln.strip()][:50]
    if not lines:
        return ","
    best_delim, best_var, best_modal = ",", float("inf"), 0
    for d in CANDIDATE_DELIMS:
        cols = [ln.count(d) + 1 for ln in lines]
        if not cols:
            continue
        modal = max(set(cols), key=cols.count)
        var = sum((c - modal) ** 2 for c in cols)
        if (var < best_var) or (var == best_var and modal > best_modal):
            best_delim, best_var, best_modal = d, var, modal
    return best_delim

def _sniff_text_encoding(path: Path) -> str:
    with open(path, "rb") as fb:
        head = fb.read(4096)
    if head.startswith(b"\xff\xfe"): return "utf-16-le"
    if head.startswith(b"\xfe\xff"): return "utf-16-be"
    if head.startswith(b"\xef\xbb\xbf"): return "utf-8-sig"
    if b"\x00" in head[:100]: return "utf-16-le"
    try:
        head.decode("utf-8"); return "utf-8"
    except Exception:
        return "cp1252"

def _norm_header(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", h.strip().lower())

BASE_FORMATTED_SYNS = {
    "wellidentifier", "formatteduwi", "welluwiformatted",
    "enterwellidentifieruwi", "prodstringuwiformatted"
}
NUMERIC_UWI_SYNS = {"welluwi", "welluwi.", "welluwi ", "welluwi_", "welluwi-"}

def _is_formatted_header_key(key: str) -> bool:
    return key in BASE_FORMATTED_SYNS or ("uwi" in key and "formatted" in key) or key == "wellidentifier"

def _is_numeric_uwi_key(key: str) -> bool:
    return key in NUMERIC_UWI_SYNS or ("uwi" in key and "formatted" not in key and "identifier" not in key)

def _drop_all_empty_columns(rows: list[list[str]]) -> list[list[str]]:
    if not rows:
        return rows
    cols = list(zip(*rows))
    keep_idx = []
    for i, col in enumerate(cols):
        data_cells = col[1:]
        if any((c or "").strip() for c in data_cells):
            keep_idx.append(i)
    new_rows = []
    for r in rows:
        new_rows.append([r[i] for i in keep_idx])
    return new_rows

def sniff_csv_dialect_and_newline(raw: str) -> tuple[csv.Dialect, str]:
    sniffer = csv.Sniffer()
    sample = raw[:8192]
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

# --- replace the whole normalize_csv_file with this version ---
import io

def normalize_csv_file(path: Path, well_label: str, wrapped_uwi: str,
                       dashboard: str, sheet: str) -> None:
    """
    Read with detected dialect, WRITE as true comma-separated CSV (Excel-safe).
    Preserves all original columns, appends UWI_Formatted / UWI_Short / Dashboard / Sheet.
    """
    if path.suffix.lower() != ".csv":
        return

    # Read raw text and detect encoding + dialect
    enc = _sniff_text_encoding(path)
    raw = path.read_text(encoding=enc, errors="replace")

    # Detect original delimiter/quoting just for parsing
    dialect, _line_term = sniff_csv_dialect_and_newline(raw)
    reader = csv.reader(io.StringIO(raw), dialect=dialect)
    rows = [row for row in reader]
    if not rows:
        return

    header = [(h or "").strip() for h in rows[0]]
    data   = [list(r) for r in rows[1:]]

    # Map header names (recognize formatted/numeric UWI columns)
    mapped = []
    seen_formatted_idx = None
    for i, h in enumerate(header):
        key = _norm_header(h)
        if _is_formatted_header_key(key):
            mapped.append("UWI_Formatted"); seen_formatted_idx = i
        elif _is_numeric_uwi_key(key):
            mapped.append("UWI_Numeric")
        else:
            mapped.append(h)
    header = mapped

    # Ensure all rows have at least len(header) cells
    for r in data:
        if len(r) < len(header):
            r += [""] * (len(header) - len(r))

    # Fill/append our meta columns WITHOUT disturbing other columns
    if ADD_UWI_FORMATTED:
        if seen_formatted_idx is None:
            header.append("UWI_Formatted")
            for r in data: r.append(wrapped_uwi)
        else:
            for r in data:
                if not (r[seen_formatted_idx] or "").strip():
                    r[seen_formatted_idx] = wrapped_uwi

    if ADD_UWI_SHORT:
        if "UWI_Short" not in header:
            header.append("UWI_Short")
            short = to_short_uwi(wrapped_uwi)
            for r in data: r.append(short)

    if ADD_PROVENANCE:
        if "Dashboard" not in header:
            header.append("Dashboard")
            for r in data: r.append(dashboard)
        if "Sheet" not in header:
            header.append("Sheet")
            for r in data: r.append(sheet)

    out_rows = [header] + data

    if STRIP_EMPTY_TRAILING_COLS:
        out_rows = _drop_all_empty_columns(out_rows)

    # ---- WRITE as *true* CSV (comma, quote as needed, BOM, CRLF) ----
    tmp = path.with_suffix(".csv.tmp")
    with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
        # Use the built-in 'excel' dialect (comma, quote-minimal) and CRLF line endings
        class ExcelDialect(csv.Dialect):
            delimiter        = ","
            quotechar        = '"'
            doublequote      = True
            skipinitialspace = False
            lineterminator   = "\r\n"
            quoting          = csv.QUOTE_MINIMAL

        writer = csv.writer(f, dialect=ExcelDialect)
        # Normalize all rows to the same width as header
        width = len(out_rows[0])
        for r in out_rows:
            if len(r) < width:
                r = r + [""] * (width - len(r))
            elif len(r) > width:
                r = r[:width]
            writer.writerow(r)

    tmp.replace(path)

# -------- manifest/skip logic ----------
def compute_missing_sheets_for_dashboard(
    well_root: Path,
    well_label: str,
    dash_code: str,
    allow_sheets: Optional[List[str]]
) -> Tuple[Optional[List[str]], Optional[List[str]]]:
    """
    Returns (all_sheets_from_manifest, missing_sheets).
    If sheets.txt absent, returns (None, None).
    """
    dash_dir = well_root / dash_code
    manifest = dash_dir / "sheets.txt"
    if not manifest.exists():
        return (None, None)

    try:
        raw = manifest.read_text(encoding="utf-8").splitlines()
    except Exception:
        raw = []

    all_sheets = [ln.strip() for ln in raw if ln.strip()]
    if not all_sheets:
        return ([], [])

    kept = [s for s in all_sheets if should_keep_sheet(s, allow_sheets)]

    missing = []
    for sheet in kept:
        safe_sheet = sanitize_name(sheet)
        csv_path  = dash_dir / f"{well_label}__{safe_sheet}.csv"
        xlsx_path = dash_dir / f"{well_label}__{safe_sheet}.xlsx"
        if not (csv_path.exists() or xlsx_path.exists()):
            missing.append(sheet)

    return (all_sheets, missing)

def s3_manifest_for_dashboard(well_label: str, dash_code: str) -> Optional[list[str]]:
    key = f"Data/{well_label}/{dash_code}/sheets.txt"
    txt = s3_read_text(key)
    if not txt:
        return None
    return [ln.strip() for ln in txt.splitlines() if ln.strip()]

def s3_sheet_exists_for(well_label: str, dash_code: str, sheet: str) -> bool:
    safe_sheet = sanitize_name(sheet)
    base = f"Data/{well_label}/{dash_code}/{well_label}__{safe_sheet}"
    return s3_exists(base + ".csv") or s3_exists(base + ".xlsx")

def compute_missing_remote(well_label: str,
                           dash_code: str,
                           allow_sheets: Optional[List[str]]) -> Tuple[Optional[list[str]], Optional[list[str]]]:
    """
    Returns (all_sheets_from_manifest, missing_sheets) based on S3 only.
    If no remote manifest -> (None, None).
    """
    all_sheets = s3_manifest_for_dashboard(well_label, dash_code)
    if all_sheets is None:
        return (None, None)
    kept = [s for s in all_sheets if should_keep_sheet(s, allow_sheets)]
    missing = [s for s in kept if not s3_sheet_exists_for(well_label, dash_code, s)]
    return (all_sheets, missing)


# --------------- per-well & per-dashboard ---------------
def process_one_dashboard(driver,
                          worker_tmp_dir: Path,
                          well_root: Path,
                          well_label: str,
                          wrapped_uwi: str,
                          dash_code: str,
                          dash_base: str,
                          allow_sheets: Optional[List[str]],
                          force: bool,
                          timeout: int,
                          push_to_s3: bool = False,
                          purge_local: bool = False,
                          check_remote: bool = True):  # default True per your scenario
    dash_dir = well_root / dash_code
    dash_dir.mkdir(parents=True, exist_ok=True)

    # ---------- 1) LOCAL short-circuit via manifest ----------
    local_manifest, local_missing = compute_missing_sheets_for_dashboard(
        well_root, well_label, dash_code, allow_sheets
    )

    if not force and local_manifest is not None and local_missing is not None and len(local_missing) == 0:
        print(f"      [{dash_code}] ✓ Local complete — skipping.")
        return

    # ---------- 2) REMOTE short-circuit (no full download) ----------
    # If local is empty / unknown, and remote says complete, skip without Selenium.
    if not force and check_remote and (local_manifest is None or (local_missing is not None and len(local_missing) > 0)):
        r_all, r_missing = compute_missing_remote(well_label, dash_code, allow_sheets)
        if r_all is not None:
            if len(r_missing) == 0:
                # Refresh local manifest to match remote (optional)
                (dash_dir / "sheets.txt").write_text("\n".join(r_all), encoding="utf-8")
                print(f"      [{dash_code}] ✓ Remote complete — skipping.")
                return
            else:
                print(f"      [{dash_code}] remote missing -> {r_missing}")
                # We will scrape only missing (remote + local union)
                if local_missing is None:
                    filtered_to_get = r_missing
                else:
                    # union local-missing and remote-missing
                    needed = set(r_missing) | set(local_missing)
                    filtered_to_get = [s for s in r_all if s in needed and should_keep_sheet(s, allow_sheets)]
        else:
            filtered_to_get = None  # no remote manifest; we’ll open Tableau
    else:
        filtered_to_get = local_missing  # may be None (means: need to discover sheets)

    # ---------- 3) OPEN dashboard only when we must ----------
    driver.get(url_for(dash_code, dash_base, wrapped_uwi)); pause()
    guard_session_reset(driver)
    enter_viz_context(driver, timeout); pause()

    state = open_crosstab_and_wait_state(driver, timeout=max(120, timeout))
    if state == "empty":
        (dash_dir / "sheets.txt").write_text("", encoding="utf-8")
        print(f"      [{dash_code}] No sheets to select — skipping.")
        close_dialog(driver)
        return

    ensure_csv_format(driver, timeout)
    sheets = list_crosstab_sheets(driver, timeout)
    (dash_dir / "sheets.txt").write_text("\n".join(sheets), encoding="utf-8")
    print(f"      [{dash_code}] sheets (raw): {sheets}")

    # Apply allow-list
    filtered = [s for s in sheets if should_keep_sheet(s, allow_sheets)]
    if allow_sheets is not None:
        print(f"      [{dash_code}] filtered -> {filtered}")
    if not filtered:
        close_dialog(driver)
        return
    pause()

    # Decide final list to download:
    if force:
        to_get = filtered
    else:
        if filtered_to_get is None:
            # compute locally now that we know sheet list
            to_get = []
            for s in filtered:
                safe = sanitize_name(s)
                if not ((dash_dir / f"{well_label}__{safe}.csv").exists() or
                        (dash_dir / f"{well_label}__{safe}.xlsx").exists()):
                    # also skip if it already exists in S3 (we don't want duplicates)
                    if not s3_sheet_exists_for(well_label, dash_code, s):
                        to_get.append(s)
        else:
            # reduce to items that are still missing locally and remotely
            to_get = []
            for s in filtered:
                if s in filtered_to_get:
                    safe = sanitize_name(s)
                    local_have = ((dash_dir / f"{well_label}__{safe}.csv").exists() or
                                  (dash_dir / f"{well_label}__{safe}.xlsx").exists())
                    remote_have = s3_sheet_exists_for(well_label, dash_code, s)
                    if not (local_have or remote_have):
                        to_get.append(s)

    if not to_get:
        print(f"      [{dash_code}] ✓ Nothing to download (local/remote already has all).")
        close_dialog(driver)
        return

    # Mark in progress for this well (folder-level marker)
    s3_mark_inprogress(well_label)

    # ---------- 4) Download loop ----------
    success_count, fail_count = 0, 0
    for sheet in to_get:
        safe_sheet = sanitize_name(sheet)

        # race-safe re-check just before clicking:
        existing_csv  = dash_dir / f"{well_label}__{safe_sheet}.csv"
        existing_xlsx = dash_dir / f"{well_label}__{safe_sheet}.xlsx"
        if (existing_csv.exists() or existing_xlsx.exists()) or s3_sheet_exists_for(well_label, dash_code, sheet):
            print(f"      [{dash_code}] ✓ SKIP (already exists local/S3): {safe_sheet}")
            continue

        try:
            state = open_crosstab_and_wait_state(driver, timeout=max(120, timeout))
        except TimeoutException:
            guard_session_reset(driver)
            state = open_crosstab_and_wait_state(driver, timeout=max(120, timeout))

        if state == "empty":
            print(f"      [{dash_code}] became empty unexpectedly — stopping.")
            close_dialog(driver)
            break

        ensure_csv_format(driver, timeout)

        current = get_selected_sheet_name(driver)
        if norm(current) != norm(sheet):
            select_sheet_by_name(driver, sheet)

        ensure_csv_format(driver, timeout)
        click_dialog_export(driver)

        saved = wait_for_download_and_move(worker_tmp_dir, dash_dir, well_label, sheet, timeout=180)
        if not saved:
            print(f"      [{dash_code}] ✗ {sheet} -> download timed out")
            fail_count += 1
            continue

        try:
            normalize_csv_file(saved, well_label, wrapped_uwi, dash_code, sheet)
        except Exception as e:
            print(f"      [{dash_code}] note: normalize failed on {saved.name}: {e}")

        print(f"      [{dash_code}] ✓ {sheet} -> {saved.name}")
        success_count += 1

        if push_to_s3:
            rel = saved.relative_to(OUT_BASE)
            remote_key = f"Data/{rel.as_posix()}"
            if s3_copyto_if_new(saved, remote_key):
                print(f"      [{dash_code}] ↑ uploaded to S3: {remote_key}")
                if purge_local:
                    try:
                        saved.unlink()
                        print(f"      [{dash_code}] (purged local copy)")
                    except Exception as e:
                        print(f"      [{dash_code}] note: purge failed: {e}")
            else:
                print(f"      [{dash_code}] (S3 already has {remote_key}; skipped upload)")

        pause()

    close_dialog(driver)

    # ---------- 5) Mark final status ----------
    try:
        # Re-check remote completeness after our uploads
        r_all, r_missing = compute_missing_remote(well_label, dash_code, allow_sheets)
        if r_all is not None and len(r_missing) == 0:
            s3_mark_complete(well_label)
        else:
            s3_mark_incomplete(well_label)
    except Exception:
        s3_mark_incomplete(well_label)


def process_one_well(driver, worker_tmp_dir: Path, out_base: Path, raw_uwi: str,
                     selected_dashboards: List[str],
                     sheets_map: Optional[Dict[str, List[str]]],
                     force: bool = False,
                     timeout: int = TIMEOUT,
                     push_to_s3: bool = False,
                     purge_local: bool = False,
                     check_remote: bool = False):
    raw_uwi = (raw_uwi or "").strip()
    well_label = well_label_from_entry(raw_uwi)   # folder & filename prefix (filesystem-safe)
    wrapped = ensure_wrapped(raw_uwi)             # exact value used in URL & CSV

    well_root = out_base / well_label
    well_root.mkdir(parents=True, exist_ok=True)
    print(f"   -> {raw_uwi}  (folder: {well_label})")

    for code in selected_dashboards:
        base = DASHBOARDS[code]
        try:
            allow = None if sheets_map is None else sheets_map.get(code, sheets_map.get("*"))
            process_one_dashboard(driver, worker_tmp_dir, well_root,
                                  well_label, wrapped,
                                  code, base, allow, force, timeout,
                                  push_to_s3=push_to_s3,
                                  purge_local=purge_local,
                                  check_remote=check_remote)
        except Exception as e:
            print(f"      [{code}] ERROR: {e}")
        pause()

# --------------- multiprocessing ---------------
def worker_main(worker_id: int, wells: List[str],
                selected_dashboards: List[str],
                sheets_map: Optional[Dict[str, List[str]]],
                force: bool,
                headless: bool,
                timeout: int,
                delay: float,
                push_to_s3: bool = False,
                purge_local: bool = False,
                check_remote: bool = False):
    global DELAY
    DELAY = delay

    tmp_dir = OUT_BASE / f"_tmp_worker_{worker_id}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    driver = None
    try:
        driver = make_driver(tmp_dir, headless=headless)

        for idx, uwi in enumerate(wells, 1):
            lock_id = (uwi or "").strip()  # EXACT wells.txt entry for S3 locks
            if not acquire_lock(lock_id):
                print(f"[worker {worker_id}] LOCKED elsewhere: {lock_id}")
                continue
            hb = None
            try:
                hb = start_lock_heartbeat(lock_id)
                process_one_well(driver, tmp_dir, OUT_BASE, uwi,
                                 selected_dashboards, sheets_map, force, timeout,
                                 push_to_s3=push_to_s3,
                                 purge_local=purge_local,
                                 check_remote=check_remote)
            finally:
                try:
                    if hb and hasattr(hb, "stop"): hb.stop()
                except Exception:
                    pass
                release_lock(lock_id)
            pause()
    finally:
        try:
            if driver: driver.quit()
        except Exception:
            pass

def chunkify(seq: List[str], n: int) -> List[List[str]]:
    n = max(1, n)
    k, m = divmod(len(seq), n)
    out = []
    start = 0
    for i in range(n):
        size = k + (1 if i < m else 0)
        out.append(seq[start:start+size])
        start += size
    return out

def load_wells(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.lstrip().startswith("#")]

# --------------------- main ---------------------
def parse_dashboards_spec(spec: Optional[str]) -> List[str]:
    if not spec or _norm_name(spec) == "all":
        return list(DASHBOARDS.keys())
    wanted = [p.strip() for p in spec.split(",") if p.strip()]
    out = []
    for w in wanted:
        if w in DASHBOARDS:
            out.append(w)
        else:
            print(f"[warn] Unknown dashboard '{w}' (valid: {list(DASHBOARDS.keys())})")
    return out or list(DASHBOARDS.keys())

def parse_sheets_spec(spec: Optional[str]) -> Optional[Dict[str, List[str]]]:
    """
    Accepts:
      - None or 'all' -> None (download all sheets)
      - Comma list -> apply these names/keywords to all dashboards
      - Per-dashboard -> 'Well_Summary_Report:SheetA|SheetB;Reservoir_Evaluation:Foo'
    """
    if not spec or _norm_name(spec) in ("all",):
        return None
    spec = spec.strip()

    if ";" in spec or ":" in spec:
        out: Dict[str, List[str]] = {}
        parts = [p.strip() for p in spec.split(";") if p.strip()]
        for p in parts:
            if ":" not in p:
                continue
            dash, items = p.split(":", 1)
            dash = dash.strip()
            keys = [k.strip() for k in re.split(r"[,\|]", items) if k.strip()]
            out[dash] = keys
        return out if out else None

    keys = [k.strip() for k in spec.split(",") if k.strip()]
    return {code: keys for code in DASHBOARDS.keys()}

def main():
    global OUT_BASE
    parser = argparse.ArgumentParser(description="AER dashboards multi-scraper (idempotent, S3 locks optional).")
    parser.add_argument("--workers", type=int, default=WORKERS, help="Number of parallel browser windows")
    parser.add_argument("--wells", type=str, default=WELLS_FILE, help="Path to wells.txt")
    parser.add_argument("--dashboards", type=str, default="all",
                        help=("Which dashboards to scrape. Examples:\n"
                              "  all (default)\n"
                              "  Well_Summary_Report\n"
                              "  Well_Summary_Report,Reservoir_Evaluation\n"))
    parser.add_argument("--sheets", type=str, default="all",
                        help=("Sheet selection. 'all' (default), "
                              "'SheetA,SheetB', or per-dashboard "
                              "'Well_Summary_Report:SheetX|SheetY;Reservoir_Evaluation:Foo'"))
    parser.add_argument("--force", action="store_true", help="Re-download even if files exist")
    parser.add_argument("--out-base", type=str, default=None, help="Override output base dir (default ./Data)")
    parser.add_argument("--headless", action="store_true", help="Run browser headless")
    parser.add_argument("--timeout", type=int, default=TIMEOUT, help="Selenium wait timeout (sec)")
    parser.add_argument("--delay", type=float, default=DELAY, help="Small delay between actions (sec)")
    parser.add_argument("--push-to-s3", action="store_true",
                        help="After each successful download, upload the file to S3 if it doesn't exist.")
    parser.add_argument("--purge-local", action="store_true",
                        help="Delete local file after successful S3 upload to keep disk small.")
    parser.add_argument("--check-remote", action="store_true",
                        help="Also check S3 when deciding whether a sheet is already present.")
    args = parser.parse_args()

    if args.out_base:
        OUT_BASE = Path(args.out_base)
    OUT_BASE.mkdir(parents=True, exist_ok=True)

    wells = load_wells(args.wells)
    if not wells:
        print("No UWIs in wells.txt"); sys.exit(1)

    selected_dashboards = parse_dashboards_spec(args.dashboards)
    sheets_map = parse_sheets_spec(args.sheets)

    print(f"[info] Dashboards: {selected_dashboards}")
    print(f"[info] Sheets: {'ALL' if sheets_map is None else sheets_map}")
    print(f"[info] OUT_BASE: {OUT_BASE.resolve()}")
    print(f"[info] Headless: {args.headless}, Timeout: {args.timeout}s, Delay: {args.delay}s")

    groups = chunkify(wells, args.workers)
    procs: List[Process] = []
    for wid, group in enumerate(groups, 1):
        p = Process(target=worker_main, args=(
            wid, group, selected_dashboards, sheets_map,
            args.force, args.headless, args.timeout, args.delay
        ), kwargs=dict(push_to_s3=args.push_to_s3,
                       purge_local=args.purge_local,
                       check_remote=args.check_remote),
                    daemon=False)
        p.start()
        procs.append(p)

    exit_code = 0
    for p in procs:
        p.join()
        if p.exitcode not in (0, None):
            exit_code = p.exitcode
    sys.exit(exit_code)

if __name__ == "__main__":
    try:
        set_start_method("spawn")
    except RuntimeError:
        pass
    main()
