# aer_multi_dash_mp.py
import os, sys, time, re, shutil, argparse, html, csv
from pathlib import Path
from urllib.parse import urlencode
from multiprocessing import Process, set_start_method
from typing import List, Optional, Dict

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

# ========= CONFIG =========
WELLS_FILE = "../wells.txt"  # one UWI per line (either "01-01-013-16W4" or "00/01-01-013-16W4/0")
OUT_BASE   = Path("Data")         # root output
HEADLESS   = False                # True for headless runs
TIMEOUT    = 60                   # seconds for waits
DELAY      = 1.0                  # seconds between actions
WORKERS    = 2                    # default; override with --workers

# Normalizer behaviour
ADD_UWI_FORMATTED = True     # ensure there is exactly one UWI_Formatted
ADD_UWI_SHORT     = True    # don't append UWI_Short
ADD_PROVENANCE    = True    # don't append Dashboard/Sheet
STRIP_EMPTY_TRAILING_COLS = True

# ==========================================

# Dashboards to scrape (folder name -> base view URL)
DASHBOARDS: Dict[str, str] = {
    "Well_Summary_Report":
        "https://www2.aer.ca/t/Production/views/PRD_0100_Well_Summary_Report/WellSummaryReport",
    "Well_Gas_Analysis":
        "https://www2.aer.ca/t/Production/views/0125_Well_Gas_Analysis_Data_EXT/WellGasAnalysis",
    "Reservoir_Evaluation":
        "https://www2.aer.ca/t/Production/views/0150_IMB_Well_Reservoir_Eval_EXT/ResourceEvaluation",
}

# ----------------- small utils -----------------
def pause(): time.sleep(DELAY)

def sanitize_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s.strip())

def norm(s: Optional[str]) -> str:
    if s is None: return ""
    s = html.unescape(s)
    return re.sub(r"\s+", " ", s.strip())

def is_wrapped_uwi(txt: str) -> bool:
    return txt.startswith("00/") and txt.endswith("/0")

def to_short_uwi(uwi: str) -> str:
    """Return short UWI like '01-01-013-16W4' whether input is short or '00/.../0'."""
    u = uwi.strip()
    if is_wrapped_uwi(u):
        u = u[3:-2]  # strip '00/' and '/0'
    return u

def wrap_uwi(uwi: str) -> str:
    """Return Tableau-required wrapped UWI '00/<short>/0' for URL queries."""
    short = to_short_uwi(uwi)
    return f"00/{short}/0"

def url_for(dash_base: str, uwi: str) -> str:
    qs = urlencode({
        ":showVizHome": "no",
        ":toolbar": "yes",
        "Enter Well Identifier (UWI)": wrap_uwi(uwi),
    })
    return f"{dash_base}?{qs}"

def find_browser_binary():
    for cand in [
        os.environ.get("CHROME_BIN"),
        os.environ.get("GOOGLE_CHROME_BIN"),
        shutil.which("google-chrome"),
        shutil.which("google-chrome-stable"),
        shutil.which("chromium-browser"),
        shutil.which("chromium"),
        "/snap/bin/chromium",
    ]:
        if cand and os.path.exists(cand):
            return cand
    return None

# --------------- selenium helpers ---------------
def make_driver(download_dir: Path):
    from selenium.webdriver.chrome.options import Options
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,1000")
    # Per-worker download dir
    opts.add_experimental_option("prefs", {
        "download.default_directory": str(download_dir.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
    })
    binpath = find_browser_binary()
    if binpath:
        opts.binary_location = binpath
    # Selenium Manager (>=4.6) resolves the driver automatically
    return webdriver.Chrome(options=opts)

def enter_viz_context(driver):
    """Find context (top or single iframe) that contains the toolbar Download icon."""
    driver.switch_to.default_content()
    try:
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-tb-test-id='tb-icons-DownloadBaseIcon']"))
        ); return
    except Exception:
        pass
    for fr in driver.find_elements(By.TAG_NAME, "iframe"):
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(fr)
            WebDriverWait(driver, int(TIMEOUT/2)).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-tb-test-id='tb-icons-DownloadBaseIcon']"))
            ); return
        except Exception:
            continue
    raise RuntimeError("Download icon not found (toolbar hidden or different layout).")

def open_download_flyout(driver):
    icon = WebDriverWait(driver, TIMEOUT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "[data-tb-test-id='tb-icons-DownloadBaseIcon']"))
    )
    driver.execute_script("arguments[0].closest('button').click();", icon)
    pause()

def open_crosstab(driver):
    item = WebDriverWait(driver, TIMEOUT).until(
        EC.element_to_be_clickable((
            By.XPATH, "//*[@data-tb-test-id='download-flyout-TextMenuItem' and .//span[normalize-space()='Crosstab']]"
        ))
    )
    driver.execute_script("arguments[0].click();", item)
    pause()

def close_dialog(driver):
    """Best-effort close of the Crosstab dialog."""
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
    # fallback ESC
    try:
        driver.switch_to.active_element.send_keys("\ue00c")  # Keys.ESCAPE
        pause()
    except Exception:
        pass

def _find_reset_dialog(driver, timeout=3):
    """
    Return the dialog WebElement if the 'Session Ended by Server' / 'reset the view'
    modal is present; else return None.
    """
    try:
        dlg = WebDriverWait(driver, timeout, poll_frequency=0.25).until(
            EC.presence_of_element_located((
                By.XPATH,
                # robust: match either title text or message text inside a dialog
                "//*[(@role='dialog' or contains(@class,'dialog')) and "
                "(.//text()[contains(., 'Session Ended by Server')] or "
                ".//text()[contains(., 'reset the view')])]"
            ))
        )
        return dlg
    except TimeoutException:
        return None

def click_no_on_reset_dialog(driver, timeout=3) -> bool:
    """
    If the reset modal is shown, click 'No' and return True. Otherwise return False.
    """
    dlg = _find_reset_dialog(driver, timeout=timeout)
    if not dlg:
        return False
    try:
        # Prefer explicit 'No' button
        no_btn = None
        for xp in [
            ".//button[normalize-space()='No']",
            ".//button[@data-tb-test-id='no' or @aria-label='No']",
            ".//button[contains(., 'No')]",
        ]:
            try:
                no_btn = dlg.find_element(By.XPATH, xp)
                break
            except Exception:
                pass
        if not no_btn:
            # If buttons are generic, pick the one that is NOT 'Yes'
            btns = dlg.find_elements(By.XPATH, ".//button")
            for b in btns:
                t = (b.text or "").strip().lower()
                if t != "yes":
                    no_btn = b
                    break
        if no_btn:
            driver.execute_script("arguments[0].click();", no_btn)
            time.sleep(0.5)
            return True
    except StaleElementReferenceException:
        pass
    return False

def guard_session_reset(driver):
    """Call this opportunistically; safe if the dialog isn't present."""
    try:
        return click_no_on_reset_dialog(driver, timeout=2)
    except Exception:
        return False

def crosstab_state(driver) -> str:
    """
    Returns one of:
      - 'ready'  : at least one sheet and inner Download button is clickable
      - 'empty'  : dialog shows 'No sheets to select'
      - 'unknown': dialog present but neither condition matched
    """
    dlg = driver.find_element(By.XPATH, "//*[@role='dialog']")
    # detect empty message
    try:
        dlg.find_element(By.XPATH, ".//*[contains(normalize-space(),'No sheets to select')]")
        return "empty"
    except Exception:
        pass
    # detect sheets + clickable Download
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

def open_crosstab_and_wait_state(driver) -> str:
    guard_session_reset(driver)    # <— add
    open_download_flyout(driver); pause()
    guard_session_reset(driver)    # <— add
    open_crosstab(driver); pause()
    guard_session_reset(driver)    # <— add
    state = wait_for_crosstab_ready_or_empty(driver)
    pause()
    return state

def ensure_csv_format(driver):
    dlg = WebDriverWait(driver, TIMEOUT).until(EC.presence_of_element_located((By.XPATH, "//*[@role='dialog']")))
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

def list_crosstab_sheets(driver) -> List[str]:
    dlg = WebDriverWait(driver, TIMEOUT).until(EC.presence_of_element_located((By.XPATH, "//*[@role='dialog']")))
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

# --- robust download watcher (prevents random suffixes) ---
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
    """Lightweight sniff: .xlsx starts with PK; otherwise assume .csv."""
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
                               short_uwi: str,
                               sheet_name: str,
                               timeout=180) -> Optional[Path]:
    """
    Wait for a brand-new CSV/XLSX created after we clicked Download,
    ensure it's not still growing, then move+rename to:
      target_dir / f"{short_uwi}__{sheet}.<ext>"
    """
    start = time.time()
    deadline = start + timeout

    before_csv = set(p for p in worker_download_dir.glob("*")
                     if p.suffix.lower() in VALID_EXTS)

    candidate: Optional[Path] = None
    while time.time() < deadline:
        now_csv = set(p for p in worker_download_dir.glob("*")
                      if p.suffix.lower() in VALID_EXTS)
        new_csv = list(now_csv - before_csv)
        if new_csv:
            new_csv.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            f = new_csv[0]
            if _size_stable(f):
                candidate = f
                break
        time.sleep(0.25)

    # Fallback if no final ext yet: pick newest non-temp and sniff
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
    target = target_dir / f"{short_uwi}__{safe_sheet}{ext}"
    cnt = 1
    while target.exists():
        target = target_dir / f"{short_uwi}__{safe_sheet}_{cnt}{ext}"
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

# ---------- CSV normalization (consistent join keys) ----------
import csv

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
        # prefer lowest variance, then higher modal (more columns)
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

# liberal recognition of formatted / numeric UWI headers
BASE_FORMATTED_SYNS = {
    "wellidentifier", "formatteduwi", "welluwiformatted",
    "enterwellidentifieruwi", "prodstringuwiformatted"
}
NUMERIC_UWI_SYNS = {"welluwi", "welluwi.", "welluwi ", "welluwi_", "welluwi-", "welluwi—"}

def _is_formatted_header_key(key: str) -> bool:
    return key in BASE_FORMATTED_SYNS or ("uwi" in key and "formatted" in key) or key == "wellidentifier"

def _is_numeric_uwi_key(key: str) -> bool:
    return key in NUMERIC_UWI_SYNS or ("uwi" in key and "formatted" not in key and "identifier" not in key)

def _drop_all_empty_columns(rows: list[list[str]]) -> list[list[str]]:
    """Remove columns where every data cell (excluding header) is empty."""
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

def normalize_csv_file(path: Path, short_uwi: str, wrapped_uwi: str,
                       dashboard: str, sheet: str) -> None:
    """
    Read with robust encoding + delimiter detection, standardize headers,
    optionally ensure a single 'UWI_Formatted' column, and write back as UTF-8 BOM
    using the same delimiter. Optionally strip trailing empty columns.
    """
    if path.suffix.lower() != ".csv":
        return

    enc = _sniff_text_encoding(path)
    raw = path.read_text(encoding=enc, errors="replace")
    delim = _detect_delimiter(raw)

    # parse with detected delimiter
    parsed: list[list[str]] = [next(csv.reader([ln], delimiter=delim)) for ln in raw.splitlines()]
    if not parsed:
        return

    header = [h.strip() for h in parsed[0]]
    data   = parsed[1:]

    # rename header synonyms (don’t add extras here)
    mapped = []
    seen_formatted_idx = None
    for i, h in enumerate(header):
        key = _norm_header(h)
        if _is_formatted_header_key(key):
            mapped.append("UWI_Formatted")
            seen_formatted_idx = i
        elif _is_numeric_uwi_key(key):
            mapped.append("UWI_Numeric")
        else:
            mapped.append(h)

    header = mapped

    # optionally ensure exactly one UWI_Formatted column
    if ADD_UWI_FORMATTED:
        if seen_formatted_idx is None:
            header.append("UWI_Formatted")
            for r in data:
                r.append(wrapped_uwi)
        else:
            # if present but empty, fill empties with wrapped_uwi
            for r in data:
                if seen_formatted_idx < len(r):
                    if not (r[seen_formatted_idx] or "").strip():
                        r[seen_formatted_idx] = wrapped_uwi

    # (intentionally NOT adding UWI_Short / Dashboard / Sheet unless you set flags)
    if ADD_UWI_SHORT:
        if "UWI_Short" not in header:
            header.append("UWI_Short")
            for r in data: r.append(short_uwi)
    if ADD_PROVENANCE:
        if "Dashboard" not in header:
            header.append("Dashboard")
            for r in data: r.append(dashboard)
        if "Sheet" not in header:
            header.append("Sheet")
            for r in data: r.append(sheet)

    out_rows = [header] + data

    # drop columns that are entirely empty (lots of Tableau sheets have a trailing empty field)
    if STRIP_EMPTY_TRAILING_COLS:
        out_rows = _drop_all_empty_columns(out_rows)

    # write back as UTF-8 with BOM (Excel-friendly) using same delimiter
    tmp = path.with_suffix(".csv.tmp")
    with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=delim, quoting=csv.QUOTE_MINIMAL)
        w.writerows(out_rows)
    tmp.replace(path)


# --------------- per-well & per-dashboard ---------------
def process_one_dashboard(driver, worker_tmp_dir: Path, well_root: Path, short_uwi: str, dash_code: str, dash_base: str):
    dash_dir = well_root / dash_code
    dash_dir.mkdir(parents=True, exist_ok=True)

    # Open dashboard
    driver.get(url_for(dash_base, short_uwi)); pause()
    guard_session_reset(driver)
    enter_viz_context(driver); pause()

    # Open dialog and determine if there are sheets
    state = open_crosstab_and_wait_state(driver)
    if state == "empty":
        (dash_dir / "sheets.txt").write_text("", encoding="utf-8")
        print(f"      [{dash_code}] No sheets to select — skipping.")
        close_dialog(driver)
        return

    # state == 'ready'
    ensure_csv_format(driver)
    sheets = list_crosstab_sheets(driver)
    (dash_dir / "sheets.txt").write_text("\n".join(sheets), encoding="utf-8")
    print(f"      [{dash_code}] sheets: {sheets}")
    pause()

    # Download each sheet (re-open dialog each time to avoid drift)
    for sheet in sheets:
        try:
            state = open_crosstab_and_wait_state(driver)
        except TimeoutException:
            guard_session_reset(driver)
            state = open_crosstab_and_wait_state(driver)

        if state == "empty":
            print(f"      [{dash_code}] became empty unexpectedly — skipping remaining sheets.")
            close_dialog(driver)
            break

        ensure_csv_format(driver)

        # avoid toggling off the preselected first tile
        current = get_selected_sheet_name(driver)
        if norm(current) != norm(sheet):
            select_sheet_by_name(driver, sheet)

        ensure_csv_format(driver)
        click_dialog_export(driver)

        saved = wait_for_download_and_move(worker_tmp_dir, dash_dir, short_uwi, sheet, timeout=180)
        if saved:
            try:
                normalize_csv_file(saved, short_uwi, wrap_uwi(short_uwi), dash_code, sheet)
            except Exception as e:
                print(f"      [{dash_code}] note: normalize failed on {saved.name}: {e}")
            print(f"      [{dash_code}] ✓ {sheet} -> {saved.name}")
        else:
            print(f"      [{dash_code}] ✗ {sheet} -> download timed out")
        pause()

def process_one_well(driver, worker_tmp_dir: Path, out_base: Path, raw_uwi: str):
    short_uwi = to_short_uwi(raw_uwi)
    well_root = out_base / sanitize_name(short_uwi)
    well_root.mkdir(parents=True, exist_ok=True)
    print(f"   -> {raw_uwi}  (short: {short_uwi})")

    for code, base in DASHBOARDS.items():
        try:
            process_one_dashboard(driver, worker_tmp_dir, well_root, short_uwi, code, base)
        except Exception as e:
            print(f"      [{code}] ERROR: {e}")
        pause()

# --------------- multiprocessing ---------------
def worker_main(worker_id: int, wells: List[str]):
    tmp_dir = OUT_BASE / f"_tmp_worker_{worker_id}"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    driver = None
    try:
        driver = make_driver(tmp_dir)
        for idx, uwi in enumerate(wells, 1):
            print(f"[worker {worker_id}] ({idx}/{len(wells)}) {uwi}")
            try:
                process_one_well(driver, tmp_dir, OUT_BASE, uwi)
            except Exception as e:
                print(f"[worker {worker_id}] ERROR on {uwi}: {e}")
                # recycle driver & retry once
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = make_driver(tmp_dir)
                try:
                    process_one_well(driver, tmp_dir, OUT_BASE, uwi)
                except Exception as e2:
                    print(f"[worker {worker_id}] RETRY failed on {uwi}: {e2}")
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
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=WORKERS, help="Number of parallel browser windows")
    parser.add_argument("--wells", type=str, default=WELLS_FILE, help="Path to wells.txt")
    args = parser.parse_args()

    OUT_BASE.mkdir(parents=True, exist_ok=True)
    wells = load_wells(args.wells)
    if not wells:
        print("No UWIs in wells.txt"); sys.exit(1)

    groups = chunkify(wells, args.workers)
    procs: List[Process] = []
    for wid, group in enumerate(groups, 1):
        p = Process(target=worker_main, args=(wid, group), daemon=False)
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
