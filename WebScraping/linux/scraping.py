#!/usr/bin/env python3
# Multi-process AER Tableau scraper (manifest-first, minimal logs)
# - File names use FULL wrapped UWI (e.g., AC/07-.../0) -> sanitized
# - Saves into well folder (fixes prior temp-only bug)

import os, sys, time, re, shutil, argparse, html, csv
from pathlib import Path
from urllib.parse import quote
from multiprocessing import Process, set_start_method
from typing import List, Optional, Dict, Tuple
from scrape_and_push import rclone_copy

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

import subprocess, shlex  # add

# ---- S3 constants (match your existing upload path) ----
RCLONE_BIN = os.environ.get("RCLONE_BIN", "rclone")
S3_REMOTE_BASE = "s3aer:aer-scrape-prod/Data"  # used for both read + upload

def _run_quiet(cmd: list[str]) -> tuple[int, str, str]:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.returncode, p.stdout, p.stderr

def s3_list_dash_files(wrapped_uwi: str, dash_code: str) -> Optional[set[str]]:
    """List files in S3 dash folder; returns set of filenames or None on error."""
    if not shutil.which(RCLONE_BIN): return None
    remote_dir = f"{S3_REMOTE_BASE}/{sanitize_name(wrapped_uwi)}/{dash_code}"
    rc, out, _ = _run_quiet([RCLONE_BIN, "lsf", remote_dir, "--files-only"])
    if rc != 0: return None
    return {ln.strip().rstrip("/") for ln in out.splitlines() if ln.strip()}

def s3_read_manifest(wrapped_uwi: str, dash_code: str) -> Optional[List[str]]:
    """Read sheets.txt from S3; returns list of sheets or None if missing/error."""
    if not shutil.which(RCLONE_BIN): return None
    remote_manifest = f"{S3_REMOTE_BASE}/{sanitize_name(wrapped_uwi)}/{dash_code}/sheets.txt"
    rc, out, _ = _run_quiet([RCLONE_BIN, "cat", remote_manifest])
    if rc != 0: return None
    return [ln.strip() for ln in out.splitlines() if ln.strip()]


# ======= constants (no CLI for timeout/delay) =======
WELLS_FILE = "../wells.txt"
OUT_BASE   = Path("Data")
TIMEOUT    = 40           # seconds for Selenium waits
DELAY      = 0.3          # small pause between UI actions
WORKERS    = 2

# Normalizer behaviour
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

# Tableau flags/fragments, matching live behaviour
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
FRAGMENTS = {
    "Well_Summary_Report": "#3",
    "Reservoir_Evaluation": "",
    "Well_Gas_Analysis": "",
}

# ----------------- small utils -----------------
def pause(): time.sleep(DELAY)
def sanitize_name(s: str) -> str: return re.sub(r"[^A-Za-z0-9_.-]+", "_", (s or "").strip())

def norm(s: Optional[str]) -> str:
    if s is None: return ""
    s = html.unescape(s)
    return re.sub(r"\s+", " ", s.strip())

# Accept any 1–2 alphanumeric prefix, slash, body, slash, single digit suffix
WRAPPED_RE = re.compile(r"^([A-Za-z0-9]{1,2})/(.+)/(\d)$")

def is_wrapped_uwi(txt: str) -> bool:
    return bool(WRAPPED_RE.match((txt or "").strip()))

def to_short_uwi(uwi: str) -> str:
    m = WRAPPED_RE.match((uwi or "").strip())
    return m.group(2) if m else (uwi or "").strip()

def ensure_wrapped(uwi: str) -> str:
    """Return '<prefix>/<short>/d' if already wrapped, else '00/<input>/0'."""
    u = (uwi or "").strip()
    return u if is_wrapped_uwi(u) else f"00/{u}/0"

def file_label_from_wrapped(wrapped: str) -> str:
    """Filesystem-safe file stem from the full wrapped UWI."""
    return sanitize_name(wrapped)  # turns slashes into underscores

def url_for(code: str, base: str, wrapped: str) -> str:
    """
    Build URL like:
    ...?Enter%20Well%20Identifier%20(UWI)=AC/07-13-.../0&%3Aembed=y...#fragment
    Keep '/' inside the value unencoded.
    """
    key = quote("Enter Well Identifier (UWI)", safe="()")
    val = quote(ensure_wrapped(wrapped), safe="/")
    return f"{base}?{key}={val}{FLAGS.get(code,'')}{FRAGMENTS.get(code,'')}"

def parse_dashboards_spec(spec: Optional[str]) -> List[str]:
    if not spec or spec.strip().lower() == "all":
        return list(DASHBOARDS.keys())
    wanted = [p.strip() for p in spec.split(",") if p.strip()]
    return [w for w in wanted if w in DASHBOARDS] or list(DASHBOARDS.keys())

# --------------- selenium helpers ---------------
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
        if cand and os.path.exists(cand): return cand
    return None

def make_driver(download_dir: Path, headless: bool):
    from selenium.webdriver.chrome.options import Options
    opts = Options()
    if headless: opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,1000")
    opts.add_experimental_option("prefs", {
        "download.default_directory": str(download_dir.resolve()),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
    })
    binpath = find_browser_binary()
    if binpath: opts.binary_location = binpath
    return webdriver.Chrome(options=opts)

def enter_viz_context(driver):
    driver.switch_to.default_content()
    try:
        WebDriverWait(driver, TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-tb-test-id='tb-icons-DownloadBaseIcon']"))
        ); return
    except Exception:
        pass
    for fr in driver.find_elements(By.TAG_NAME, "iframe"):
        try:
            driver.switch_to.default_content(); driver.switch_to.frame(fr)
            WebDriverWait(driver, TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-tb-test-id='tb-icons-DownloadBaseIcon']"))
            ); return
        except Exception:
            continue
    raise RuntimeError("Download icon not found (toolbar hidden or different layout).")

def open_download_flyout(driver):
    icon = WebDriverWait(driver, TIMEOUT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "[data-tb-test-id='tb-icons-DownloadBaseIcon']"))
    )
    driver.execute_script("arguments[0].closest('button').click();", icon); pause()

def open_crosstab(driver):
    item = WebDriverWait(driver, TIMEOUT).until(
        EC.element_to_be_clickable((By.XPATH,
            "//*[@data-tb-test-id='download-flyout-TextMenuItem' and .//span[normalize-space()='Crosstab']]"
        ))
    )
    driver.execute_script("arguments[0].click();", item); pause()

def close_dialog(driver):
    for xp in [
        "//*[@role='dialog']//button[@aria-label='Close']",
        "//*[@role='dialog']//button[normalize-space()='Close']",
        "//button[@aria-label='Close']",
    ]:
        try: driver.find_element(By.XPATH, xp).click(); pause(); return
        except Exception: pass
    try: driver.switch_to.active_element.send_keys("\ue00c"); pause()
    except Exception: pass

def _find_reset_dialog(driver, timeout=3):
    try:
        return WebDriverWait(driver, timeout, poll_frequency=0.25).until(EC.presence_of_element_located((
            By.XPATH,
            "//*[(@role='dialog' or contains(@class,'dialog')) and "
            "(.//text()[contains(., 'Session Ended by Server')] or "
            ".//text()[contains(., 'reset the view')])]"
        )))
    except TimeoutException:
        return None

def click_no_on_reset_dialog(driver, timeout=3) -> bool:
    dlg = _find_reset_dialog(driver, timeout=timeout)
    if not dlg: return False
    try:
        no_btn = None
        for xp in [".//button[normalize-space()='No']",
                   ".//button[@data-tb-test-id='no' or @aria-label='No']",
                   ".//button[contains(., 'No')]"]:
            try: no_btn = dlg.find_element(By.XPATH, xp); break
            except Exception: pass
        if not no_btn:
            for b in dlg.find_elements(By.XPATH, ".//button"):
                if (b.text or "").strip().lower() != "yes": no_btn = b; break
        if no_btn:
            driver.execute_script("arguments[0].click();", no_btn); time.sleep(0.5); return True
    except StaleElementReferenceException:
        pass
    return False

def guard_session_reset(driver):
    try: return click_no_on_reset_dialog(driver, timeout=2)
    except Exception: return False

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
            if btn.is_enabled(): return "ready"
        except Exception:
            pass
    return "unknown"

def wait_for_crosstab_ready_or_empty(driver, timeout: Optional[int] = None) -> str:
    if timeout is None: timeout = TIMEOUT
    WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, "//*[@role='dialog']")))
    wait = WebDriverWait(driver, timeout, poll_frequency=0.25)
    def _cond(d):
        st = crosstab_state(d)
        return st if st in ("ready", "empty") else False
    return wait.until(_cond)

def open_crosstab_and_wait_state(driver) -> str:
    guard_session_reset(driver); open_download_flyout(driver); pause()
    guard_session_reset(driver); open_crosstab(driver);      pause()
    guard_session_reset(driver)
    state = wait_for_crosstab_ready_or_empty(driver, timeout=TIMEOUT); pause()
    return state

# --- download watcher ---
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
                               file_label: str, sheet_name: str, timeout=180) -> Optional[Path]:
    """
    Moves from per-worker download dir into target_dir as:
      target_dir / f"{file_label}__{sheet}.<ext>"
    where file_label is the FULL wrapped UWI sanitized (e.g., AC_07-13-..._0).
    """
    start = time.time(); deadline = start + timeout
    before = {p for p in worker_download_dir.glob("*") if p.suffix.lower() in VALID_EXTS}
    candidate: Optional[Path] = None
    while time.time() < deadline:
        now = {p for p in worker_download_dir.glob("*") if p.suffix.lower() in VALID_EXTS}
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
        f = files[0]
        if not _size_stable(f): return None
        candidate = f
    ext = candidate.suffix.lower()
    if ext not in VALID_EXTS: ext = _guess_ext(candidate)
    safe_sheet = sanitize_name(sheet_name)
    target = target_dir / f"{file_label}__{safe_sheet}{ext}"
    cnt = 1
    while target.exists():
        target = target_dir / f"{file_label}__{safe_sheet}_{cnt}{ext}"; cnt += 1
    try: shutil.move(str(candidate), str(target))
    except Exception:
        shutil.copy2(candidate, target)
        try: candidate.unlink()
        except Exception: pass
    return target

def xpath_literal(s):
    if "'" not in s: return f"'{s}'"
    if '"' not in s: return f'"{s}"'
    parts = s.split("'"); return "concat(" + ", \"'\", ".join([f"'{p}'" for p in parts]) + ")"

# ---------- CSV normalization ----------
CANDIDATE_DELIMS = [",", ";", "\t", "|"]

def _detect_delimiter(text: str) -> str:
    lines = [ln for ln in text.splitlines() if ln.strip()][:50]
    if not lines: return ","
    best_delim, best_var, best_modal = ",", float("inf"), 0
    for d in CANDIDATE_DELIMS:
        cols = [ln.count(d) + 1 for ln in lines]
        if not cols: continue
        modal = max(set(cols), key=cols.count)
        var = sum((c - modal) ** 2 for c in cols)
        if (var < best_var) or (var == best_var and modal > best_modal):
            best_delim, best_var, best_modal = d, var, modal
    return best_delim

def _sniff_text_encoding(path: Path) -> str:
    with open(path, "rb") as fb: head = fb.read(4096)
    if head.startswith(b"\xff\xfe"): return "utf-16-le"
    if head.startswith(b"\xfe\xff"): return "utf-16-be"
    if head.startswith(b"\xef\xbb\xbf"): return "utf-8-sig"
    if b"\x00" in head[:100]: return "utf-16-le"
    try: head.decode("utf-8"); return "utf-8"
    except Exception: return "cp1252"

def _norm_header(h: str) -> str: return re.sub(r"[^a-z0-9]+", "", h.strip().lower())

BASE_FORMATTED_SYNS = {"wellidentifier","formatteduwi","welluwiformatted","enterwellidentifieruwi","prodstringuwiformatted"}
NUMERIC_UWI_SYNS    = {"welluwi","welluwi.","welluwi ","welluwi_","welluwi-"}

def _is_formatted_header_key(key: str) -> bool:
    return key in BASE_FORMATTED_SYNS or ("uwi" in key and "formatted" in key) or key == "wellidentifier"
def _is_numeric_uwi_key(key: str) -> bool:
    return key in NUMERIC_UWI_SYNS or ("uwi" in key and "formatted" not in key and "identifier" not in key)

def _drop_all_empty_columns(rows: list[list[str]]) -> list[list[str]]:
    if not rows: return rows
    cols = list(zip(*rows)); keep = []
    for i, col in enumerate(cols):
        if any((c or "").strip() for c in col[1:]): keep.append(i)
    return [[r[i] for i in keep] for r in rows]

def normalize_csv_file(path: Path, short_uwi: str, wrapped_uwi: str, dashboard: str, sheet: str) -> None:
    if path.suffix.lower() != ".csv": return
    enc = _sniff_text_encoding(path)
    raw = path.read_text(encoding=enc, errors="replace")
    delim = _detect_delimiter(raw)
    parsed: list[list[str]] = [next(csv.reader([ln], delimiter=delim)) for ln in raw.splitlines()]
    if not parsed: return
    header = [h.strip() for h in parsed[0]]; data = parsed[1:]
    mapped, seen_formatted_idx = [], None
    for i, h in enumerate(header):
        key = _norm_header(h)
        if _is_formatted_header_key(key): mapped.append("UWI_Formatted"); seen_formatted_idx = i
        elif _is_numeric_uwi_key(key):    mapped.append("UWI_Numeric")
        else:                              mapped.append(h)
    header = mapped
    if ADD_UWI_FORMATTED:
        if seen_formatted_idx is None:
            header.append("UWI_Formatted")
            for r in data: r.append(wrapped_uwi)
        else:
            for r in data:
                if seen_formatted_idx < len(r) and not (r[seen_formatted_idx] or "").strip():
                    r[seen_formatted_idx] = wrapped_uwi
    if ADD_UWI_SHORT and "UWI_Short" not in header:
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
    if STRIP_EMPTY_TRAILING_COLS: out_rows = _drop_all_empty_columns(out_rows)
    tmp = path.with_suffix(".csv.tmp")
    with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=delim, quoting=csv.QUOTE_MINIMAL); w.writerows(out_rows)
    tmp.replace(path)

# -------- manifest/skip logic ----------
def compute_missing_sheets_for_dashboard(
    well_root: Path,
    file_label: str,
    dash_code: str
) -> Tuple[Optional[List[str]], Optional[List[str]]]:
    """
    file_label is the FULL wrapped UWI sanitized (used in filenames).
    """
    dash_dir = well_root / dash_code
    manifest = dash_dir / "sheets.txt"
    if not manifest.exists(): return (None, None)
    try: raw = manifest.read_text(encoding="utf-8").splitlines()
    except Exception: raw = []
    all_sheets = [ln.strip() for ln in raw if ln.strip()]
    if not all_sheets: return ([], [])
    missing = []
    for sheet in all_sheets:
        safe = sanitize_name(sheet)
        if not ((dash_dir / f"{file_label}__{safe}.csv").exists() or (dash_dir / f"{file_label}__{safe}.xlsx").exists()):
            missing.append(sheet)
    return (all_sheets, missing)

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

def click_dialog_export(driver, timeout: int):
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

# --------------- per-well & per-dashboard ---------------
def process_one_dashboard(driver, worker_tmp_dir: Path, well_root: Path,
                          short_uwi: str, wrapped_uwi: str, file_label: str,
                          dash_code: str, dash_base: str, force: bool) -> Tuple[int,int]:
    dash_dir = well_root / dash_code
    dash_dir.mkdir(parents=True, exist_ok=True)
    # If we don't have a local manifest, try to hydrate it from S3.
    if not force:
        local_manifest = dash_dir / "sheets.txt"
        if not local_manifest.exists():
            s3_files = s3_list_dash_files(wrapped_uwi, dash_code)
            if s3_files is not None and "sheets.txt" in s3_files:
                s3_sheets = s3_read_manifest(wrapped_uwi, dash_code)
                if s3_sheets is not None:
                    # Write manifest locally to enable the existing fast-path logic
                    try:
                        local_manifest.write_text("\n".join(s3_sheets), encoding="utf-8")
                    except Exception:
                        pass


    if not force:
        manifest_sheets, missing = compute_missing_sheets_for_dashboard(well_root, file_label, dash_code)
        if manifest_sheets is not None and missing is not None and len(missing) == 0:
            return (0, len(manifest_sheets))  # nothing to do

    driver.get(url_for(dash_code, dash_base, wrapped_uwi)); pause()
    guard_session_reset(driver); enter_viz_context(driver); pause()

    state = open_crosstab_and_wait_state(driver)
    if state == "empty":
        (dash_dir / "sheets.txt").write_text("", encoding="utf-8")
        close_dialog(driver); return (0, 0)

    # pull/refresh manifest now
    ensure_csv_format(driver, TIMEOUT)
    sheets = list_crosstab_sheets(driver, TIMEOUT)
    (dash_dir / "sheets.txt").write_text("\n".join(sheets), encoding="utf-8")
    pause()

    to_download, skipped = [], 0
    if force:
        to_download = sheets[:]
    else:
        for s in sheets:
            safe = sanitize_name(s)
            if (dash_dir / f"{file_label}__{safe}.csv").exists() or (dash_dir / f"{file_label}__{safe}.xlsx").exists():
                skipped += 1
            else:
                to_download.append(s)
        if not to_download:
            try:
                rclone_copy(
                    str(dash_dir),
                    f"{S3_REMOTE_BASE}/{sanitize_name(wrapped_uwi)}/{dash_code}",
                    rclone_bin="rclone"
                )
            except Exception as e:
                print(f"[warn] upload failed for {short_uwi}/{dash_code}: {e}")

            close_dialog(driver); return (0, skipped)

    downloaded = 0
    for sheet in to_download:
        try:
            state = open_crosstab_and_wait_state(driver)
        except TimeoutException:
            guard_session_reset(driver); state = open_crosstab_and_wait_state(driver)
        if state == "empty": close_dialog(driver); break
        ensure_csv_format(driver, TIMEOUT)
        current = get_selected_sheet_name(driver)
        if norm(current) != norm(sheet): select_sheet_by_name(driver, sheet)
        ensure_csv_format(driver, TIMEOUT); click_dialog_export(driver, TIMEOUT)

        saved = wait_for_download_and_move(
            worker_download_dir=worker_tmp_dir,
            target_dir=dash_dir,
            file_label=file_label,
            sheet_name=sheet,
            timeout=max(180, TIMEOUT),
        )
        if saved:
            try: normalize_csv_file(saved, short_uwi, wrapped_uwi, dash_code, sheet)
            except Exception: pass
            downloaded += 1
        pause()

    close_dialog(driver)
    return (downloaded, skipped)

def process_one_well(driver, worker_tmp_dir: Path, out_base: Path, raw_uwi: str,
                     selected_dashboards: List[str], force: bool):
    # Folder by SHORT UWI (no slashes), file names by FULL WRAPPED UWI
    short_uwi  = to_short_uwi(raw_uwi)
    wrapped_uwi = ensure_wrapped(raw_uwi)
    file_label = file_label_from_wrapped(wrapped_uwi)

    well_root = out_base / sanitize_name(wrapped_uwi)
    well_root.mkdir(parents=True, exist_ok=True)

    total_dl, total_skip = 0, 0
    for code in selected_dashboards:
        base = DASHBOARDS[code]
        try:
            dl, sk = process_one_dashboard(
                driver, worker_tmp_dir, well_root,
                short_uwi, wrapped_uwi, file_label,
                code, base, force
            )
            total_dl += dl; total_skip += sk
        except Exception as e:
            print(f"[warn] {short_uwi}/{code}: {e}")
        pause()
    print(f"well {short_uwi}: downloaded {total_dl}, skipped {total_skip}")

# --------------- multiprocessing ---------------
def worker_main(worker_id: int, wells: List[str], selected_dashboards: List[str], force: bool, headless: bool):
    tmp_dir = OUT_BASE / f"_tmp_worker_{worker_id}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    driver = None
    try:
        driver = make_driver(tmp_dir, headless=headless)
        for idx, uwi in enumerate(wells, 1):
            try:
                process_one_well(driver, tmp_dir, OUT_BASE, uwi, selected_dashboards, force)
            except Exception as e:
                print(f"[warn] worker {worker_id} error on {uwi}: {e}")
                try: driver.quit()
                except Exception: pass
                driver = make_driver(tmp_dir, headless=headless)
                try:
                    process_one_well(driver, tmp_dir, OUT_BASE, uwi, selected_dashboards, force)
                except Exception as e2:
                    print(f"[warn] worker {worker_id} retry failed: {e2}")
            pause()
    finally:
        try:
            if driver: driver.quit()
        except Exception:
            pass

def chunkify(seq: List[str], n: int) -> List[List[str]]:
    n = max(1, n); k, m = divmod(len(seq), n)
    out, start = [], 0
    for i in range(n):
        size = k + (1 if i < m else 0)
        out.append(seq[start:start+size]); start += size
    return out

def load_wells(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.lstrip().startswith("#")]

# quick planning numbers
def plan_manifest_summary(wells: List[str], dashboards: List[str], out_base: Path) -> Tuple[int,int,int]:
    manifests = complete_dash = missing_total = 0
    for raw in wells:
        label = sanitize_name(ensure_wrapped(raw))
        well_root = out_base / label
        for code in dashboards:
            dash_dir = well_root / code
            mf = dash_dir / "sheets.txt"
            if not mf.exists(): continue
            manifests += 1
            try:
                sheets = [ln.strip() for ln in mf.read_text(encoding="utf-8").splitlines() if ln.strip()]
            except Exception:
                sheets = []
            if not sheets: continue
            file_label = file_label_from_wrapped(ensure_wrapped(raw))
            missing_here = 0
            for s in sheets:
                safe = sanitize_name(s)
                if not ((dash_dir / f"{file_label}__{safe}.csv").exists() or (dash_dir / f"{file_label}__{safe}.xlsx").exists()):
                    missing_here += 1
            if missing_here == 0: complete_dash += 1
            else: missing_total += missing_here
    return manifests, complete_dash, missing_total

# --------------------- main ---------------------
def main():
    parser = argparse.ArgumentParser(description="AER dashboards scraper (manifest-first skip).")
    parser.add_argument("--workers", type=int, default=WORKERS)
    parser.add_argument("--wells", type=str, default=WELLS_FILE)
    parser.add_argument("--dashboards", type=str, default="all")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--out-base", type=str, default=None)
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()

    global OUT_BASE
    if args.out_base: OUT_BASE = Path(args.out_base)
    OUT_BASE.mkdir(parents=True, exist_ok=True)

    wells = load_wells(args.wells)
    if not wells:
        print("[error] no UWIs in wells file"); sys.exit(1)

    selected_dashboards = parse_dashboards_spec(args.dashboards)
    mf, complete, missing = plan_manifest_summary(wells, selected_dashboards, OUT_BASE)
    print(f"[info] plan: wells={len(wells)}, manifests={mf}, complete_dash={complete}, missing_sheets={missing}")
    print(f"[info] OUT_BASE={OUT_BASE.resolve()}, headless={args.headless}")

    groups = chunkify(wells, args.workers)
    procs: List[Process] = []
    for wid, group in enumerate(groups, 1):
        p = Process(target=worker_main, args=(wid, group, selected_dashboards, args.force, args.headless), daemon=False)
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
