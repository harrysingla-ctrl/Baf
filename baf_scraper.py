"""
BAF/DAA Scraper v5 - Correct scheme codes + working equity allocation source
=============================================================================
Strategy:
  1. Auto-discover scheme codes from AMFI's NAV text file (plain HTTP, reliable)
  2. Fetch equity allocation from AMFI's portfolio CSV download
  3. Fall back to manual seed values if scraping fails

Install:  pip install requests
Run:      python baf_scraper.py --scrape --export
Output:   data.js       (used by index.html)
          history.json  (running history across months)

To find scheme codes manually:
  Download: https://www.amfiindia.com/spages/NAVAll.txt
  Search for fund name — scheme code is the first semicolon-separated field.
"""

import re, json, time, logging, argparse
from datetime import datetime
from pathlib import Path
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# ── Fund definitions ──────────────────────────────────────────────────────────
# search_name: used to find the scheme code in AMFI NAV file (case-insensitive)
# manual_equity: set this (e.g. 45.2) if you want to override with a known value

FUNDS = [
    {
        "id": "icici_baf",
        "name": "ICICI Pru Balanced Advantage",
        "search_name": "icici prudential balanced advantage fund - direct plan - growth",
        "amfi_scheme_code": None,
        "manual_equity": None,
    },
    {
        "id": "hdfc_baf",
        "name": "HDFC Balanced Advantage",
        "search_name": "hdfc balanced advantage fund - direct plan - growth",
        "amfi_scheme_code": None,
        "manual_equity": None,
    },
    {
        "id": "edelweiss_baf",
        "name": "Edelweiss Balanced Advantage",
        "search_name": "edelweiss balanced advantage fund - direct plan - growth",
        "amfi_scheme_code": None,
        "manual_equity": None,
    },
    {
        "id": "dsp_daa",
        "name": "DSP Dynamic Asset Allocation",
        "search_name": "dsp dynamic asset allocation fund - direct plan - growth",
        "amfi_scheme_code": None,
        "manual_equity": None,
    },
    {
        "id": "kotak_baf",
        "name": "Kotak Balanced Advantage",
        "search_name": "kotak balanced advantage fund - direct plan - growth",
        "amfi_scheme_code": None,
        "manual_equity": None,
    },
    {
        "id": "nippon_baf",
        "name": "Nippon India Balanced Advantage",
        "search_name": "nippon india balanced advantage fund - direct plan - growth",
        "amfi_scheme_code": None,
        "manual_equity": None,
    },
]

# ── MANUAL SEED (edit these if automated scraping fails) ─────────────────────
# Get values from each fund's monthly factsheet page (links in summary below).
# Set to a float like 45.2 — leave as None to rely on automated scraping.
MANUAL_EQUITY_SEED = {
    "icici_baf":     None,
    "hdfc_baf":      None,
    "edelweiss_baf": None,
    "dsp_daa":       None,
    "kotak_baf":     None,
    "nippon_baf":    None,
}

FACTSHEET_URLS = {
    "icici_baf":     "https://www.icicipruamc.com/mutual-fund/hybrid-funds/icici-prudential-balanced-advantage-fund",
    "hdfc_baf":      "https://www.hdfcfund.com/our-products/equity/hdfc-balanced-advantage-fund",
    "edelweiss_baf": "https://www.edelweissmf.com/funds/edelweiss-balanced-advantage-fund",
    "dsp_daa":       "https://www.dspim.com/mutual-fund/dsp-dynamic-asset-allocation-fund",
    "kotak_baf":     "https://www.kotakmf.com/funds/details/kotak-balanced-advantage-fund",
    "nippon_baf":    "https://mf.nipponindiaim.com/FundDetails?id=nippon-india-balanced-advantage-fund",
}

# ── Step 1: Discover scheme codes from AMFI NAV file ─────────────────────────

AMFI_NAV_URL = "https://www.amfiindia.com/spages/NAVAll.txt"


def discover_scheme_codes(funds: list) -> dict:
    """
    Download AMFI's NAV text file and find each fund's scheme code.
    File format (semicolon-separated):
        SchemeCode;ISIN1;ISIN2;SchemeName;NetAssetValue;Date
    Returns {fund_id: scheme_code}
    """
    log.info("Downloading AMFI NAV file to discover scheme codes...")
    try:
        r = requests.get(AMFI_NAV_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
        nav_text = r.text
        log.info(f"NAV file: {len(nav_text):,} chars, {nav_text.count(chr(10))} lines")
    except Exception as e:
        log.error(f"Failed to download NAV file: {e}")
        return {}

    codes = {}
    for fund in funds:
        target = fund["search_name"].lower().strip()
        found = False

        # Pass 1: exact match on scheme name field
        for line in nav_text.splitlines():
            parts = line.split(";")
            if len(parts) >= 4:
                name = parts[3].strip().lower()
                if name == target:
                    codes[fund["id"]] = parts[0].strip()
                    log.info(f"  ✅ {fund['name']}: {parts[0].strip()} (exact)")
                    found = True
                    break

        if found:
            continue

        # Pass 2: fuzzy keyword match (require "direct" + "growth")
        keywords = [w for w in target.split() if len(w) > 4
                    and w not in ("direct", "growth", "plan", "fund")]
        best_code, best_name, best_score = None, None, 0
        for line in nav_text.splitlines():
            parts = line.split(";")
            if len(parts) >= 4:
                name = parts[3].strip().lower()
                if "direct" in name and "growth" in name:
                    score = sum(1 for k in keywords if k in name)
                    if score > best_score:
                        best_score = score
                        best_code = parts[0].strip()
                        best_name = parts[3].strip()

        if best_code and best_score >= 2:
            codes[fund["id"]] = best_code
            log.info(f"  ✅ {fund['name']}: {best_code} fuzzy→'{best_name}' (score {best_score})")
        else:
            log.warning(f"  ❌ {fund['name']}: not found in NAV file")

    return codes


# ── Step 2: Fetch equity allocation ──────────────────────────────────────────

def fetch_equity_from_amfi_csv(scheme_code: str, month: int, year: int) -> float | None:
    """
    Try AMFI's consolidated portfolio download.
    URL: https://portal.amfiindia.com/DownloadPortfolioReport_Po.aspx?tp=1&frmdt=DD-MM-YYYY&todt=DD-MM-YYYY
    This returns a text/CSV file with all fund portfolios for the date range.
    We search for our scheme code and extract the equity allocation %.
    """
    from_dt = f"01-{month:02d}-{year}"
    to_dt   = f"15-{month:02d}-{year}"

    url = (
        f"https://portal.amfiindia.com/DownloadPortfolioReport_Po.aspx"
        f"?tp=1&frmdt={from_dt}&todt={to_dt}"
    )
    try:
        log.info(f"    AMFI portfolio CSV: {from_dt} to {to_dt}")
        r = requests.get(url, headers=HEADERS, timeout=45)
        log.info(f"    → HTTP {r.status_code}, {len(r.content):,} bytes")
        if r.status_code != 200 or len(r.text.strip()) < 100:
            return None

        text = r.text
        lower = text.lower()

        # Find the section for this scheme code
        idx = lower.find(scheme_code.lower())
        if idx == -1:
            log.debug(f"    Scheme code {scheme_code} not found in CSV")
            return None

        # Look at surrounding text for equity allocation
        chunk = lower[max(0, idx - 500):idx + 3000]
        patterns = [
            r'equity(?!\s*arbitrage)[^\n,;]{0,100}([\d]{2,3}\.[\d]{1,2})\s*%?',
            r'net equity[^\n,;]{0,80}([\d]{2,3}\.[\d]{1,2})',
            r'([\d]{2,3}\.[\d]{1,2})[^\n,;]{0,60}equity(?!\s*arbitrage)',
        ]
        for p in patterns:
            m = re.search(p, chunk)
            if m:
                val = float(m.group(1))
                if 10 < val < 100:
                    log.info(f"    ✅ AMFI CSV: {val}%")
                    return val

        # Try to find percentage lines in structured rows around the scheme
        # Some formats: "Equity|45.23" or "EQUITY,45.23"
        for line in chunk.splitlines():
            if "equity" in line and "arbitrage" not in line:
                nums = re.findall(r'[\d]{2,3}\.[\d]{1,2}', line)
                for n in nums:
                    val = float(n)
                    if 10 < val < 100:
                        log.info(f"    ✅ AMFI CSV row: {val}%")
                        return val

    except Exception as e:
        log.debug(f"    AMFI CSV failed: {e}")

    return None


def fetch_equity_from_mfindia(scheme_code: str) -> float | None:
    """
    MF India portfolio page — server-rendered HTML table.
    """
    url = f"https://www.mfindia.com/Fundpage/Funddetail?code={scheme_code}&tab=Portfolio"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return None
        lower = r.text.lower()
        patterns = [
            r'equity(?!\s*arbitrage)[^<\d]{0,100}([\d]{2,3}\.[\d]{1,2})\s*%',
            r'net equity[^<\d]{0,80}([\d]{2,3}\.[\d]{1,2})',
            r'([\d]{2,3}\.[\d]{1,2})\s*%[^%<]{0,80}equity(?!\s*arbitrage)',
        ]
        for p in patterns:
            m = re.search(p, lower)
            if m:
                val = float(m.group(1))
                if 10 < val < 100:
                    log.info(f"    ✅ MF India: {val}%")
                    return val
    except Exception as e:
        log.debug(f"    MF India failed: {e}")
    return None


def scrape_fund(fund: dict, scheme_code: str, period: str) -> float | None:
    year, month = int(period[:4]), int(period[5:7])

    # Try 1: AMFI CSV current month
    val = fetch_equity_from_amfi_csv(scheme_code, month, year)
    if val:
        return round(val, 2)
    time.sleep(2)

    # Try 2: AMFI CSV previous month
    prev_m = month - 1 or 12
    prev_y = year if month > 1 else year - 1
    val = fetch_equity_from_amfi_csv(scheme_code, prev_m, prev_y)
    if val:
        return round(val, 2)
    time.sleep(2)

    # Try 3: MF India
    val = fetch_equity_from_mfindia(scheme_code)
    if val:
        return round(val, 2)

    # Try 4: manual_equity on the fund definition
    if fund.get("manual_equity"):
        log.info(f"  ↩  Fund manual_equity: {fund['manual_equity']}%")
        return float(fund["manual_equity"])

    # Try 5: MANUAL_EQUITY_SEED dict
    if MANUAL_EQUITY_SEED.get(fund["id"]):
        log.info(f"  ↩  MANUAL_EQUITY_SEED: {MANUAL_EQUITY_SEED[fund['id']]}%")
        return float(MANUAL_EQUITY_SEED[fund["id"]])

    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scrape", action="store_true")
    parser.add_argument("--export", action="store_true")
    args = parser.parse_args()

    now = datetime.now()
    if now.day < 12:
        m = now.month - 1 or 12
        y = now.year if now.month > 1 else now.year - 1
    else:
        m, y = now.month, now.year
    period = f"{y}-{m:02d}"

    log.info(f"=== BAF/DAA Scraper v5 === Period: {period}")

    hist_path = Path("history.json")
    history = json.loads(hist_path.read_text()) if hist_path.exists() else {}

    results, failed = {}, []

    if args.scrape:
        # Step 1: discover scheme codes
        log.info("\n── Discovering scheme codes ──")
        discovered = discover_scheme_codes(FUNDS)
        fund_map = {f["id"]: f for f in FUNDS}
        for fid, code in discovered.items():
            fund_map[fid]["amfi_scheme_code"] = code

        # Step 2: scrape allocations
        log.info("\n── Scraping equity allocations ──")
        for fund in FUNDS:
            fid = fund["id"]
            code = fund.get("amfi_scheme_code")
            log.info(f"\n--- {fund['name']} (code: {code}) ---")

            val = scrape_fund(fund, code, period) if code else None

            if val is None:
                log.warning(f"  ❌ No equity data for {fid}")
                failed.append(fund["name"])
                if fid in history and history[fid].get("net_equity"):
                    val = history[fid]["net_equity"][-1]
                    log.info(f"  ↩  Last history: {val}%")
                else:
                    log.warning("  No history. Skipping.")
                    continue

            if fid not in history:
                history[fid] = {"periods": [], "net_equity": []}
            if period not in history[fid]["periods"]:
                history[fid]["periods"].append(period)
                history[fid]["net_equity"].append(val)
            else:
                idx = history[fid]["periods"].index(period)
                history[fid]["net_equity"][idx] = val

            results[fid] = val

        hist_path.write_text(json.dumps(history, indent=2))
        log.info("\nhistory.json saved.")

    if args.export:
        tracker_data = {
            "_isSample": False,
            "generated_at": now.isoformat(),
            "funds": {
                fid: {
                    "periods": history[fid]["periods"],
                    "net_equity": history[fid]["net_equity"],
                }
                for fid in history
            },
        }
        Path("data.js").write_text(
            "const TRACKER_DATA = " + json.dumps(tracker_data, indent=2) + ";"
        )
        log.info("data.js exported ✅")

    # ── Summary ──
    print("\n" + "=" * 60)
    print(f"Period : {period}")
    print(f"Scraped: {len(results)}/{len(FUNDS)} funds")

    if results:
        print("\nNET EQUITY ALLOCATIONS:")
        for fid, eq in results.items():
            name = next(f["name"] for f in FUNDS if f["id"] == fid)
            bar = "█" * int(eq / 5) + "░" * (20 - int(eq / 5))
            print(f"  {name:<35} {bar} {eq:.1f}%")
    else:
        print("\n⚠️  Automated scraping found no data.")
        print()
        print("Two options to fix this:")
        print()
        print("OPTION A — Wait for AMFI data (published after 10th of month)")
        print("  Re-run the scraper after the 10th next month.")
        print()
        print("OPTION B — Manual seed (takes 5 min, works immediately)")
        print("  1. Visit each fund factsheet, find 'Net Equity' or")
        print("     'Asset Allocation' section, note the equity %.")
        print("  2. Edit baf_scraper.py → MANUAL_EQUITY_SEED dict → fill in values.")
        print("  3. Re-run: python baf_scraper.py --scrape --export")
        print()
        print("  Factsheet URLs:")
        for fid, url in FACTSHEET_URLS.items():
            name = next(f["name"] for f in FUNDS if f["id"] == fid)
            print(f"    {name}: {url}")
    print("=" * 60)


if __name__ == "__main__":
    main()
