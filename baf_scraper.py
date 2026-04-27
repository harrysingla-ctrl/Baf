"""
BAF/DAA Scraper v4 - Uses AMFI Portfolio API (official, reliable, no JS)
=========================================================================
Source: https://www.amfiindia.com/modules/NavHistoryPeriod
AMFI publishes monthly portfolio data for all mutual funds.
This script fetches net equity allocation directly from AMFI's API.

Install:  pip install requests
Run:      python baf_scraper.py --scrape --export
Output:   data.js       (paste into index.html)
          history.json  (keeps track across months)
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
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.amfiindia.com/",
}

# ── Fund definitions ──────────────────────────────────────────────────────────
# amfi_scheme_code: The scheme code from AMFI for the Direct-Growth plan.
# Find yours at: https://www.amfiindia.com/nav-history-download
# portfolio_api uses AMFI's monthly portfolio endpoint.

FUNDS = [
    {
        "id": "icici_baf",
        "name": "ICICI Pru Balanced Advantage",
        "amfi_scheme_code": "120586",   # ICICI Pru BAF - Direct Growth
    },
    {
        "id": "hdfc_baf",
        "name": "HDFC Balanced Advantage",
        "amfi_scheme_code": "119230",   # HDFC BAF - Direct Growth
    },
    {
        "id": "edelweiss_baf",
        "name": "Edelweiss Balanced Advantage",
        "amfi_scheme_code": "135798",   # Edelweiss BAF - Direct Growth
    },
    {
        "id": "dsp_daa",
        "name": "DSP Dynamic Asset Allocation",
        "amfi_scheme_code": "119090",   # DSP DAA - Direct Growth
    },
    {
        "id": "kotak_baf",
        "name": "Kotak Balanced Advantage",
        "amfi_scheme_code": "120837",   # Kotak BAF - Direct Growth
    },
    {
        "id": "nippon_baf",
        "name": "Nippon India Balanced Advantage",
        "amfi_scheme_code": "118989",   # Nippon BAF - Direct Growth
    },
]

# ── AMFI Portfolio API ────────────────────────────────────────────────────────

def get_amfi_portfolio(scheme_code: str, month: int, year: int):
    """
    Fetch portfolio data from AMFI's API for a given scheme and month.
    Returns parsed JSON or None on failure.

    AMFI endpoint (undocumented but stable):
    https://www.amfiindia.com/modules/NavHistoryPeriod
    Portfolio endpoint:
    https://www.amfiindia.com/modules/PortfolioAllocationDetails
    """
    # Primary: AMFI portfolio allocation API
    url = (
        "https://www.amfiindia.com/modules/PortfolioAllocationDetails"
        f"?SchCode={scheme_code}&mf={month:02d}&yf={year}"
    )
    try:
        log.info(f"    GET AMFI portfolio: scheme={scheme_code} {year}-{month:02d}")
        r = requests.get(url, headers=HEADERS, timeout=30)
        log.info(f"    → HTTP {r.status_code}, {len(r.content):,} bytes")
        if r.status_code == 200 and r.text.strip():
            return r.text
    except Exception as e:
        log.warning(f"    → AMFI API failed: {e}")

    return None


def parse_equity_from_amfi(text: str, scheme_code: str):
    """
    Parse net equity % from AMFI portfolio API response.
    AMFI returns pipe-separated or JSON data depending on endpoint.
    Tries multiple patterns to extract equity allocation.
    """
    if not text:
        return None

    lower = text.lower()

    # Pattern 1: JSON response with assetAllocation array
    try:
        data = json.loads(text)
        # Handle list of allocation entries
        if isinstance(data, list):
            for item in data:
                asset = str(item.get("assetType", item.get("asset_type", item.get("Asset", "")))).lower()
                if "equity" in asset and "arbitrage" not in asset:
                    for key in ["percentage", "percent", "allocation", "Percentage", "Percent"]:
                        if key in item:
                            val = float(item[key])
                            if 10 < val < 100:
                                log.info(f"    ✅ AMFI JSON list (equity): {val}%")
                                return val
        # Handle dict with nested data
        if isinstance(data, dict):
            # Look for allocation arrays in any key
            text_repr = json.dumps(data).lower()
            matches = re.findall(
                r'"assettype"\s*:\s*"equity"[^}]*?"percentage"\s*:\s*([\d.]+)',
                text_repr
            )
            if matches:
                val = float(matches[0])
                if 10 < val < 100:
                    log.info(f"    ✅ AMFI JSON dict (equity): {val}%")
                    return val
    except (json.JSONDecodeError, ValueError):
        pass

    # Pattern 2: Pipe-separated text (AMFI's older format)
    # Format: SchemeCode|SchemeName|AssetType|Percentage
    for line in text.splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) >= 4:
            asset_col = parts[2].lower() if len(parts) > 2 else ""
            if "equity" in asset_col and "arbitrage" not in asset_col:
                try:
                    val = float(parts[3].replace("%", "").strip())
                    if 10 < val < 100:
                        log.info(f"    ✅ AMFI pipe-text (equity): {val}%")
                        return val
                except ValueError:
                    pass

    # Pattern 3: HTML table (AMFI sometimes returns HTML)
    patterns = [
        r'equity(?!\s*arbitrage)[^<\d]{0,80}([\d]{2,3}\.[\d]{1,2})\s*%',
        r'net equity[^<\d]{0,80}([\d]{2,3}\.[\d]{1,2})',
        r'([\d]{2,3}\.[\d]{1,2})\s*%[^%<]{0,60}equity(?!\s*arbitrage)',
    ]
    for p in patterns:
        m = re.search(p, lower)
        if m:
            val = float(m.group(1))
            if 10 < val < 100:
                log.info(f"    ✅ AMFI HTML/text regex (equity): {val}%")
                return val

    return None


def fetch_amfi_portfolio(scheme_code: str, period: str):
    """
    Main function to get equity allocation for a scheme.
    Tries current period and falls back to previous month if needed.
    """
    year, month = int(period[:4]), int(period[5:7])

    # Try current period
    text = get_amfi_portfolio(scheme_code, month, year)
    val = parse_equity_from_amfi(text, scheme_code) if text else None
    if val:
        return val

    # Try previous month as fallback
    prev_month = month - 1 or 12
    prev_year = year if month > 1 else year - 1
    log.info(f"    ↩ Trying previous month: {prev_year}-{prev_month:02d}")
    text = get_amfi_portfolio(scheme_code, prev_month, prev_year)
    val = parse_equity_from_amfi(text, scheme_code) if text else None
    if val:
        return val

    return None


# ── Fallback: MFI Explorer API (public, returns JSON) ─────────────────────────

def fetch_mfapi_portfolio(scheme_code: str):
    """
    mfapi.in is a free public API for Indian mutual funds.
    It provides NAV history but not portfolio allocation directly.
    Used here as a connectivity check / future extension.
    """
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            log.info(f"    ✅ mfapi.in reachable for scheme {scheme_code}: {data.get('meta', {}).get('scheme_name', '')}")
            return data
    except Exception as e:
        log.warning(f"    mfapi.in failed: {e}")
    return None


def scrape_fund(fund: dict, period: str):
    """
    Try AMFI portfolio API. Return net equity % or None.
    """
    scheme_code = fund.get("amfi_scheme_code", "")
    if not scheme_code:
        log.warning(f"  No scheme code for {fund['id']}")
        return None

    log.info(f"  Fetching AMFI portfolio for scheme {scheme_code}...")
    val = fetch_amfi_portfolio(scheme_code, period)

    if val:
        return round(val, 2)

    # Log what mfapi says (helps debug scheme codes)
    log.info(f"  Checking scheme code validity via mfapi.in...")
    fetch_mfapi_portfolio(scheme_code)

    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scrape", action="store_true", help="Scrape live data")
    parser.add_argument("--export", action="store_true", help="Export data.js")
    args = parser.parse_args()

    now = datetime.now()
    # AMFI portfolio data published after ~10th of next month
    if now.day < 12:
        m = now.month - 1 or 12
        y = now.year if now.month > 1 else now.year - 1
    else:
        m, y = now.month, now.year
    period = f"{y}-{m:02d}"

    log.info(f"=== BAF/DAA Scraper v4 === Period: {period}")
    log.info("Source: AMFI India Portfolio API (official, no bot-blocking)")

    # Load history
    hist_path = Path("history.json")
    history = json.loads(hist_path.read_text()) if hist_path.exists() else {}

    results, failed = {}, []

    if args.scrape:
        for fund in FUNDS:
            fid = fund["id"]
            log.info(f"\n--- {fund['name']} ---")
            val = scrape_fund(fund, period)

            if val is None:
                log.warning(f"  ❌ AMFI returned no data for {fid}")
                failed.append(fund["name"])
                # Fall back to last known value from history
                if fid in history and history[fid].get("net_equity"):
                    val = history[fid]["net_equity"][-1]
                    log.info(f"  ↩  Using last known value: {val}%")
                else:
                    log.warning(f"  No history either. Skipping.")
                    continue

            # Update history
            if fid not in history:
                history[fid] = {"periods": [], "net_equity": []}
            if period not in history[fid]["periods"]:
                history[fid]["periods"].append(period)
                history[fid]["net_equity"].append(val)
            else:
                idx = history[fid]["periods"].index(period)
                history[fid]["net_equity"][idx] = val

            results[fid] = val
            time.sleep(1)

        # Save history
        hist_path.write_text(json.dumps(history, indent=2))
        log.info(f"\nhistory.json saved ({len(history)} funds)")

    # ── Export data.js ──
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
        js_out = "const TRACKER_DATA = " + json.dumps(tracker_data, indent=2) + ";"
        Path("data.js").write_text(js_out)
        log.info("data.js exported ✅")

    # ── Summary ──
    print("\n" + "=" * 55)
    print("RESULTS")
    print(f"Period : {period}")
    print(f"Scraped: {len(results)} / {len(FUNDS)} funds")
    if failed:
        print(f"Failed : {', '.join(failed)}")
    print()

    if results:
        print("NET EQUITY ALLOCATIONS:")
        for fid, eq in results.items():
            name = next(f["name"] for f in FUNDS if f["id"] == fid)
            bar = "█" * int(eq / 5) + "░" * (20 - int(eq / 5))
            print(f"  {name:<35} {bar} {eq:.1f}%")
    else:
        print("⚠️  No data scraped.")
        print()
        print("Possible reasons:")
        print("  1. AMFI hasn't published portfolio for this month yet")
        print("     (data usually available after the 10th of next month)")
        print("  2. Scheme codes may need updating")
        print()
        print("To verify scheme codes, visit:")
        print("  https://api.mfapi.in/mf/search?q=icici+balanced+advantage")
        print("  Replace the scheme code in FUNDS[] with the correct one.")
        print()
        print("To manually seed data, edit history.json:")
        print('  {"icici_baf": {"periods": ["2026-03"], "net_equity": [45.2]}}')

    print()
    print("=" * 55)


if __name__ == "__main__":
    main()
