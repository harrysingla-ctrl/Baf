"""
BAF/DAA Scraper v3 - Uses Groww & Tickertape (stable URLs, no PDFs)
=====================================================================
AMC factsheet PDF URLs change every month and block bots.
This version scrapes from Groww and Tickertape which have stable URLs,
publish the same SEBI-mandated data, and are publicly accessible.

Install:  pip install requests beautifulsoup4
Run:      python baf_scraper.py
Output:   data.js  (paste into index.html)
          history.json  (keeps track across months)
"""

import re, json, time, logging
from datetime import datetime
from pathlib import Path
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# ── Fund definitions ──────────────────────────────────────────────────────────
# Each fund has multiple URLs tried in order. First success wins.
# Groww and Tickertape show asset allocation % on public pages.

FUNDS = [
    {
        "id": "icici_baf",
        "name": "ICICI Pru Balanced Advantage",
        "urls": [
            # Groww shows "Asset Allocation" section with Equity/Debt %
            "https://groww.in/mutual-funds/icici-prudential-balanced-advantage-fund-direct-growth",
            # Tickertape shows allocation breakdown
            "https://www.tickertape.in/mutualfunds/icici-pru-balanced-advantage-fund-M_ICCVB",
            # ET Money
            "https://www.etmoney.com/mutual-funds/icici-prudential-balanced-advantage-fund-direct-growth/16443",
        ],
    },
    {
        "id": "hdfc_baf",
        "name": "HDFC Balanced Advantage",
        "urls": [
            "https://groww.in/mutual-funds/hdfc-balanced-advantage-fund-direct-growth",
            "https://www.tickertape.in/mutualfunds/hdfc-balanced-advantage-fund-M_HDFBA",
            "https://www.etmoney.com/mutual-funds/hdfc-balanced-advantage-fund-direct-plan-growth/16024",
        ],
    },
    {
        "id": "edelweiss_baf",
        "name": "Edelweiss Balanced Advantage",
        "urls": [
            "https://groww.in/mutual-funds/edelweiss-balanced-advantage-direct-plan-growth",
            "https://www.tickertape.in/mutualfunds/edelweiss-balanced-advantage-fund-M_EDLBA",
        ],
    },
    {
        "id": "dsp_daa",
        "name": "DSP Dynamic Asset Allocation",
        "urls": [
            "https://groww.in/mutual-funds/dsp-dynamic-asset-allocation-fund-direct-growth",
            "https://www.tickertape.in/mutualfunds/dsp-dynamic-asset-allocation-fund-M_DSPDAA",
        ],
    },
    {
        "id": "kotak_baf",
        "name": "Kotak Balanced Advantage",
        "urls": [
            "https://groww.in/mutual-funds/kotak-balanced-advantage-fund-direct-growth",
            "https://www.tickertape.in/mutualfunds/kotak-balanced-advantage-fund-M_KOTBA",
        ],
    },
    {
        "id": "nippon_baf",
        "name": "Nippon India Balanced Advantage",
        "urls": [
            "https://groww.in/mutual-funds/nippon-india-balanced-advantage-fund-direct-growth",
            "https://www.tickertape.in/mutualfunds/nippon-india-balanced-advantage-fund-M_NIPBA",
        ],
    },
]

# ── Parsing helpers ───────────────────────────────────────────────────────────

def get_page(url, timeout=20):
    """Fetch a URL, return (html_text, status_code). Returns ('', 0) on failure."""
    try:
        log.info(f"    GET {url[:70]}")
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        log.info(f"    → HTTP {r.status_code}, {len(r.content):,} bytes")
        if r.status_code == 200:
            return r.text, 200
        return "", r.status_code
    except Exception as e:
        log.warning(f"    → Failed: {e}")
        return "", 0


def parse_groww(html, fund_id):
    """
    Groww pages embed JSON in a <script id="__NEXT_DATA__"> tag.
    The allocation data is inside schemeData.schemeAllocation or similar.
    Also try regex on the raw HTML for equity percentage.
    """
    # Strategy 1: Extract __NEXT_DATA__ JSON
    try:
        soup = BeautifulSoup(html, "html.parser")
        script = soup.find("script", {"id": "__NEXT_DATA__"})
        if script:
            data = json.loads(script.string)
            # Walk the JSON looking for equity allocation
            text = json.dumps(data).lower()
            # Groww stores allocation as {"assetType":"equity","allocation":45.23}
            matches = re.findall(
                r'"assettype"\s*:\s*"equity"[^}]*?"allocation"\s*:\s*([\d.]+)',
                text
            )
            if matches:
                val = float(matches[0])
                if 10 < val < 100:
                    log.info(f"    ✅ Groww JSON (equity allocation): {val}%")
                    return val

            # Also try "equityallocation" or "equity":XX pattern in JSON
            m = re.search(r'"equity"\s*:\s*([\d.]+)', text)
            if m:
                val = float(m.group(1))
                if 10 < val < 100:
                    log.info(f"    ✅ Groww JSON (equity key): {val}%")
                    return val
    except Exception as e:
        log.debug(f"    Groww JSON parse failed: {e}")

    # Strategy 2: Raw HTML regex — Groww renders "Equity\n45.23%" style text
    patterns = [
        # "Equity" label followed by percentage nearby
        r'equity["\s<>/a-z]*?([\d]+\.[\d]+)\s*%',
        r'"equity"[^}]{0,100}([\d]+\.[\d]+)',
        r'equity.*?(\d{2,3}\.\d{1,2})\s*%',
        # allocation table style
        r'([\d]+\.[\d]+)\s*%[^%]{0,50}equity',
    ]
    lower_html = html.lower()
    for p in patterns:
        m = re.search(p, lower_html)
        if m:
            val = float(m.group(1))
            if 10 < val < 100:
                log.info(f"    ✅ Groww HTML regex: {val}%")
                return val

    return None


def parse_tickertape(html, fund_id):
    """
    Tickertape shows allocation in their page. They use server-rendered HTML
    with class names like 'holding-percent' or embed data in script tags.
    """
    lower_html = html.lower()

    # Look for allocation table rows with equity
    patterns = [
        r'equity[^<]*?<[^>]+>([\d]+\.[\d]+)\s*%',
        r'<td[^>]*>equity<[^>]*><td[^>]*>([\d]+\.[\d]+)',
        r'"equity".*?"percent".*?([\d]+\.[\d]+)',
        r'net equity.*?([\d]+\.[\d]+)\s*%',
        r'equity allocation.*?([\d]+\.[\d]+)\s*%',
    ]
    for p in patterns:
        m = re.search(p, lower_html)
        if m:
            val = float(m.group(1))
            if 10 < val < 100:
                log.info(f"    ✅ Tickertape: {val}%")
                return val

    # Try to find __NEXT_DATA__ too (Tickertape is also Next.js)
    try:
        soup = BeautifulSoup(html, "html.parser")
        script = soup.find("script", {"id": "__NEXT_DATA__"})
        if script:
            text = json.dumps(json.loads(script.string)).lower()
            m = re.search(r'"equity"[^}]{0,50}([\d]+\.[\d]+)', text)
            if m:
                val = float(m.group(1))
                if 10 < val < 100:
                    log.info(f"    ✅ Tickertape JSON: {val}%")
                    return val
    except Exception:
        pass

    return None


def parse_etmoney(html, fund_id):
    """ET Money shows portfolio allocation as a donut chart with data attributes."""
    lower_html = html.lower()
    patterns = [
        r'equity.*?(\d{2,3}\.\d{1,2})\s*%',
        r'(\d{2,3}\.\d{1,2})\s*%.*?equity',
        r'"equity".*?(\d{2,3}\.\d{1,2})',
    ]
    for p in patterns:
        m = re.search(p, lower_html)
        if m:
            val = float(m.group(1))
            if 10 < val < 100:
                log.info(f"    ✅ ET Money: {val}%")
                return val
    return None


PARSER_MAP = {
    "groww.in": parse_groww,
    "tickertape.in": parse_tickertape,
    "etmoney.com": parse_etmoney,
}


def scrape_fund(fund):
    """Try each URL for a fund. Return net equity % or None."""
    for url in fund["urls"]:
        html, status = get_page(url)
        if not html:
            time.sleep(2)
            continue

        # Pick parser based on domain
        parser = None
        for domain, fn in PARSER_MAP.items():
            if domain in url:
                parser = fn
                break
        if parser is None:
            parser = parse_groww  # default

        val = parser(html, fund["id"])
        if val and 10 < val < 100:
            return round(val, 2)

        time.sleep(3)  # polite delay between attempts

    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    now = datetime.now()
    # AMFI data available after ~10th of next month
    if now.day < 12:
        m = now.month - 1 or 12
        y = now.year if now.month > 1 else now.year - 1
    else:
        m, y = now.month, now.year
    period = f"{y}-{m:02d}"

    log.info(f"=== BAF/DAA Scraper v3 === Period: {period}")
    log.info("Source: Groww.in / Tickertape.in (stable public URLs)")

    # Load history
    hist_path = Path("history.json")
    history = json.loads(hist_path.read_text()) if hist_path.exists() else {}

    results, failed = {}, []

    for fund in FUNDS:
        fid = fund["id"]
        log.info(f"\n--- {fund['name']} ---")
        val = scrape_fund(fund)

        if val is None:
            log.warning(f"  ❌ All sources failed for {fid}")
            failed.append(fund["name"])
            # Fall back to last known value
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
        time.sleep(3)

    # Save history
    hist_path.write_text(json.dumps(history, indent=2))

    # Build output
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

    # ── Summary ──
    print("\n" + "=" * 55)
    print("RESULTS")
    print(f"Period scraped : {period}")
    print(f"Funds succeeded: {len(results)} / {len(FUNDS)}")
    if failed:
        print(f"Failed         : {', '.join(failed)}")
        print()
        print(">>> If scraping failed, websites may have changed structure.")
        print("    Run the manual fallback below instead.")
    print()
    print("NET EQUITY ALLOCATIONS:")
    for fid, eq in results.items():
        name = next(f["name"] for f in FUNDS if f["id"] == fid)
        bar = "█" * int(eq / 5) + "░" * (20 - int(eq / 5))
        print(f"  {name:<35} {bar} {eq:.1f}%")
    print()
    print("data.js created ✅")
    print()
    print("NEXT STEP:")
    print("  1. Open data.js → Select All → Copy")
    print("  2. Open index.html in any text editor (Notepad is fine)")
    print("  3. Find line that starts with:  const TRACKER_DATA =")
    print("  4. Select from that line to the closing  };")
    print("  5. Paste what you copied → Save")
    print("  6. Open index.html in Chrome → Done!")
    print("=" * 55)

    if len(results) == 0:
        print()
        print("⚠️  ALL FUNDS FAILED. Try the manual fallback:")
        print()
        print("MANUAL FALLBACK (takes ~5 minutes):")
        for fund in FUNDS:
            print(f"  {fund['name']}: {fund['urls'][0]}")
        print()
        print("  Visit each URL in your browser, find the 'Asset Allocation'")
        print("  section, note the Equity %. Then edit history.json manually:")
        print()
        print('  { "icici_baf": {"periods": ["2026-04"], "net_equity": [45.2]}, ... }')


if __name__ == "__main__":
    main()
