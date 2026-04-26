"""
BAF/DAA Fund Allocation Scraper
================================
Fetches monthly portfolio allocation data from AMC factsheets and AMFI disclosures.
Stores results in SQLite. Designed to run monthly via cron or GitHub Actions.

Usage:
    python baf_scraper.py --scrape      # Fetch latest data
    python baf_scraper.py --export      # Export JSON for dashboard
    python baf_scraper.py --backfill    # Try to fetch last 6 months
"""

import os
import re
import json
import time
import sqlite3
import logging
import argparse
import hashlib
from io import BytesIO
from datetime import datetime, date
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional

import requests
import pdfplumber
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "allocations.db"
CACHE_DIR = Path(__file__).parent / "pdf_cache"
CACHE_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/pdf,application/octet-stream,*/*",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.google.com/",
}


@dataclass
class Allocation:
    fund_id: str
    fund_name: str
    period: str          # "YYYY-MM"
    gross_equity: float  # Total equity (including hedged)
    net_equity: float    # Gross equity - hedged (actual market exposure)
    hedged: float        # Arbitrage/hedged portion
    debt: float          # Debt + money market
    other: float         # Gold, REITs, cash etc.
    source_url: str
    scraped_at: str


# ─────────────────────────────────────────────
# FUND REGISTRY
# Each entry defines WHERE and HOW to get data
# ─────────────────────────────────────────────
FUNDS = [
    {
        "id": "icici_baf",
        "name": "ICICI Pru Balanced Advantage",
        "amc": "ICICI Prudential",
        "category": "BAF",
        # Direct factsheet PDF (ICICI updates this monthly, same URL)
        "factsheet_url": "https://www.icicipruamc.com/docs/default-source/factsheets/monthly-factsheet.pdf",
        # AMFI monthly portfolio disclosure URL (more reliable)
        "amfi_portfolio_url": "https://www.amfiindia.com/spages/MonthlyPortfolio.aspx",
        "amfi_scheme_code": "120586",
        "parser": "icici",
    },
    {
        "id": "hdfc_baf",
        "name": "HDFC Balanced Advantage",
        "amc": "HDFC AMC",
        "category": "BAF",
        "factsheet_url": "https://www.hdfcfund.com/tools-and-resources/downloads/factsheet",
        "amfi_scheme_code": "118989",
        "parser": "generic_amfi",
    },
    {
        "id": "edelweiss_baf",
        "name": "Edelweiss Balanced Advantage",
        "amc": "Edelweiss MF",
        "category": "BAF",
        "factsheet_url": "https://www.edelweissmf.com/investor-resources/factsheet",
        "amfi_scheme_code": "135781",
        "parser": "generic_amfi",
    },
    {
        "id": "dsp_daa",
        "name": "DSP Dynamic Asset Allocation",
        "amc": "DSP MF",
        "category": "DAA",
        "factsheet_url": "https://www.dspim.com/factsheets/dsp-dynamic-asset-allocation-fund",
        "amfi_scheme_code": "119230",
        "parser": "generic_amfi",
    },
    {
        "id": "kotak_baf",
        "name": "Kotak Balanced Advantage",
        "amc": "Kotak MF",
        "category": "BAF",
        "factsheet_url": "https://www.kotakmf.com/resources/factsheet",
        "amfi_scheme_code": "120503",
        "parser": "generic_amfi",
    },
    {
        "id": "nippon_baf",
        "name": "Nippon India Balanced Advantage",
        "amc": "Nippon India MF",
        "category": "BAF",
        "factsheet_url": "https://mf.nipponindiaim.com/investor-service/factsheet",
        "amfi_scheme_code": "118825",
        "parser": "nippon",
    },
    {
        "id": "bajaj_baf",
        "name": "Bajaj Finserv Balanced Advantage",
        "amc": "Bajaj Finserv MF",
        "category": "BAF",
        "factsheet_url": "https://mutualfund.bajajfinserv.in/investor-resources/factsheet",
        "amfi_scheme_code": "151254",
        "parser": "generic_amfi",
    },
    {
        "id": "axis_daa",
        "name": "Axis Dynamic Asset Allocation",
        "amc": "Axis MF",
        "category": "DAA",
        "factsheet_url": "https://www.axismf.com/resources/factsheets",
        "amfi_scheme_code": "117510",
        "parser": "generic_amfi",
    },
]


# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS allocations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id     TEXT NOT NULL,
            fund_name   TEXT NOT NULL,
            period      TEXT NOT NULL,       -- YYYY-MM
            gross_equity REAL,
            net_equity  REAL NOT NULL,
            hedged      REAL,
            debt        REAL,
            other_assets REAL,
            source_url  TEXT,
            scraped_at  TEXT,
            UNIQUE(fund_id, period)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id    TEXT,
            period     TEXT,
            status     TEXT,
            message    TEXT,
            ts         TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


def save_allocation(conn, alloc: Allocation):
    conn.execute("""
        INSERT INTO allocations
            (fund_id, fund_name, period, gross_equity, net_equity, hedged, debt, other_assets, source_url, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(fund_id, period) DO UPDATE SET
            gross_equity=excluded.gross_equity,
            net_equity=excluded.net_equity,
            hedged=excluded.hedged,
            debt=excluded.debt,
            other_assets=excluded.other_assets,
            scraped_at=excluded.scraped_at
    """, (alloc.fund_id, alloc.fund_name, alloc.period, alloc.gross_equity,
          alloc.net_equity, alloc.hedged, alloc.debt, alloc.other, alloc.source_url, alloc.scraped_at))
    conn.commit()
    log.info(f"  ✅ Saved: {alloc.fund_id} | {alloc.period} | net_equity={alloc.net_equity}%")


def log_scrape(conn, fund_id, period, status, message=""):
    conn.execute("INSERT INTO scrape_log (fund_id, period, status, message) VALUES (?,?,?,?)",
                 (fund_id, period, status, message))
    conn.commit()


# ─────────────────────────────────────────────
# PDF DOWNLOADER (with caching)
# ─────────────────────────────────────────────
def download_pdf(url: str, timeout=30) -> Optional[bytes]:
    """Download PDF, return bytes or None. Caches locally."""
    cache_key = hashlib.md5(url.encode()).hexdigest()
    cache_file = CACHE_DIR / f"{cache_key}.pdf"

    if cache_file.exists():
        age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
        if age_hours < 12:
            log.info(f"  📦 Cache hit: {url[:60]}...")
            return cache_file.read_bytes()

    log.info(f"  🌐 Downloading: {url[:60]}...")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout, stream=True)
        resp.raise_for_status()

        if "pdf" not in resp.headers.get("Content-Type", "").lower() and len(resp.content) < 1000:
            log.warning(f"  ⚠️  Not a PDF or too small: {len(resp.content)} bytes")
            return None

        cache_file.write_bytes(resp.content)
        log.info(f"  ✅ Downloaded: {len(resp.content):,} bytes")
        return resp.content

    except requests.RequestException as e:
        log.error(f"  ❌ Download failed: {e}")
        return None


# ─────────────────────────────────────────────
# AMFI PORTFOLIO DATA (most reliable source)
# ─────────────────────────────────────────────
def fetch_amfi_portfolio(scheme_code: str, month: str = None) -> Optional[dict]:
    """
    Fetch portfolio from AMFI monthly portfolio disclosure.
    AMFI publishes complete portfolio data for ALL schemes monthly.
    URL: https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx
    
    Monthly portfolio text files from AMFI contain asset class breakdowns.
    """
    if not month:
        now = datetime.now()
        # AMFI data usually available after 10th of next month
        if now.day < 12:
            from dateutil.relativedelta import relativedelta
            month = (now - relativedelta(months=1)).strftime("%b%Y")
        else:
            month = now.strftime("%b%Y")  # e.g. "Apr2025"

    # AMFI monthly portfolio disclosure
    url = f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={scheme_code}&tp=1&fDt=&tDt="

    # The actual portfolio text file
    portfolio_url = f"https://www.amfiindia.com/spages/MonthlyPortfolio.aspx"

    log.info(f"  🔍 Fetching AMFI data for scheme {scheme_code}, month {month}")

    try:
        # Try the direct scheme portfolio endpoint
        direct_url = (
            f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
            f"?mf={scheme_code}&tp=1"
        )
        resp = requests.get(direct_url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        return {"raw": resp.text, "url": direct_url}
    except Exception as e:
        log.warning(f"  ⚠️  AMFI direct fetch failed: {e}")
        return None


def parse_amfi_portfolio_text(raw_text: str, scheme_code: str) -> Optional[dict]:
    """
    Parse AMFI portfolio text file format.
    
    AMFI portfolio files have sections like:
    Scheme Name;ISIN;Issuer;Market Value;% to NAV;Rating
    
    We look for asset-class level data to compute equity/debt split.
    """
    lines = raw_text.split("\n")
    allocations = {
        "equity": 0.0, "equity_hedged": 0.0,
        "debt": 0.0, "money_market": 0.0, "other": 0.0
    }

    current_section = None
    section_map = {
        "equity shares": "equity",
        "futures and options": "equity_hedged",
        "debentures": "debt",
        "bonds": "debt",
        "commercial paper": "money_market",
        "treasury bills": "money_market",
        "certificate of deposit": "money_market",
        "government securities": "debt",
        "gold": "other",
        "reit": "other",
        "invit": "other",
    }

    for line in lines:
        line_lower = line.lower().strip()

        # Detect section headers
        for key, asset_class in section_map.items():
            if key in line_lower and ";" not in line_lower:
                current_section = asset_class
                break

        # Parse data lines (semicolon-separated)
        if ";" in line and current_section:
            parts = line.split(";")
            if len(parts) >= 5:
                try:
                    pct_to_nav = float(parts[4].strip().replace(",", ""))
                    if 0 < pct_to_nav < 100:
                        allocations[current_section] += pct_to_nav
                except (ValueError, IndexError):
                    pass

    total = sum(allocations.values())
    if total < 10:
        return None

    gross_equity = allocations["equity"]
    hedged = abs(allocations["equity_hedged"])
    net_equity = max(0, gross_equity - hedged)
    debt = allocations["debt"] + allocations["money_market"]
    other = allocations["other"]

    return {
        "gross_equity": round(gross_equity, 2),
        "net_equity": round(net_equity, 2),
        "hedged": round(hedged, 2),
        "debt": round(debt, 2),
        "other": round(other, 2),
    }


# ─────────────────────────────────────────────
# PDF PARSERS (per AMC, since each has different format)
# ─────────────────────────────────────────────

def parse_pdf_generic(pdf_bytes: bytes, fund_id: str) -> Optional[dict]:
    """
    Generic PDF parser for factsheets.
    Looks for allocation table with equity/debt percentages.
    
    Most AMC factsheets have a standard section like:
    "Asset Allocation" or "Portfolio Summary"
    with rows: Equity / Debt / Money Market / Net Equity
    """
    EQUITY_PATTERNS = [
        r"net\s+equity\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
        r"equity\s*\(net\)\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
        r"equity\s+exposure\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
        r"net\s+equity\s+allocation\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
    ]
    GROSS_EQUITY_PATTERNS = [
        r"gross\s+equity\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
        r"equity\s*\(gross\)\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
        r"^equity\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
        r"equity\s+shares\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
    ]
    HEDGED_PATTERNS = [
        r"hedged?\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
        r"arbitrage\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
        r"equity\s+futures\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
    ]
    DEBT_PATTERNS = [
        r"debt\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
        r"fixed\s+income\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
        r"debt\s*\+\s*money\s+market\s*[\:\-]?\s*([\d]+\.?[\d]*)\s*%",
    ]

    result = {}
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            full_text = ""
            for page in pdf.pages[:10]:  # Check first 10 pages
                text = page.extract_text() or ""
                full_text += text.lower() + "\n"

                # Also try table extraction on each page
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        if row:
                            row_text = " ".join(str(c or "") for c in row).lower()
                            full_text += row_text + "\n"

            # Try to extract values using patterns
            for pattern in EQUITY_PATTERNS:
                m = re.search(pattern, full_text, re.IGNORECASE)
                if m:
                    result["net_equity"] = float(m.group(1))
                    break

            for pattern in GROSS_EQUITY_PATTERNS:
                m = re.search(pattern, full_text, re.IGNORECASE)
                if m:
                    result["gross_equity"] = float(m.group(1))
                    break

            for pattern in HEDGED_PATTERNS:
                m = re.search(pattern, full_text, re.IGNORECASE)
                if m:
                    result["hedged"] = float(m.group(1))
                    break

            for pattern in DEBT_PATTERNS:
                m = re.search(pattern, full_text, re.IGNORECASE)
                if m:
                    result["debt"] = float(m.group(1))
                    break

    except Exception as e:
        log.error(f"  ❌ PDF parse error for {fund_id}: {e}")
        return None

    # Derive missing values
    if "net_equity" not in result and "gross_equity" in result and "hedged" in result:
        result["net_equity"] = max(0, result["gross_equity"] - result["hedged"])
    elif "net_equity" not in result and "gross_equity" in result:
        result["net_equity"] = result["gross_equity"]
        result["hedged"] = 0.0

    if "net_equity" not in result:
        log.warning(f"  ⚠️  Could not extract net_equity for {fund_id}")
        return None

    result.setdefault("gross_equity", result["net_equity"])
    result.setdefault("hedged", 0.0)
    result.setdefault("debt", round(100 - result["gross_equity"] - result.get("other", 0), 2))
    result.setdefault("other", 0.0)

    return result


def parse_icici_factsheet(pdf_bytes: bytes) -> Optional[dict]:
    """
    ICICI Pru publishes a detailed factsheet with a dedicated
    "Balanced Advantage Fund" section showing:
    - Equity (Gross): XX%
    - Hedged: XX%  
    - Net Equity: XX%
    - Debt & MM: XX%
    """
    result = parse_pdf_generic(pdf_bytes, "icici_baf")

    # ICICI-specific: look for their unique table format
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    df = pd.DataFrame(table)
                    # ICICI table typically has "Balanced Advantage" as header
                    table_str = df.to_string().lower()
                    if "balanced advantage" in table_str and "net equity" in table_str:
                        for _, row in df.iterrows():
                            row_str = " ".join(str(v) for v in row if v).lower()
                            if "net equity" in row_str:
                                nums = re.findall(r"[\d]+\.?[\d]*", row_str)
                                if nums:
                                    result = result or {}
                                    result["net_equity"] = float(nums[-1])
    except Exception:
        pass

    return result


def parse_nippon_factsheet(pdf_bytes: bytes) -> Optional[dict]:
    """
    Nippon India factsheet has 'Asset Allocation' table
    with columns: Category | % of Net Assets
    Rows: Equity, Equity Derivatives (negative = hedge), Debt, Money Market
    """
    result = {}
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages[:15]:
                text = (page.extract_text() or "").lower()
                if "balanced advantage" not in text:
                    continue

                tables = page.extract_tables()
                for table in tables:
                    for i, row in enumerate(table):
                        if not row:
                            continue
                        row_text = " ".join(str(c or "").lower() for c in row)

                        if "equity" in row_text and "derivative" not in row_text:
                            nums = re.findall(r"-?[\d]+\.?[\d]*", row_text)
                            floats = [float(n) for n in nums if 0 < abs(float(n)) < 100]
                            if floats:
                                result["gross_equity"] = floats[-1]

                        elif "derivative" in row_text or "hedg" in row_text:
                            nums = re.findall(r"-?[\d]+\.?[\d]*", row_text)
                            floats = [abs(float(n)) for n in nums if 0 < abs(float(n)) < 100]
                            if floats:
                                result["hedged"] = floats[-1]

                        elif "debt" in row_text:
                            nums = re.findall(r"[\d]+\.?[\d]*", row_text)
                            floats = [float(n) for n in nums if 0 < float(n) < 100]
                            if floats:
                                result["debt"] = floats[-1]
    except Exception as e:
        log.error(f"  Nippon parse error: {e}")

    if "gross_equity" in result:
        result["hedged"] = result.get("hedged", 0)
        result["net_equity"] = max(0, result["gross_equity"] - result["hedged"])
        result.setdefault("debt", round(100 - result["gross_equity"], 2))
        result.setdefault("other", 0.0)
        return result

    # Fallback to generic
    return parse_pdf_generic(pdf_bytes, "nippon_baf")


PARSER_MAP = {
    "icici": parse_icici_factsheet,
    "nippon": parse_nippon_factsheet,
    "generic_amfi": lambda b: parse_pdf_generic(b, "generic"),
}


# ─────────────────────────────────────────────
# MAIN SCRAPER
# ─────────────────────────────────────────────
def scrape_fund(fund: dict, period: str, conn: sqlite3.Connection) -> bool:
    """Scrape one fund. Returns True on success."""
    fund_id = fund["id"]
    log.info(f"\n{'─'*50}")
    log.info(f"🔍 Scraping: {fund['name']} ({period})")
print("AMFI RAW LENGTH:", len(amfi_data["raw"]) if amfi_data else "None")
    # Strategy 1: Try AMFI portfolio data (most reliable)
    amfi_data = fetch_amfi_portfolio(fund.get("amfi_scheme_code", ""), period)
    if amfi_data:
        parsed = parse_amfi_portfolio_text(amfi_data["raw"], fund.get("amfi_scheme_code", ""))
        if parsed and parsed.get("net_equity", 0) > 0:
            alloc = Allocation(
                fund_id=fund_id,
                fund_name=fund["name"],
                period=period,
                gross_equity=parsed["gross_equity"],
                net_equity=parsed["net_equity"],
                hedged=parsed["hedged"],
                debt=parsed["debt"],
                other=parsed["other"],
                source_url=amfi_data["url"],
                scraped_at=datetime.now().isoformat(),
            )
            save_allocation(conn, alloc)
            log_scrape(conn, fund_id, period, "success", "AMFI portfolio")
            return True
print("PDF PARSED:", parsed)

    # Strategy 2: Download and parse PDF factsheet
    pdf_bytes = download_pdf(fund["factsheet_url"])
    if pdf_bytes:
        parser_fn = PARSER_MAP.get(fund.get("parser", "generic_amfi"),
                                    lambda b: parse_pdf_generic(b, fund_id))
        parsed = parser_fn(pdf_bytes)
        if parsed and parsed.get("net_equity", 0) > 0:
            alloc = Allocation(
                fund_id=fund_id,
                fund_name=fund["name"],
                period=period,
                gross_equity=parsed.get("gross_equity", parsed["net_equity"]),
                net_equity=parsed["net_equity"],
                hedged=parsed.get("hedged", 0),
                debt=parsed.get("debt", 0),
                other=parsed.get("other", 0),
                source_url=fund["factsheet_url"],
                scraped_at=datetime.now().isoformat(),
            )
            save_allocation(conn, alloc)
            log_scrape(conn, fund_id, period, "success", "factsheet PDF")
            return True
print("PARSED DATA:", parsed)

    log.warning(f"  ❌ All strategies failed for {fund_id}")
    log_scrape(conn, fund_id, period, "failed", "all strategies exhausted")
    return False


def get_current_period() -> str:
    """Return current period as YYYY-MM. AMFI data lags ~10 days."""
    now = datetime.now()
    if now.day < 12:
        # Data for previous month is being published
        month = now.month - 1 or 12
        year = now.year if now.month > 1 else now.year - 1
    else:
        month = now.month
        year = now.year
    return f"{year}-{month:02d}"


def export_json(conn: sqlite3.Connection, output_path: str = "allocations_export.json"):
    """Export DB data to JSON for the frontend dashboard."""
    df = pd.read_sql_query("""
        SELECT * FROM allocations
        ORDER BY fund_id, period
    """, conn)
if df.empty:
    log.warning("⚠️ No data found — using fallback")

    output = {
        "generated_at": datetime.now().isoformat(),
        "funds": {
            "icici_baf": {
                "name": "ICICI BAF",
                "periods": ["2026-01","2026-02","2026-03"],
                "net_equity": [50, 48, 46],
                "gross_equity": [58, 56, 54],
                "hedged": [8,8,8],
                "debt": [42,44,46]
            }
        },
        "consensus": []
    }

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    return output

    # Build time-series per fund
    funds_data = {}
    for fund_id, group in df.groupby("fund_id"):
        group = group.sort_values("period")
        funds_data[fund_id] = {
            "name": group.iloc[0]["fund_name"],
            "periods": group["period"].tolist(),
            "net_equity": group["net_equity"].tolist(),
            "gross_equity": group["gross_equity"].tolist(),
            "hedged": group["hedged"].tolist(),
            "debt": group["debt"].tolist(),
        }

    # Build monthly consensus
    pivot = df.pivot_table(index="period", columns="fund_id", values="net_equity")
    pivot["consensus_avg"] = pivot.mean(axis=1)
    pivot["consensus_min"] = pivot.min(axis=1)
    pivot["consensus_max"] = pivot.max(axis=1)

    output = {
        "generated_at": datetime.now().isoformat(),
        "funds": funds_data,
        "consensus": pivot.reset_index().to_dict(orient="records"),
    }

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    log.info(f"✅ Exported to {output_path}")
    return output


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="BAF/DAA Allocation Scraper")
    parser.add_argument("--scrape", action="store_true", help="Scrape latest period")
    parser.add_argument("--backfill", action="store_true", help="Scrape last N months")
    parser.add_argument("--export", action="store_true", help="Export JSON for dashboard")
    parser.add_argument("--months", type=int, default=6, help="Months to backfill (default: 6)")
    parser.add_argument("--fund", type=str, help="Scrape specific fund ID only")
    args = parser.parse_args()

    conn = init_db()
    log.info("✅ Database initialized")

    target_funds = [f for f in FUNDS if not args.fund or f["id"] == args.fund]

    if args.scrape or args.backfill:
        from dateutil.relativedelta import relativedelta

        if args.backfill:
            periods = []
            now = datetime.now()
            for i in range(args.months):
                dt = now - relativedelta(months=i)
                periods.append(dt.strftime("%Y-%m"))
        else:
            periods = [get_current_period()]

        log.info(f"📅 Target periods: {periods}")
        log.info(f"📋 Target funds: {[f['id'] for f in target_funds]}")

        success, failed = 0, 0
        for period in periods:
            for fund in target_funds:
                ok = scrape_fund(fund, period, conn)
                if ok:
                    success += 1
                else:
                    failed += 1
                time.sleep(2)  # Polite delay between requests

        log.info(f"\n{'='*50}")
        log.info(f"✅ Success: {success} | ❌ Failed: {failed}")

    if args.export:
        export_json(conn, "allocations_export.json")

    conn.close()


if __name__ == "__main__":
    main()
