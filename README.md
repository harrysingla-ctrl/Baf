# BAF/DAA Allocation Tracker

A real-time dashboard for tracking **Balanced Advantage Fund (BAF)** and **Dynamic Asset Allocation (DAA)** net equity allocations as collective market valuation signals.

🔗 **Live Demo**: https://harrysingla-ctrl.github.io/Baf

## 📊 What This Does

- **Scrapes** monthly portfolio data from AMFI disclosures and AMC factsheets (8 major funds)
- **Analyzes** net equity allocation trends as early indicators of market sentiment
- **Visualizes** consensus, divergence, and momentum across BAF/DAA funds
- **AI-Powered**: Claude analyzes collective positioning for market insights
- **Automated**: GitHub Actions runs monthly scraper on 12th and 27th

## 🎯 Funds Tracked

| Fund | AMC | Category |
|------|-----|----------|
| ICICI Pru Balanced Advantage | ICICI Prudential | BAF |
| HDFC Balanced Advantage | HDFC AMC | BAF |
| Edelweiss Balanced Advantage | Edelweiss MF | BAF |
| DSP Dynamic Asset Allocation | DSP MF | DAA |
| Kotak Balanced Advantage | Kotak MF | BAF |
| Nippon India Balanced Advantage | Nippon India MF | BAF |
| Bajaj Finserv Balanced Advantage | Bajaj Finserv MF | BAF |
| Axis Dynamic Asset Allocation | Axis MF | DAA |

## 📈 Dashboards

### 1. **Dashboard Tab** 📊
- **Consensus Gauge**: Average net equity + valuation signal (Bullish/Bearish/Neutral)
- **Fund Cards**: Current allocation, MoM change, 8-month trend sparkline
- **Click to Focus**: Zoom into individual fund or compare all

### 2. **Trends Tab** 📈
- **8-Month Table**: Net equity evolution across all funds
- **MoM Heatmap**: Month-over-month changes visualized
- **Trend Direction**: Rising/falling/flat indicators

### 3. **AI Signals Tab** 🧠
- **Claude Analysis**: Market signal interpretation of collective positioning
- **Key Metrics**: Consensus, most bullish, most defensive, largest moves
- **Market Context**: Indian mutual fund + 3-year horizon

### 4. **Setup Tab** ⚙️
- **Installation** steps
- **Automation** guide
- **Legal basis** for data scraping

## 🚀 Quick Start

### Deploy to GitHub Pages (Already Done!)

Your repository is configured to serve on GitHub Pages.

**To make it live:**
1. Go to repository settings → Pages
2. Select "Deploy from a branch"
3. Choose `main` branch, `/root` directory
4. Save — your dashboard goes live in ~1 minute

### Add Real Data

```bash
# 1. Clone your repo
git clone https://github.com/harrysingla-ctrl/Baf.git
cd Baf

# 2. Install dependencies
pip install requests pdfplumber pandas python-dateutil

# 3. Scrape latest month
python baf_scraper.py --scrape --export

# 4. Push data
git add allocations_export.json
git commit -m "Update with real fund data"
git push# Baf