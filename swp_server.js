/**
 * SWP Calculator - Node.js Server
 * Systematic Withdrawal Plan Calculator
 *
 * Usage:
 *   npm install express cors
 *   node swp_server.js
 *
 * API Endpoints:
 *   POST /api/calculate   - Run SWP calculation
 *   GET  /api/health      - Health check
 *   GET  /                - Serves swp_calculator.html (place in same folder)
 */

const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const https = require('https');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '5mb' }));
app.use(express.static(__dirname));

const MFAPI_BASE = 'https://api.mfapi.in';
const cache = {
  search: new Map(),
  history: new Map()
};

const CACHE_TTL_MS = {
  search: 15 * 60 * 1000,
  history: 24 * 60 * 60 * 1000
};

// Serve HTML frontend if it exists in the same folder
app.get('/', (req, res) => {
  const htmlFile = path.join(__dirname, 'swp_calculator.html');
  if (fs.existsSync(htmlFile)) {
    res.sendFile(htmlFile);
  } else {
    res.json({ message: 'SWP Calculator API. Place swp_calculator.html in this directory for the UI.' });
  }
});

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.get('/api/funds/search', async (req, res) => {
  try {
    const q = (req.query.q || '').toString().trim();
    console.info('[fund-search]', { q, ip: req.ip });
    if (q.length < 2) {
      return res.json({ results: [] });
    }

    const key = q.toLowerCase();
    const cached = getFromCache(cache.search, key, CACHE_TTL_MS.search);
    if (cached) {
      return res.json({ results: cached, cached: true });
    }

    const data = await httpGetJson(`${MFAPI_BASE}/mf/search?q=${encodeURIComponent(q)}`);
    const results = Array.isArray(data)
      ? data.slice(0, 30).map((f) => ({
          schemeCode: String(f.schemeCode),
          schemeName: f.schemeName
        }))
      : [];

    setInCache(cache.search, key, results);
    res.json({ results, cached: false });
  } catch (err) {
    res.status(500).json({ error: 'Failed to search funds', details: err.message });
  }
});

app.get('/api/funds/:code/history', async (req, res) => {
  try {
    const code = String(req.params.code || '').trim();
    console.info('[fund-history]', { code, ip: req.ip });
    if (!/^\d+$/.test(code)) {
      return res.status(400).json({ error: 'Invalid scheme code' });
    }

    const result = await loadFundHistory(code);
    res.json({ ...result, cached: false });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch fund history', details: err.message });
  }
});

/**
 * Main SWP Calculation Endpoint
 *
 * Request body:
 * {
 *   initialInvestment: 1000000,        // Number - investment amount in ₹
 *   investmentDate: "2018-03-15",      // String - ISO date YYYY-MM-DD
 *   swpStartDate: "2018-04-12",        // String - ISO date YYYY-MM-DD
 *   annualWithdrawalRate: 0.10,        // Number - e.g. 0.10 for 10%
 *   schemeCode: "122640"              // String/Number - MFAPI scheme code
 * }
 *
 * Response:
 * {
 *   summary: { ... },
 *   schedule: [ { month, date, nav, unitsRedeemed, unitsRemaining, portfolioValue, amountWithdrawn, xirr }, ... ],
 *   fund: { schemeCode, schemeName, fundHouse, category, inceptionDate, latestDate, dataPoints, returns },
 *   warnings: []
 * }
 */
app.post('/api/calculate', (req, res) => {
  try {
    const { initialInvestment, investmentDate, swpStartDate, annualWithdrawalRate, schemeCode, navData, schemeName } = req.body;
    console.info('[calculate]', { schemeCode, investmentDate, swpStartDate, annualWithdrawalRate, ip: req.ip });

    // Validate inputs
    const errors = validateInputs({ initialInvestment, investmentDate, swpStartDate, annualWithdrawalRate, schemeCode });
    if (errors.length > 0) {
      return res.status(400).json({ error: errors.join('; ') });
    }

    const resultPromise = calculateSWPFromScheme({
      initialInvestment,
      investmentDate,
      swpStartDate,
      annualWithdrawalRate,
      schemeCode,
      navData,
      schemeName
    });
    resultPromise.then((result) => res.json(result)).catch((err) => {
      console.error('Calculation error:', err.message);
      res.status(500).json({ error: 'Calculation failed: ' + err.message });
    });
  } catch (err) {
    console.error('Calculation error:', err.message);
    res.status(500).json({ error: 'Calculation failed: ' + err.message });
  }
});

// ---- Validation ----
function validateInputs({ initialInvestment, investmentDate, swpStartDate, annualWithdrawalRate, schemeCode }) {
  const errs = [];
  if (!initialInvestment || isNaN(initialInvestment) || initialInvestment <= 0)
    errs.push('initialInvestment must be a positive number');
  if (!investmentDate || !/^\d{4}-\d{2}-\d{2}$/.test(investmentDate))
    errs.push('investmentDate must be YYYY-MM-DD');
  if (!swpStartDate || !/^\d{4}-\d{2}-\d{2}$/.test(swpStartDate))
    errs.push('swpStartDate must be YYYY-MM-DD');
  if (annualWithdrawalRate === undefined || isNaN(annualWithdrawalRate) || annualWithdrawalRate <= 0 || annualWithdrawalRate > 1)
    errs.push('annualWithdrawalRate must be between 0 and 1 (e.g., 0.10 for 10%)');
  if (!schemeCode || !/^\d+$/.test(String(schemeCode)))
    errs.push('schemeCode must be a valid numeric code');
  return errs;
}

async function loadFundHistory(code) {
  const todayIST = getDateKeyInTimeZone('Asia/Kolkata');
  const cached = getFromCache(cache.history, code, CACHE_TTL_MS.history, { dayKey: todayIST });
  if (cached) return cached;

  const payload = await httpGetJson(`${MFAPI_BASE}/mf/${code}`);
  const rows = Array.isArray(payload.data) ? payload.data : [];
  if (!rows.length) {
    throw new Error('No historical data found for scheme code');
  }

  const navData = {};
  for (const row of rows) {
    const iso = parseMFAPIDate(row.date);
    const nav = parseFloat(row.nav);
    if (!iso || !isFinite(nav) || nav <= 0) continue;
    navData[iso] = nav;
  }

  const dates = Object.keys(navData).sort();
  if (!dates.length) {
    throw new Error('Unable to parse historical NAV data');
  }

  const priceHistory = dates.map((date, index) => {
    const nav = navData[date];
    const previousDate = index > 0 ? dates[index - 1] : null;
    const previousNav = previousDate ? navData[previousDate] : null;
    const dailyReturn = previousNav ? (nav - previousNav) / previousNav : null;
    const cumulativeReturn = index > 0 ? (nav / navData[dates[0]]) - 1 : 0;

    return {
      date,
      nav,
      dailyReturn,
      dailyReturnPercent: dailyReturn !== null ? dailyReturn * 100 : null,
      cumulativeReturn,
      cumulativeReturnPercent: cumulativeReturn * 100
    };
  });

  const firstDate = dates[0];
  const latestDate = dates[dates.length - 1];
  const firstNav = navData[firstDate];
  const latestNav = navData[latestDate];
  const historySpanYears = Math.max((new Date(latestDate) - new Date(firstDate)) / (365.25 * 86400000), 0);
  const totalReturn = firstNav > 0 ? (latestNav / firstNav) - 1 : null;
  const annualizedReturn = totalReturn !== null && historySpanYears > 0
    ? Math.pow(1 + totalReturn, 1 / historySpanYears) - 1
    : null;

  const result = {
    schemeCode: code,
    schemeName: payload.meta && payload.meta.scheme_name ? payload.meta.scheme_name : `Scheme ${code}`,
    fundHouse: payload.meta && payload.meta.fund_house ? payload.meta.fund_house : null,
    category: payload.meta && payload.meta.scheme_category ? payload.meta.scheme_category : null,
    inceptionDate: firstDate,
    latestDate,
    dataPoints: dates.length,
    navData,
    priceHistory,
    returns: {
      totalReturn,
      totalReturnPercent: totalReturn !== null ? totalReturn * 100 : null,
      annualizedReturn,
      annualizedReturnPercent: annualizedReturn !== null ? annualizedReturn * 100 : null,
      latestDailyReturn: priceHistory.length > 1 ? priceHistory[priceHistory.length - 1].dailyReturn : null,
      latestDailyReturnPercent: priceHistory.length > 1 ? priceHistory[priceHistory.length - 1].dailyReturnPercent : null,
      firstNav,
      latestNav
    }
  };

  setInCache(cache.history, code, result, { dayKey: todayIST });
  return result;
}

function parseMFAPIDate(dateStr) {
  const parts = String(dateStr || '').split('-');
  if (parts.length !== 3) return null;
  const day = parts[0].padStart(2, '0');
  const month = parts[1].padStart(2, '0');
  const year = parts[2];
  if (!/^\d{4}$/.test(year)) return null;
  return `${year}-${month}-${day}`;
}

async function httpGetJson(url, options = {}) {
  const retries = options.retries ?? 2;
  const timeoutMs = options.timeoutMs ?? 8000;
  let lastError = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await requestJson(url, timeoutMs);
    } catch (err) {
      lastError = err;
      if (attempt < retries) {
        await wait(250 * (attempt + 1));
      }
    }
  }

  throw lastError || new Error('Request failed');
}

function requestJson(url, timeoutMs) {
  return new Promise((resolve, reject) => {
    const request = https.get(url, (response) => {
      let data = '';
      response.on('data', (chunk) => {
        data += chunk;
      });
      response.on('end', () => {
        try {
          resolve(JSON.parse(data || '{}'));
        } catch (err) {
          reject(new Error(`Invalid JSON from upstream: ${err.message}`));
        }
      });
    });

    request.setTimeout(timeoutMs, () => {
      request.destroy(new Error(`Upstream request timed out after ${timeoutMs}ms`));
    });

    request.on('error', (err) => reject(err));
  });
}

// ---- Core SWP Calculation ----
function calculateSWP({ initialInvestment, investmentDate, swpStartDate, annualWithdrawalRate, navData }) {
  // Sort NAV keys
  const navKeys = Object.keys(navData).sort();

  // Get NAV on or just before a date
  function getNAV(dateStr) {
    if (navData[dateStr] !== undefined) return { date: dateStr, nav: navData[dateStr] };
    let best = null;
    for (const k of navKeys) {
      if (k <= dateStr) best = k;
      else break;
    }
    if (best) return { date: best, nav: navData[best] };
    return null;
  }

  // NAV at investment date
  const investNAVInfo = getNAV(investmentDate);
  if (!investNAVInfo) throw new Error('No NAV found for investment date');

  const investNAV = investNAVInfo.nav;
  let unitsHeld = initialInvestment / investNAV;

  const cfAmounts = [-initialInvestment];
  const cfDates = [parseISODateUTC(investmentDate)];

  const schedule = [];
  let totalWithdrawn = 0;
  let month = 1;

  const latestNAVDate = navKeys[navKeys.length - 1];

  let currentDate = parseISODateUTC(swpStartDate);

  while (true) {
    if (unitsHeld <= 0) break;

    const dateStr = formatDate(currentDate);
    if (dateStr > latestNAVDate) break;

    const navInfo = getNAV(dateStr);
    if (!navInfo) break;

    const nav = navInfo.nav;
    const actualDateStr = navInfo.date;

    const portfolioValue = unitsHeld * nav;
    const monthlyWithdrawal = portfolioValue * (annualWithdrawalRate / 12);

    if (monthlyWithdrawal >= portfolioValue) break;

    const unitsRedeemed = monthlyWithdrawal / nav;
    unitsHeld -= unitsRedeemed;
    const portfolioAfter = unitsHeld * nav;

    cfAmounts.push(monthlyWithdrawal);
    cfDates.push(parseISODateUTC(actualDateStr));

    // Running XIRR
    let xirrVal = null;
    if (cfAmounts.length >= 2) {
      const tempCF = [...cfAmounts, portfolioAfter];
      const tempDates = [...cfDates, parseISODateUTC(actualDateStr)];
      xirrVal = computeXIRR(tempCF, tempDates);
    }

    totalWithdrawn += monthlyWithdrawal;

    schedule.push({
      month,
      date: actualDateStr,
      nav: round(nav, 2),
      unitsRedeemed: round(unitsRedeemed, 6),
      unitsRemaining: round(unitsHeld, 6),
      portfolioValue: round(portfolioAfter, 2),
      amountWithdrawn: round(monthlyWithdrawal, 2),
      xirr: xirrVal !== null ? round(xirrVal, 6) : null,
      xirrPercent: xirrVal !== null ? round(xirrVal * 100, 4) : null
    });

    month++;
    currentDate = addMonths(parseISODateUTC(swpStartDate), month - 1);
  }

  if (!schedule.length) throw new Error('No withdrawal rows generated. Check dates and NAV data range.');

  const lastRow = schedule[schedule.length - 1];
  const currentPortfolio = lastRow.portfolioValue;
  const netGainLoss = totalWithdrawn + currentPortfolio - initialInvestment;

  // Final XIRR
  const finalCF = [...cfAmounts, currentPortfolio];
  const finalDates = [...cfDates, parseISODateUTC(lastRow.date)];
  const finalXIRR = computeXIRR(finalCF, finalDates);

  return {
    summary: {
      initialInvestment: round(initialInvestment, 2),
      investmentDate,
      swpStartDate,
      annualWithdrawalRate,
      investNAVDate: investNAVInfo.date,
      investNAV: round(investNAV, 2),
      investNAVExact: round(investNAV, 6),
      unitsPurchasedAtStart: round(initialInvestment / investNAV, 6),
      latestNAVDate,
      latestNAV: round(navData[latestNAVDate], 2),
      totalWithdrawn: round(totalWithdrawn, 2),
      currentPortfolioValue: round(currentPortfolio, 2),
      netGainLoss: round(netGainLoss, 2),
      xirr: finalXIRR !== null ? round(finalXIRR, 6) : null,
      xirrPercent: finalXIRR !== null ? round(finalXIRR * 100, 4) : null,
      monthsProcessed: schedule.length
    },
    schedule
  };
}

// ---- XIRR (Newton-Raphson) ----
function computeXIRR(cashflows, dates) {
  function npv(r) {
    return cashflows.reduce((acc, cf, i) => {
      const t = (dates[i] - dates[0]) / (365.25 * 86400000);
      return acc + cf / Math.pow(1 + r, t);
    }, 0);
  }
  function dnpv(r) {
    return cashflows.reduce((acc, cf, i) => {
      const t = (dates[i] - dates[0]) / (365.25 * 86400000);
      return acc - t * cf / Math.pow(1 + r, t + 1);
    }, 0);
  }
  let r = 0.1;
  for (let i = 0; i < 300; i++) {
    const fr = npv(r);
    if (Math.abs(fr) < 1e-7) break;
    const dfr = dnpv(r);
    if (Math.abs(dfr) < 1e-12) break;
    r = r - fr / dfr;
    if (r < -0.9999) r = -0.9999;
  }
  return isNaN(r) || !isFinite(r) ? null : r;
}

async function calculateSWPFromScheme({ initialInvestment, investmentDate, swpStartDate, annualWithdrawalRate, schemeCode, navData: providedNavData, schemeName }) {
  let fund = null;
  let navData = null;

  if (providedNavData && typeof providedNavData === 'object' && Object.keys(providedNavData).length > 0) {
    navData = providedNavData;
  } else {
    fund = await loadFundHistory(String(schemeCode));
    navData = fund.navData;
  }

  const navKeys = Object.keys(navData).sort();

  const firstAvailable = navKeys[0];
  const latestAvailable = navKeys[navKeys.length - 1];

  const clampedInvestmentDate = investmentDate < firstAvailable ? firstAvailable : investmentDate > latestAvailable ? latestAvailable : investmentDate;
  const clampedSwpStartDate = swpStartDate < firstAvailable ? firstAvailable : swpStartDate > latestAvailable ? latestAvailable : swpStartDate;
  const effectiveSwpStartDate = clampedSwpStartDate < clampedInvestmentDate ? clampedInvestmentDate : clampedSwpStartDate;

  const result = calculateSWP({
    initialInvestment,
    investmentDate: clampedInvestmentDate,
    swpStartDate: effectiveSwpStartDate,
    annualWithdrawalRate,
    navData
  });

  result.fund = fund
    ? {
        schemeCode: fund.schemeCode,
        schemeName: fund.schemeName,
        fundHouse: fund.fundHouse,
        category: fund.category,
        inceptionDate: fund.inceptionDate,
        latestDate: fund.latestDate,
        dataPoints: fund.dataPoints,
        returns: fund.returns
      }
    : {
        schemeCode: String(schemeCode),
        schemeName: schemeName || `Scheme ${schemeCode}`,
        fundHouse: null,
        category: null,
        inceptionDate: navKeys[0],
        latestDate: navKeys[navKeys.length - 1],
        dataPoints: navKeys.length,
        returns: null
      };
  result.warnings = [];
  if (clampedInvestmentDate !== investmentDate) {
    result.warnings.push(`Investment date was adjusted to ${clampedInvestmentDate} to fit available data.`);
  }
  if (effectiveSwpStartDate !== swpStartDate) {
    result.warnings.push(`SWP start date was adjusted to ${effectiveSwpStartDate} to fit available data.`);
  }

  result.audit = buildCalculationAudit(result);
  if (!result.audit.passed) {
    result.warnings.push('Internal consistency checks found a mismatch. Please verify inputs and results.');
  }

  return result;
}

function buildCalculationAudit(result) {
  const summary = result.summary || {};
  const rows = Array.isArray(result.schedule) ? result.schedule : [];
  const first = rows[0] || null;
  const last = rows[rows.length - 1] || null;

  const EPS_MONEY = 2;
  const EPS_UNITS = 0.05;

  const checks = [];

  const reconstructedUnits = first
    ? Number(first.unitsRemaining || 0) + Number(first.unitsRedeemed || 0)
    : null;
  const unitsDelta = reconstructedUnits !== null
    ? Math.abs(Number(summary.unitsPurchasedAtStart || 0) - reconstructedUnits)
    : null;
  checks.push({
    name: 'units-at-start',
    passed: reconstructedUnits !== null && unitsDelta <= EPS_UNITS,
    delta: unitsDelta,
    tolerance: EPS_UNITS
  });

  const withdrawnFromRows = rows.reduce((acc, row) => acc + Number(row.amountWithdrawn || 0), 0);
  const withdrawnDelta = Math.abs(withdrawnFromRows - Number(summary.totalWithdrawn || 0));
  checks.push({
    name: 'total-withdrawn',
    passed: withdrawnDelta <= EPS_MONEY,
    delta: withdrawnDelta,
    tolerance: EPS_MONEY
  });

  const expectedNet = Number(summary.totalWithdrawn || 0) + Number(summary.currentPortfolioValue || 0) - Number(summary.initialInvestment || 0);
  const netDelta = Math.abs(expectedNet - Number(summary.netGainLoss || 0));
  checks.push({
    name: 'net-gain-loss',
    passed: netDelta <= EPS_MONEY,
    delta: netDelta,
    tolerance: EPS_MONEY
  });

  checks.push({
    name: 'months-processed',
    passed: rows.length === Number(summary.monthsProcessed || 0),
    expected: Number(summary.monthsProcessed || 0),
    actual: rows.length
  });

  const latestNAVDate = String(summary.latestNAVDate || '');
  const boundaryPass = last ? String(last.date || '') <= latestNAVDate : false;
  checks.push({
    name: 'latest-nav-boundary',
    passed: boundaryPass,
    latestNAVDate,
    lastRowDate: last ? last.date : null
  });

  return {
    passed: checks.every((c) => c.passed),
    checks
  };
}

function formatDate(d) {
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}

function addMonths(date, n) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  d.setUTCMonth(d.getUTCMonth() + n);
  return d;
}

function parseISODateUTC(dateStr) {
  const parts = String(dateStr || '').split('-');
  if (parts.length !== 3) {
    throw new Error(`Invalid ISO date: ${dateStr}`);
  }

  const year = Number(parts[0]);
  const month = Number(parts[1]);
  const day = Number(parts[2]);
  const date = new Date(Date.UTC(year, month - 1, day));

  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day) || formatDate(date) !== `${parts[0]}-${parts[1]}-${parts[2]}`) {
    throw new Error(`Invalid ISO date: ${dateStr}`);
  }

  return date;
}

function round(n, decimals) {
  return Math.round(n * Math.pow(10, decimals)) / Math.pow(10, decimals);
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getFromCache(map, key, ttlMs, options = {}) {
  const item = map.get(key);
  if (!item) return null;
  if (options.dayKey && item.dayKey !== options.dayKey) {
    map.delete(key);
    return null;
  }
  if (Date.now() - item.time > ttlMs) {
    map.delete(key);
    return null;
  }
  return item.value;
}

function setInCache(map, key, value, meta = {}) {
  map.set(key, { time: Date.now(), value, ...meta });
}

function getDateKeyInTimeZone(timeZone) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(new Date());

  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  const day = parts.find((part) => part.type === 'day')?.value;
  return `${year}-${month}-${day}`;
}

// ---- Start Server ----
if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`\n╔════════════════════════════════════════╗`);
    console.log(`║     SWP Calculator Server Running      ║`);
    console.log(`╚════════════════════════════════════════╝`);
    console.log(`  URL:    http://localhost:${PORT}`);
    console.log(`  API:    http://localhost:${PORT}/api/calculate`);
    console.log(`  Health: http://localhost:${PORT}/api/health\n`);
  });
}

module.exports = { app, calculateSWP, computeXIRR }; // for testing and serverless adapters
