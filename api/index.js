// ============================================================
// DONATION BRIDGE — Vercel API
// api/index.js — FINAL v9 (Queue Support, anti double donate)
// + Auto Currency Convert to IDR (native fetch, no node-fetch)
// ============================================================

const express = require('express');
const { Redis } = require('@upstash/redis');
const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ── Redis client ──────────────────────────────────────────────

let redis = null;

if (process.env.UPSTASH_REDIS_REST_URL && process.env.UPSTASH_REDIS_REST_TOKEN) {
  redis = new Redis({
    url:   process.env.UPSTASH_REDIS_REST_URL,
    token: process.env.UPSTASH_REDIS_REST_TOKEN,
  });
  console.log("✅ Redis Upstash terhubung");
} else {
  console.warn("⚠️ Redis tidak dikonfigurasi, pakai in-memory");
}

// ── In-memory fallback ────────────────────────────────────────

let memoryQueue = [];

const defaultDonation = {
  id:        "START",
  donator:   "System",
  amount:    0,
  message:   "Ready",
  timestamp: 0
};

// ── Currency Conversion to IDR ────────────────────────────────

const FALLBACK_RATES_TO_IDR = {
  IDR: 1,
  MYR: 3400,   // Ringgit Malaysia
  USD: 16300,  // US Dollar
  SGD: 12100,  // Singapore Dollar
  EUR: 17600,  // Euro
  GBP: 20500,  // British Pound
  AUD: 10500,  // Australian Dollar
  JPY: 108,    // Japanese Yen
  THB: 460,    // Thai Baht
  PHP: 280,    // Philippine Peso
  BND: 12100,  // Brunei Dollar
  KHR: 4,      // Cambodian Riel
  LAK: 0.7,    // Lao Kip
  MMK: 7.7,    // Myanmar Kyat
  VND: 0.65,   // Vietnamese Dong
  CNY: 2250,   // Chinese Yuan
  HKD: 2100,   // Hong Kong Dollar
  TWD: 510,    // Taiwan Dollar
  KRW: 12,     // Korean Won
  SAR: 4350,   // Saudi Riyal
  AED: 4440,   // UAE Dirham
  INR: 196,    // Indian Rupee
  PKR: 58,     // Pakistani Rupee
  BDT: 148,    // Bangladeshi Taka
  NPR: 122,    // Nepalese Rupee
  LKR: 54,     // Sri Lankan Rupee
  NZD: 9700,   // New Zealand Dollar
  CAD: 12000,  // Canadian Dollar
  CHF: 18500,  // Swiss Franc
  SEK: 1550,   // Swedish Krona
  NOK: 1500,   // Norwegian Krone
  DKK: 2360,   // Danish Krone
};

// Cache rate per currency (TTL 30 menit)
const rateCache = {
  rates:     {},
  fetchedAt: {},
  TTL_MS:    30 * 60 * 1000,
};

function detectCurrency(body) {
  const d = body.data || body;

  // Field eksplisit (prioritas utama)
  const explicit = (
    d.currency           ||
    d.currency_code      ||
    d.payment_currency   ||
    d.supporter_currency ||
    d.amount_currency    ||
    ""
  ).toString().trim().toUpperCase();

  if (explicit && explicit.length === 3) return explicit;

  // Deteksi dari amount_formatted / amount_raw (misal "MYR 10.00", "RM 10")
  const formatted = (d.amount_formatted || d.amount_raw || "").toString().trim().toUpperCase();

  const matchCode = formatted.match(/^([A-Z]{2,4})\s?[\d.,]/);
  if (matchCode) {
    const candidate = matchCode[1];
    if (FALLBACK_RATES_TO_IDR[candidate] !== undefined) return candidate;
  }

  if (/^RM\s?[\d.,]/.test(formatted))   return "MYR";
  if (/^SG\$\s?[\d.,]/.test(formatted)) return "SGD";
  if (/^A\$\s?[\d.,]/.test(formatted))  return "AUD";
  if (/^\$\s?[\d.,]/.test(formatted))   return "USD";
  if (/^£\s?[\d.,]/.test(formatted))    return "GBP";
  if (/^€\s?[\d.,]/.test(formatted))    return "EUR";
  if (/^¥\s?[\d.,]/.test(formatted))    return "JPY";

  return "IDR";
}

async function getRateToIDR(currency) {
  if (currency === "IDR") return 1;

  const now = Date.now();

  // Gunakan cache jika masih fresh
  if (
    rateCache.rates[currency] &&
    rateCache.fetchedAt[currency] &&
    (now - rateCache.fetchedAt[currency]) < rateCache.TTL_MS
  ) {
    console.log(`💱 Rate dari cache: 1 ${currency} = ${rateCache.rates[currency]} IDR`);
    return rateCache.rates[currency];
  }

  // Fetch live rate menggunakan native fetch (Node 18+, no dependency)
  try {
    const controller = new AbortController();
    const timeout    = setTimeout(() => controller.abort(), 5000);

    const res = await fetch(
      `https://api.frankfurter.app/latest?base=${currency}&symbols=IDR`,
      { signal: controller.signal }
    );

    clearTimeout(timeout);

    if (res.ok) {
      const data = await res.json();
      if (data && data.rates && data.rates.IDR) {
        const liveRate = Math.round(data.rates.IDR);
        rateCache.rates[currency]     = liveRate;
        rateCache.fetchedAt[currency] = now;
        console.log(`💱 Live rate: 1 ${currency} = ${liveRate} IDR`);
        return liveRate;
      }
    }
    throw new Error("Rate tidak tersedia dari frankfurter.app");
  } catch (err) {
    console.warn(`⚠️ Gagal fetch live rate ${currency}: ${err.message} — pakai fallback`);
    return FALLBACK_RATES_TO_IDR[currency] || null;
  }
}

async function convertToIDR(rawAmount, currency) {
  const amount = parseInt(String(rawAmount).replace(/\D/g, "")) || 0;
  if (amount <= 0)        return { amountIDR: 0,      originalAmount: 0,      currency, rate: 1, converted: false };
  if (currency === "IDR") return { amountIDR: amount, originalAmount: amount, currency, rate: 1, converted: false };

  const rate = await getRateToIDR(currency);
  if (!rate) {
    console.warn(`⚠️ Currency tidak dikenal: ${currency}, amount digunakan as-is`);
    return { amountIDR: amount, originalAmount: amount, currency, rate: 1, converted: false };
  }

  const amountIDR = Math.round(amount * rate);
  console.log(`💱 Konversi: ${amount} ${currency} × ${rate} = ${amountIDR} IDR`);
  return { amountIDR, originalAmount: amount, currency, rate, converted: true };
}

// ── Redis queue helpers ───────────────────────────────────────

async function pushDonation(data) {
  try {
    if (redis) {
      await redis.rpush("donationQueue", JSON.stringify(data));
      console.log("✅ Redis RPUSH sukses:", JSON.stringify(data));
    } else {
      memoryQueue.push(data);
      console.log("✅ Memory PUSH sukses:", JSON.stringify(data));
    }
  } catch (err) {
    console.error("❌ pushDonation error:", err.message);
    memoryQueue.push(data);
  }
}

async function peekQueue() {
  try {
    if (redis) {
      const items = await redis.lrange("donationQueue", 0, -1);
      console.log("📦 Redis LRANGE raw count:", items ? items.length : 0);
      if (!items || items.length === 0) return [];
      return items.map(raw => {
        const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
        return {
          id:        String(parsed.id        || "START"),
          donator:   String(parsed.donator   || "System"),
          amount:    Number(parsed.amount    || 0),
          message:   String(parsed.message   || ""),
          timestamp: Number(parsed.timestamp || 0)
        };
      });
    } else {
      return [...memoryQueue];
    }
  } catch (err) {
    console.error("❌ peekQueue error:", err.message);
    return [...memoryQueue];
  }
}

async function ackDonations(lastTimestamp) {
  try {
    if (redis) {
      const items = await redis.lrange("donationQueue", 0, -1);
      if (!items || items.length === 0) return;

      const remaining = items.filter(raw => {
        const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
        return Number(parsed.timestamp || 0) > lastTimestamp;
      });

      await redis.del("donationQueue");
      if (remaining.length > 0) {
        for (const item of remaining) {
          await redis.rpush("donationQueue", typeof item === "string" ? item : JSON.stringify(item));
        }
      }
      console.log(`✅ ACK done. Sisa queue: ${remaining.length}`);
    } else {
      memoryQueue = memoryQueue.filter(d => Number(d.timestamp || 0) > lastTimestamp);
    }
  } catch (err) {
    console.error("❌ ackDonations error:", err.message);
  }
}

// ── Middleware ────────────────────────────────────────────────

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin",  "*");
  res.header("Access-Control-Allow-Headers", "*");
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();
  next();
});

// ── Routes ────────────────────────────────────────────────────

app.get('/', (req, res) => {
  res.send("DONATION BRIDGE AKTIF v9 - Redis Queue + Auto Currency Convert");
});

app.get('/api/donations/queue', async (req, res) => {
  try {
    const since   = Number(req.query.since || 0);
    const all     = await peekQueue();
    const newOnes = all.filter(d => d.timestamp > since);
    console.log(`📤 GET /queue since=${since} → ${newOnes.length} item baru`);
    res.status(200).json({ donations: newOnes });
  } catch (err) {
    console.error("❌ GET queue error:", err.message);
    res.status(200).json({ donations: [] });
  }
});

app.post('/api/donations/ack', async (req, res) => {
  try {
    const lastTimestamp = Number(req.body.lastTimestamp || 0);
    console.log("📨 ACK lastTimestamp:", lastTimestamp);
    await ackDonations(lastTimestamp);
    res.status(200).json({ status: "OK" });
  } catch (err) {
    console.error("❌ ACK error:", err.message);
    res.status(200).json({ status: "ERROR", message: err.message });
  }
});

app.get('/api/donations/latest', async (req, res) => {
  try {
    const all = await peekQueue();
    if (all.length === 0) {
      return res.status(200).json(defaultDonation);
    }
    const latest = all.reduce((a, b) => a.timestamp > b.timestamp ? a : b);
    console.log("📤 GET /latest (legacy):", JSON.stringify(latest));
    res.status(200).json(latest);
  } catch (err) {
    console.error("❌ GET latest error:", err.message);
    res.status(200).json(defaultDonation);
  }
});

// ── Webhook Sociabuzz ─────────────────────────────────────────

app.post('/api/webhook/sociabuzz', async (req, res) => {
  console.log("════════════════════════════════════");
  console.log("📦 WEBHOOK MASUK");
  console.log("Body:", JSON.stringify(req.body));
  console.log("════════════════════════════════════");

  try {
    const d = req.body.data || req.body;

    const rawName = (
      d.supporter      ||
      d.supporter_name ||
      d.donator_name   ||
      d.sender_name    ||
      d.name           ||
      d.donator        ||
      d.from           ||
      d.user_name      ||
      d.username       ||
      d.nickname       ||
      ""
    ).toString().trim();

    console.log("👤 Nama:", rawName);

    if (!rawName || rawName.toLowerCase() === "anonymous") {
      return res.status(200).json({ status: "SKIP_ANONYMOUS" });
    }

    const rawAmount = d.amount_raw || d.amount || d.net_amount || d.total || d.price || d.value || 0;

    // ── AUTO CURRENCY CONVERT ──────────────────────────────────
    const currency = detectCurrency(req.body);
    const { amountIDR, originalAmount, rate, converted } = await convertToIDR(rawAmount, currency);

    console.log(`💰 Amount: ${originalAmount} ${currency}${converted ? ` → ${amountIDR} IDR (rate: ${rate})` : " (IDR, no convert)"}`);

    if (amountIDR <= 0) {
      return res.status(200).json({ status: "SKIP_ZERO" });
    }
    // ──────────────────────────────────────────────────────────

    const donationId = (
      d.id             ||
      d.order_id       ||
      d.transaction_id ||
      d.invoice_id     ||
      d.ref_id         ||
      `${rawName.toLowerCase().replace(/\s/g,"_")}_${amountIDR}_${Date.now()}`
    ).toString();

    const baseMessage = (d.message || d.note || d.description || "").toString().trim();
    const convertNote = converted
      ? `[${originalAmount} ${currency} → Rp${amountIDR.toLocaleString("id-ID")}]${baseMessage ? " " + baseMessage : ""}`
      : baseMessage;

    const donation = {
      id:        donationId,
      donator:   rawName,
      amount:    amountIDR,   // ← selalu IDR
      message:   convertNote,
      timestamp: Date.now(),
    };

    console.log("✅ DONATION PUSH KE QUEUE:", JSON.stringify(donation));
    await pushDonation(donation);
    res.status(200).json({ status: "OK", donation });

  } catch (err) {
    console.error("❌ Webhook error:", err.message);
    res.status(200).json({ status: "ERROR", message: err.message });
  }
});

// ── Test Inject ───────────────────────────────────────────────

app.post('/api/test/inject', async (req, res) => {
  try {
    const { name, amount, message } = req.body;
    if (!name || !amount) {
      return res.status(400).json({ status: "ERROR", message: "name dan amount wajib" });
    }
    const donation = {
      id:        `test_${name.toLowerCase().replace(/\s/g,"_")}_${Date.now()}`,
      donator:   name.toString().trim(),
      amount:    parseInt(String(amount).replace(/\D/g, "")) || 0,
      message:   (message || "TEST").toString(),
      timestamp: Date.now(),
    };
    console.log("🧪 TEST INJECT:", JSON.stringify(donation));
    await pushDonation(donation);
    res.status(200).json({ status: "OK", donation });
  } catch (err) {
    res.status(500).json({ status: "ERROR", message: err.message });
  }
});

// ── Test Page ─────────────────────────────────────────────────

app.get('/test', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Test Inject Donation</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: Arial, sans-serif; background: #1a1a2e; color: #eee; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 20px; }
    .card { background: #16213e; border-radius: 12px; padding: 30px; width: 100%; max-width: 480px; box-shadow: 0 8px 32px rgba(0,0,0,0.4); }
    h2 { text-align: center; margin-bottom: 24px; font-size: 22px; color: #e94560; }
    label { display: block; font-size: 13px; color: #aaa; margin-bottom: 4px; margin-top: 14px; }
    input, select { width: 100%; padding: 11px 14px; background: #0f3460; border: 1px solid #e9456033; border-radius: 8px; color: #fff; font-size: 15px; outline: none; transition: border 0.2s; }
    input:focus, select:focus { border-color: #e94560; }
    select option { background: #0f3460; }
    .tiers { display: flex; gap: 8px; margin-top: 14px; flex-wrap: wrap; }
    .tier-btn { flex: 1; min-width: 80px; padding: 8px 4px; background: #0f3460; border: 1px solid #e9456033; border-radius: 8px; color: #ccc; font-size: 13px; cursor: pointer; text-align: center; transition: all 0.2s; }
    .tier-btn:hover { background: #e9456022; border-color: #e94560; color: #fff; }
    .tier-btn.blackhole { border-color: #9b59b6; }
    .tier-btn.blackhole:hover { background: #9b59b622; border-color: #9b59b6; }
    .tier-btn.smite { border-color: #e67e22; }
    .tier-btn.smite:hover { background: #e67e2222; border-color: #e67e22; }
    .tier-btn.nuke { border-color: #27ae60; }
    .tier-btn.nuke:hover { background: #27ae6022; border-color: #27ae60; }
    button.submit { width: 100%; padding: 13px; margin-top: 20px; background: #e94560; color: white; border: none; border-radius: 8px; font-size: 16px; font-weight: bold; cursor: pointer; transition: background 0.2s; }
    button.submit:hover { background: #c73652; }
    button.submit:disabled { background: #555; cursor: not-allowed; }
    #result { margin-top: 18px; padding: 14px; background: #0f3460; border-radius: 8px; display: none; font-size: 13px; line-height: 1.6; word-break: break-all; border-left: 3px solid #e94560; }
    #result.ok { border-color: #27ae60; }
    #result.err { border-color: #e94560; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; margin-bottom: 6px; }
    .badge.ok { background: #27ae6033; color: #2ecc71; }
    .badge.err { background: #e9456033; color: #e94560; }
    pre { margin-top: 6px; white-space: pre-wrap; color: #ccc; }
    .currency-note { font-size: 11px; color: #888; margin-top: 4px; }
  </style>
</head>
<body>
  <div class="card">
    <h2>🧪 Test Inject Donation</h2>

    <label>Nama Donatur</label>
    <input type="text" id="name" placeholder="Contoh: BayuGaming" value="TestUser" />

    <label>Amount</label>
    <input type="number" id="amount" placeholder="Contoh: 50000" value="10000" />

    <label>Currency</label>
    <select id="currency">
      <option value="IDR" selected>IDR — Rupiah Indonesia</option>
      <option value="MYR">MYR — Ringgit Malaysia</option>
      <option value="USD">USD — US Dollar</option>
      <option value="SGD">SGD — Singapore Dollar</option>
      <option value="EUR">EUR — Euro</option>
      <option value="GBP">GBP — British Pound</option>
      <option value="AUD">AUD — Australian Dollar</option>
      <option value="JPY">JPY — Japanese Yen</option>
      <option value="THB">THB — Thai Baht</option>
      <option value="PHP">PHP — Philippine Peso</option>
      <option value="BND">BND — Brunei Dollar</option>
      <option value="CNY">CNY — Chinese Yuan</option>
      <option value="KRW">KRW — Korean Won</option>
      <option value="SAR">SAR — Saudi Riyal</option>
      <option value="AED">AED — UAE Dirham</option>
      <option value="INR">INR — Indian Rupee</option>
    </select>
    <div class="currency-note">* Selain IDR akan otomatis dikonversi ke IDR via live rate (fallback ke rate hardcoded jika gagal)</div>

    <label>Preset Tier (IDR)</label>
    <div class="tiers">
      <div class="tier-btn nuke"      onclick="setAmount(50000)">🚀 Nuke<br><small>Rp50.000</small></div>
      <div class="tier-btn smite"     onclick="setAmount(100000)">⚡ Smite<br><small>Rp100.000</small></div>
      <div class="tier-btn blackhole" onclick="setAmount(200000)">🕳️ BlackHole<br><small>Rp200.000</small></div>
    </div>

    <label>Pesan</label>
    <input type="text" id="message" placeholder="Pesan opsional" value="Test donasi" />

    <button class="submit" id="btn" onclick="sendTest()">🚀 Kirim Test Donasi</button>
    <div id="result"></div>
  </div>

  <script>
    function setAmount(val) {
      document.getElementById('amount').value = val;
      document.getElementById('currency').value = 'IDR';
    }

    async function sendTest() {
      const name     = document.getElementById('name').value.trim();
      const amount   = document.getElementById('amount').value;
      const currency = document.getElementById('currency').value;
      const message  = document.getElementById('message').value.trim();
      const result   = document.getElementById('result');
      const btn      = document.getElementById('btn');

      if (!name || !amount) {
        result.style.display = 'block';
        result.className = 'err';
        result.innerHTML = '<span class="badge err">ERROR</span><pre>Nama dan amount wajib diisi!</pre>';
        return;
      }

      btn.disabled = true;
      btn.textContent = '⏳ Mengirim...';
      result.style.display = 'block';
      result.className = '';
      result.innerHTML = '⏳ Mengirim donasi ke queue...';

      try {
        const res = await fetch('/api/webhook/sociabuzz', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name,
            donator:  name,
            amount:   parseInt(amount),
            currency: currency,
            message:  message
          })
        });
        const data = await res.json();
        result.className = 'ok';
        result.innerHTML = '<span class="badge ok">✅ BERHASIL</span><pre>' + JSON.stringify(data, null, 2) + '</pre>';
      } catch (err) {
        result.className = 'err';
        result.innerHTML = '<span class="badge err">❌ ERROR</span><pre>' + err.message + '</pre>';
      } finally {
        btn.disabled = false;
        btn.textContent = '🚀 Kirim Test Donasi';
      }
    }
  </script>
</body>
</html>`);
});

module.exports = app;
