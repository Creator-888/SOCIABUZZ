// ============================================================
// DONATION BRIDGE — Vercel API
// api/index.js — FINAL v8 (Queue Support, anti double donate)
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
  res.send("DONATION BRIDGE AKTIF v8 - Redis Queue");
});

app.get('/api/donations/queue', async (req, res) => {
  try {
    const since = Number(req.query.since || 0);
    const all   = await peekQueue();
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
    const amount    = parseInt(String(rawAmount).replace(/\D/g, "")) || 0;

    console.log("💰 Amount:", amount);

    if (amount <= 0) {
      return res.status(200).json({ status: "SKIP_ZERO" });
    }

    const donationId = (
      d.id             ||
      d.order_id       ||
      d.transaction_id ||
      d.invoice_id     ||
      d.ref_id         ||
      `${rawName.toLowerCase().replace(/\s/g,"_")}_${amount}_${Date.now()}`
    ).toString();

    const donation = {
      id:        donationId,
      donator:   rawName,
      amount:    amount,
      message:   (d.message || d.note || d.description || "").toString().trim(),
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
    input { width: 100%; padding: 11px 14px; background: #0f3460; border: 1px solid #e9456033; border-radius: 8px; color: #fff; font-size: 15px; outline: none; transition: border 0.2s; }
    input:focus { border-color: #e94560; }
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
  </style>
</head>
<body>
  <div class="card">
    <h2>🧪 Test Inject Donation</h2>

    <label>Nama Donatur</label>
    <input type="text" id="name" placeholder="Contoh: BayuGaming" value="TestUser" />

    <label>Amount (IDR)</label>
    <input type="number" id="amount" placeholder="Contoh: 50000" value="10000" />

    <label>Preset Tier</label>
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
    }

    async function sendTest() {
      const name    = document.getElementById('name').value.trim();
      const amount  = document.getElementById('amount').value;
      const message = document.getElementById('message').value.trim();
      const result  = document.getElementById('result');
      const btn     = document.getElementById('btn');

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
        const res = await fetch('/api/test/inject', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name, amount: parseInt(amount), message })
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
