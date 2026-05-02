// ============================================================
// Sociabuzz → Roblox Bridge (Vercel Serverless)
// ============================================================

let latestDonation = {
    id: "START",
    donator: "System",
    amount: 0,
    message: "Ready",
    timestamp: 0
};

const BLOCKED_NAMES = ["anonymous", "anon", "system", "unknown", ""];

function isBlocked(name) {
    if (!name) return true;
    return BLOCKED_NAMES.includes(name.trim().toLowerCase());
}

async function parseBody(req) {
    return new Promise((resolve) => {
        let raw = "";
        req.on("data", chunk => raw += chunk);
        req.on("end", () => {
            try { 
                resolve(JSON.parse(raw)); 
            } catch (e) { 
                resolve({}); 
            }
        });
    });
}

module.exports = async function handler(req, res) {
    const url = req.url.split("?")[0];

    // CORS Headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST');

    try {
        // 1. Cek Status Server
        if (req.method === "GET" && (url === "/api" || url === "/api/index")) {
            res.writeHead(200, { "Content-Type": "text/plain" });
            return res.end("SERVER AKTIF");
        }

        // 2. Endpoint untuk Roblox (Polling)
        if (req.method === "GET" && url === "/api/donations/latest") {
            res.writeHead(200, { "Content-Type": "application/json" });
            return res.end(JSON.stringify(latestDonation));
        }

        // 3. Endpoint untuk Webhook Sociabuzz
        // URL di Dashboard Sociabuzz harus: https://sociabuzz-rust.vercel.app/api/webhook/sociabuzz
        if (req.method === "POST" && (url === "/api/webhook/sociabuzz" || url === "/api/webhook")) {
            const d = await parseBody(req);
            
            const rawName = d.donator_name || d.supporter_name || d.user_name || d.name || "";
            const finalName = rawName.trim();

            if (isBlocked(finalName)) {
                res.writeHead(200, { "Content-Type": "text/plain" });
                return res.end("OK_SKIPPED");
            }

            latestDonation = {
                id: d.order_id || d.transaction_id || Date.now().toString(),
                donator: finalName,
                amount: parseInt(d.amount_raw || d.amount || 0),
                message: d.message || d.note || "",
                timestamp: Math.floor(Date.now() / 1000)
            };

            console.log(`[SOCIABUZZ] Donasi Baru: ${finalName} - Rp${latestDonation.amount}`);
            res.writeHead(200, { "Content-Type": "text/plain" });
            return res.end("OK");
        }
    } catch (err) {
        console.error("Internal Error:", err);
        res.writeHead(500);
        res.end("Internal Server Error");
    }

    res.writeHead(404);
    res.end("NOT FOUND");
};