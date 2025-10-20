const express = require("express");
const fs = require("fs");
const { exec } = require("child_process");
const cors = require("cors");
const path = require("path");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const webServer = http.createServer(app);
const wss = new WebSocket.Server({ server: webServer });

const PORT_WEB = 3001;
const PORT_WS = 8765;

const receiptFolder = "C:\\kasir_print";
const receiptHtmlPath = path.join(receiptFolder, "receipt.html");
const receiptPdfPath = path.join(receiptFolder, "receipt.pdf");
const sumatraPath = `\"C:\\Users\\abhif\\AppData\\Local\\SumatraPDF\\SumatraPDF.exe\"`;
const printerName = "Star BSC10";
const printerShareName = "\\\\localhost\\Star BSC10";
const drawerCommand = Buffer.from([27, 112, 0, 25, 250]);
const clientHtmlPath = path.join(__dirname, "client.html");

// ✅ LAZY LOAD Puppeteer - hanya load jika dibutuhkan
let puppeteer = null;
let browserInstance = null;

async function getBrowser() {
    if (!puppeteer) {
        puppeteer = require("puppeteer");
    }
    
    // ✅ REUSE browser instance untuk performa lebih cepat
    if (!browserInstance || !browserInstance.isConnected()) {
        console.log("🚀 Launching new browser instance...");
        browserInstance = await puppeteer.launch({ 
            headless: "new",
            args: ['--no-sandbox', '--disable-setuid-sandbox']
        });
    }
    
    return browserInstance;
}

app.use(cors());
app.use(express.json({ limit: "2mb" }));

// Pastikan folder tersedia
if (!fs.existsSync(receiptFolder)) {
    fs.mkdirSync(receiptFolder);
    console.log("📁 Folder dibuat:", receiptFolder);
}

// Endpoint test
app.get("/test", (req, res) => {
    res.send("🧪 Server printer aktif dan siap menerima print.");
});

// ✅ ENDPOINT CETAK - OPTIMIZED VERSION
app.post("/print", async (req, res) => {
    const startTime = Date.now();
    console.log("\n🖨️ ========== PRINT REQUEST RECEIVED ==========");
    
    const html = typeof req.body.html === "string" ? req.body.html : "";
    
    if (!html) {
        console.error("❌ Data html kosong");
        return res.status(400).send("❌ Data html harus berupa string.");
    }

    // ✅ IMMEDIATELY respond to client - don't make them wait!
    res.status(202).send("🖨️ Print job queued");
    
    // ✅ Process print in background (non-blocking)
    (async () => {
        try {
            console.log("💾 Writing HTML to file...");
            fs.writeFileSync(receiptHtmlPath, html, "utf-8");
            
            console.log("🚀 Generating PDF...");
            const browser = await getBrowser();
            const page = await browser.newPage();
            
            // ✅ Optimize page load - skip unnecessary resources
            await page.setRequestInterception(true);
            page.on('request', (req) => {
                if (['image', 'stylesheet', 'font'].includes(req.resourceType())) {
                    req.abort();
                } else {
                    req.continue();
                }
            });
            
            await page.setContent(html, { waitUntil: "domcontentloaded" }); // ✅ Faster than networkidle0
            
            await page.pdf({ 
                path: receiptPdfPath, 
                width: "72mm", 
                printBackground: true,
                preferCSSPageSize: true
            });
            
            await page.close(); // ✅ Close page, keep browser open
            
            const pdfTime = Date.now() - startTime;
            console.log(`✅ PDF generated in ${pdfTime}ms`);

            // ✅ Print command
            const printCommand = `${sumatraPath} -print-to \"${printerName}\" -silent \"${receiptPdfPath}\"`;
            console.log("🖨️ Sending to printer...");
            
            exec(printCommand, (error, stdout, stderr) => {
                const totalTime = Date.now() - startTime;
                if (error) {
                    console.error("❌ Print error:", error.message);
                } else {
                    console.log(`✅ Print completed in ${totalTime}ms total`);
                }
            });
            
        } catch (err) {
            console.error("❌ Print error:", err.message);
        }
    })();
});

// ✅ ALTERNATIVE: Direct HTML print (jika printer support HTML)
app.post("/print-direct", (req, res) => {
    const html = typeof req.body.html === "string" ? req.body.html : "";
    
    if (!html) {
        return res.status(400).send("❌ Data html harus berupa string.");
    }

    // ✅ Immediately respond
    res.status(202).send("🖨️ Print job queued");
    
    // ✅ Background processing
    const tempHtmlPath = path.join(receiptFolder, `receipt_${Date.now()}.html`);
    fs.writeFileSync(tempHtmlPath, html, "utf-8");
    
    // ✅ Print HTML directly (faster, no PDF conversion)
    const printCommand = `${sumatraPath} -print-to \"${printerName}\" -silent \"${tempHtmlPath}\"`;
    
    exec(printCommand, (error) => {
        if (error) {
            console.error("❌ Print error:", error.message);
        } else {
            console.log("✅ Direct HTML print completed");
        }
        // Cleanup temp file
        setTimeout(() => fs.unlinkSync(tempHtmlPath), 5000);
    });
});

// Endpoint buka drawer
app.post("/open-drawer", (req, res) => {
    console.log("🚪 Open drawer request received");
    const filePath = path.join(receiptFolder, "drawer_test.bin");
    try {
        fs.writeFileSync(filePath, drawerCommand);
        exec(`copy /B \"${filePath}\" \"${printerShareName}\"`, (err) => {
            if (err) {
                console.error("❌ Gagal COPY drawer:", err.message);
                return res.status(500).send("Gagal membuka drawer");
            }
            console.log("✅ Drawer terbuka");
            res.send("Drawer opened successfully");
        });
    } catch (e) {
        console.error("❌ Error:", e.message);
        res.status(500).send("Internal error");
    }
});

// ✅ NEW: Clear pole display endpoint
app.post("/clear-pole", (req, res) => {
    console.log("🧹 Clear pole display request");
    // Broadcast clear command via WebSocket
    const payload = JSON.stringify({ line1: '', line2: '', clear: true });
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    });
    res.send("Pole display cleared");
});

// Serve client HTML
app.get("/", (req, res) => {
    fs.readFile(clientHtmlPath, (err, content) => {
        if (err) return res.status(404).send("Not found");
        res.writeHead(200, { "Content-Type": "text/html" });
        res.end(content);
    });
});

// WebSocket Broadcast
wss.on("connection", (ws) => {
    console.log("🔌 WebSocket connected");
    ws.on("message", (message) => {
        const lines = message.toString().split("\n");
        const payload = JSON.stringify({ line1: lines[0] || '', line2: lines[1] || '' });
        wss.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) client.send(payload);
        });
    });
});

// ✅ Cleanup browser on exit
process.on('SIGINT', async () => {
    console.log('🔒 Shutting down...');
    if (browserInstance) {
        await browserInstance.close();
    }
    process.exit(0);
});

// Start Server
webServer.listen(PORT_WS, () => {
    console.log(`🌐 Web & WebSocket server di http://localhost:${PORT_WS}`);
});

app.listen(PORT_WEB, () => {
    console.log(`🚀 Printer server aktif di http://localhost:${PORT_WEB}`);
});