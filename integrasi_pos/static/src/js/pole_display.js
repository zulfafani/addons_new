/** @odoo-module **/
import { patch } from "@web/core/utils/patch";
import { Order, Orderline } from "@point_of_sale/app/store/models";

// One global WebSocket instance
let displaySocket;
if (!displaySocket || displaySocket.readyState !== WebSocket.OPEN) {
    displaySocket = new WebSocket("ws://localhost:8765");
}

displaySocket.addEventListener('open', () => {
    console.log("✅ Pole Display WebSocket connected");
});

displaySocket.addEventListener('error', (error) => {
    console.error("❌ Pole Display WebSocket error:", error);
});

displaySocket.addEventListener('close', () => {
    console.warn("🔌 Pole Display WebSocket closed");
});

function sendToPole(line1, line2) {
    const safe1 = (line1 || "").substring(0, 20);
    const safe2 = (line2 || "").substring(0, 20);
    if (displaySocket.readyState === WebSocket.OPEN) {
        displaySocket.send(`${safe1}\n${safe2}`);
        console.log(`📤 Sent to pole: Line1="${safe1}" | Line2="${safe2}"`);
    } else {
        console.warn("⚠️ Pole display WebSocket not open. Attempting reconnect...");
        if (displaySocket.readyState === WebSocket.CLOSED) {
            displaySocket = new WebSocket("ws://localhost:8765");
        }
    }
}

// Track produk yang baru ditambahkan
patch(Order.prototype, {
    setup() {
        super.setup(...arguments);
        this._lastAddedProductName = null;
    },

    add_product(product, options) {
        // Simpan nama produk yang baru ditambahkan
        this._lastAddedProductName = product.display_name || product.name;
        
        const result = super.add_product(product, options);
        
        // Kirim ke pole display setelah produk ditambahkan
        this._sendNewProductToPole(product, options?.quantity || 1);
        
        return result;
    },

    _sendNewProductToPole(product, quantity) {
        // Format total dengan pemisah ribuan
        const totalAmount = this.get_total_with_tax();
        const formattedTotal = new Intl.NumberFormat('id-ID', {
            minimumFractionDigits: 0,
            maximumFractionDigits: 0
        }).format(totalAmount);
        
        // Line 1: Nama produk yang baru ditambahkan (rata kiri)
        const productName = product.display_name || product.name;
        const line1 = productName.padEnd(20).substring(0, 20);
        
        // Line 2: Total dengan Qty (rata kanan)
        let totalText;
        if (quantity > 1) {
            totalText = `${quantity}x Rp.${formattedTotal}`;
        } else {
            totalText = `Total Rp.${formattedTotal}`;
        }
        const line2 = totalText.padStart(20).substring(0, 20);
        
        sendToPole(line1, line2);
    }
});

// Patch Orderline untuk tracking perubahan quantity
patch(Orderline.prototype, {
    set_quantity(quantity, keep_price) {
        const oldQuantity = this.get_quantity();
        const result = super.set_quantity(quantity, keep_price);
        
        // Jika quantity berubah, update pole display
        if (oldQuantity !== quantity && this.order) {
            this.order._sendNewProductToPole(this.product, quantity);
        }
        
        return result;
    }
});