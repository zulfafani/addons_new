/** @odoo-module **/
import { patch } from "@web/core/utils/patch";
import { Order } from "@point_of_sale/app/store/models";

// One global WebSocket instance
let displaySocket;
if (!displaySocket || displaySocket.readyState !== WebSocket.OPEN) {
    displaySocket = new WebSocket("ws://localhost:8765");
}

function sendToPole(line1, line2) {
    const safe1 = (line1 || "").substring(0, 20);
    const safe2 = (line2 || "").substring(0, 20);
    if (displaySocket.readyState === WebSocket.OPEN) {
        displaySocket.send(`${safe1}\n${safe2}`);
    } else {
        console.warn("Pole display WebSocket not open.");
    }
}

patch(Order.prototype, {
    add_product(product, options) {
        const result = super.add_product(product, options);
        this._sendToPoleDisplay();
        return result;
    },
    remove_orderline(orderline) {
        const result = super.remove_orderline(orderline);
        this._sendToPoleDisplay();
        return result;
    },
    set_quantity(quantity) {
        const result = super.set_quantity(quantity);
        this._sendToPoleDisplay();
        return result;
    },
    _sendToPoleDisplay() {
        const orderlines = this.get_orderlines();
        const lastLine = orderlines.length
            ? orderlines[orderlines.length - 1].product.display_name
            : "Next Customer";
        
        // Format total dengan pemisah ribuan
        const totalAmount = this.get_total_with_tax();
        const formattedTotal = new Intl.NumberFormat('id-ID', {
            minimumFractionDigits: 0,
            maximumFractionDigits: 0
        }).format(totalAmount);
        
        // Line 1: Nama produk rata kiri (maksimal 20 karakter)
        const line1 = lastLine.padEnd(20).substring(0, 20);
        
        // Line 2: Total rata kanan (maksimal 20 karakter)
        const totalText = `Total Rp.${formattedTotal}`;
        const line2 = totalText.padStart(20).substring(0, 20);
        
        sendToPole(line1, line2);
    }
});