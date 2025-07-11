/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { OrderReceipt } from "@point_of_sale/app/screens/receipt_screen/receipt/order_receipt";

patch(OrderReceipt.prototype, {
    get orderHtml() {
        const order = this.props.data;
        const lines = order.orderlines || [];

        let body = "";

        lines.forEach((line, idx) => {
            const number = String(idx + 1).padStart(2, '0');
            const price = this.formatCurrency(line.price);
            const total = this.formatCurrency(line.price * line.quantity);
            const qtyPrice = `${line.quantity} x ${price}`;
            const product = line.product_name;

            body += `
                <div>${number}. ${product}</div>
                <div style="padding-left: 10px;">${qtyPrice.padEnd(20)} ${total}</div>
            `;
        });

        return `
            <div class="pos-receipt pos-receipt-center-align">
                <div><strong>${order.company.name}</strong></div>
                <div>${order.company.street}</div>
                <div>NPWP: ${order.company.npwp || "-"}</div>
                <div>SALES INVOICE</div>
                <div>Copy Receipt (1)</div>
                <hr>
                <div>Transaction No: ${order.name}</div>
                <div>Cashier: ${order.cashier || "-"}</div>
                <div>Transaction Date: ${order.date.local_str}</div>
                <div>Due Date: ${order.date_due || order.date.local_str}</div>
                <div>Print Date: ${order.date.local_str}</div>
                <hr>
                <div class="orderlines">${body}</div>
                <hr>
                <div>Total</div>
                <div class="d-flex justify-between"><strong>${this.formatCurrency(order.total_with_tax)}</strong></div>
                <hr>
                <div>Payment</div>
                ${order.paymentlines.map(p => `
                    <div>${p.payment_method.name} ${p.card_number || ""}</div>
                    <div style="text-align:right;">${this.formatCurrency(p.amount)}</div>
                `).join("")}
                <hr>
                <div>Change: ${this.formatCurrency(order.change || 0)}</div>
                <hr>
                <div>DPP: ${this.formatCurrency(order.total_without_tax)}</div>
                <div>PPN: ${this.formatCurrency(order.tax)}</div>
                <hr>
                <div>${order.total_items} Item ${order.total_quantity} Pcs</div>
                ${order.customer ? `
                    <div>${order.customer.id} - ${order.customer.name}</div>
                    <div>Point: ${order.customer.recent_points}</div>
                    <div>Balance Point: ${order.customer.total_points}</div>
                    <div>Expired: ${order.customer.expired_date || "-"}</div>
                ` : ""}
                <hr>
                <div>${order.company.name}</div>
                <div>NPWP: ${order.company.npwp || "-"}</div>
                <div>${order.company.street}</div>
            </div>
        `;
    },

    formatCurrency(val) {
        return this.env.utils.formatCurrency(val || 0);
    },
});
