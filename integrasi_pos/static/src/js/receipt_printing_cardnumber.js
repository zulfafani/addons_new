/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { Order } from "@point_of_sale/app/store/models";

patch(Order.prototype, {
    export_for_printing() {
        const result = super.export_for_printing();

        console.log("=== DEBUG export_for_printing CARD ===");
        console.log("Original paymentlines:", result.paymentlines);
        console.log("Order paymentlines:", this.paymentlines);

        // Add card numbers to payment lines for receipt display
        if (result.paymentlines && Array.isArray(result.paymentlines)) {
            result.paymentlines = result.paymentlines.map((paymentLine, index) => {
                // Find the corresponding payment line from the order
                const orderPaymentLine = this.paymentlines.find(line => 
                    line.payment_method?.id === paymentLine.payment_method_id ||
                    line.payment_method?.id === paymentLine.payment_method?.id
                ) || this.paymentlines[index]; // Fallback to index matching
                
                const updatedLine = {
                    ...paymentLine,
                    card_number: orderPaymentLine?.card_number || null
                };

                console.log(`Payment line ${index}:`, {
                    original: paymentLine,
                    orderLine: orderPaymentLine,
                    updated: updatedLine
                });

                return updatedLine;
            });
        }

        // Also add global card number for backup
        const cardNumbers = this.paymentlines
            .filter((line) => {
                const method = this.pos.payment_methods.find(pm => pm.id === line.payment_method?.id);
                return method && method.type !== "cash" && line.card_number;
            })
            .map((line) => line.card_number);

        if (cardNumbers.length > 0) {
            result.nomor_kartu = cardNumbers[0];
        }

        console.log("Final result paymentlines:", result.paymentlines);
        console.log("=== END DEBUG CARD ===");

        return result;
    },

    export_as_JSON() {
        const result = super.export_as_JSON();

        const cardNumbers = this.paymentlines
            .filter((line) => {
                const method = this.pos.payment_methods.find(pm => pm.id === line.payment_method?.id);
                return method && method.type !== "cash" && line.card_number;
            })
            .map((line) => line.card_number);

        // Optional: Only take the first card number if multiple present
        if (cardNumbers.length > 0) {
            result.nomor_kartu = cardNumbers[0];
        }

        return result;
    }
});