/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { InvoiceButton } from "@point_of_sale/app/screens/ticket_screen/invoice_button/invoice_button";
import { useService } from "@web/core/utils/hooks";
import { CustomNumpadPopUp } from "./custom_numpad_popup";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { _t } from "@web/core/l10n/translation";

patch(InvoiceButton.prototype, {
    setup() {
        super.setup(); // 1. Pastikan memanggil setup parent
        this.popup = useService("popup");
        this.orm = useService("orm");
        this.report = useService("report");
    },

    async _downloadInvoice(orderId) {
        try {
            // 2. Tampilkan popup input PIN/invoice number
            const { confirmed, payload } = await this.popup.add(CustomNumpadPopUp, {
                title: _t("Enter Invoice Number"),
                body: _t("Please input the invoice number before proceeding."),
            });

            // 4. Dapatkan data invoice dari server
            const [order] = await this.orm.read(
                "pos.order",
                [orderId],
                ["account_move"],
                { load: false }
            );

            // 5. Validasi invoice exists
            if (!order?.account_move) {
                throw new Error(_t("Invoice not found"));
            }

            // 6. Download invoice
            await this.report.doAction("account.account_invoices", [order.account_move]);
            
        } catch (error) {
            this.popup.add(ErrorPopup, {
                title: _t("Error"),
                body: error.message || _t("Failed to download invoice"),
            });
        }
    }
});