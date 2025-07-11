/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { InvoiceButton } from "@point_of_sale/app/screens/ticket_screen/invoice_button/invoice_button";
import { ReprintReceiptButton } from "@point_of_sale/app/screens/ticket_screen/reprint_receipt_button/reprint_receipt_button";
import { useService } from "@web/core/utils/hooks";
import { CustomNumpadPopUp } from "./custom_numpad_popup";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { _t } from "@web/core/l10n/translation";
import { OrderReceipt } from "@point_of_sale/app/screens/receipt_screen/receipt/order_receipt";

// 🔁 PATCH UNTUK INVOICE BUTTON
patch(InvoiceButton.prototype, {
    setup() {
        super.setup();
        this.popup = useService("popup");
        this.orm = useService("orm");
        this.report = useService("report");
    },

    async _downloadInvoice(orderId) {
        try {
            const { confirmed } = await this.popup.add(CustomNumpadPopUp, {
                title: _t("Enter Invoice Number"),
                body: _t("Please input the invoice number before proceeding."),
            });

            if (!confirmed) return;

            const [order] = await this.orm.read(
                "pos.order",
                [orderId],
                ["account_move"],
                { load: false }
            );

            if (!order?.account_move) {
                throw new Error(_t("Invoice not found"));
            }

            await this.report.doAction("account.account_invoices", [order.account_move]);

        } catch (error) {
            this.popup.add(ErrorPopup, {
                title: _t("Error"),
                body: error.message || _t("Failed to download invoice"),
            });
        }
    }
});

// 🔁 PATCH UNTUK REPRINT RECEIPT BUTTON
patch(ReprintReceiptButton.prototype, {
    setup() {
        super.setup();
        this.popup = useService("popup");
    },

    async click() {
        if (!this.props.order) return;

        const config = this.pos.config || {};
        const requireManagerValidation = config.validate_reprint_receipt;

        if (requireManagerValidation) {
            const result = await this.popup.add(CustomNumpadPopUp, {
                title: _t("Manager Authorization Required"),
                body: _t("Please enter manager PIN to reprint this receipt."),
                noteRequired: true,
            });

            if (!result || !result.confirmed) {
                await this.popup.add(ErrorPopup, {
                    title: _t("Access Denied"),
                    body: _t("You are not authorized to reprint the receipt."),
                });
                return;
            }
        }

        const printed = await this.printer.print(OrderReceipt, {
            data: this.props.order.export_for_printing(),
            formatCurrency: this.env.utils.formatCurrency,
        });

        if (!printed) {
            this.pos.showScreen("ReprintReceiptScreen", {
                order: this.props.order,
            });
        }
    }
});
