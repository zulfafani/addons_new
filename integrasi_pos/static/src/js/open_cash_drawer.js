/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";
import { useState } from "@odoo/owl";
import { OrderReceipt } from "@point_of_sale/app/screens/receipt_screen/receipt/order_receipt";
import { useService } from "@web/core/utils/hooks";

//
// ===============================
// 1️⃣ POPUP NUMERIC KEYBOARD
// ===============================
export class NumericKeyboardPopup extends AbstractAwaitablePopup {
    static template = "integrasi_pos.NumericKeyboardPopup";
    static defaultProps = {
        confirmText: "OK",
        cancelText: "Batal",
        title: "Input Angka",
        maxLength: 4,
        placeholder: "0000",
        body: ""
    };

    setup() {
        super.setup();
        this.state = useState({
            inputValue: "",
            error: ""
        });
    }

    onKeyPress(key) {
        if (key === "backspace") {
            this.state.inputValue = this.state.inputValue.slice(0, -1);
        } else if (key === "clear") {
            this.state.inputValue = "";
        } else if (this.state.inputValue.length < this.props.maxLength) {
            this.state.inputValue += key;
        }
        this.state.error = "";
    }

    getPayload() {
        if (this.state.inputValue.length !== this.props.maxLength) {
            this.state.error = `Input harus tepat ${this.props.maxLength} digit`;
            return null;
        }
        return this.state.inputValue;
    }

    confirm() {
        const payload = this.getPayload();
        if (payload) {
            super.confirm();
        }
    }
}

//
// ===============================
// 2️⃣ BUKA CASH DRAWER
// ===============================
async function triggerCashDrawer() {
    try {
        await fetch("http://localhost:3001/open-drawer", {
            method: "POST",
        });
        console.log("✅ Cash drawer opened");
    } catch (err) {
        console.error("❌ Gagal membuka drawer:", err);
    }
}

console.log("🔥 Patch PaymentScreen aktif");

//
// ===============================
// 4️⃣ PATCH PAYMENT SCREEN
// ===============================
patch(PaymentScreen.prototype, {
    setup() {
        super.setup();
        this._rendererService = useService("renderer"); // Ambil renderer
    },

    async _finalizeValidation() {
        if (this.currentOrder.is_paid_with_cash() || this.currentOrder.get_change()) {
            this.hardwareProxy.openCashbox();
        }

        this.currentOrder.date_order = luxon.DateTime.now();
        for (const line of this.paymentLines) {
            if (!line.amount === 0) {
                this.currentOrder.remove_paymentline(line);
            }
        }
        this.currentOrder.finalized = true;

        this.env.services.ui.block();
        let syncOrderResult;
        try {
            syncOrderResult = await this.pos.push_single_order(this.currentOrder);
            if (!syncOrderResult) return;

            if (this.shouldDownloadInvoice() && this.currentOrder.is_to_invoice()) {
                if (syncOrderResult[0]?.account_move) {
                    await this.report.doAction("account.account_invoices", [
                        syncOrderResult[0].account_move,
                    ]);
                } else {
                    throw {
                        code: 401,
                        message: "Backend Invoice",
                        data: { order: this.currentOrder },
                    };
                }
            }
        } catch (error) {
            if (error instanceof ConnectionLostError) {
                this.pos.showScreen(this.nextScreen);
                Promise.reject(error);
                return error;
            } else {
                throw error;
            }
        } finally {
            this.env.services.ui.unblock();
        }

        if (
            syncOrderResult &&
            syncOrderResult.length > 0 &&
            this.currentOrder.wait_for_push_order()
        ) {
            await this.postPushOrderResolve(syncOrderResult.map((res) => res.id));
        }

        await this.afterOrderValidation(!!syncOrderResult && syncOrderResult.length > 0);

        // ==== START: Print to Node.js ====
        // ==== START: Print to Node.js ====
        try {
            const htmlVNode = await this._rendererService.toHtml(OrderReceipt, {
                data: this.pos.get_order().export_for_printing(),
                formatCurrency: this.env.utils.formatCurrency,
            });

            const html = htmlVNode?.outerHTML || "";

            await fetch("http://localhost:3001/print", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ html }),
            });

            // ✅ Open drawer after successful print
            await triggerCashDrawer();
            console.log("✅ Drawer dibuka setelah print");
        } catch (error) {
            console.error("❌ Gagal kirim struk ke printer lokal:", error);
        }

        // ==== END: Print to Node.js ====

        this.pos.showScreen(this.nextScreen);
    },

    async addNewPaymentLine(paymentMethod) {
        const result = this.currentOrder.add_paymentline(paymentMethod);

        if (!this.pos.get_order().check_paymentlines_rounding()) {
            this._display_popup_error_paymentlines_rounding();
        }

        if (result) {
            this.numberBuffer.reset();

            if (paymentMethod.type === "cash") {
                console.log("[POS] 💵 Drawer dibuka (cash)");
            } else {
                const { confirmed, payload } = await this.popup.add(NumericKeyboardPopup, {
                    title: "Input 4 Digit Terakhir Kartu",
                    maxLength: 4,
                    placeholder: "Contoh: 1234",
                    body: "Masukkan 4 digit terakhir dari nomor kartu"
                });

                if (confirmed && payload) {
                    const paymentLine = this.currentOrder.selected_paymentline;
                    if (paymentLine) {
                        paymentLine.card_number = payload;
                    }
                    console.log("[POS] 💳 Kartu tersimpan:", payload);
                } else if (!confirmed) {
                    const paymentLine = this.currentOrder.selected_paymentline;
                    if (paymentLine) {
                        this.currentOrder.remove_paymentline(paymentLine);
                    }
                    return false;
                }
            }
            return true;
        } else {
            await this.popup.add(ErrorPopup, {
                title: "Error",
                body: "Sudah ada pembayaran elektronik dalam proses.",
            });
            return false;
        }
    },
});
