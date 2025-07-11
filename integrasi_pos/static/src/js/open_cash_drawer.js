/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";
import { useState } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { ConnectionLostError } from "@web/core/network/rpc_service";

function formatDisplayLine(label, value) {
    const totalWidth = 20;
    const left = label.padEnd(12, " ");
    const right = value.toString().padStart(totalWidth - left.length, " ");
    return left + right;
}

async function triggerCashDrawer() {
    try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 2000); // 2 detik timeout
        
        const response = await fetch("http://localhost:3001/open-drawer", { 
            method: "POST",
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        if (response.ok) {
            console.log("✅ Cash drawer opened");
            return true;
        } else {
            console.warn("⚠️ Drawer service responded with error:", response.status);
            return false;
        }
    } catch (err) {
        if (err.name === 'AbortError') {
            console.warn("⚠️ Drawer service timeout (service mungkin tidak berjalan)");
        } else {
            console.warn("⚠️ Drawer service tidak tersedia:", err.message);
        }
        return false;
    }
}

export class NumericKeyboardPopup extends AbstractAwaitablePopup {
    static template = "integrasi_pos.NumericKeyboardPopup";
    static defaultProps = {
        confirmText: "OK",
        cancelText: "Batal",
        title: "Input Angka",
        placeholder: "Contoh: 1.2.3.4",
        body: ""
    };

    setup() {
        super.setup();
        this.state = useState({ inputValue: "", error: "" });
    }

    onKeyPress(key) {
        if (key === "clear") {
            this.state.inputValue = "";
        } else {
            this.state.inputValue += key;
        }
        this.state.error = "";
    }

    getPayload() {
        if (!this.state.inputValue.trim()) {
            this.state.error = "Input tidak boleh kosong";
            return null;
        }
        return this.state.inputValue;
    }

    confirm() {
        const payload = this.getPayload();
        if (payload) super.confirm();
    }
}

patch(PaymentScreen.prototype, {
    setup() {
        super.setup();
        this._rendererService = useService("renderer");
        this.orm = useService("orm");
        this.popup = useService("popup");
    },

    async _finalizeValidation() {
        // === CEK IS_PIC EMPLOYEE ===
        const cashierId = this.pos.get_cashier()?.id;
        if (cashierId) {
            try {
                const employeeData = await this.orm.searchRead(
                    "hr.employee",
                    [["id", "=", cashierId]],
                    ["is_pic"]
                );
                if (employeeData?.[0]?.is_pic) {
                    await this.popup.add(ErrorPopup, {
                        title: "Akses Ditolak",
                        body: "Anda tidak dapat memvalidasi karena status Anda adalah PIC.",
                    });
                    return;
                }
            } catch (error) {
                console.error("Error checking employee PIC status:", error);
            }
        }

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
        let syncSuccess = false;
        
        try {
            syncOrderResult = await this.pos.push_single_order(this.currentOrder);
            if (!syncOrderResult) {
                this.env.services.ui.unblock();
                return;
            }

            syncSuccess = true;

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
            this.env.services.ui.unblock();
            
            if (error instanceof ConnectionLostError) {
                this.pos.showScreen(this.nextScreen);
                return Promise.reject(error);
            } else {
                throw error;
            }
        } finally {
            this.env.services.ui.unblock();
        }

        if (syncOrderResult && syncOrderResult.length > 0 && this.currentOrder.wait_for_push_order()) {
            await this.postPushOrderResolve(syncOrderResult.map((res) => res.id));
        }

        await this.afterOrderValidation(!!syncOrderResult && syncOrderResult.length > 0);

        // === Trigger cash drawer hanya kalau sync berhasil ===
        if (syncSuccess) {
            await triggerCashDrawer();
        }

        // === Send data to Pole Display ===
        if (syncSuccess) {
            try {
                const total = this.currentOrder.get_total_with_tax().toFixed(4);
                const change = this.currentOrder.get_change().toFixed(4);
                const line1 = formatDisplayLine("Total", total);
                const line2 = formatDisplayLine("Change", change);

                const ws = new WebSocket("ws://localhost:8765");
                ws.onerror = () => {
                    console.warn("⚠️ Pole display tidak tersedia");
                };
                ws.onopen = () => {
                    ws.send(`${line1}\n${line2}`);
                    setTimeout(() => ws.close(), 1000);
                };
            } catch (err) {
                console.warn("⚠️ Pole display service tidak tersedia");
            }
        }

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
                await triggerCashDrawer();
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