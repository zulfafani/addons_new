/** @odoo-module **/

import { SelectionPopup } from "@point_of_sale/app/utils/input_popups/selection_popup";
import { patch } from "@web/core/utils/patch";
import { useService } from "@web/core/utils/hooks";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { _t } from "@web/core/l10n/translation";
import { PopUpSuccesError } from "./pop_up_error";

const originalSetup = SelectionPopup.prototype.setup;
const originalSelectItem = SelectionPopup.prototype.selectItem;

patch(SelectionPopup.prototype, {
    setup() {
        originalSetup.call(this);
        this.rpc = useService("rpc");
        this.orm = useService("orm");
        this.pos = usePos();
        this.popup = useService("popup");
    },

    async selectItem(itemId) {
        const selectedItem = this.props.list.find((item) => item.id === itemId);
        if (selectedItem && selectedItem.item && selectedItem.item.id) {
            try {
                // **1️⃣ Cek apakah employee ini kasir (`is_cashier = True`) atau salesperson (`is_sales_person = True`)**
                const employeeData = await this.orm.searchRead(
                    "hr.employee",
                    [["id", "=", selectedItem.item.id]],
                    ["is_cashier", "is_sales_person", "is_pic"]
                );
    
                if (!employeeData.length) {
                    await this.showErrorPopup(
                        _t("Error: Data Tidak Ditemukan"),
                        _t("Karyawan yang dipilih tidak valid.")
                    );
                    return;
                }
    
                const { is_cashier, is_pic } = employeeData[0];
    
                // **2️⃣ Jika bukan kasir dan bukan salesperson, tampilkan error**
                if (!is_cashier && !is_pic) {
                    await this.showErrorPopup(
                        _t("Error: Bukan Kasir atau Salesperson"),
                        _t("Karyawan yang dipilih bukan kasir maupun salesperson. Silakan pilih yang valid.")
                    );
                    return;
                }
    
                // **3️⃣ Pastikan sesi POS tersedia sebelum melanjutkan**
                const sessionId = this.pos.pos_session ? this.pos.pos_session.id : null;
                if (!sessionId) {
                    await this.showErrorPopup(
                        _t("Error: Sesi POS"),
                        _t("Sesi POS tidak tersedia. Silakan buka sesi terlebih dahulu.")
                    );
                    return;
                }
    
                // **4️⃣ Jika kasir, lanjutkan ke log cashier**
                if (is_cashier) {
                    const response = await this.rpc("/pos/log_cashier", {
                        employee_id: selectedItem.item.id,
                        session_id: sessionId,
                    });
    
                    if (response.success) {
                        console.log("Cashier login successful:", response);
                        return await originalSelectItem.call(this, itemId);
                    } else {
                        const errorMessages = {
                            cashier_shift_closed: _t("Tidak dapat login. Shift untuk kasir ini sudah ditutup pada sesi ini."),
                            cashier_already_logged_in: _t("Kasir ini sudah login di sesi lain. Silakan logout terlebih dahulu."),
                            payment_disabled: _t("Kasir ini tidak memiliki akses untuk melakukan pembayaran."),
                            another_cashier_active: _t("Tidak dapat login. Kasir lain sudah aktif di sesi ini."),
                        };
                    
                        const errorMessage = errorMessages[response.error] || _t("Gagal memilih kasir.");
                        await this.showErrorPopup(_t("Error"), errorMessage);
                        return;
                    }
                }
    
                // **5️⃣ Jika salesperson, langsung lanjutkan tanpa error**
                return await originalSelectItem.call(this, itemId);
    
            } catch (error) {
                console.error("Error selecting item:", error);
                let errorMessage = _t("Terjadi kesalahan saat memilih karyawan.");
                if (error.message) errorMessage += _t(`\nDetail: ${error.message}`);
                if (error.data && error.data.message) errorMessage += _t(`\nServer: ${error.data.message}`);
    
                await this.showErrorPopup(_t("Error: Sistem"), errorMessage);
                return;
            }
        }
    
        return await originalSelectItem.call(this, itemId);
    },
    
    async showErrorPopup(title, message) {
        const { confirmed } = await this.popup.add(PopUpSuccesError, {
            title: title || _t("Error"),
            body: message,
            confirmText: _t("OK"),
        });
        
        if (confirmed) {
            return true;
        }
        return false;
    }
});