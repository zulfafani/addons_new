/** @odoo-module **/

import { SelectionPopup } from "@point_of_sale/app/utils/input_popups/selection_popup";
import { patch } from "@web/core/utils/patch";
import { useService } from "@web/core/utils/hooks";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { _t } from "@web/core/l10n/translation";

const originalSetup = SelectionPopup.prototype.setup;
const originalSelectItem = SelectionPopup.prototype.selectItem;

patch(SelectionPopup.prototype, {
    setup() {
        originalSetup.call(this);
        this.rpc = useService("rpc");
        this.orm = useService("orm");
        this.pos = usePos();
    },

    async selectItem(itemId) {
        const selectedItem = this.props.list.find((item) => item.id === itemId);
        if (selectedItem && selectedItem.item && selectedItem.item.id) {
            try {
                const sessionId = this.pos.pos_session ? this.pos.pos_session.id : null;
                if (!sessionId) {
                    console.warn("POS session ID is not available");
                    alert(_t("Sesi POS tidak tersedia. Silakan buka sesi terlebih dahulu."));
                    return;
                }

                const response = await this.rpc("/pos/log_cashier", {
                    employee_id: selectedItem.item.id,
                    session_id: sessionId,
                });

                if (response.success) {
                    if (response.is_new_log) {
                        console.log("New cashier log created with ID:", response.log_id);
                    } else {
                        console.log("Existing cashier log found with ID:", response.log_id);
                    }
                    
                    if (response.end_shift_created) {
                        console.log("End shift record created successfully with ID:", response.end_shift_id);
                    } else {
                        console.log("Existing end shift record found with ID:", response.end_shift_id);
                    }
                    
                    return await originalSelectItem.call(this, itemId);
                } else if (response.error === 'cashier_shift_closed') {
                    alert(_t("Tidak dapat login. Shift untuk kasir ini belum ditutup."));
                    return;
                } else {
                    console.error("Failed to log cashier:", response.error);
                    alert(_t(`Gagal memilih kasir: ${response.error || 'Kesalahan tidak diketahui'}`));
                    return;
                }
            } catch (error) {
                console.error("Error logging cashier:", error);
                let errorMessage = _t("Terjadi kesalahan saat memilih kasir.");
                if (error.message) {
                    errorMessage += _t(` Detail: ${error.message}`);
                }
                if (error.data && error.data.message) {
                    errorMessage += _t(` Server: ${error.data.message}`);
                }
                alert(errorMessage);
                return;
            }
        }

        return await originalSelectItem.call(this, itemId);
    },
});