/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { ClosePosPopup } from "@point_of_sale/app/popup/close_pos_popup/close_pos_popup";

patch(ClosePosPopup.prototype, {
    // Override canConfirm to ensure "Expected" and "Difference" logic doesn't block session close
    canConfirm() {
        // Tetap gunakan validasi default untuk input float yang valid
        return Object.values(this.state.payments)
            .map((v) => v.counted)
            .every(this.env.utils.isValidFloat);
    },
});
