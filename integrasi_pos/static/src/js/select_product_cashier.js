/** @odoo-module **/

import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";
import { patch } from "@web/core/utils/patch";
import { useService } from "@web/core/utils/hooks";
import { _t } from "@web/core/l10n/translation";

patch(PaymentScreen.prototype, {
    setup() {
        super.setup();
        this.orm = useService("orm");
    },

    async validateOrder(isForceValidate) {
        const isShiftClosed = await this.isEndShiftClosed();
        if (isShiftClosed) {
            this.showAlertAndReload(_t("Cannot validate order. The shift is closed for the current cashier."));
            return;
        }
        return super.validateOrder(isForceValidate);
    },

    async isEndShiftClosed() {
        const currentSession = this.pos.pos_session.id;
        const currentCashier = this.pos.get_cashier();
        if (!currentCashier) {
            return false;
        }
        const endShift = await this.orm.searchRead(
            'end.shift',
            [
                ['session_id', '=', currentSession],
                ['cashier_id', '=', currentCashier.id],
                ['state', '=', 'closed']
            ],
            ['id'],
            { limit: 1 }
        );
        return endShift.length > 0;
    },

    showAlertAndReload(message) {
        alert(message);
        setTimeout(() => {
            window.location.reload();
        }, 500);
    }
});