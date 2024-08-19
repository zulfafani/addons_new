/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { Order } from "@point_of_sale/app/store/models";

const originalExportForPrinting = Order.prototype.export_for_printing;

patch(Order.prototype, {
    export_for_printing() {
        const result = originalExportForPrinting.call(this);

        if (Array.isArray(result.paymentlines)) {
            result.paymentlines = result.paymentlines.map((line, index) => {
                const paymentLine = this.paymentlines[index];
                const isCash = this.pos.payment_methods.find(
                    (pm) => pm.id === line.payment_method_id
                )?.type === "cash";

                if (!isCash && paymentLine?.card_number) {
                    line.name = `${line.name} (****${paymentLine.card_number})`;
                }
                return line;
            });
        }

        return result;
    },
});
