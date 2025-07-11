/** @odoo-module **/

import { RefundButton } from "@point_of_sale/app/screens/product_screen/control_buttons/refund_button/refund_button";
import { patch } from "@web/core/utils/patch";
import { useService } from "@web/core/utils/hooks";
import { CustomNumpadPopUp } from "./custom_numpad_popup";

patch(RefundButton.prototype, {
    setup() {
        this.popup = useService("popup");
        this.pos = useService("pos");
        this.rpc = useService("rpc");
    },

    async click() {
        const configSettings = this.pos.config;
        const managerValidation = configSettings.manager_validation;
        const validateEndShift = configSettings.validate_refund;

        let confirmed = true;
        if (managerValidation && validateEndShift) {
            const result = await this.popup.add(CustomNumpadPopUp, {
                title: "Enter Manager PIN",
                body: "Please enter the manager's PIN to proceed with the refund.",
            });
            confirmed = result.confirmed;
        }

        if (confirmed) {
            const order = this.pos.get_order();

            // 🔑 Tandai alur refund di level POS agar order baru tidak diisi default partner
            this.pos.in_refund_flow = true;

            // Tandai order aktif sebagai refund (untuk berjaga-jaga)
            order.is_refund_order = true;

            const partner = order.get_partner();
            const searchDetails = partner ? { fieldName: "PARTNER", searchTerm: partner.name } : {};

            this.pos.showScreen("TicketScreen", {
                ui: { filter: "SYNCED", searchDetails },
                destinationOrder: order,
                isRefund: true,
            });
        }
    },
});
