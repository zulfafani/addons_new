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
        // Get config settings to check if manager validation is required
        const configSettings = this.pos.config;
        const managerValidation = configSettings.manager_validation;
        const validateEndShift = configSettings.validate_refund;
        
        let confirmed = true; // Default to true if no validation needed
        
        if (managerValidation && validateEndShift) {
            // Only show the popup if manager validation is required
            const result = await this.popup.add(CustomNumpadPopUp, {
                title: "Enter Manager PIN",
                body: "Please enter the manager's PIN to proceed with the refund.",
            });
            confirmed = result.confirmed;
        }

        // If PIN is correct (or no validation required), continue to TicketScreen
        if (confirmed) {
            const order = this.pos.get_order();
            
            // Mark as refund order to prevent default customer override
            order.is_refund_order = true;
            
            const partner = order.get_partner();
            const searchDetails = partner ? { fieldName: "PARTNER", searchTerm: partner.name } : {};

            this.pos.showScreen("TicketScreen", {
                ui: { filter: "SYNCED", searchDetails },
                destinationOrder: order,
                // Add this flag to indicate we're creating a refund
                isRefund: true
            });
        }
    },
});