/** @odoo-module **/

import { _t } from "@web/core/l10n/translation";
import { Component } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { ConfirmPopup } from "@point_of_sale/app/utils/confirm_popup/confirm_popup";

// 🧩 Import Custom Numpad Popup
import { CustomNumpadPopUp } from "./custom_numpad_popup";

export class VoidSalesButton extends Component {
    static template = "VoidSalesButton";
    static props = { label: { type: String, optional: true } };

    setup() {
        this.popup = useService("popup");
        this.pos = usePos();
        this.label = this.props.label || _t("Void Sales");
    }

    async onClick() {
        const order = this.pos.get_order();
        const config = this.pos.config;

        if (!order || order.is_empty()) {
            await this.popup.add(ConfirmPopup, {
                title: _t("No Items"),
                body: _t("There are no items to void."),
            });
            return;
        }

        // 🔐 Step 1: Check Manager Validation
        if (config.manager_validation && config.validate_void_sales) {
            const { confirmed } = await this.popup.add(CustomNumpadPopUp, {
                title: _t("Manager Approval Required"),
                body: _t("Please enter the manager PIN to proceed."),
            });

            if (!confirmed) {
                return; // 🛑 Abort if manager rejected
            }
        }

        // 🔔 Step 2: Confirmation to void
        const { confirmed } = await this.popup.add(ConfirmPopup, {
            title: _t("Confirm Void"),
            body: _t("Are you sure you want to void all items in this order?"),
            confirmText: _t("Yes"),
            cancelText: _t("Cancel"),
        });

        if (confirmed) {
            order.orderlines.reset(); // 💥 Execute VOID
        }
    }
}

// 🧬 Inject into ProductScreen
ProductScreen.addControlButton({
    component: VoidSalesButton,
    condition: () => true,
    position: ['before', 'SetPriceButton'],
});