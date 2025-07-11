/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { _t } from "@web/core/l10n/translation";
import { Component } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { CustomNumpadPopUp } from "./custom_numpad_popup";

export class CashDrawerButton extends Component {
    static template = "CashDrawerButton";
    static props = { label: { type: String, optional: true } };

    setup() {
        this.popup = useService("popup");
        this.pos = usePos();
        this.notification = useService("notification");
        this.label = this.props.label || _t("Cash Drawer");
    }

    async onClick() {
        const config = this.pos.config || {};
        const requireValidation = config.validate_cash_drawer;

        if (requireValidation) {
            const result = await this.popup.add(CustomNumpadPopUp, {
                title: _t("Manager Authorization Required"),
                body: _t("Enter PIN to open cash drawer."),
                noteRequired: true,
            });

            if (!result || !result.confirmed) {
                await this.popup.add(ErrorPopup, {
                    title: _t("Access Denied"),
                    body: _t("You are not authorized to open the cash drawer."),
                });
                return;
            }
        }

        try {
            const response = await fetch("http://localhost:3001/open-drawer", {
                method: "POST",
            });

            if (!response.ok) {
                throw new Error("Failed to open drawer");
            }

            this.popup.add(ErrorPopup, {
                title: _t("Success"),
                body: _t("Cash drawer has been opened."),
            });
        } catch (error) {
            console.error("Drawer error:", error.message);
            this.popup.add(ErrorPopup, {
                title: _t("Failure"),
                body: _t("Could not open the cash drawer."),
            });
        }
    }
}

// ✅ Tambahkan tombol ke ProductScreen
ProductScreen.addControlButton({
    component: CashDrawerButton,
    condition: () => true,
    position: ["before", "SetPriceButton"],
});
