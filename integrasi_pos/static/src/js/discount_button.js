/** @odoo-module **/

import { _t } from "@web/core/l10n/translation";
import { Component } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { DiscountAmountPopUp } from "./discount_amount_popup";
import { CustomNumpadPopUp } from "./custom_numpad_popup";

export class DiscountAmountButton extends Component {
    static template = "DiscountAmountButton";
    static props = { label: { type: String, optional: true } };

    setup() {
        this.popup = useService("popup");
        this.pos = usePos();
        this.label = this.props.label || _t("Discount Amount");
    }

    async onClick() {
        try {
            const config = this.pos.config;

            const managerValidation = config?.manager_validation;
            const validateDiscountAmount = config?.validate_discount_amount;

            console.log("🧪 Config flags:", {
                managerValidation,
                validateDiscountAmount,
                managerPin: config?.manager_pin,
            });

            if (managerValidation && validateDiscountAmount) {
                const { confirmed } = await this.popup.add(CustomNumpadPopUp, {
                    title: _t("Manager Validation"),
                    startingValue: "",
                });

                if (confirmed) {
                    await this.popup.add(DiscountAmountPopUp);
                }
            } else {
                await this.popup.add(DiscountAmountPopUp);
            }
        } catch (error) {
            console.error("❌ Error during DiscountAmountButton flow:", error);
        }
    }
}

ProductScreen.addControlButton({
    component: DiscountAmountButton,
    condition: () => true,
    position: ['before', 'SetPriceButton'],
});
