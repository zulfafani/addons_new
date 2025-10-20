/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { DiscountButton } from "@pos_discount/overrides/components/discount_button/discount_button";
import { _t } from "@web/core/l10n/translation";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { NumberPopup } from "@point_of_sale/app/utils/input_popups/number_popup";
import { CustomNumpadPopUp } from "./custom_numpad_popup";

patch(DiscountButton.prototype, {
    async click() {
        const config = this.pos.config || {};
        const requireValidation = config.validate_discount_button;

        if (requireValidation) {
            const result = await this.popup.add(CustomNumpadPopUp, {
                title: _t("Manager Authorization Required"),
                body: _t("Enter PIN to apply discount."),
                noteRequired: true,
            });

            if (!result || !result.confirmed) {
                await this.popup.add(ErrorPopup, {
                    title: _t("Access Denied"),
                    body: _t("You are not authorized to apply discounts."),
                });
                return;
            }
        }

        const { confirmed, payload } = await this.popup.add(NumberPopup, {
            title: _t("Discount Percentage"),
            startingValue: this.pos.config.discount_pc,
            isInputSelected: true,
        });

        if (confirmed) {
            const val = Math.max(0, Math.min(100, parseFloat(payload)));
            await this.apply_discount(val);
        }
    },
});
