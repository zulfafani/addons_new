/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { _t } from "@web/core/l10n/translation";
import { CustomNumpadPopUp } from "./custom_numpad_popup";
import { InputNumberPopUpQty } from "./input_number_popup_qty";

patch(ProductScreen.prototype, {
    async onNumpadClick(buttonValue) {
        const keyAlias = {
            Backspace: "⌫",
        };
        const resolvedKey = keyAlias[buttonValue] || buttonValue;

        const numberKeys = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ".", "+/-", "⌫"];
        const restrictedModes = {
            quantity: "validate_add_remove_quantity",
            discount: "validate_discount",
            price: "validate_price_change",
            "⌫": "validate_order_line_deletion",
        };

        const config = this.pos.config;
        const mode = this.pos.numpadMode;

        if (["quantity", "discount", "price"].includes(resolvedKey)) {
            this.numberBuffer.capture();
            this.numberBuffer.reset();
            this.pos.numpadMode = resolvedKey;
            return;
        }

        if (resolvedKey === "⌫") {
            if (config.manager_validation && config["validate_order_line_deletion"]) {
                const { confirmed } = await this.popup.add(CustomNumpadPopUp, {
                    title: _t("Enter Manager PIN"),
                    body: _t("You need manager access to delete input."),
                });
                if (!confirmed) return;
            }
            // ✅ FIXED: Use sendKey instead of backspace
            this.numberBuffer.sendKey("Backspace");
            return;
        }

        if (numberKeys.includes(resolvedKey)) {
            if (
                config.manager_validation &&
                restrictedModes[mode] &&
                config[restrictedModes[mode]]
            ) {
                const { confirmed } = await this.popup.add(CustomNumpadPopUp, {
                    title: _t("Enter Manager PIN"),
                    body: _t("Please enter the manager's PIN to proceed."),
                });
                if (!confirmed) return;
            }

            const result = await this.popup.add(InputNumberPopUpQty, {
                title: _t(`Enter ${mode}`),
                body: _t("Masukkan nilai yang diinginkan."),
                contextType: mode,
            });

            if (!result || result.input === undefined) return;

            const value = parseFloat(result.input);
            if (isNaN(value)) return;

            const selectedLine = this.currentOrder.get_selected_orderline();
            if (!selectedLine) return;

            if (mode === "quantity") {
                selectedLine.set_quantity(value);
            } else if (mode === "discount") {
                selectedLine.set_discount(value);
            } else if (mode === "price") {
                selectedLine.set_unit_price(value);
                selectedLine.price_type = "manual";
            }

            this.numberBuffer.reset();
            return;
        }

        super.onNumpadClick(resolvedKey);
    },
});
