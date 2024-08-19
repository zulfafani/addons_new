/** @odoo-module **/

import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";
import { _t } from "@web/core/l10n/translation";
import { useService } from "@web/core/utils/hooks";
import { useState, useRef, onMounted } from "@odoo/owl";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";

export class CustomNumpadPopUp extends AbstractAwaitablePopup {
    static template = "integrasi_pos.CustomNumpadPopUp";

    setup() {
        super.setup();
        this.pos = useService("pos");
        this.popup = useService("popup");
        this.notification = useService("notification");
        this.inputRef = useRef("pinInput");

        onMounted(() => {
            this.inputRef.el?.focus();
        });

        this.state = useState({
            inputValue: "",
            displayValue: "",
        });

        this.handleTyping = (ev) => {
            const input = ev.target.value;
            const lastChar = input.slice(-1);
            if (!/^\d$/.test(lastChar)) return;
            if (this.state.inputValue.length >= 4) return;
            this.state.inputValue += lastChar;
            this.state.displayValue += "*";
        };
    }

    addNumber(num) {
        if (this.state.inputValue.length >= 4) return;
        this.state.inputValue += num;
        this.state.displayValue += "*";
    }

    removeLastChar() {
        this.state.inputValue = this.state.inputValue.slice(0, -1);
        this.state.displayValue = this.state.displayValue.slice(0, -1);
    }

    clearInput() {
        this.state.inputValue = "";
        this.state.displayValue = "";
    }

    async confirmInput() {
        const enteredPin = this.state.inputValue;
        const expectedPin = this.pos.config.manager_pin;

        if (!enteredPin) {
            await this.popup.add(ErrorPopup, {
                title: _t("Invalid Input"),
                body: _t("Please enter a valid PIN."),
            });
            return;
        }

        if (!expectedPin) {
            await this.popup.add(ErrorPopup, {
                title: _t("Configuration Error"),
                body: _t("Manager PIN is not set in POS configuration."),
            });
            return;
        }

        if (enteredPin === expectedPin) {
            this.notification.add(_t("PIN Validated Successfully"), { type: "success" });
            this.confirm({ confirmed: true });
        } else {
            await this.popup.add(ErrorPopup, {
                title: _t("Invalid PIN"),
                body: _t("The entered PIN is incorrect. Please try again."),
            });
        }
    }
}
