/** @odoo-module **/

import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";
import { _t } from "@web/core/l10n/translation";
import { useService } from "@web/core/utils/hooks";
import { useState, useRef, onMounted } from "@odoo/owl";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";

export class InputNumberPopUpQty extends AbstractAwaitablePopup {
    static template = "integrasi_pos.InputNumberPopUpQty";

    setup() {
        super.setup();
        this.popup = useService("popup");
        this.inputRef = useRef("numberInput");

        onMounted(() => {
            this.inputRef.el?.focus();
        });

        this.state = useState({
            inputValue: "",
        });

        this.handleTyping = (ev) => {
            const value = ev.target.value;
            if (/^[0-9]*$/.test(value)) {
                this.state.inputValue = value;
            }
        };

        this.handleKeyClick = (key) => {
            if (key === "⌫") {
                this.removeLastChar();
            } else {
                this.addNumber(key);
            }
        };
    }

    confirmInput() {
        const input = this.state.inputValue.trim();
        if (!input || isNaN(parseFloat(input))) {
            this.popup.add(ErrorPopup, {
                title: _t("Input tidak valid"),
                body: _t("Harap masukkan angka yang valid."),
            });
            return;
        }

        this.props.resolve({ input: parseFloat(input) });
        this.cancel();
    }

    addNumber(num) {
        if (num === "." || this.state.inputValue.includes(".")) return;
        this.state.inputValue += num;
    }

    removeLastChar() {
        this.state.inputValue = this.state.inputValue.slice(0, -1);
    }

    clearInput() {
        this.state.inputValue = "";
    }
}
