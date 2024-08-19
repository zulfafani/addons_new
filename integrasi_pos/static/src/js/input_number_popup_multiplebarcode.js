/** @odoo-module **/

import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";
import { _t } from "@web/core/l10n/translation";
import { useService } from "@web/core/utils/hooks";
import { useState, useRef, onMounted } from "@odoo/owl";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";

export class InputNumberPopUp extends AbstractAwaitablePopup {
    static template = "integrasi_pos.InputNumberPopUp";

    setup() {
        super.setup();
        this.popup = useService("popup");
        this.inputRef = useRef("numberInput");
        this.products = this.props.productList || [];

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
        const index = parseInt(this.state.inputValue) - 1;
        if (!isNaN(index) && this.products?.[index]) {
            this.props.resolve({ productIndex: index });
            this.cancel();
        } else {
            this.popup.add(ErrorPopup, {
                title: _t("Nomor tidak valid"),
                body: _t("Silakan masukkan nomor produk yang tersedia."),
            });
        }
    }

    addNumber(num) {
        // prevent dot character as this is integer input only
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
