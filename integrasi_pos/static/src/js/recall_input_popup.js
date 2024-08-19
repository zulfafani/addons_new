/** @odoo-module **/

import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";
import { useState, useRef, onMounted } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";

export class RecallNumberPopup extends AbstractAwaitablePopup {
    static template = "integrasi_pos.RecallNumberPopup";

    setup() {
        super.setup();
        this.popup = useService("popup");
        this.inputRef = useRef("input");
        this.state = useState({ input: "" });

        onMounted(() => this.inputRef.el?.focus());
    }

    confirm() {
        const input = this.state.input.trim();
        const number = parseInt(input, 10);

        if (!input || isNaN(number)) {
            this.popup.add(ErrorPopup, {
                title: "❌ Input Tidak Valid",
                body: "Harap masukkan nomor yang benar.",
            });
            return;
        }

        this.props.resolve({ confirmed: true, payload: number });
        this.cancel();
    }

    cancel() {
        this.props.resolve({ confirmed: false });
        super.cancel();
    }

    appendNumber(num) {
        this.state.input += num.toString();
    }

    removeLast() {
        this.state.input = this.state.input.slice(0, -1);
    }

    clearAll() {
        this.state.input = "";
    }
}
