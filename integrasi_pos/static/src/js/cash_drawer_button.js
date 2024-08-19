/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";
import { TextInputPopup } from "@point_of_sale/app/utils/input_popups/text_input_popup";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { _t } from "@web/core/l10n/translation";
import { Component } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";

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
        try {
            const response = await fetch("http://localhost:3001/open-drawer", {
                method: "POST",
            });

            if (!response.ok) {
                throw new Error("Gagal membuka drawer");
            }

            this.popup.add(ErrorPopup, {
                title: _t("Berhasil"),
                body: _t("Cash drawer telah dibuka."),
            });
        } catch (error) {
            console.error("Drawer error:", error.message);
            this.popup.add(ErrorPopup, {
                title: _t("Gagal"),
                body: _t("Tidak dapat membuka cash drawer."),
            });
        }
    }

}

// Tambahkan tombol ke ProductScreen
ProductScreen.addControlButton({
    component: CashDrawerButton,
    condition: () => true,
    position: ["before", "SetPriceButton"],
});

// Auto-test connection ketika module loaded (optional)
// setTimeout(() => {
//     const button = new CashDrawerButton({}, {});
//     button.testConnection();
// }, 2000);
