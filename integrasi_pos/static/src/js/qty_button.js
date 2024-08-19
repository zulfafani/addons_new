/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";

patch(ProductScreen.prototype, {
    _barcodeProductAction(code) {
        super._barcodeProductAction(code);

        // **Matikan mode numpad setelah produk ditambahkan**
        setTimeout(() => {
            this.pos.numpadMode = null;
        }, 100);
    },

    async _barcodeGS1Action(parsed_results) {
        await super._barcodeGS1Action(parsed_results);

        // **Matikan mode numpad setelah produk ditambahkan**
        setTimeout(() => {
            this.pos.numpadMode = null;
        }, 100);
    },

    onNumpadClick(buttonValue) {
        if (["quantity", "discount", "price"].includes(buttonValue)) {
            this.pos.numpadMode = buttonValue;
        }
        return super.onNumpadClick(buttonValue);
    },

    selectLine(orderline) {
        super.selectLine(orderline);

        // **Aktifkan mode Qty hanya ketika user memilih orderline**
        if (this.pos.numpadMode === null) {
            this.pos.numpadMode = "quantity";
        }
    }
});
