/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";

patch(ProductScreen.prototype, {
    async clickProduct(product) {
        await super.clickProduct(product);
        this.numberBuffer.reset(); // Kosongkan buffer numpad setelah pilih produk
        this.pos.numpadMode = null; // Pastikan tombol Qty tidak menyala otomatis
    },

    async _barcodeProductAction(code) {
        await super._barcodeProductAction(code);
        this.numberBuffer.reset();
        this.pos.numpadMode = null;
    },

    onNumpadClick(buttonValue) {
        if (buttonValue === "quantity") {
            this.pos.numpadMode = "quantity"; // Aktifkan hanya saat tombol Qty ditekan
        } else {
            super.onNumpadClick(buttonValue);
        }
    },
});
