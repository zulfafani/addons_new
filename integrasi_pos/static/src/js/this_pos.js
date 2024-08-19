import { patch } from "@web/core/utils/patch";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";

patch(ProductScreen.prototype, {
    setup() {
        super.setup();

        // Inject POS manually into BarcodeReader
        this.pos.barcodeReader.pos = this.pos;
    },
});
