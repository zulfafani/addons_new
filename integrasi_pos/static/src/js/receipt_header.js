/** @odoo-module */
import { Orderline } from "@point_of_sale/app/store/models";
import { patch } from "@web/core/utils/patch";

// Patch hanya untuk menambahkan method helper
patch(Orderline.prototype, {
    getProductBarcode() {
        const line = this.props.line;
        
        // Try different possible sources for barcode
        if (line.barcode) {
            return line.barcode;
        }
        
        // If barcode is in product object
        if (line.product && line.product.barcode) {
            return line.product.barcode;
        }
        
        // If using get_product method
        if (line.get_product && typeof line.get_product === 'function') {
            const product = line.get_product();
            if (product && product.barcode) {
                return product.barcode;
            }
        }
        
        return null;
    }
});