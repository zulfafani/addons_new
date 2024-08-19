/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { ProductsWidget } from "@point_of_sale/app/screens/product_screen/product_list/product_list";

patch(ProductsWidget.prototype, {
    /**
     * Override: only show products if a search query is entered.
     */
    get productsToDisplay() {
        const { db } = this.pos;
        let list = [];

        // ✅ Only show products when search is active
        if (this.searchWord !== "") {
            list = db.search_product_in_category(this.selectedCategoryId, this.searchWord);
        } else {
            // ❌ When no search input, show nothing
            return [];
        }

        // Filter out tip products and sort alphabetically
        list = list.filter((product) => !this.getProductListToNotDisplay().includes(product.id));
        return list.sort((a, b) => a.display_name.localeCompare(b.display_name));
    },
});
