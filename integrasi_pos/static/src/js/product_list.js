/** @odoo-module */

import { patch } from "@web/core/utils/patch";
import { ProductsWidget } from "@point_of_sale/app/screens/product_screen/product_list/product_list";
import { ProductCard } from "@point_of_sale/app/generic_components/product_card/product_card";

// ==================== PATCH PRODUCTCARD TO LIST FORMAT ====================
patch(ProductCard.prototype, {
    
    /**
     * Get product SKU/Default Code
     * Mengambil SKU dari product object jika tersedia
     */
    get productSKU() {
        try {
            const pos = this.env.services.pos;
            if (!pos || !pos.db) return '';
            
            const product = pos.db.get_product_by_id(this.props.productId);
            return product?.default_code || '';
        } catch (error) {
            console.error('Error getting product SKU:', error);
            return '';
        }
    }
    
});

// Patch template untuk menggunakan layout list
patch(ProductCard, {
    template: "point_of_sale.ProductCardList",
});

// ==================== PATCH PRODUCTSWIDGET ====================
patch(ProductsWidget.prototype, {
    setup() {
        super.setup(...arguments);
        if (this.pos.selectedCategoryId !== 0) {
            this.pos.setSelectedCategoryId(0);
        }
    },
    
    getCategories() {
        return [];
    },
    
    get selectedCategoryId() {
        return 0;
    },
    
    get productsToDisplay() {
        const { db } = this.pos;
        let list = [];

        if (this.searchWord !== "") {
            list = db.search_product_in_category(0, this.searchWord);
        } else {
            return [];
        }

        list = list.filter((product) => !this.getProductListToNotDisplay().includes(product.id));
        
        return list.sort((a, b) => a.display_name.localeCompare(b.display_name));
    },
    
    updateProductList(event) {
        // Do nothing - category selection is disabled
    }
});