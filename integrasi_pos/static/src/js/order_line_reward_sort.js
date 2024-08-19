/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { Order } from "@point_of_sale/app/store/models";

// 🩹 Patch to reorder reward lines after associated product
patch(Order.prototype, {
    reorderRewardLines() {
        const lines = [...this.orderlines];
        const mainProducts = [];
        const rewardLines = [];

        for (const line of lines) {
            const name = line.get_full_product_name();
            const price = line.get_price_with_tax();

            if (price < 0 || name.toLowerCase().includes("per point on")) {
                rewardLines.push(line);
            } else {
                mainProducts.push(line);
            }
        }

        const reordered = [];
        for (const productLine of mainProducts) {
            reordered.push(productLine);
            const rewards = rewardLines.filter((rLine) =>
                rLine.get_full_product_name().includes(productLine.get_full_product_name())
            );
            reordered.push(...rewards);
        }

        this.orderlines.reset();
        for (const line of reordered) {
            this.orderlines.add(line);
        }
    },
});
