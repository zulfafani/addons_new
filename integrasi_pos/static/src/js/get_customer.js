/** @odoo-module **/
import { Order } from "@point_of_sale/app/store/models";
import { patch } from "@web/core/utils/patch";

patch(Order.prototype, {
    setup() {
        super.setup(...arguments);

        // Properly initialize as a refund if needed
        this.is_refund_order = this.is_refund_order || false;

        // Only apply default partner for non-refund orders
        if (!this.is_refund_order && this.pos.config.default_partner_id) {
            const default_customer = this.pos.config.default_partner_id[0];
            const partner = this.pos.db.get_partner_by_id(default_customer);
            this.set_partner(partner);
        }
    },

    set_is_refund_order(is_refund) {
        this.is_refund_order = is_refund;
    },

    add_orderline(line) {
        const result = super.add_orderline(...arguments);

        if (line && line.quantity < 0 && !this.is_refund_order) {
            this.set_is_refund_order(true);
        }

        return result;
    },

    async add_product(product, options = {}) {
        const result = await super.add_product(...arguments);

        // If quantity is negative and not already marked as refund
        const qty = options.quantity ?? 1;
        if (qty < 0 && !this.is_refund_order) {
            this.set_is_refund_order(true);
        }

        return result;
    },

    init_from_JSON(json) {
        super.init_from_JSON(...arguments);
        const hasNegativeLines = json.lines?.some(line => line[2]?.qty < 0);
        this.is_refund_order = !!hasNegativeLines;
    }
});
