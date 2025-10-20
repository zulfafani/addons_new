/** @odoo-module **/
import { Order } from "@point_of_sale/app/store/models";
import { patch } from "@web/core/utils/patch";

patch(Order.prototype, {
    setup() {
        super.setup(...arguments);

        // Pastikan flag terdefinisi
        this.is_refund_order = this.is_refund_order || false;

        // 🔍 Debug logging
        console.log("🔍 Order setup debug:", {
            is_refund: this.is_refund_order,
            has_partner: !!this.partner,
            default_partner_config: this.pos.config.default_partner_id,
            partners_count: this.pos.partners ? this.pos.partners.length : 0,
            db_partners_count: this.pos.db ? Object.keys(this.pos.db.partner_by_id || {}).length : 0
        });

        // ✅ Set default partner HANYA jika:
        // 1. Bukan refund order
        // 2. Belum ada partner yang di-set
        // 3. Ada default partner di config
        if (!this.is_refund_order && !this.partner && this.pos.config.default_partner_id) {
            const default_customer_id = this.pos.config.default_partner_id[0];
            console.log("🔍 Looking for default customer ID:", default_customer_id);
            
            // Coba ambil dari db
            let partner = this.pos.db.get_partner_by_id(default_customer_id);
            
            // Jika tidak ada di db, coba dari array partners
            if (!partner && this.pos.partners) {
                partner = this.pos.partners.find(p => p.id === default_customer_id);
                console.log("🔍 Found in partners array:", !!partner);
            }
            
            if (partner) {
                this.set_partner(partner);
                console.log("✅ Default customer loaded:", partner.name);
            } else {
                console.error("❌ Default customer not found:", {
                    id: default_customer_id,
                    available_partners: this.pos.partners ? this.pos.partners.map(p => ({id: p.id, name: p.name})) : []
                });
            }
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
        const qty = options.quantity ?? 1;
        if (qty < 0 && !this.is_refund_order) {
            this.set_is_refund_order(true);
        }
        return result;
    },

    init_from_JSON(json) {
        super.init_from_JSON(...arguments);
        const hasNegativeLines = json.lines?.some((line) => line[2]?.qty < 0);
        this.is_refund_order = !!hasNegativeLines;
    },
});