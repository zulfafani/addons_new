/** @odoo-module **/
import { Order } from "@point_of_sale/app/store/models";
import { patch } from "@web/core/utils/patch";

patch(Order.prototype, {

    setup() {
        super.setup(...arguments);
        console.log("hello world");
        if(this.pos.config.default_partner_id){
            var default_customer = this.pos.config.default_partner_id[0];
            var partner = this.pos.db.get_partner_by_id(default_customer);
            this.set_partner(partner);
        }
        },	
});