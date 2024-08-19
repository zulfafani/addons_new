/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { Orderline } from "@point_of_sale/app/store/models";

patch(Orderline.prototype, {
    setup(_defaultObj, options) {
        super.setup(...arguments);
        if (options.json) {
            this.salesperson = typeof options.json.salesperson === "string" ? options.json.salesperson : "";
            this.user_id = Number(options.json.user_id) || 0;
        }
    },

    export_as_JSON() {
        const json = super.export_as_JSON.call(this);
        json.salesperson = String(this.salesperson || "");
        json.user_id = Number(this.user_id || 0);
        return json;
    },

    init_from_JSON(json) {
        super.init_from_JSON(...arguments);
        this.salesperson = typeof json.salesperson === "string" ? json.salesperson : "";
        this.user_id = Number(json.user_id) || 0;
    },

    get_salesperson() {
        return {
            name: String(this.salesperson || ""),
            id: Number(this.user_id || 0),
        };
    },

    getDisplayData() {
        return {
            ...super.getDisplayData(),
            salesperson: String(this.salesperson || ""),
            user_id: Number(this.user_id || 0),
        };
    },
});
