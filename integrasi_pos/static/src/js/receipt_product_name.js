/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { Orderline } from "@point_of_sale/app/store/models";
import { Order } from "@point_of_sale/app/store/models";

const originalExport = Order.prototype.export_for_printing;
patch(Order.prototype, {
    export_for_printing() {
        const result = originalExport.call(this);

        // Footer dinamis dari nama perusahaan
        // result.footer = "Powered by POSVIT";


        return result;
    },
});