/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { Orderline } from "@point_of_sale/app/generic_components/orderline/orderline";

// ✅ Tambahkan shape baru dengan validasi tipe
patch(Orderline.props.line.shape, {
    salesperson: { type: String, optional: true },
    user_id: { type: Number, optional: true },
});
