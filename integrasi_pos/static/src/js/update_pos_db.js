/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { PosDB } from "@point_of_sale/app/store/db";

patch(PosDB.prototype, {
    add_cashier_log(log) {
        const logs = JSON.parse(localStorage.getItem("pos_cashier_logs") || "[]");
        logs.push(log);
        localStorage.setItem("pos_cashier_logs", JSON.stringify(logs));
    },

    get_cashier_logs() {
        return JSON.parse(localStorage.getItem("pos_cashier_logs") || "[]");
    },

    remove_cashier_log(id) {
        let logs = JSON.parse(localStorage.getItem("pos_cashier_logs") || "[]");
        logs = logs.filter(log => log.id !== id);
        localStorage.setItem("pos_cashier_logs", JSON.stringify(logs));
    },

    clear_cashier_logs() {
        localStorage.removeItem("pos_cashier_logs");
    }
});
