/** @odoo-module */
import { PosStore } from "@point_of_sale/app/store/pos_store";
import { patch } from "@web/core/utils/patch";

// Ubah title segera setelah file dimuat
document.title = 'POSVIT';

patch(PosStore.prototype, {
    async setup() {
        document.title = 'POSVIT'; // Pastikan tetap ada di sini
        await super.setup(...arguments);
    },

    async _processData() {
        await super._processData(...arguments);
        document.title = 'POSVIT';
    }
});
