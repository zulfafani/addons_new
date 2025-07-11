/** @odoo-module */
import { PosStore } from "@point_of_sale/app/store/pos_store";
import { patch } from "@web/core/utils/patch";

patch(PosStore.prototype, {
    async openCashControl() {
        // Skip popup dan langsung set opening cash ke 0
        if (this.pos_session.state === 'opening_control') {
            this.pos_session.state = "opened";
            
            // Gunakan this.orm bukan this.data
            await this.orm.call("pos.session", "set_cashbox_pos", [
                this.pos_session.id,
                0, // Opening cash = 0
                "", // Notes kosong
            ]);
            
            return;
        }
        
        // Untuk closing, tetap panggil method asli
        return super.openCashControl(...arguments);
    },
});