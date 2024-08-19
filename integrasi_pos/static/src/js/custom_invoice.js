/** @odoo-module **/

import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";
import { patch } from "@web/core/utils/patch";

patch(PaymentScreen.prototype, {
    onMounted() {
        super.onMounted();
        // Menjalankan toggleIsToInvoice secara otomatis
        if (!this.currentOrder.is_to_invoice()) {
            this.toggleIsToInvoice();
        }
        
        // Kode existing untuk payment method
        if (this.payment_methods_from_config.length == 1) {
            this.addNewPaymentLine(this.payment_methods_from_config[0]);
        }
    }
});