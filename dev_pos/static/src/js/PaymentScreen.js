odoo.define('dev_pos.PaymentScreen', function(require) {
    'use strict';

    const PaymentScreen = require('point_of_sale.PaymentScreen');
    const Registries = require('point_of_sale.Registries');

    const PosInvoiceAutomatePaymentScreen = (PaymentScreen) =>
        class extends PaymentScreen {
            constructor() {
                super(...arguments);
                // Tambahkan logika untuk mengatur invoice_only ketika payment dilakukan
                this.paymentLines.on('add remove', this, this.set_invoice_only);
                this.set_invoice_only();
            }

            set_invoice_only() {
                const isInvoiceOnly = this.env.pos.config.invoice_auto_check;
                this.currentOrder.set_to_invoice(isInvoiceOnly);
            }

            async validateOrder(isForceValidate) {
                // Kode validasi order tetap tidak berubah
                await super.validateOrder(isForceValidate);
            }

            async _finalizeValidation() {
                // Kode finalisasi validasi tetap tidak berubah
                await super._finalizeValidation();
            }
        };

    Registries.Component.extend(PaymentScreen, PosInvoiceAutomatePaymentScreen);

    return PaymentScreen;
});
