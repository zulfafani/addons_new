/** @odoo-module **/

import { registry } from "@web/core/registry";
import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";
import { OrderReceipt } from "@point_of_sale/app/screens/receipt_screen/receipt/order_receipt";

class CustomPaymentScreen extends PaymentScreen {
    async _finalizeValidation() {
        await super._finalizeValidation(); // Memanggil metode dari PaymentScreen asli

        // Setelah validasi selesai, cek apakah harus mencetak receipt otomatis
        if (this.shouldPrintReceiptAutomatically()) {
            await this.printReceipt();
        }
    }

    shouldPrintReceiptAutomatically() {
        // Tambahkan logika di sini untuk menentukan apakah receipt harus dicetak otomatis
        // Contoh sederhana: hanya cetak jika konfigurasi PoS mengizinkan cetak otomatis
        return this.pos.config.iface_print_auto && !this.currentOrder._printed;
    }

    async printReceipt() {
        const printResult = await this.printer.print(
            OrderReceipt,
            {
                data: this.pos.get_order().export_for_printing(),
                formatCurrency: this.env.utils.formatCurrency,
            },
            { webPrintFallback: true }
        );

        if (printResult && this.pos.config.iface_print_skip_screen) {
            // Jika konfigurasi PoS mengizinkan, setelah cetak, kembali ke layar produk
            this.pos.removeOrder(this.currentOrder);
            this.pos.add_new_order();
            this.pos.showScreen("ProductScreen");
        }
    }
}

registry.category("pos_screens").add("PaymentScreen", CustomPaymentScreen, { force: true });
