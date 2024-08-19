/** @odoo-module **/

import { Component } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { RecallNumberPopup } from "./recall_input_popup";

class HoldTransactionButton extends Component {
    static template = 'integrasi_pos.HoldTransactionButton';

    setup() {
        this.pos = usePos();
        this.popup = useService("popup");

        // Init heldOrders if not yet defined
        if (!this.pos.heldOrders) {
            this.pos.heldOrders = [];
        }
    }

    async onClickHoldOrder() {
        const currentOrder = this.pos.get_order();
        if (currentOrder && !currentOrder.is_empty()) {
            const cloneOrder = currentOrder.export_as_JSON();
            this.pos.heldOrders.push(cloneOrder);
            this.pos.add_new_order();

            window.alert("✅ Transaksi berhasil di-hold dan siap melayani pelanggan berikutnya.");
        } else {
            window.alert("⚠️ Tidak ada item dalam transaksi.");
        }
    }
}

class RecallTransactionButton extends Component {
    static template = 'integrasi_pos.RecallTransactionButton';

    setup() {
        this.pos = usePos();
        this.popup = useService("popup");
    }

    async onClickRecallOrder() {
        if (!this.pos.heldOrders || this.pos.heldOrders.length === 0) {
            await this.popup.add(ErrorPopup, {
                title: "⚠️ Tidak Ada Transaksi",
                body: "Belum ada transaksi yang di-hold.",
            });
            return;
        }

        const orderList = this.pos.heldOrders
            .map((order, index) => `#${index + 1}: ${order.lines.length} item`)
            .join("\n");

        const { confirmed, payload } = await this.popup.add(RecallNumberPopup, {
            title: "Pulihkan Transaksi",
            body: `Pilih nomor transaksi:\n${orderList}`,
        });

        if (!confirmed) return;

        const index = parseInt(payload, 10) - 1;

        if (isNaN(index) || index < 0 || index >= this.pos.heldOrders.length) {
            await this.popup.add(ErrorPopup, {
                title: "❌ Input Tidak Valid",
                body: "Nomor transaksi tidak ditemukan.",
            });
            return;
        }

        const orderData = this.pos.heldOrders[index];
        const newOrder = this.pos.add_new_order();
        newOrder.init_from_JSON(orderData);
        this.pos.set_order(newOrder);
        this.pos.heldOrders.splice(index, 1);

        await this.popup.add(ErrorPopup, {
            title: "✅ Transaksi Dipulihkan",
            body: "Transaksi berhasil dipulihkan.",
        });
    }
}

// Tambahkan tombol ke ProductScreen
ProductScreen.addControlButton({
    component: HoldTransactionButton,
    condition: () => true,
});

ProductScreen.addControlButton({
    component: RecallTransactionButton,
    condition: () => true,
});
