/** @odoo-module **/

import { Component, useState } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { RecallNumberPopup } from "./recall_input_popup";
import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";

// === Storage Helper untuk LocalStorage ===
const STORAGE_KEY = "pos_held_orders";

const StorageHelper = {
    // Simpan ke localStorage
    save(orders) {
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(orders));
        } catch (e) {
            console.error("Error saving to localStorage:", e);
        }
    },

    // Load dari localStorage
    load() {
        try {
            const data = localStorage.getItem(STORAGE_KEY);
            return data ? JSON.parse(data) : [];
        } catch (e) {
            console.error("Error loading from localStorage:", e);
            return [];
        }
    },

    // Hapus semua data
    clear() {
        try {
            localStorage.removeItem(STORAGE_KEY);
        } catch (e) {
            console.error("Error clearing localStorage:", e);
        }
    }
};

// === Popup untuk input Notes Hold Transaction ===
export class HoldNotesPopup extends AbstractAwaitablePopup {
    static template = "integrasi_pos.HoldNotesPopup";
    setup() {
        this.state = useState({ input: "" });
    }
    getPayload() {
        return this.state.input.trim();
    }
}

// === Button Hold Transaction ===
class HoldTransactionButton extends Component {
    static template = "integrasi_pos.HoldTransactionButton";

    setup() {
        this.pos = usePos();
        this.popup = useService("popup");

        // Load data dari localStorage saat pertama kali
        if (!this.pos.heldOrders) {
            this.pos.heldOrders = StorageHelper.load();
        }
    }

    async onClickHoldOrder() {
        const currentOrder = this.pos.get_order();
        if (!currentOrder || currentOrder.is_empty()) {
            window.alert("⚠️ Tidak ada item dalam transaksi.");
            return;
        }

        // Popup minta notes
        const { confirmed, payload } = await this.popup.add(HoldNotesPopup, {
            title: "Tambah Catatan",
            body: "Masukkan catatan transaksi (contoh: Nama pelanggan atau keterangan)",
        });

        if (!confirmed || !payload) {
            await this.popup.add(ErrorPopup, {
                title: "❌ Catatan Wajib",
                body: "Anda harus mengisi catatan untuk hold transaksi.",
            });
            return;
        }

        // Simpan order + notes
        const cloneOrder = currentOrder.export_as_JSON();
        const heldOrder = {
            data: cloneOrder,
            notes: payload,
            timestamp: new Date().toISOString(),
        };

        this.pos.heldOrders.push(heldOrder);
        
        // Simpan ke localStorage
        StorageHelper.save(this.pos.heldOrders);

        this.pos.add_new_order();

        await this.popup.add(ErrorPopup, {
            title: "✅ Hold Transaksi",
            body: `Transaksi berhasil di-hold dengan catatan: "${payload}"`,
        });
    }
}

// === Button Recall Transaction ===
class RecallTransactionButton extends Component {
    static template = "integrasi_pos.RecallTransactionButton";

    setup() {
        this.pos = usePos();
        this.popup = useService("popup");

        // Load data dari localStorage saat pertama kali
        if (!this.pos.heldOrders) {
            this.pos.heldOrders = StorageHelper.load();
        }
    }

    async onClickRecallOrder() {
        // Refresh data dari localStorage
        this.pos.heldOrders = StorageHelper.load();

        if (!this.pos.heldOrders || this.pos.heldOrders.length === 0) {
            await this.popup.add(ErrorPopup, {
                title: "⚠️ Tidak Ada Transaksi",
                body: "Belum ada transaksi yang di-hold.",
            });
            return;
        }

        // Callback untuk handle delete dari popup
        const handleDelete = (index) => {
            this.pos.heldOrders.splice(index, 1);
            StorageHelper.save(this.pos.heldOrders);
        };

        const { confirmed, payload } = await this.popup.add(RecallNumberPopup, {
            title: "Pulihkan Transaksi",
            subtitle: "Pilih nomor transaksi:",
            orders: [...this.pos.heldOrders], // Kirim copy dari orders
            onDelete: handleDelete, // Kirim callback untuk delete
        });

        if (!confirmed) return;

        // Refresh data setelah popup ditutup (mungkin ada yang dihapus)
        this.pos.heldOrders = StorageHelper.load();

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
        newOrder.init_from_JSON(orderData.data);
        this.pos.set_order(newOrder);
        
        // Hapus dari array dan update localStorage
        this.pos.heldOrders.splice(index, 1);
        StorageHelper.save(this.pos.heldOrders);

        await this.popup.add(ErrorPopup, {
            title: "✅ Transaksi Dipulihkan",
            body: `Transaksi dengan catatan "${orderData.notes}" berhasil dipulihkan.`,
        });
    }
}

// === Button untuk Clear All Held Orders (Optional) ===
class ClearHeldOrdersButton extends Component {
    static template = "integrasi_pos.ClearHeldOrdersButton";

    setup() {
        this.pos = usePos();
        this.popup = useService("popup");
    }

    async onClickClearAll() {
        if (!this.pos.heldOrders || this.pos.heldOrders.length === 0) {
            await this.popup.add(ErrorPopup, {
                title: "⚠️ Tidak Ada Data",
                body: "Tidak ada transaksi yang perlu dihapus.",
            });
            return;
        }

        const confirmed = window.confirm(
            `⚠️ Yakin ingin menghapus semua ${this.pos.heldOrders.length} transaksi yang di-hold?`
        );

        if (confirmed) {
            this.pos.heldOrders = [];
            StorageHelper.clear();
            
            await this.popup.add(ErrorPopup, {
                title: "✅ Data Dihapus",
                body: "Semua transaksi yang di-hold telah dihapus.",
            });
        }
    }
}

// === Tambahkan tombol ke ProductScreen ===
ProductScreen.addControlButton({
    component: HoldTransactionButton,
    condition: () => true,
});

ProductScreen.addControlButton({
    component: RecallTransactionButton,
    condition: () => true,
});

// Optional: Tombol untuk clear all held orders
// ProductScreen.addControlButton({
//     component: ClearHeldOrdersButton,
//     condition: () => true,
// });