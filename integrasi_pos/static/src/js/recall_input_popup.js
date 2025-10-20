/** @odoo-module **/

import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";
import { useState, useRef, onMounted } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";

export class RecallNumberPopup extends AbstractAwaitablePopup {
    static template = "integrasi_pos.RecallNumberPopup";

    setup() {
        super.setup();
        this.popup = useService("popup");
        this.inputRef = useRef("input");
        this.state = useState({ 
            input: "",
            orders: this.props.orders || [] // Terima data orders dari parent
        });

        this.appendNumber = this.appendNumber.bind(this);
        this.removeLast = this.removeLast.bind(this);
        this.clearAll = this.clearAll.bind(this);
        this.deleteOrder = this.deleteOrder.bind(this);

        onMounted(() => this.inputRef.el?.focus());
    }

    async deleteOrder(index) {
        const order = this.state.orders[index];
        const confirmed = window.confirm(
            `⚠️ Yakin ingin menghapus transaksi?\n\nCatatan: ${order.notes}\nJumlah item: ${order.data.lines.length}`
        );

        if (confirmed) {
            // Hapus dari array
            this.state.orders.splice(index, 1);
            
            // Notifikasi ke parent untuk update storage
            if (this.props.onDelete) {
                this.props.onDelete(index);
            }

            await this.popup.add(ErrorPopup, {
                title: "✅ Transaksi Dihapus",
                body: `Transaksi dengan catatan "${order.notes}" telah dihapus.`,
            });

            // Jika tidak ada order lagi, tutup popup
            if (this.state.orders.length === 0) {
                await this.popup.add(ErrorPopup, {
                    title: "ℹ️ Tidak Ada Transaksi",
                    body: "Semua transaksi telah dihapus.",
                });
                this.cancel();
            }
        }
    }

    confirm() {
        const input = this.state.input.trim();
        const number = parseInt(input, 10);

        if (!input || isNaN(number)) {
            this.popup.add(ErrorPopup, {
                title: "❌ Input Tidak Valid",
                body: "Harap masukkan nomor yang benar.",
            });
            return;
        }

        if (number < 1 || number > this.state.orders.length) {
            this.popup.add(ErrorPopup, {
                title: "❌ Nomor Tidak Valid",
                body: `Harap masukkan nomor antara 1 hingga ${this.state.orders.length}.`,
            });
            return;
        }

        this.props.resolve({ confirmed: true, payload: number });
        this.cancel();
    }

    cancel() {
        this.props.resolve({ confirmed: false });
        super.cancel();
    }

    appendNumber(num) {
        this.state.input += num.toString();
    }

    removeLast() {
        this.state.input = this.state.input.slice(0, -1);
    }

    clearAll() {
        this.state.input = "";
    }
}