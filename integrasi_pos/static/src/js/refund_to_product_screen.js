/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { TicketScreen } from "@point_of_sale/app/screens/ticket_screen/ticket_screen";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { _t } from "@web/core/l10n/translation";

patch(TicketScreen.prototype, {
    async onDoRefund() {
        const order = this.getSelectedOrder();

        if (order && this._doesOrderHaveSoleItem(order)) {
            if (!this._prepareAutoRefundOnOrder(order)) return;
        }

        if (!order) {
            this._state.ui.highlightHeaderNote = !this._state.ui.highlightHeaderNote;
            return;
        }

        const partner = order.get_partner();
        const allToRefundDetails = this._getRefundableDetails(partner);

        if (allToRefundDetails.length === 0) {
            this._state.ui.highlightHeaderNote = !this._state.ui.highlightHeaderNote;
            return;
        }

        const invoicedOrderIds = new Set(
            allToRefundDetails
                .filter(
                    (detail) =>
                        this._state.syncedOrders.cache[detail.orderline.orderBackendId]?.state === "invoiced"
                )
                .map((detail) => detail.orderline.orderBackendId)
        );

        if (invoicedOrderIds.size > 1) {
            await this.popup.add(ErrorPopup, {
                title: _t("Multiple Invoiced Orders Selected"),
                body: _t(
                    "You have selected orderlines from multiple invoiced orders. To proceed refund, please select orderlines from the same invoiced order."
                ),
            });
            return;
        }

        const destinationOrder =
            this.props.destinationOrder &&
            partner === this.props.destinationOrder.get_partner() &&
            !this.pos.doNotAllowRefundAndSales()
                ? this.props.destinationOrder
                : this._getEmptyOrder(partner);

        // Tandai sebagai refund secepatnya
        destinationOrder.is_refund_order = true;

        // 🚫 JANGAN lock dulu, set partner dahulu untuk override default customer
        const wasLocked = !!destinationOrder.locked_partner;
        destinationOrder.locked_partner = false;

        // Paksa set partner ke partner order asli (bukan default)
        if (partner) {
            destinationOrder.set_partner(partner);
        } else {
            // Kalau memang tanpa partner, kosongkan (menghindari default yang terlanjur kepasang)
            destinationOrder.set_partner(null);
        }

        // Setelah partner benar, baru lock lagi
        destinationOrder.locked_partner = wasLocked || true;

        const originalToDestinationLineMap = new Map();

        for (const refundDetail of allToRefundDetails) {
            if (refundDetail.qty <= 0) continue;

            const product = this.pos.db.get_product_by_id(refundDetail.orderline.productId);
            if (!product) continue;

            const options = this._prepareRefundOrderlineOptions(refundDetail);
            options.merge = false;

            const newLine = await destinationOrder.add_product(product, options);
            if (!newLine || newLine.get_quantity() === 0) {
                console.warn("❌ Gagal tambah produk refund:", product.display_name);
                continue;
            }

            originalToDestinationLineMap.set(refundDetail.orderline.id, newLine);
            refundDetail.destinationOrderUid = destinationOrder.uid;
        }

        for (const refundDetail of allToRefundDetails) {
            const originalLine = refundDetail.orderline;
            const destLine = originalToDestinationLineMap.get(originalLine.id);
            if (!destLine) continue;

            if (originalLine.comboParent) {
                const comboParent = originalToDestinationLineMap.get(originalLine.comboParent.id);
                if (comboParent) destLine.comboParent = comboParent;
            }

            if (originalLine.comboLines?.length > 0) {
                destLine.comboLines = originalLine.comboLines
                    .map((cl) => originalToDestinationLineMap.get(cl.id))
                    .filter(Boolean);
            }
        }

        if (order.fiscal_position_not_found) {
            await this.popup.add(ErrorPopup, {
                title: _t("Fiscal Position not found"),
                body: _t(
                    "The fiscal position used in the original order is not loaded. Make sure it is loaded by adding it in the POS configuration."
                ),
            });
            return;
        }

        destinationOrder.fiscal_position = order.fiscal_position;

        // Jangan panggil addAdditionalRefundInfo (tidak ada di Odoo 17)
        // await this.addAdditionalRefundInfo(order, destinationOrder);

        console.log("✅ Refund Lines:");
        destinationOrder.orderlines.forEach((line) => {
            const prod = line.get_product?.();
            console.log(` - ${prod?.display_name ?? "??"} | Qty: ${line.get_quantity()}`);
        });

        // Set sebagai order aktif dan tampilkan ProductScreen
        this.pos.set_order(destinationOrder);
        this.pos.showScreen("ProductScreen");

        // 🧹 Bersihkan flag alur refund setelah selesai
        this.pos.in_refund_flow = false;
    },
});
