/** @odoo-module */

import { patch } from "@web/core/utils/patch";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { _t } from "@web/core/l10n/translation";
import { useBarcodeReader } from "@point_of_sale/app/barcode/barcode_reader_hook";
import { ConfirmPopup } from "@point_of_sale/app/utils/confirm_popup/confirm_popup";
import { useService } from "@web/core/utils/hooks";
import { useState } from "@odoo/owl";

patch(ProductScreen.prototype, {
    setup() {
        super.setup(...arguments);
        this.notification = useService("pos_notification");
        
        // Setup mobile pane state
        if (!this.pos.mobile_pane) {
            this.pos.mobile_pane = 'products'; // default to products view
        }
        
        useBarcodeReader({
            coupon: this._onCouponScan,
        });
    },
    
    // Method untuk set mobile pane
    setMobilePane(pane) {
        this.pos.mobile_pane = pane;
    },
    
    _onCouponScan(code) {
        this.currentOrder.activateCode(code.base_code);
    },
    
    async updateSelectedOrderline({ buffer, key }) {
        const selectedLine = this.currentOrder.get_selected_orderline();
        if (key === "-") {
            if (selectedLine && selectedLine.eWalletGiftCardProgram) {
                this.notification.add(
                    _t("You cannot set negative quantity or price to gift card or ewallet."),
                    4000
                );
                return;
            }
        }
        if (
            selectedLine &&
            selectedLine.is_reward_line &&
            !selectedLine.manual_reward &&
            (key === "Backspace" || key === "Delete")
        ) {
            const reward = this.pos.reward_by_id[selectedLine.reward_id];
            const { confirmed } = await this.popup.add(ConfirmPopup, {
                title: _t("Deactivating reward"),
                body: _t(
                    "Are you sure you want to remove %s from this order?\n You will still be able to claim it through the reward button.",
                    reward.description
                ),
                cancelText: _t("No"),
                confirmText: _t("Yes"),
            });
            if (confirmed) {
                buffer = null;
            } else {
                return;
            }
        }
        return super.updateSelectedOrderline({ buffer, key });
    },
    
    _setValue(val) {
        const selectedLine = this.currentOrder.get_selected_orderline();
        if (
            !selectedLine ||
            !selectedLine.is_reward_line ||
            (selectedLine.is_reward_line && ["", "remove"].includes(val))
        ) {
            super._setValue(val);
        }
        if (!selectedLine) {
            return;
        }
        if (selectedLine.is_reward_line && val === "remove") {
            this.currentOrder.disabledRewards.add(selectedLine.reward_id);
            const { couponCache } = this.pos;
            const coupon = couponCache[selectedLine.coupon_id];
            if (
                coupon &&
                coupon.id > 0 &&
                this.currentOrder.codeActivatedCoupons.find((c) => c.code === coupon.code)
            ) {
                delete couponCache[selectedLine.coupon_id];
                this.currentOrder.codeActivatedCoupons.splice(
                    this.currentOrder.codeActivatedCoupons.findIndex((coupon) => {
                        return coupon.id === selectedLine.coupon_id;
                    }),
                    1
                );
            }
        }
        if (!selectedLine.is_reward_line || (selectedLine.is_reward_line && val === "remove")) {
            this.currentOrder._updateRewards();
        }
    },
    
    async _barcodeProductAction(code) {
        await super._barcodeProductAction(code);
        this.currentOrder._updateRewards();
    },
    

    async _barcodeGS1Action(code) {
        await super._barcodeGS1Action(code);
        this.currentOrder._updateRewards();
    },

    async _showDecreaseQuantityPopup() {
        const result = await super._showDecreaseQuantityPopup();
        if (result) {
            this.currentOrder._updateRewards();
        }
    },
});

// Patch template untuk menggunakan template yang sudah diperbaiki
patch(ProductScreen, {
    template: "point_of_sale.ProductScreenPatched",
});