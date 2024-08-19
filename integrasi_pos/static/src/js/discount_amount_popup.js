/** @odoo-module **/

import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";
import { _t } from "@web/core/l10n/translation";
import { useState, useRef, onMounted } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { jsonrpc } from "@web/core/network/rpc_service";

export class DiscountAmountPopUp extends AbstractAwaitablePopup {
    static template = "integrasi_pos.DiscountAmountPopUp";

    setup() {
        super.setup();
        this.popup = useService("popup");
        this.pos = useService("pos");

        this.inputRef = useRef("discountInput");
        onMounted(() => {
            this.inputRef.el?.focus();
        });

        this.state = useState({
            inputValue: "",
        });

        this.handleTyping = (ev) => {
            const value = ev.target.value;
            // Validasi hanya angka dan 1 titik desimal
            if (/^[0-9]*\.?[0-9]*$/.test(value)) {
                this.state.inputValue = value;
            }
        };
    }

    addNumber(num) {
        if (num === "." && this.state.inputValue.includes(".")) return;
        this.state.inputValue += num;
    }

    removeLastChar() {
        this.state.inputValue = this.state.inputValue.slice(0, -1);
    }

    clearInput() {
        this.state.inputValue = "";
    }

    async confirmInput() {
        const value = parseFloat(this.state.inputValue);
        if (isNaN(value) || value <= 0) {
            await this.popup.add(ErrorPopup, {
                title: _t("Invalid Discount"),
                body: _t("Please enter a valid discount amount greater than 0."),
            });
            return;
        }

        const order = this.pos.get_order();
        if (!order) {
            await this.popup.add(ErrorPopup, {
                title: _t("No Order"),
                body: _t("No current order available."),
            });
            return;
        }

        // Find all products
        const allProducts = Object.values(this.pos.db.product_by_id);
        const discountProductData = allProducts.find(
            (product) => product.default_code === "Discount001"
        );

        if (!discountProductData) {
            await this.popup.add(ErrorPopup, {
                title: _t("Discount Product Not Found"),
                body: _t("The 'Discount001' product was not found. Ensure it's created and loaded in POS."),
            });
            return;
        }

        // Get product via ID
        const discountProductId = discountProductData.id;
        const discountProduct = this.pos.db.get_product_by_id(discountProductId);

        if (!discountProduct) {
            await this.popup.add(ErrorPopup, {
                title: _t("Product Not Loaded in POS"),
                body: _t("The 'Discount001' product exists but is not loaded in your POS session."),
            });
            return;
        }

        order.add_product(discountProduct, {
            price: -value,
            quantity: 1,
            extras: { is_manual_discount: true }
        });

        this.confirm(value.toFixed(2));
    }
}
