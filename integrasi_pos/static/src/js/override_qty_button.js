/** @odoo-module **/
import { patch } from "@web/core/utils/patch";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { SetPricelistButton } from "@point_of_sale/app/screens/product_screen/control_buttons/pricelist_button/pricelist_button";
import { _t } from "@web/core/l10n/translation";
import { CustomNumpadPopUp } from "./custom_numpad_popup";
import { InputNumberPopUpQty } from "./input_number_popup_qty";

patch(SetPricelistButton.prototype, {
    async click() {
        const config = this.pos.config || {};

        // ✅ cek jika validasi PIN aktif
        if (config.manager_validation && config.validate_pricelist) {
            const { confirmed } = await this.popup.add(CustomNumpadPopUp, {
                title: _t("Enter Manager PIN"),
                body: _t("You need manager access to change the pricelist."),
            });

            if (!confirmed) {
                return; // ❌ stop jika gagal
            }
        }

        // ✅ panggil original click()
        return super.click(...arguments);
    },
});

patch(ProductScreen.prototype, {
    setup() {
        super.setup();
        // Store original keydown handler reference
        this._originalKeydownHandler = this._onKeyDown?.bind(this);
    },

    // ✅ Method untuk validasi manager yang bisa dipakai berulang
    async validateManagerAccess(mode, product = null) {
        const config = this.pos.config || {};
        const restrictedModes = {
            quantity: "validate_add_remove_quantity",
            discount: "validate_discount", 
            price: "validate_price_change",
            delete: "validate_order_line_deletion",
        };

        if (!config.manager_validation || !restrictedModes[mode] || !config[restrictedModes[mode]]) {
            return true; // No validation required
        }

        // Special case for price mode: check if product has is_fixed_price
        if (mode === "price") {
            const hasFixedPrice = product && product.is_fixed_price === true;
            if (!hasFixedPrice) {
                return true; // No validation needed if price is not fixed
            }
        }

        // Show manager PIN popup
        const { confirmed } = await this.popup.add(CustomNumpadPopUp, {
            title: _t("Enter Manager PIN"),
            body: _t("Please enter the manager's PIN to proceed."),
        });

        return confirmed;
    },

    // ✅ Method untuk mendapatkan product dari selected line
    getProductFromSelectedLine() {
        const selectedLine = this.currentOrder.get_selected_orderline();
        if (!selectedLine) return null;

        let product = null;
        if (selectedLine.product) {
            product = selectedLine.product;
        } else if (selectedLine.get_product && typeof selectedLine.get_product === 'function') {
            product = selectedLine.get_product();
        } else if (selectedLine.product_id) {
            const productId = Array.isArray(selectedLine.product_id) 
                ? selectedLine.product_id[0] 
                : selectedLine.product_id;
            product = this.pos.db.get_product_by_id(productId);
        }
        
        return product;
    },

    // ✅ FIXED: Proper keyboard input handling
    async _onKeyDown(ev) {
        const key = ev.key;
        const mode = this.pos.numpadMode;
        
        console.log("KeyDown event:", key, "Mode:", mode);

        // Handle Backspace for deletion
        if (key === "Backspace") {
            const isValidated = await this.validateManagerAccess("delete");
            if (!isValidated) {
                ev.preventDefault();
                ev.stopPropagation();
                return;
            }
        }

        // Handle numeric keys in restricted modes
        if (/^[0-9.]$/.test(key) && ["quantity", "discount", "price"].includes(mode)) {
            const product = this.getProductFromSelectedLine();
            const isValidated = await this.validateManagerAccess(mode, product);
            
            if (!isValidated) {
                ev.preventDefault();
                ev.stopPropagation();
                return;
            }
        }

        // Handle Enter key in restricted modes
        if (key === "Enter" && ["quantity", "discount", "price"].includes(mode)) {
            const product = this.getProductFromSelectedLine();
            const isValidated = await this.validateManagerAccess(mode, product);
            
            if (!isValidated) {
                ev.preventDefault();
                ev.stopPropagation();
                return;
            }
        }

        // Call original keydown handler if validation passed
        if (this._originalKeydownHandler) {
            return this._originalKeydownHandler(ev);
        } else if (super._onKeyDown) {
            return super._onKeyDown(ev);
        }
    },

    // ✅ MODIFIED: Enhanced onNumpadClick for UI numpad
    async onNumpadClick(buttonValue) {
        const keyAlias = {
            Backspace: "⌫",
        };
        const resolvedKey = keyAlias[buttonValue] || buttonValue;
        const numberKeys = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ".", "+/-", "⌫"];
        
        const config = this.pos.config || {};
        const mode = this.pos.numpadMode;

        console.log("Numpad click:", resolvedKey, "Mode:", mode);

        // Handle mode switching
        if (["quantity", "discount", "price"].includes(resolvedKey)) {
            this.numberBuffer.capture();
            this.numberBuffer.reset();
            this.pos.numpadMode = resolvedKey;
            return;
        }

        // Handle backspace/delete
        if (resolvedKey === "⌫") {
            const isValidated = await this.validateManagerAccess("delete");
            if (!isValidated) return;
            
            this.numberBuffer.sendKey("Backspace");
            return;
        }

        // Handle number input
        if (numberKeys.includes(resolvedKey)) {
            const selectedLine = this.currentOrder.get_selected_orderline();
            const product = this.getProductFromSelectedLine();
            
            // Check if manager validation is required
            const isValidated = await this.validateManagerAccess(mode, product);
            if (!isValidated) return;

            // Show input popup for value entry
            try {
                const result = await this.popup.add(InputNumberPopUpQty, {
                    title: _t(`Enter ${mode}`),
                    body: _t("Masukkan nilai yang diinginkan."),
                    contextType: mode,
                });

                if (!result || result.input === undefined || result.input === null) {
                    console.log("User cancelled input or provided invalid input");
                    return;
                }

                const value = parseFloat(result.input);
                if (isNaN(value) || value < 0) {
                    console.log("Invalid numeric value entered:", result.input);
                    return;
                }

                if (!selectedLine) {
                    console.log("No order line selected");
                    return;
                }

                // Apply the changes
                if (mode === "quantity") {
                    if (value === 0) {
                        this.currentOrder.remove_orderline(selectedLine);
                    } else {
                        selectedLine.set_quantity(value);
                    }
                } else if (mode === "discount") {
                    const discountValue = Math.min(Math.max(value, 0), 100);
                    selectedLine.set_discount(discountValue);
                } else if (mode === "price") {
                    selectedLine.set_unit_price(value);
                    selectedLine.price_type = "manual";
                }

                this.numberBuffer.reset();
                return;

            } catch (error) {
                console.error("Error in onNumpadClick:", error);
                return;
            }
        }

        // Call original method for other keys
        if (super.onNumpadClick) {
            return super.onNumpadClick(resolvedKey);
        }
    },
});