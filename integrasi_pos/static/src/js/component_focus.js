/** @odoo-module **/

import { registry } from "@web/core/registry";
import { FormController } from "@web/views/form/form_controller";
import { formView } from "@web/views/form/form_view";
import { useEffect } from "@odoo/owl";

class InventoryStockFormController extends FormController {
    setup() {
        super.setup();
        
        // Use effect to focus barcode input when component is mounted
        useEffect(() => {
            this.focusBarcodeInput();
        });
        
        // Listen for keydown events to always refocus the barcode input
        useEffect(() => {
            const keydownHandler = this.onGlobalKeyDown.bind(this);
            document.addEventListener("keydown", keydownHandler);
            return () => {
                document.removeEventListener("keydown", keydownHandler);
            };
        });
    }
    
    /**
     * Handler for global keydown events
     * @param {KeyboardEvent} event
     */
    onGlobalKeyDown(event) {
        try {
            // Only process if we have a record with state data
            if (!this.props.record || !this.props.record.data) return;
            
            // Focus on barcode input if in the right state
            if (this.props.record.data.state === 'in_progress') {
                this.focusBarcodeInput();
            }
        } catch (error) {
            console.error("Error in keydown handler:", error);
        }
    }
    
    /**
     * Focus the barcode input field if it exists
     */
    focusBarcodeInput() {
        try {
            // Get the record and check state
            const record = this.props.record;
            if (!record || !record.data) return;
            
            if (record.data.state === 'in_progress') {
                // Use setTimeout to let the UI update first
                setTimeout(() => {
                    const inputEl = document.querySelector('.barcode_input input');
                    if (inputEl) {
                        inputEl.focus();
                        // For debugging
                        console.log("Barcode input focused");
                    } else {
                        console.log("Barcode input element not found");
                    }
                }, 100);
            }
        } catch (error) {
            console.error("Error focusing barcode input:", error);
        }
    }
    
    /**
     * @override
     */
    async onFieldChanged(record, fieldName) {
        await super.onFieldChanged(record, fieldName);
        
        try {
            if (fieldName === "barcode_input" && record && record.data.state === 'in_progress') {
                // Process barcode input
                const barcodeValue = record.data.barcode_input;
                console.log("Barcode scanned:", barcodeValue);
                
                // Clear the input field and refocus
                if (barcodeValue) {
                    record.update({ barcode_input: "" });
                }
                
                this.focusBarcodeInput();
            }
            
            // Also refocus when state changes to in_progress
            if (fieldName === "state" && record && record.data.state === 'in_progress') {
                this.focusBarcodeInput();
            }
        } catch (error) {
            console.error("Error processing field change:", error);
        }
    }
}

// Register the custom form view with our controller
export const inventoryStockFormView = {
    ...formView,
    Controller: InventoryStockFormController,
};

registry.category("views").add("inventory_stock_form_focus", inventoryStockFormView);