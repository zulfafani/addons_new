/** @odoo-module **/

import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";
import { patch } from "@web/core/utils/patch";
import { useService } from "@web/core/utils/hooks";
import { _t } from "@web/core/l10n/translation";

patch(PaymentScreen.prototype, {
    setup() {
        super.setup();
        this.orm = useService("orm");
    },

    async validateOrder(isForceValidate) {
        this.numberBuffer.capture();
        
        // Check if the shift is closed
        const isShiftClosed = await this.isEndShiftClosed();
        if (isShiftClosed) {
            this.showAlertAndReload(_t("Cannot validate order. The shift is closed for the current cashier."));
            return;
        }
    
        // Check if the order is valid
        if (this.pos.config.cash_rounding) {
            if (!this.pos.get_order().check_paymentlines_rounding()) {
                this._display_popup_error_paymentlines_rounding();
                return;
            }
        }
    
        // Log the current order
        const currentOrder = this.currentOrder; // Use the getter for currentOrder
        console.log("Current Order:", currentOrder); // Log current order
    
        // Get the associated customer
        const customer = currentOrder.get_partner(); // Use the correct method to get the partner
        console.log("Customer:", customer); // Log customer
    
        // Validate customer's credit limits
        // if (customer) {
        //     const creditCheckResult = await this.validateCustomerCredit(customer.id); // Ensure you are passing the correct customer ID
        //     console.log("Credit Check Result:", creditCheckResult); // Log credit check result
        //     if (creditCheckResult === "block") {
        //         this.popup.add(ErrorPopup, {
        //             title: _t("Cannot validate order."),
        //             body: _t("Customer's credit exceeds the block amount."),
        //         });
        //         return;
        //     } else if (creditCheckResult === "warn") {
        //         const proceed = confirm(_t("Customer's credit exceeds the warning amount. Do you want to proceed?"));
        //         if (!proceed) {
        //             return;
        //         }
        //     }
        // } else {
        //     this.popup.add(ErrorPopup, {
        //         title: _t("No Customer Selected"),
        //         body: _t("Please select a customer before validating the order."),
        //     });
        //     return;
        // }

        if (currentOrder && currentOrder.lines) {
            for (const line of currentOrder.lines) {
                if (!line.sales_person) {
                    // Use salesperson from your SetProductListButton if available
                    if (line.salesperson) {
                        line.sales_person = line.salesperson;
                    } else {
                        // Fallback to current POS user
                        line.sales_person = this.pos.user.name;
                    }
                }
                console.log(`Set sales_person on line ${line.uuid} to ${line.sales_person}`);
            }
        }
    
        // Proceed with order validation
        if (await this._isOrderValid(isForceValidate)) {
            // Remove pending payments before finalizing the validation
            for (const line of this.paymentLines) {
                if (!line.is_done()) {
                    this.currentOrder.remove_paymentline(line);
                }
            }
            await this._finalizeValidation();
        }
    },

    async isEndShiftClosed() {
        const currentSession = this.pos.pos_session.id;
        const currentCashier = this.pos.get_cashier();
        if (!currentCashier) {
            return false;
        }
        const endShift = await this.orm.searchRead(
            'end.shift',
            [
                ['session_id', '=', currentSession],
                ['cashier_id', '=', currentCashier.id],
                ['state', '=', 'closed']
            ],
            ['id'],
            { limit: 1 }
        );
        return endShift.length > 0;
    },

    // async validateCustomerCredit(customerId) {
    //     const partner = await this.orm.searchRead(
    //         'res.partner',
    //         [['id', '=', customerId]],
    //         ['credit_amount', 'warn_amount', 'block_amount']
    //     );
    //     if (partner.length > 0) {
    //         const { credit_amount, warn_amount, block_amount } = partner[0];
    //         console.log(`Credit Amount: ${credit_amount}, Warn Amount: ${warn_amount}, Block Amount: ${block_amount}`); // Log values
    //         if (credit_amount >= block_amount) {
    //             return "block";
    //         } else if (credit_amount >= warn_amount) {
    //             return "warn";
    //         }
    //     }
    //     return "ok";
    // },

    showAlert(message) {
        alert(message);
    },

    showAlertAndReload(message) {
        alert(message);
        setTimeout(() => {
            window.location.reload();
        }, 500);
    },
});
