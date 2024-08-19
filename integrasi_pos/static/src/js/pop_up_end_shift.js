/** @odoo-module */
import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";
import { _t } from "@web/core/l10n/translation";
import { useRef, onMounted } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";

export class EndShiftSessionButtonPopup extends AbstractAwaitablePopup {
    static template = "integrasi_pos.EndShiftSessionButtonPopup";
    static defaultProps = {
        confirmText: _t("Save"),
        cancelText: _t("Discard"),
        title: _t("End Shift Cashier"),
    };

    setup() {
        super.setup();
        this.orm = useService("orm");
        this.pos = useService("pos");
        this.cashierField = useRef("cashierField");
        this.startShiftField = useRef("startShiftField");
        this.endingShiftField = useRef("endingShiftField");
        
        this.state = {
            cashiers: [],
            startShiftTime: null,
            currentCashierId: null,
        };

        onMounted(() => {
            this.updateFields();
        });
    }

    async updateFields() {
        await this.fetchEmployees();
        await this.fetchCurrentSession();
        this.setEndingShiftField();
        this.render();

        setTimeout(() => {
            if (this.endingShiftField.el && !this.endingShiftField.el.value) {
                this.setEndingShiftField();
            }
        }, 100);
    }

    async fetchEmployees() {
        try {
            const employees = await this.orm.searchRead(
                'hr.employee',
                [['active', '=', true]],
                ['id', 'name']
            );
            this.state.cashiers = employees;
        } catch (error) {
            console.error("Error fetching employees:", error);
            alert(_t("Error fetching employees. Please try again."));
        }
    }

    async fetchCurrentSession() {
        try {
            const currentSession = this.pos.get_order().pos_session_id;
            await this.fetchLatestCashierLog(currentSession);
        } catch (error) {
            console.error("Error fetching current session:", error);
            alert(_t("Error fetching current session. Please try again."));
        }
    }

    async fetchLatestCashierLog(currentSession) {
        try {
            const cashierLogs = await this.orm.searchRead(
                'pos.cashier.log',
                [
                    ['session_id', '=', currentSession],
                    ['state', '=', 'opened']
                ],
                ['employee_id', 'timestamp'],
                { order: 'timestamp desc', limit: 1 }
            );

            if (cashierLogs.length > 0) {
                const employeeId = cashierLogs[0].employee_id[0];
                this.setStartShiftField(cashierLogs[0].timestamp);
                this.setCashierField(employeeId);
            } else {
                console.warn("No open cashier log found for the current session.");
                const currentCashier = this.pos.get_cashier();
                if (currentCashier && currentCashier.id) {
                    this.setCashierField(currentCashier.id);
                    this.setStartShiftField(new Date().toISOString());
                } else {
                    alert(_t("No active cashier found. Please select a cashier."));
                }
            }
        } catch (error) {
            console.error("Error fetching cashier log:", error);
            this.setStartShiftField(new Date().toISOString());
            alert(_t("Error fetching cashier log. Using current time as start shift."));
        }
    }

    setStartShiftField(timestamp) {
        const date = new Date(timestamp);
        if (isNaN(date)) {
            console.error("Invalid date format for timestamp:", timestamp);
            date = new Date();
        }
        this.state.startShiftTime = this.formatDateToLocalWithSeconds(date);
    
        if (this.startShiftField && this.startShiftField.el) {
            this.startShiftField.el.value = this.state.startShiftTime;
        } else {
            console.warn("Start shift field or its element is not available.", this.startShiftField);
        }
    }

    setCashierField(employeeId) {
        if (this.cashierField && this.cashierField.el) {
            this.cashierField.el.value = employeeId.toString();
            this.state.currentCashierId = employeeId;
        } else {
            console.warn("Cashier field or its element is not available.", this.cashierField);
        }
    }

    setEndingShiftField() {
        if (this.endingShiftField && this.endingShiftField.el) {
            const now = new Date();
            this.endingShiftField.el.value = this.formatDateToLocalWithSeconds(now);
        } else {
            console.warn("Ending shift field or its element is not available.", this.endingShiftField);
        }
    }

    formatDateToLocalWithSeconds(date) {
        const offset = date.getTimezoneOffset();
        const localDate = new Date(date.getTime() - (offset * 60 * 1000));
        return localDate.toISOString().slice(0, 19).replace('T', ' ');
    }
    
    formatDateToUTC(dateString) {
        const date = new Date(dateString);
        return date.toISOString().slice(0, 19).replace('T', ' ');
    }

    formatDateForSearch(dateString) {
        const date = new Date(dateString);
        // Format: YYYY-MM-DD HH:mm:ss
        return date.getUTCFullYear() + '-' +
               String(date.getUTCMonth() + 1).padStart(2, '0') + '-' +
               String(date.getUTCDate()).padStart(2, '0') + ' ' +
               String(date.getUTCHours()).padStart(2, '0') + ':' +
               String(date.getUTCMinutes()).padStart(2, '0') + ':' +
               String(date.getUTCSeconds()).padStart(2, '0');
    }
    
    async confirm() {
        const cashierId = parseInt(this.cashierField.el.value);
        const startShiftField = this.startShiftField.el.value;
        const endingShiftField = this.endingShiftField.el.value;
    
        if (!cashierId || !startShiftField || !endingShiftField) {
            alert(_t("Missing Fields: Please fill out all fields before saving."));
            return;
        }
    
        try {
            const currentSession = this.pos.get_order().pos_session_id;

            const startDateStr = this.formatDateForSearch(startShiftField);
            const endDateStr = this.formatDateForSearch(endingShiftField);
    
            // Fetch the existing end.shift record
            const existingEndShift = await this.orm.searchRead(
                'end.shift',
                [
                    ['session_id', '=', currentSession],
                    ['cashier_id', '=', cashierId],
                    ['state', '=', 'in_progress'],
                    ['start_date', '=', startDateStr],
                ],
                ['id', 'line_ids'],
                { limit: 1 }
            );
    
            if (existingEndShift.length === 0) {
                throw new Error("No open end.shift record found for this cashier and session.");
            }
    
            const endShiftId = existingEndShift[0].id;

            // Fetch and calculate payment data
            const posOrders = await this.orm.searchRead(
                'pos.order',
                [
                    ['session_id', '=', currentSession],
                    ['state', '=', 'invoiced'],
                    ['create_date', '>=', startDateStr],
                    ['create_date', '<=', endDateStr]
                ],
                ['payment_ids']
            );
    
            const paymentData = {};
            for (const order of posOrders) {
                const payments = await this.orm.searchRead(
                    'pos.payment',
                    [
                        ['id', 'in', order.payment_ids],
                    ],
                    ['payment_date', 'payment_method_id', 'amount']
                );
    
                for (const payment of payments) {
                    const methodId = payment.payment_method_id[0];
                    const amount = payment.amount;
                    const paymentDate = payment.payment_date;
            
                    if (methodId in paymentData) {
                        paymentData[methodId].expected_amount += amount;
                        if (new Date(paymentDate) > new Date(paymentData[methodId].payment_date)) {
                            paymentData[methodId].payment_date = paymentDate;
                        }
                    } else {
                        paymentData[methodId] = {
                            payment_method_id: methodId,
                            expected_amount: amount,
                            payment_date: paymentDate,
                        };
                    }
                }
            }
    
            const line_ids = Object.values(paymentData);
    
            // Update the existing end.shift record
            await this.orm.write('end.shift', [endShiftId], {
                start_date: this.formatDateToUTC(startShiftField),
                end_date: this.formatDateToUTC(endingShiftField),
                line_ids: [[5, 0, 0]].concat(line_ids.map(line => [0, 0, line])),
            });
    
            // Call action_close on the updated end.shift record
            await this.orm.call(
                "end.shift",
                "action_close",
                [endShiftId]
            );
    
            // Close the cashier log
            const cashierLogs = await this.orm.searchRead(
                'pos.cashier.log',
                [
                    ['session_id', '=', currentSession],
                    ['employee_id', '=', cashierId],
                    ['state', '=', 'opened']
                ],
                ['id']
            );
    
            // Set disable_payment to True for the employee
            await this.orm.write('hr.employee', [cashierId], { disable_payment: true });
    
            alert(_t("Success: Shift data has been updated and closed successfully. The cashier's payment ability has been disabled."));
            this.cancel();
            
            // Refresh the page after successful confirmation
            setTimeout(() => {
                window.location.reload();
            }, 500);  // Wait for 0.5 second before refreshing
        } catch (error) {
            console.error("Error updating shift data:", error);
            let errorMessage = "An unknown error occurred.";
            if (error.data && error.data.message) {
                errorMessage = error.data.message;
            } else if (error.message) {
                errorMessage = error.message;
            }
            alert(_t("Error: An error occurred while updating the shift data. Details: ") + errorMessage);
            
            if (error.data) {
                console.error("Additional error details:", error.data);
            }
        }
    }
}