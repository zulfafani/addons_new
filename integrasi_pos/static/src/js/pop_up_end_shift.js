/** @odoo-module */
import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";
import { _t } from "@web/core/l10n/translation";
import { useRef, onMounted } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { PopUpSuccesError } from "./pop_up_error";

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
        this.popup = useService("popup");
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
            // Dapatkan kasir yang sedang aktif
            const currentCashier = this.pos.get_cashier();
            if (!currentCashier || !currentCashier.id) {
                throw new Error("Tidak ada kasir yang aktif");
            }
    
            // Ambil data kasir yang sesuai dengan ID kasir aktif
            const employees = await this.orm.searchRead(
                'hr.employee',
                [['id', '=', currentCashier.id]],
                ['id', 'name']
            );
            
            if (employees && employees.length > 0) {
                this.state.cashiers = employees;
                this.state.currentCashierId = currentCashier.id;
            } else {
                throw new Error("Data kasir tidak ditemukan");
            }
        } catch (error) {
            console.error("Error fetching employees:", error);
            await this.showErrorPopup(
                _t("Error: Data Kasir"), 
                _t("Gagal mengambil data kasir. Silakan coba lagi.")
            );
        }
    }
    
    async fetchCurrentSession() {
        try {
            const currentSession = this.pos.get_order().pos_session_id;
            const currentCashier = this.pos.get_cashier();
            
            if (!currentCashier || !currentCashier.id) {
                throw new Error("Tidak ada kasir yang aktif");
            }
            
            await this.fetchLatestCashierLog(currentSession);
        } catch (error) {
            console.error("Error fetching current session:", error);
            await this.showErrorPopup(
                _t("Error: Sesi POS"), 
                _t("Gagal mengambil data sesi POS saat ini. Silakan coba lagi.")
            );
        }
    }

    async fetchLatestCashierLog(currentSession) {
        try {
            const currentCashier = this.pos.get_cashier();
            if (!currentCashier || !currentCashier.id) {
                throw new Error("Tidak ada kasir yang aktif");
            }
    
            // ✅ Ambil log kasir yang PALING AWAL, bukan yang terbaru
            const cashierLogs = await this.orm.searchRead(
                'pos.cashier.log',
                [
                    ['session_id', '=', currentSession],
                    ['state', '=', 'opened'],
                    ['employee_id', '=', currentCashier.id],  // ambil log untuk kasir ini saja
                ],
                ['employee_id', 'timestamp'],
                { order: 'timestamp asc', limit: 1 }  // ambil timestamp paling awal
            );
    
            if (cashierLogs.length > 0) {
                const employeeId = cashierLogs[0].employee_id[0];
                this.setStartShiftField(cashierLogs[0].timestamp);
                this.setCashierField(employeeId);
            } else {
                console.warn("Tidak ditemukan log kasir untuk sesi ini.");
                this.setStartShiftField(new Date().toISOString());
                this.setCashierField(currentCashier.id);
            }
        } catch (error) {
            console.error("Gagal mengambil log kasir:", error);
            this.setStartShiftField(new Date().toISOString());
            await this.showErrorPopup(
                _t("Error: Log Kasir"),
                _t("Gagal mengambil log kasir. Menggunakan waktu saat ini sebagai waktu mulai shift.")
            );
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
            await this.showErrorPopup(
                _t("Error: Data Tidak Lengkap"), 
                _t("Mohon lengkapi semua field sebelum menyimpan.")
            );
            return;
        }
    
        try {
            const currentSession = this.pos.get_order().pos_session_id;

            const startDateStr = this.formatDateForSearch(startShiftField);
            const endDateStr = this.formatDateForSearch(endingShiftField);
    
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
                throw new Error("Tidak ditemukan catatan shift yang terbuka untuk kasir dan sesi ini.");
            }
    
            const endShiftId = existingEndShift[0].id;

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
    
            await this.orm.write('end.shift', [endShiftId], {
                start_date: this.formatDateToUTC(startShiftField),
                end_date: this.formatDateToUTC(endingShiftField),
                line_ids: [[5, 0, 0]].concat(line_ids.map(line => [0, 0, line])),
            });
    
            await this.orm.call(
                "end.shift",
                "action_close",
                [endShiftId]
            );
    
            const cashierLogs = await this.orm.searchRead(
                'pos.cashier.log',
                [
                    ['session_id', '=', currentSession],
                    ['employee_id', '=', cashierId],
                    ['state', '=', 'opened']
                ],
                ['id']
            );
    
            await this.orm.write('hr.employee', [cashierId], { disable_payment: true });
    
            await this.showSuccessPopup(
                _t("Notification"), 
                _t("Anda Telah Menyelesaikan Shift.")
            );
            this.cancel();
        } catch (error) {
            console.error("Error updating shift data:", error);
            let errorMessage = _t("Terjadi kesalahan yang tidak diketahui.");
            if (error.data && error.data.message) {
                errorMessage = error.data.message;
            } else if (error.message) {
                errorMessage = error.message;
            }
            await this.showErrorPopup(
                _t("Notification"), 
                _t("Gagal menyelesaikan shift. Silakan coba kembali atau hubungi Administrator. Error: ") + _t("Terjadi kesalahan saat menyimpan data shift. Detail: ") + errorMessage
            );
            
            if (error.data) {
                console.error("Additional error details:", error.data);
            }
        }
    }

    async showErrorPopup(title, message) {
        const { confirmed } = await this.popup.add(PopUpSuccesError, {
            title: title || _t("Error"),
            body: message,
            confirmText: _t("OK"),
        });
        
        if (confirmed) {
            return true;
        }
        return false;
    }
    
    async showSuccessPopup(title, message) {
        const { confirmed } = await this.popup.add(PopUpSuccesError, {
            title: title || _t("Success"),
            body: message,
            confirmText: _t("OK"),
        });
        
        if (confirmed) {
            setTimeout(() => {
                window.location.reload();
            }, 500);
        }
    }
}