/** @odoo-module */

import { ClosePosPopup } from "@point_of_sale/app/navbar/closing_popup/closing_popup";
import { patch } from "@web/core/utils/patch";
import { CustomNumpadPopUp } from "./custom_numpad_popup";
import { useService } from "@web/core/utils/hooks";
import { _t } from "@web/core/l10n/translation";
import { ConnectionLostError } from "@web/core/network/rpc_service";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";

patch(ClosePosPopup.prototype, {
    setup() {
        console.log("✅ Setup called");
        super.setup();
        
        // ✅ KOSONGKAN SEMUA CASH MOVES - tidak tampilkan Opening, Cash in/out, dll
        if (this.props.default_cash_details) {
            this.props.default_cash_details.moves = [];
        }
        
        // Kosongkan juga untuk other payment methods jika ada
        if (this.props.other_payment_methods) {
            this.props.other_payment_methods.forEach(pm => {
                if (pm.moves) {
                    pm.moves = [];
                }
            });
        }
        
        if (!this.popup) {
            this.popup = useService("popup");
        }
        if (!this.rpc) {
            this.rpc = useService("rpc");
        }
        this.orm = useService("orm");
        
        // Load expected immediately after state initialization
        this.loadExpectedData();
    },
    
    async loadExpectedData() {
        // Wait for next tick to ensure state is ready
        await new Promise(resolve => setTimeout(resolve, 0));
        await this._loadExpectedFromEndShift();
    },

    getInitialState() {
        const initialState = {
            notes: "",
            payments: {},
        };

        // Set cash counted ke "0"
        if (this.pos.config.cash_control && this.props.default_cash_details) {
            initialState.payments[this.props.default_cash_details.id] = {
                counted: "0",
            };
        }

        // Set semua bank counted ke "0"
        this.props.other_payment_methods.forEach((pm) => {
            initialState.payments[pm.id] = {
                counted: "0",
            };
        });

        return initialState;
    },

    async _loadExpectedFromEndShift() {
        console.log("🔍 DEBUG: _loadExpectedFromEndShift() called");
        console.log("🔍 Session ID:", this.pos.pos_session.id);
        
        try {
            // 1. Ambil end.shift.line yang sesuai dengan session
            const lines = await this.orm.call(
                "end.shift.line",
                "search_read",
                [
                    [["end_shift_id.session_id", "=", this.pos.pos_session.id]],
                    ["payment_method_id", "expected_amount"],
                ]
            );
            console.log("🔍 End Shift Lines:", lines);

            // 2. Ambil semua end.shift untuk mendapatkan total modal
            const shifts = await this.orm.call(
                "end.shift",
                "search_read",
                [
                    [["session_id", "=", this.pos.pos_session.id]],
                    ["modal"],
                ]
            );
            console.log("🔍 End Shifts:", shifts);

            // 3. Hitung total modal dari semua shift
            const totalModal = shifts.reduce((sum, shift) => {
                return sum + (shift.modal || 0.0);
            }, 0.0);
            console.log("🔍 Total Modal:", totalModal);

            // 4. Masukkan expected dari end.shift.line ke state untuk payment method bank
            lines.forEach((line) => {
                const pmId = line.payment_method_id[0];
                if (!this.state.payments[pmId]) {
                    this.state.payments[pmId] = { counted: "0" };
                }
                // Hanya set expected untuk bank, bukan cash
                if (this.props.default_cash_details && pmId !== this.props.default_cash_details.id) {
                    this.state.payments[pmId].expected = line.expected_amount || 0.0;
                }
            });

            // 5. Set expected untuk CASH = Total Modal + Cash Payment Amount
            if (this.pos.config.cash_control && this.props.default_cash_details) {
                const cashPmId = this.props.default_cash_details.id;
                console.log("🔍 Cash Payment Method ID:", cashPmId);
                console.log("🔍 props.default_cash_details.amount:", this.props.default_cash_details.amount);
                
                if (!this.state.payments[cashPmId]) {
                    this.state.payments[cashPmId] = { counted: "0" };
                }
                
                // ✅ PENTING: Expected Cash = Total Modal + Cash Payment dari PoS
                const cashPaymentAmount = this.props.default_cash_details.payment_amount || 0.0;
                const cashMoves = this.props.default_cash_details.moves 
                    ? this.props.default_cash_details.moves.reduce((sum, move) => sum + move.amount, 0.0)
                    : 0.0;
                
                // Expected = Total Modal + Cash Payment + Cash Moves
                this.state.payments[cashPmId].expected = totalModal + cashPaymentAmount + cashMoves;
                
                console.log("🔍 Final Expected Cash:", this.state.payments[cashPmId].expected);
                console.log("🔍 Breakdown - Modal:", totalModal, "Payment:", cashPaymentAmount, "Moves:", cashMoves);
            }
            
            console.log("🔍 Final state.payments:", this.state.payments);
        } catch (error) {
            console.error("❌ Error loading expected from end shift:", error);
        }
    },

    getDifference(paymentId) {
        const counted = parseFloat(this.state.payments[paymentId]?.counted || 0);
        const expected = this.state.payments[paymentId]?.expected || 0.0;
        return counted - expected;
    },

    async closeSession() {
        // 1. Check for unclosed shifts
        const unclosedShifts = await this.orm.call(
            "end.shift",
            "check_unclosed_shifts",
            [this.pos.pos_session.id]
        );

        if (unclosedShifts && unclosedShifts.length > 0) {
            await this.popup.add(ErrorPopup, {
                title: _t("Cannot Close Session"),
                body: _t(
                    "There are shifts that are not done yet. Please confirm all the shifts before closing the session."
                ),
            });
            return;
        }

        // 2. Show CustomNumpadPopup for cash counted
        console.log("🎯 Showing CustomNumpadPopUp...");
        
        const { confirmed, payload } = await this.popup.add(CustomNumpadPopUp, {
            title: _t("Close Session"),
            confirmText: _t("Confirm"),
            cancelText: _t("Cancel"),
            value: this.pos.config.cash_control
                ? parseFloat(
                      this.state.payments[this.props.default_cash_details.id]?.counted || "0"
                  ) || 0
                : 0,
        });

        console.log("📝 CustomNumpadPopUp result:", { confirmed, payload });

        if (!confirmed) {
            console.log("❌ User cancelled closing session");
            return;
        }

        // 3. Update counted cash value
        if (this.pos.config.cash_control && payload !== undefined && payload !== null) {
            const numericValue = parseFloat(payload);
            if (!isNaN(numericValue)) {
                this.state.payments[this.props.default_cash_details.id].counted =
                    numericValue.toString();
                console.log("💵 Updated counted cash:", numericValue);
            }
        }

        // 4. Customer display update
        this.customerDisplay?.update({ closeUI: true });
        
        // 5. Push orders
        console.log("📤 Pushing orders...");
        const syncSuccess = await this.pos.push_orders_with_closing_popup();
        if (!syncSuccess) {
            console.log("❌ Failed to sync orders");
            return;
        }
        console.log("✅ Orders synced successfully");

        // 6. Post closing cash details
        if (this.pos.config.cash_control) {
            const countedCash = parseFloat(
                this.state.payments[this.props.default_cash_details.id].counted
            );
            
            console.log("💰 Posting closing cash details:", countedCash);
            
            try {
                const response = await this.orm.call(
                    "pos.session",
                    "post_closing_cash_details",
                    [this.pos.pos_session.id],
                    {
                        counted_cash: countedCash,
                    }
                );
                
                if (!response.successful) {
                    console.log("❌ Failed to post cash details:", response);
                    return this.handleClosingError(response);
                }
                
                console.log("✅ Cash details posted successfully");
            } catch (error) {
                console.error("❌ Error posting cash details:", error);
                throw error;
            }
        }

        // 7. Update closing control state
        console.log("📝 Updating closing control state...");
        try {
            await this.orm.call("pos.session", "update_closing_control_state_session", [
                this.pos.pos_session.id,
                this.state.notes,
            ]);
            console.log("✅ Closing control state updated");
        } catch (error) {
            if (!error.data || error.data.message !== "This session is already closed.") {
                console.error("❌ Error updating closing state:", error);
                throw error;
            }
            console.log("⚠️ Session already closed (expected)");
        }

        // 8. Close session from UI
        console.log("🔐 Closing session...");
        try {
            const bankPaymentMethodDiffPairs = this.props.other_payment_methods
                .filter((pm) => pm.type == "bank")
                .map((pm) => [pm.id, this.getDifference(pm.id)]);
                
            console.log("💳 Bank payment differences:", bankPaymentMethodDiffPairs);
            
            const response = await this.orm.call("pos.session", "close_session_from_ui", [
                this.pos.pos_session.id,
                bankPaymentMethodDiffPairs,
            ]);
            
            if (!response.successful) {
                console.log("❌ Failed to close session:", response);
                return this.handleClosingError(response);
            }
            
            console.log("✅ Session closed successfully, redirecting to backend...");
            this.pos.redirectToBackend();
        } catch (error) {
            console.error("❌ Error closing session:", error);
            if (error instanceof ConnectionLostError) throw error;
            await this.popup.add(ErrorPopup, {
                title: _t("Closing session error"),
                body: _t(
                    "An error has occurred when trying to close the session.\nYou will be redirected to the back-end to manually close the session."
                ),
            });
            this.pos.redirectToBackend();
        }
    },
});