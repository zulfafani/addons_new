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
        super.setup();
        if (!this.popup) {
            this.popup = useService("popup");
        }
        if (!this.rpc) {
            this.rpc = useService("rpc");  // 👈 this is the missing part
        }
    },
    

    async closeSession() {
        const unclosedShifts = await this.orm.call(
            "end.shift",
            "check_unclosed_shifts",
            [this.pos.pos_session.id]
        );

        if (unclosedShifts && unclosedShifts.length > 0) {
            await this.popup.add(ErrorPopup, {
                title: _t("Cannot Close Session"),
                body: _t("There are still cashier shifts that are not closed. Please close all cashier shifts before closing the session."),
            });
            return;
        }

        // 🔒 Manager validation logic
        const config = this.pos.config;

        const managerValidation = config?.manager_validation;
        const validateCloseSession = config?.validate_close_session;

        if (managerValidation && validateCloseSession) {
            await this.popup.add(ErrorPopup, {
                title: _t("Manager Approval Required"),
                body: _t("Closing this session requires manager validation. Please get approval before proceeding."),
            });
        }

        // Show CustomNumpadPopup when closing session
        const { confirmed, payload } = await this.popup.add(CustomNumpadPopUp, {
            title: _t("Close Session"),
            confirmText: _t("Confirm"),
            cancelText: _t("Cancel"),
            value: this.pos.config.cash_control ?
                parseFloat(this.state.payments[this.props.default_cash_details.id].counted) || 0 : 0,
        });

        if (!confirmed) {
            return;
        }

        if (this.pos.config.cash_control && payload !== undefined && payload !== null) {
            const numericValue = parseFloat(payload);
            if (!isNaN(numericValue)) {
                this.state.payments[this.props.default_cash_details.id].counted =
                    numericValue.toString();
            }
        }

        this.customerDisplay?.update({ closeUI: true });
        const syncSuccess = await this.pos.push_orders_with_closing_popup();
        if (!syncSuccess) return;

        if (this.pos.config.cash_control) {
            const response = await this.orm.call(
                "pos.session",
                "post_closing_cash_details",
                [this.pos.pos_session.id],
                {
                    counted_cash: parseFloat(
                        this.state.payments[this.props.default_cash_details.id].counted
                    ),
                }
            );
            if (!response.successful) return this.handleClosingError(response);
        }

        try {
            await this.orm.call("pos.session", "update_closing_control_state_session", [
                this.pos.pos_session.id,
                this.state.notes,
            ]);
        } catch (error) {
            if (!error.data || error.data.message !== "This session is already closed.") {
                throw error;
            }
        }

        try {
            const bankPaymentMethodDiffPairs = this.props.other_payment_methods
                .filter((pm) => pm.type == "bank")
                .map((pm) => [pm.id, this.getDifference(pm.id)]);
            const response = await this.orm.call("pos.session", "close_session_from_ui", [
                this.pos.pos_session.id,
                bankPaymentMethodDiffPairs,
            ]);
            if (!response.successful) return this.handleClosingError(response);
            this.pos.redirectToBackend();
        } catch (error) {
            if (error instanceof ConnectionLostError) throw error;
            await this.popup.add(ErrorPopup, {
                title: _t("Closing session error"),
                body: _t(
                    "An error has occurred when trying to close the session.\nYou will be redirected to the back-end to manually close the session."
                ),
            });
            this.pos.redirectToBackend();
        }
    }
});