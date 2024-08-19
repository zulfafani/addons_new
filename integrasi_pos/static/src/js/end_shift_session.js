/** @odoo-module **/
import { _t } from "@web/core/l10n/translation";
import { Component } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { EndShiftSessionButtonPopup } from "./pop_up_end_shift";
import { CustomNumpadPopUp } from "./custom_numpad_popup";

class EndShiftSessionButton extends Component {
    static template = 'integrasi_pos.EndShiftSessionButton';

    setup() {
        this.pos = usePos();
        this.popup = useService("popup");
        this.rpc = useService("rpc");
    }

    async onClick() {
        const config = this.pos.config;
        const managerValidation = config.manager_validation;
        const validateEndShift = config.validate_end_shift;

        if (managerValidation && validateEndShift) {
            const { confirmed } = await this.popup.add(CustomNumpadPopUp, {
                title: _t("Manager Validation"),
                startingValue: "",
            });

            if (confirmed) {
                await this.popup.add(EndShiftSessionButtonPopup, {
                    title: _t("End Shift Session"),
                    body: _t("Do you want to end the shift? Please fill in the details."),
                });
            }
        } else {
            await this.popup.add(EndShiftSessionButtonPopup, {
                title: _t("End Shift Session"),
                body: _t("Do you want to end the shift? Please fill in the details."),
            });
        }
    }
}

// Tambahkan tombol ke ProductScreen
ProductScreen.addControlButton({
    component: EndShiftSessionButton,
    condition: () => true,
});
