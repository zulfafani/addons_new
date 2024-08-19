/** @odoo-module **/
import { _t } from "@web/core/l10n/translation";
import { Component } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { EndShiftSessionButtonPopup } from "./pop_up_end_shift"; // Import EndShiftSessionButtonPopup

class EndShiftSessionButton extends Component {
    static template = 'integrasi_pos.EndShiftSessionButton'; // Use the correct template ID

    setup() {
        this.pos = usePos();
        this.popup = useService("popup"); // Use the popup service defined in setup
    }

    async onClick() {
        // Directly show the EndShiftSessionButtonPopup without any validation
        await this.popup.add(EndShiftSessionButtonPopup, {
            title: _t("End Shift Session"),
            body: _t("Do you want to end the shift? Please fill in the details."),
        });
    }
}

// Add the control button to the ProductScreen
ProductScreen.addControlButton({
    component: EndShiftSessionButton, // Correctly reference the button component
    condition: () => true, // Make it always available
});
