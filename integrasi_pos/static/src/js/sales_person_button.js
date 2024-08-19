/** @odoo-module **/
import { _t } from "@web/core/l10n/translation";
import { Component } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { SalesPersonButtonPopup } from "./pop_up_sales_person";

export class SalesPersonButton extends Component {
    static template = 'integrasi_pos.SalesPersonButton';

    setup() {
        this.pos = usePos();
        this.popup = useService("popup");
    }

    async onClick() {
        await this.popup.add(SalesPersonButtonPopup, {
            title: _t("Select Salesperson"),
        });
    }
}

ProductScreen.addControlButton({
    component: SalesPersonButton,
    condition: () => true,
});