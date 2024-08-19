/** @odoo-module **/

import { Component } from "@odoo/owl";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { SelectionPopup } from "@point_of_sale/app/utils/input_popups/selection_popup";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { _t } from "@web/core/l10n/translation";

export class SetProductListButton extends Component {
    setup() {
        super.setup();
        this.pos = usePos();
        this.popup = this.env.services.popup;
    }

    get productsList() {
        return this.pos.db
            .get_product_by_category(this.pos.selectedCategoryId)
            .sort((a, b) => a.display_name.localeCompare(b.display_name));
    }

    async onClick() {
        const salespersonList = this.pos.hr_employee.map((s) => ({
            id: s.id,
            item: s,
            label: s.name,
            isSelected: false,
        }));

        const { confirmed, payload: salesperson } = await this.popup.add(SelectionPopup, {
            title: _t("Select the Salesperson"),
            list: salespersonList,
        });

        if (confirmed && salesperson) {
            const line = this.pos.selectedOrder.selected_orderline;
            if (line) {
                line.salesperson = String(salesperson.name);
                line.user_id = Number(salesperson.id);
            }
        }
    }
}

SetProductListButton.template = "SalesPersonButton";

ProductScreen.addControlButton({
    component: SetProductListButton,
    condition: () => true,
});
