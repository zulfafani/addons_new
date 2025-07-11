/** @odoo-module **/

import { Component, useState } from "@odoo/owl";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { _t } from "@web/core/l10n/translation";

export class CustomSalesPersonPopup extends Component {
    static template = "CustomSalesPersonPopup";
    static props = {
        title: String,
        list: Array,
        close: Function,
    };

    setup() {
        // ✅ PERBAIKAN: Gunakan useState untuk reactivity
        this.state = useState({
            searchQuery: "",
            selectedId: null,
        });
    }

    get filteredList() {
        if (!this.state.searchQuery) {
            return this.props.list;
        }
        const query = this.state.searchQuery.toLowerCase();
        return this.props.list.filter(item => 
            item.label.toLowerCase().includes(query)
        );
    }

    onSearchInput(ev) {
        this.state.searchQuery = ev.target.value;
    }

    selectItem(item) {
        // ✅ State akan otomatis trigger re-render
        this.state.selectedId = item.id;
    }

    confirm() {
        const selected = this.props.list.find(item => item.id === this.state.selectedId);
        this.props.close({ confirmed: true, payload: selected?.item });
    }

    cancel() {
        this.props.close({ confirmed: false, payload: null });
    }
}

export class SetProductListButton extends Component {
    static template = "SalesPersonButton";

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

        const { confirmed, payload: salesperson } = await this.popup.add(CustomSalesPersonPopup, {
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

ProductScreen.addControlButton({
    component: SetProductListButton,
    condition: () => true,
});