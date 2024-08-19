/** @odoo-module **/

import { Component, onWillStart, useState } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { registry } from "@web/core/registry";

export class ProductChoicePopup extends Component {
    static template = "ProductChoicePopup";
    static props = ["title", "products"];
    static defaultProps = {
        title: "Pilih Produk",
    };

    setup() {
        this.popup = useService("popup");
        this.state = useState({ selectedId: null });
    }

    confirm() {
        const selectedProduct = this.props.products.find(p => p.id === this.state.selectedId);
        this.props.resolve({ confirmed: true, payload: selectedProduct });
    }

    cancel() {
        this.props.resolve({ confirmed: false });
    }
}

registry.category("popups").add("ProductChoicePopup", ProductChoicePopup);
