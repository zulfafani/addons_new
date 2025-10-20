/** @odoo-module */
import { SelectionPopup } from "@point_of_sale/app/utils/input_popups/selection_popup";
import { patch } from "@web/core/utils/patch";
import { useState } from "@odoo/owl";

patch(SelectionPopup.prototype, {
    setup() {
        super.setup();
        // Initialize search state only for cashier selection
        if (this.props.title && (this.props.title.includes("Cashier") || this.props.title.includes("cashier"))) {
            this.searchState = useState({
                searchQuery: "",
            });
        }
    },

    get filteredList() {
        // If not cashier selection or no search query, return original list
        if (!this.searchState || !this.searchState.searchQuery.trim()) {
            return this.props.list;
        }

        const query = this.searchState.searchQuery.toLowerCase().trim();
        
        // Filter list based on search query (case-insensitive)
        return this.props.list.filter(item => {
            return item.label && item.label.toLowerCase().includes(query);
        });
    },

    onSearchInput(event) {
        if (this.searchState) {
            this.searchState.searchQuery = event.target.value;
        }
    },

    clearSearch() {
        if (this.searchState) {
            this.searchState.searchQuery = "";
        }
    },

    get showSearchField() {
        // Show search field only for cashier selection popup
        return this.props.title && (this.props.title.includes("Cashier") || this.props.title.includes("cashier"));
    }
});