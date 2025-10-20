/** @odoo-module */

import { patch } from "@web/core/utils/patch";
import { PartnerLine } from "@point_of_sale/app/screens/partner_list/partner_line/partner_line";
import { usePos } from "@point_of_sale/app/store/pos_hook";

patch(PartnerLine.prototype, {
    setup() {
        super.setup(...arguments);
        this.pos = usePos();
    },

    /**
     * Get customer group name from partner data
     * @param {Array|Number} customerGroup - Customer group data [id, name] or just id
     * @returns {String} Customer group name
     */
    getCustomerGroupName(customerGroup) {
        if (!customerGroup) {
            return '';
        }
        
        // If it's an array [id, name]
        if (Array.isArray(customerGroup) && customerGroup.length >= 2) {
            return customerGroup[1];
        }
        
        // If it's just an ID, try to find in loaded data
        if (typeof customerGroup === 'number') {
            // You might need to load customer groups in pos session
            // For now, return just the ID
            return `Group #${customerGroup}`;
        }
        
        return '';
    },

    /**
     * Get pricelist name from partner data
     * @param {Array|Number} pricelist - Pricelist data [id, name] or just id
     * @returns {String} Pricelist name
     */
    getPricelistName(pricelist) {
        if (!pricelist) {
            return '';
        }
        
        // If it's an array [id, name]
        if (Array.isArray(pricelist) && pricelist.length >= 2) {
            return pricelist[1];
        }
        
        // If it's just an ID, try to find in pos.pricelists
        if (typeof pricelist === 'number') {
            const pricelistObj = this.pos.pricelists.find(pl => pl.id === pricelist);
            if (pricelistObj) {
                return pricelistObj.name;
            }
            return `Pricelist #${pricelist}`;
        }
        
        return '';
    }
});