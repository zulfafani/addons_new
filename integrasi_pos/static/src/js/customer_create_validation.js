/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { _t } from "@web/core/l10n/translation";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { PartnerDetailsEdit } from "@point_of_sale/app/screens/partner_list/partner_editor/partner_editor";
import { useService } from "@web/core/utils/hooks";
import { onWillStart } from "@odoo/owl";
import { useState } from "@odoo/owl";

patch(PartnerDetailsEdit.prototype, {
    setup() {
        super.setup(...arguments);
        this.orm = useService("orm");
        
        // Add vit_customer_group to intFields for proper processing
        if (!this.intFields.includes('vit_customer_group')) {
            this.intFields.push('vit_customer_group');
        }
        
        // Add vit_customer_group to changes
        const partner = this.props.partner;
        this.changes.vit_customer_group = partner.vit_customer_group ? partner.vit_customer_group[0] : false;
        
        // ✅ Make changes reactive untuk memicu re-render
        this.changes = useState(this.changes);
        
        // Load customer groups
        this.customerGroups = [];
        
        onWillStart(async () => {
            await this.loadCustomerGroups();
        });
    },

    async loadCustomerGroups() {
        try {
            this.customerGroups = await this.orm.searchRead(
                "customer.group",
                [],
                ["id", "vit_group_name", "vit_pricelist_id"],
                { order: "vit_group_name ASC" }
            );
            console.log("Customer Groups loaded:", this.customerGroups);
        } catch (error) {
            console.error("Error loading customer groups:", error);
            this.customerGroups = [];
        }
    },

    async onCustomerGroupChange(ev) {
        const customerGroupId = parseInt(ev.target.value) || false;
        
        // ✅ Update dengan reactive state
        this.changes.vit_customer_group = customerGroupId;
        
        console.log("Customer Group Changed:", customerGroupId);
        
        if (customerGroupId) {
            // Find the selected customer group
            const selectedGroup = this.customerGroups.find(
                group => group.id === customerGroupId
            );
            
            console.log("Selected Group:", selectedGroup);
            
            if (selectedGroup && selectedGroup.vit_pricelist_id) {
                // ✅ Auto-fill pricelist (akan trigger re-render karena reactive)
                this.changes.property_product_pricelist = selectedGroup.vit_pricelist_id[0];
                console.log("Pricelist set to:", this.changes.property_product_pricelist);
                
                // ✅ Render kembali field pricelist secara eksplisit
                this.render();
            }
        } else {
            // Reset pricelist to default if customer group is cleared
            const partner = this.props.partner;
            const defaultPricelist = partner.property_product_pricelist 
                ? partner.property_product_pricelist[0] 
                : this.pos.config.pricelist_id[0];
            
            this.changes.property_product_pricelist = defaultPricelist;
            this.render();
        }
    },

    async saveChanges() {
        const processedChanges = {};
        for (const [key, value] of Object.entries(this.changes)) {
            if (this.intFields.includes(key)) {
                processedChanges[key] = parseInt(value) || false;
            } else {
                processedChanges[key] = value;
            }
        }

        console.log("=== DEBUG SAVE ===");
        console.log("Raw changes:", JSON.parse(JSON.stringify(this.changes)));
        console.log("Processed changes:", JSON.parse(JSON.stringify(processedChanges)));
        console.log("Phone value:", processedChanges.phone);
        console.log("Phone type:", typeof processedChanges.phone);

        // ✅ Validasi jika 'phone' wajib diisi
        const phoneValue = processedChanges.phone;
        if (!phoneValue || phoneValue === false || (typeof phoneValue === 'string' && phoneValue.trim() === "")) {
            console.log("Phone validation FAILED");
            return this.popup.add(ErrorPopup, {
                title: _t("Phone Number Is Required"),
                body: _t("Please enter a phone number before saving the customer."),
            });
        }
        console.log("Phone validation PASSED");

        // ✅ Auto-fill pricelist dari customer group sebelum save
        if (processedChanges.vit_customer_group) {
            const selectedGroup = this.customerGroups.find(
                group => group.id === processedChanges.vit_customer_group
            );
            
            if (selectedGroup && selectedGroup.vit_pricelist_id) {
                processedChanges.property_product_pricelist = selectedGroup.vit_pricelist_id[0];
                console.log("Saving with pricelist from customer group:", processedChanges.property_product_pricelist);
            }
        }

        // Validasi jika state tidak sesuai dengan country
        if (
            processedChanges.state_id &&
            this.pos.states.find((state) => state.id === processedChanges.state_id)?.country_id[0] !== processedChanges.country_id
        ) {
            processedChanges.state_id = false;
        }

        // Validasi jika nama kosong
        if ((!this.props.partner.name && !processedChanges.name) || processedChanges.name === "") {
            return this.popup.add(ErrorPopup, {
                title: _t("A Customer Name Is Required"),
            });
        }

        processedChanges.id = this.props.partner.id || false;
        
        console.log("Final processedChanges:", JSON.parse(JSON.stringify(processedChanges)));
        
        this.props.saveChanges(processedChanges);
    },

    isFieldCommercialAndPartnerIsChild(field) {
        // Pricelist is readonly when customer group is set
        if (field === 'property_product_pricelist' && this.changes.vit_customer_group) {
            return true;
        }
        
        return (
            this.pos.isChildPartner(this.props.partner) &&
            this.pos.partner_commercial_fields.includes(field)
        );
    }
});