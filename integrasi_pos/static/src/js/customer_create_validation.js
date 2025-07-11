/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { _t } from "@web/core/l10n/translation";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { PartnerDetailsEdit } from "@point_of_sale/app/screens/partner_list/partner_editor/partner_editor";

patch(PartnerDetailsEdit.prototype, {
    async saveChanges() {
        const processedChanges = {};
        for (const [key, value] of Object.entries(this.changes)) {
            if (this.intFields.includes(key)) {
                processedChanges[key] = parseInt(value) || false;
            } else {
                processedChanges[key] = value;
            }
        }

        // ✅ Validasi jika 'phone' wajib diisi
        if (!processedChanges.phone || processedChanges.phone.trim() === "") {
            return this.popup.add(ErrorPopup, {
                title: _t("Phone Number Is Required"),
                body: _t("Please enter a phone number before saving the customer."),
            });
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
        this.props.saveChanges(processedChanges);
    },
});
