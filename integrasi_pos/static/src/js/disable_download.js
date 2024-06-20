/** @odoo-module **/

import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";
import { registry } from "@web/core/registry";

// Override the shouldDownloadInvoice method
PaymentScreen.prototype.shouldDownloadInvoice = function () {
    return false; // Always return false to prevent invoice download
};

// No need to add the PaymentScreen to the registry again, just the override is sufficient
