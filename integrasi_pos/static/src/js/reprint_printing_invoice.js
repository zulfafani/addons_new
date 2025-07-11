/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { ReprintReceiptScreen } from "@point_of_sale/app/screens/receipt_screen/reprint_receipt_screen";
import { useService } from "@web/core/utils/hooks";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";

// Function to remove circular references and reduce payload size
function getCircularReplacer() {
    const seen = new WeakSet();
    return (key, value) => {
        // Skip certain large or unnecessary properties that might cause circular refs
        if (key === '_super' || 
            key === '__proto__' || 
            key === 'constructor' ||
            key === 'rules' ||
            key === 'program_id' ||
            key === 'parent' ||
            key === 'children' ||
            key === '_origin' ||
            key === 'env' ||
            key === 'model' ||
            key === '_fields' ||
            key === '_cache' ||
            key === '_context') {
            return undefined;
        }
        
        // Skip functions
        if (typeof value === 'function') {
            return undefined;
        }
        
        if (typeof value === "object" && value !== null) {
            if (seen.has(value)) {
                return '[Circular]';
            }
            seen.add(value);
        }
        return value;
    };
}

// Function to extract only essential data for printing - SAFER VERSION
function extractEssentialReceiptData(orderData) {
    console.log("🔍 Extracting essential data...");
    
    // Use a safer approach to access nested properties
    function safeGet(obj, path, defaultValue = null) {
        try {
            return path.split('.').reduce((current, key) => current?.[key], obj) || defaultValue;
        } catch (error) {
            console.warn(`Safe get failed for path ${path}:`, error);
            return defaultValue;
        }
    }
    
    // Extract basic order info safely
    const orderInfo = {
        name: orderData.name || orderData.pos_reference || 'N/A',
        date_order: orderData.date_order || orderData.creation_date || new Date().toISOString(),
        pos_session_id: safeGet(orderData, 'pos_session_id'),
        user_id: safeGet(orderData, 'user_id'),
        cashier: safeGet(orderData, 'cashier') || safeGet(orderData, 'employee.name') || 'Administrator'
    };
    
    // Extract company info safely  
    const companyInfo = {
        name: safeGet(orderData, 'company.name') || orderData.company_name || 'PT. KEMANG KARSA LESTARI',
        street: safeGet(orderData, 'company.street') || orderData.company_street || 'JALAN KEMANG RAYA NO. 3',
        city: safeGet(orderData, 'company.city') || orderData.company_city || 'JAKARTA', 
        phone: safeGet(orderData, 'company.phone') || orderData.company_phone,
        email: safeGet(orderData, 'company.email') || orderData.company_email,
        vat: safeGet(orderData, 'company.vat') || orderData.company_vat,
        country: safeGet(orderData, 'company.country') || orderData.company_country || 'Indonesia'
    };
    
    // Extract orderlines safely
    const orderlines = [];
    if (orderData.orderlines && Array.isArray(orderData.orderlines)) {
        orderData.orderlines.forEach((line, index) => {
            try {
                orderlines.push({
                    product_name: line.product_name_wrapped || line.display_name || line.product_name || line.full_product_name || `Product ${index + 1}`,
                    quantity: parseFloat(line.qty || line.quantity || 1),
                    price_unit: parseFloat(line.price_unit || 0),
                    price_subtotal: parseFloat(line.price_subtotal || 0),
                    price_subtotal_incl: parseFloat(line.price_subtotal_incl || 0),
                    discount: parseFloat(line.discount || 0),
                    unit_name: line.unit_name || 'Units'
                });
            } catch (error) {
                console.warn(`Error processing orderline ${index}:`, error);
            }
        });
    }
    
    // Extract payment lines safely
    const paymentlines = [];
    if (orderData.paymentlines && Array.isArray(orderData.paymentlines)) {
        orderData.paymentlines.forEach((payment, index) => {
            try {
                paymentlines.push({
                    payment_method: {
                        name: safeGet(payment, 'payment_method.name') || payment.name || `Payment ${index + 1}`
                    },
                    amount: parseFloat(payment.amount || 0)
                });
            } catch (error) {
                console.warn(`Error processing payment ${index}:`, error);
            }
        });
    }
    
    // Extract totals safely
    const totals = {
        total_with_tax: parseFloat(orderData.total_with_tax || orderData.amount_total || 0),
        total_without_tax: parseFloat(orderData.total_without_tax || orderData.amount_untaxed || 0),  
        total_tax: parseFloat(orderData.total_tax || orderData.amount_tax || 0),
        change: parseFloat(orderData.change || orderData.amount_return || 0),
        total_paid: parseFloat(orderData.total_paid || orderData.amount_paid || 0)
    };
    
    // Extract tax details safely
    const taxDetails = [];
    if (orderData.tax_details && Array.isArray(orderData.tax_details)) {
        orderData.tax_details.forEach((tax, index) => {
            try {
                taxDetails.push({
                    tax: {
                        amount: parseFloat(safeGet(tax, 'tax.amount') || 11)
                    },
                    amount: parseFloat(tax.amount || 0),
                    base_amount: parseFloat(tax.base_amount || 0)
                });
            } catch (error) {
                console.warn(`Error processing tax detail ${index}:`, error);
            }
        });
    }
    
    // Extract loyalty info safely
    const loyaltyInfo = {
        points_won: safeGet(orderData, 'loyalty.points_won'),
        points_total: safeGet(orderData, 'loyalty.points_total'),
        customer_name: safeGet(orderData, 'partner_id.name') || orderData.customer_name
    };
    
    const result = {
        order: orderInfo,
        company: companyInfo,
        orderlines: orderlines,
        paymentlines: paymentlines,
        ...totals,
        tax_details: taxDetails,
        loyalty: loyaltyInfo
    };
    
    console.log("✅ Essential data extracted successfully");
    return result;
}

patch(ReprintReceiptScreen.prototype, {
    async printViaLocalhost() {
        try {
            console.log("🖨️ Starting localhost print...");
            
            let requestPayload;
            
            // Option 1: Try to get the HTML content from the receipt (safest method)
            const receiptElement = document.querySelector('.pos-receipt');
            if (receiptElement) {
                console.log("📄 Using HTML content from receipt element");
                requestPayload = {
                    html: receiptElement.outerHTML
                };
            } else {
                console.log("📄 Using order data extraction...");
                
                // Get the original order data with safe serialization
                let fullOrderData;
                try {
                    fullOrderData = this.props.order.export_for_printing();
                    console.log("📦 Raw order data extracted successfully");
                } catch (error) {
                    console.error("❌ Error getting order data:", error);
                    throw new Error("Failed to extract order data");
                }
                
                // Extract only essential data to reduce payload size and avoid circular refs
                const essentialData = extractEssentialReceiptData(fullOrderData);
                
                // Test if essential data can be serialized safely
                let serializedSize;
                try {
                    const testSerialization = JSON.stringify(essentialData, getCircularReplacer());
                    serializedSize = testSerialization.length;
                    console.log("📦 Essential data size:", serializedSize);
                } catch (serializeError) {
                    console.error("❌ Serialization test failed:", serializeError);
                    throw new Error("Data contains circular references that cannot be resolved");
                }
                
                requestPayload = {
                    receipt: essentialData
                };
            }
            
            console.log("📤 Sending request to printer server...");
            
            const response = await fetch("http://localhost:3001/print", {
                method: "POST",
                headers: { 
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                },
                body: JSON.stringify(requestPayload, getCircularReplacer()),
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`Server error: ${response.status} - ${errorText}`);
            }
            
            const result = await response.text();
            console.log("✅ Print successful:", result);
            
            // Show success notification if available
            if (this.env.services.notification) {
                this.env.services.notification.add("Receipt printed successfully!", {
                    type: "success",
                });
            }
            
        } catch (error) {
            console.error("❌ Print error:", error);
            
            let errorMessage = "Unable to print receipt via localhost.";
            
            if (error.message.includes('circular structure')) {
                errorMessage = "Receipt data contains circular references. Please try again or contact support.";
            } else if (error.message.includes('413') || error.message.includes('PayloadTooLargeError')) {
                errorMessage = "Receipt data too large. Please try again or contact support.";
            } else if (error.message.includes('ECONNREFUSED')) {
                errorMessage = "Cannot connect to printer server. Please ensure the print server is running on localhost:3001.";
            } else if (error.message.includes('Failed to fetch')) {
                errorMessage = "Network error. Please check your connection to the printer server.";
            } else if (error.message) {
                errorMessage = error.message;
            }
            
            await this.env.services.popup.add(ErrorPopup, {
                title: "Print Error",
                body: errorMessage,
            });
        }
    },
    
    // Add a method to test the connection
    async testPrinterConnection() {
        try {
            const response = await fetch("http://localhost:3001/test", {
                method: "GET",
                headers: { "Accept": "text/plain" }
            });
            
            if (response.ok) {
                const message = await response.text();
                console.log("✅ Printer server test:", message);
                return true;
            }
            return false;
        } catch (error) {
            console.error("❌ Printer server test failed:", error);
            return false;
        }
    }
});