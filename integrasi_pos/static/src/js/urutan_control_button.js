/** @odoo-module */

import { patch } from "@web/core/utils/patch";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";

// Patch untuk mengatur urutan control buttons
patch(ProductScreen.prototype, {
    
    get controlButtons() {
        const buttons = super.controlButtons;
        
        // Definisikan urutan button sesuai gambar
        const buttonOrder = [
            'RefundButton',              // 1
            'HoldTransactionButton',     // 2  
            'RecallTransactionButton',   // 3
            'DiscountButton',            // 4
            'DiscountAmountButton',      // 5 (atau posisi terakhir)
            'EnterCodeButton',           // 6
            'ResetProgramsButton',       // 7
            'RewardButton',              // 8
            'VoidSalesButton',           // 9
            'QuotationOrderButton',      // 10
            'SetSalespersonButton',      // 11
            'CustomerNoteButton',        // 12
            'DefaultPricelistButton',    // 13
            'CashRegisterButton',        // 14
            'CloseShiftButton',          // 15
        ];
        
        // Urutkan buttons berdasarkan buttonOrder
        const sortedButtons = [];
        
        // Tambahkan button sesuai urutan yang ditentukan
        buttonOrder.forEach(buttonName => {
            const button = buttons.find(b => b.name === buttonName);
            if (button) {
                sortedButtons.push(button);
            }
        });
        
        // Tambahkan button lain yang tidak ada dalam daftar (jika ada)
        buttons.forEach(button => {
            if (!buttonOrder.includes(button.name)) {
                sortedButtons.push(button);
            }
        });
        
        return sortedButtons;
    }
});