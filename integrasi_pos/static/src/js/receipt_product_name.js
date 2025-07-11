/** @odoo-module */

import { Orderline, Order } from "@point_of_sale/app/store/models";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { patch } from "@web/core/utils/patch";
import { _t } from "@web/core/l10n/translation";

// Patch untuk ProductScreen - format total dan tax display
patch(ProductScreen.prototype, {
    
    /**
     * Helper method untuk format currency tanpa desimal jika nilainya bulat
     */
    _formatCurrencyWithoutDecimals(amount) {
        // Jika amount adalah bilangan bulat, tampilkan tanpa desimal
        if (amount % 1 === 0) {
            return this.env.utils.formatCurrency(amount).replace(/[.,]00$/, '');
        }
        // Jika ada desimal, tampilkan normal
        return this.env.utils.formatCurrency(amount);
    },

    /**
     * Helper method untuk format loyalty points (selalu dibulatkan)
     */
    _formatLoyaltyPoints(points) {
        return Math.round(parseFloat(points) || 0);
    },

    /**
     * Override getter total untuk format tanpa desimal jika bilangan bulat
     */
    get total() {
        const totalAmount = this.currentOrder?.get_total_with_tax() ?? 0;
        return this._formatCurrencyWithoutDecimals(totalAmount);
    },

    /**
     * Override getter selectedOrderlineTotal untuk format tanpa desimal jika bilangan bulat
     */
    get selectedOrderlineTotal() {
        const lineTotal = this.currentOrder.get_selected_orderline()?.get_display_price() ?? 0;
        return this._formatCurrencyWithoutDecimals(lineTotal);
    },

    /**
     * Getter untuk tax amount dengan format tanpa desimal
     */
    get taxAmount() {
        const taxTotal = this.currentOrder?.get_total_tax() ?? 0;
        return this._formatCurrencyWithoutDecimals(taxTotal);
    },

    /**
     * Getter untuk subtotal amount dengan format tanpa desimal
     */
    get subtotalAmount() {
        const subtotal = this.currentOrder?.get_total_without_tax() ?? 0;
        return this._formatCurrencyWithoutDecimals(subtotal);
    },

    /**
     * Getter untuk loyalty points (dibulatkan)
     */
    get loyaltyPoints() {
        const points = this.currentOrder?.getLoyaltyPoints?.() ?? 0;
        return this._formatLoyaltyPoints(points);
    },

    /**
     * Getter untuk points won (dibulatkan)
     */
    get pointsWon() {
        const points = this.currentOrder?.getPointsWon?.() ?? 0;
        return this._formatLoyaltyPoints(points);
    },

    /**
     * Getter untuk new total loyalty (dibulatkan)
     */
    get newTotalLoyalty() {
        const total = this.currentOrder?.getNewTotal?.() ?? 0;
        return this._formatLoyaltyPoints(total);
    }
});

// Patch untuk Orderline - nama produk dengan barcode/default_code dan format harga
patch(Orderline.prototype, {
    
    /**
     * Override method get_full_product_name untuk menampilkan barcode/default_code + nama produk
     * Format: "code - product_name"
     */
    get_full_product_name() {
        const productName = this.full_product_name || this.product.display_name;
        const barcode = this.product.barcode;
        const defaultCode = this.product.default_code;
        
        // Prioritas: barcode -> default_code -> nama produk saja
        if (barcode) {
            return `${barcode} - ${productName}`;
        } else if (defaultCode) {
            return `${defaultCode} - ${productName}`;
        }
        
        // Jika tidak ada barcode dan default_code, tampilkan nama produk saja
        return productName;
    },

    /**
     * Helper method untuk format currency tanpa desimal jika nilainya bulat
     */
    _formatCurrencyWithoutDecimals(amount) {
        // Jika amount adalah bilangan bulat, tampilkan tanpa desimal
        if (amount % 1 === 0) {
            return this.env.utils.formatCurrency(amount).replace(/[.,]00$/, '');
        }
        // Jika ada desimal, tampilkan normal
        return this.env.utils.formatCurrency(amount);
    },

    /**
     * Override getDisplayData untuk memastikan productName dan format harga sesuai keinginan
     */
    getDisplayData() {
        const displayData = super.getDisplayData();
        
        // Update productName dengan format barcode + nama
        displayData.productName = this.get_full_product_name();
        
        // Update format harga unit tanpa desimal jika bilangan bulat
        displayData.unitPrice = this._formatCurrencyWithoutDecimals(this.get_unit_display_price());
        
        // Update format harga total tanpa desimal jika bilangan bulat
        displayData.price = this.get_discount_str() === "100"
            ? _t("Free")
            : (this.comboLines && this.comboLines.length > 0)
            ? ""
            : this._formatCurrencyWithoutDecimals(this.get_display_price());
        
        // Update old unit price jika ada
        if (displayData.oldUnitPrice) {
            displayData.oldUnitPrice = this._formatCurrencyWithoutDecimals(this.get_old_unit_display_price());
        }
        
        // Update price without discount
        if (displayData.price_without_discount) {
            displayData.price_without_discount = this._formatCurrencyWithoutDecimals(this.getUnitDisplayPriceBeforeDiscount());
        }
        
        return displayData;
    }
});

// Patch untuk Order - format untuk receipt printing, loyalty points, dan reorder reward lines
patch(Order.prototype, {
    
    /**
     * Helper method untuk format currency tanpa desimal jika nilainya bulat
     */
    _formatCurrencyWithoutDecimals(amount) {
        // Jika amount adalah bilangan bulat, tampilkan tanpa desimal
        if (amount % 1 === 0) {
            return this.env.utils.formatCurrency(amount).replace(/[.,]00$/, '');
        }
        // Jika ada desimal, tampilkan normal
        return this.env.utils.formatCurrency(amount);
    },

    /**
     * Helper method untuk format loyalty points (selalu dibulatkan)
     */
    _formatLoyaltyPoints(points) {
        return Math.round(parseFloat(points) || 0);
    },

    /**
     * Extract product key words from product name for matching
     */
    _extractProductKeywords(productName) {
        // Remove barcode/default_code prefix (format: "code - name")
        const nameWithoutCode = productName.includes(' - ') 
            ? productName.split(' - ').slice(1).join(' - ')
            : productName;
        
        // Convert to lowercase and split into words
        return nameWithoutCode.toLowerCase()
            .replace(/[^\w\s]/g, ' ') // Replace special chars with space
            .split(/\s+/)
            .filter(word => word.length > 2); // Only words longer than 2 chars
    },

    /**
     * Check if reward line matches product line
     */
    _isRewardForProduct(rewardLine, productLine) {
        const rewardName = rewardLine.get_full_product_name().toLowerCase();
        const productName = productLine.get_full_product_name().toLowerCase();
        
        // Remove codes and get clean product names
        const cleanRewardName = rewardName.includes(' - ') 
            ? rewardName.split(' - ').slice(1).join(' - ')
            : rewardName;
        const cleanProductName = productName.includes(' - ') 
            ? productName.split(' - ').slice(1).join(' - ')
            : productName;
        
        // Remove discount pattern from reward name for comparison
        const rewardProductName = cleanRewardName
            .replace(/^\d+%\s*(on|discount)\s*/i, '') // Remove "10% on " or "10% discount "
            .replace(/\s*\(.*?\)\s*/g, '') // Remove parentheses content
            .trim();
        
        // Check if the reward product name matches the actual product name
        return rewardProductName === cleanProductName || 
               cleanProductName.includes(rewardProductName) ||
               rewardProductName.includes(cleanProductName);
    },

    /**
     * Reorder orderlines so reward lines appear right after their associated products
     */
    reorderRewardLines() {
        const lines = [...this.orderlines];
        const mainProducts = [];
        const specificRewards = []; // Reward dengan nama produk spesifik
        const genericRewards = [];  // Reward dengan nama umum

        // Separate main products from reward lines
        for (const line of lines) {
            const name = line.get_full_product_name();
            const price = line.get_price_with_tax();

            // Identify reward lines by negative price or specific patterns
            if (price < 0 || 
                name.toLowerCase().includes("% on") || 
                name.toLowerCase().includes("per point on") ||
                name.toLowerCase().includes("discount on") ||
                name.toLowerCase().includes("reward")) {
                
                // Cek apakah reward spesifik atau umum
                if (name.toLowerCase().includes('specific products') || 
                    name.toLowerCase().includes('applicable products') ||
                    name.toLowerCase().includes('selected products') ||
                    name.toLowerCase().includes('eligible products')) {
                    genericRewards.push(line);
                } else {
                    specificRewards.push(line);
                }
            } else {
                mainProducts.push(line);
            }
        }

        const reordered = [];
        const usedRewards = new Set();

        // For each main product, add it followed by its specific rewards
        for (const productLine of mainProducts) {
            reordered.push(productLine);
            
            // Find specific rewards that match this product
            for (let i = 0; i < specificRewards.length; i++) {
                if (usedRewards.has(i)) continue;
                
                const rewardLine = specificRewards[i];
                if (this._isRewardForProduct(rewardLine, productLine)) {
                    reordered.push(rewardLine);
                    usedRewards.add(i);
                }
            }
        }

        // Add unused specific rewards at the end
        for (let i = 0; i < specificRewards.length; i++) {
            if (!usedRewards.has(i)) {
                reordered.push(specificRewards[i]);
            }
        }

        // Add generic rewards at the very end
        for (const genericReward of genericRewards) {
            reordered.push(genericReward);
        }

        // Only update if order actually changed
        let orderChanged = false;
        if (reordered.length === lines.length) {
            for (let i = 0; i < reordered.length; i++) {
                if (reordered[i].cid !== lines[i].cid) {
                    orderChanged = true;
                    break;
                }
            }
        } else {
            orderChanged = true;
        }

        if (orderChanged) {
            // Update orderlines
            this.orderlines.reset();
            for (const line of reordered) {
                this.orderlines.add(line);
            }
        }
    },

    /**
     * Override add_orderline to automatically reorder when any line is added
     */
    add_orderline(line) {
        super.add_orderline(line);
        
        // Always try to reorder after adding any line, but with longer delay
        // to ensure all related operations are complete
        setTimeout(() => {
            if (this.orderlines.length > 1) {
                this.reorderRewardLines();
            }
        }, 100);
    },

    /**
     * Override export_for_printing untuk format receipt
     */
    export_for_printing() {
        const printData = super.export_for_printing();
        
        // Format amount_total tanpa desimal jika bilangan bulat (untuk receipt)
        const originalAmountTotal = printData.amount_total;
        printData.amount_total_clean = this._formatCurrencyWithoutDecimals(originalAmountTotal);
        
        // Format amount_tax tanpa desimal jika bilangan bulat (untuk receipt)  
        const originalAmountTax = printData.amount_tax;
        printData.amount_tax_clean = this._formatCurrencyWithoutDecimals(originalAmountTax);
        
        // Jika ada loyalty points, bulatkan (untuk receipt)
        if (printData.loyalty_points) {
            printData.loyalty_points_rounded = this._formatLoyaltyPoints(printData.loyalty_points);
        }
        
        if (printData.points_won) {
            printData.points_won_rounded = this._formatLoyaltyPoints(printData.points_won);
        }
        
        if (printData.new_total) {
            printData.new_total_rounded = this._formatLoyaltyPoints(printData.new_total);
        }
        
        return printData;
    },

    /**
     * Override methods terkait loyalty points untuk pembulatan di UI
     */
    getLoyaltyPoints() {
        const points = super.getLoyaltyPoints?.() || 0;
        return Math.round(parseFloat(points));
    },
    
    getPointsWon() {
        const points = super.getPointsWon?.() || 0;
        return Math.round(parseFloat(points));
    },
    
    getNewTotal() {
        const total = super.getNewTotal?.() || 0;
        return Math.round(parseFloat(total));
    }
});