/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { Order } from "@point_of_sale/app/store/models";

// 🩹 Patch to reorder reward lines after associated product
patch(Order.prototype, {
    
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
        const productName = productLine.get_full_product_name();
        
        // Extract keywords from product name
        const productKeywords = this._extractProductKeywords(productName);
        
        // Check if reward contains significant keywords from product
        let matchCount = 0;
        for (const keyword of productKeywords) {
            if (rewardName.includes(keyword)) {
                matchCount++;
            }
        }
        
        // Consider it a match if at least 50% of keywords are found
        // or if there are 2+ keyword matches
        return matchCount >= Math.max(2, Math.ceil(productKeywords.length * 0.5));
    },

    /**
     * Reorder orderlines so reward lines appear right after their associated products
     */
    reorderRewardLines() {
        const lines = [...this.orderlines];
        const mainProducts = [];
        const rewardLines = [];

        // Separate main products from reward lines
        for (const line of lines) {
            const name = line.get_full_product_name();
            const price = line.get_price_with_tax();

            // Identify reward lines by negative price or specific patterns
            if (price < 0 || 
                name.toLowerCase().includes("% on") || 
                name.toLowerCase().includes("per point on") ||
                name.toLowerCase().includes("discount") ||
                name.toLowerCase().includes("reward")) {
                rewardLines.push(line);
            } else {
                mainProducts.push(line);
            }
        }

        const reordered = [];
        const usedRewards = new Set();

        // For each main product, add it followed by its rewards
        for (const productLine of mainProducts) {
            reordered.push(productLine);
            
            // Find rewards that match this product
            const matchingRewards = rewardLines.filter((rewardLine, index) => {
                if (usedRewards.has(index)) return false;
                return this._isRewardForProduct(rewardLine, productLine);
            });

            // Add matching rewards and mark them as used
            for (const rewardLine of matchingRewards) {
                const rewardIndex = rewardLines.indexOf(rewardLine);
                usedRewards.add(rewardIndex);
                reordered.push(rewardLine);
            }
        }

        // Add any unused reward lines at the end
        for (let i = 0; i < rewardLines.length; i++) {
            if (!usedRewards.has(i)) {
                reordered.push(rewardLines[i]);
            }
        }

        // Update orderlines
        this.orderlines.reset();
        for (const line of reordered) {
            this.orderlines.add(line);
        }
    },

    /**
     * Automatically reorder when orderlines change
     */
    add_orderline(line) {
        super.add_orderline(line);
        // Only reorder if this looks like a reward line
        const name = line.get_full_product_name();
        const price = line.get_price_with_tax();
        
        if (price < 0 || 
            name.toLowerCase().includes("% on") || 
            name.toLowerCase().includes("per point on")) {
            // Delay reordering to avoid conflicts during batch operations
            setTimeout(() => this.reorderRewardLines(), 10);
        }
    }
});