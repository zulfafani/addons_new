/** @odoo-module **/

import { Order } from "@point_of_sale/app/store/models";
import { patch } from "@web/core/utils/patch";

patch(Order.prototype, {
    
    /**
     * Override untuk handle rollback saat order direset
     */
    async _resetPrograms() {
        // Rollback points jika ada redemption
        if (this.loyalty_points_redeemed) {
            await this._rollbackLoyaltyPoints();
        }
        
        return super._resetPrograms(...arguments);
    },

    /**
     * Rollback loyalty points ke card
     */
    async _rollbackLoyaltyPoints() {
        if (!this.loyalty_points_redeemed) {
            return;
        }

        console.log("🔄 Order reset: Rolling back loyalty points...");

        try {
            const { card_id, points } = this.loyalty_points_redeemed;
            
            const rollbackResult = await this.env.services.orm.call(
                'pos.session',
                'pos_rollback_loyalty_points',
                [this.pos.pos_session.id, card_id, points, this.uid]
            );

            if (rollbackResult.success) {
                console.log("✅ Points rolled back on order reset:", rollbackResult);
                
                // Update local cache
                const loyaltyCard = this.pos.couponCache[card_id];
                if (loyaltyCard) {
                    loyaltyCard.balance = rollbackResult.new_balance;
                }
                
                // Clear redemption tracking
                delete this.loyalty_points_redeemed;
                
                // Notify user
                this.env.services.pos_notification.add(
                    `Points returned: ${points} pts\nNew Balance: ${rollbackResult.new_balance} pts`,
                    { type: 'info', duration: 3000 }
                );
            } else {
                console.error("❌ Rollback failed:", rollbackResult.error);
            }
        } catch (error) {
            console.error("❌ Rollback error:", error);
        }
    },

    /**
     * Export redemption info untuk persist
     */
    export_as_JSON() {
        const json = super.export_as_JSON(...arguments);
        json.loyalty_points_redeemed = this.loyalty_points_redeemed;
        return json;
    },

    /**
     * Import redemption info saat load order
     */
    init_from_JSON(json) {
        super.init_from_JSON(...arguments);
        this.loyalty_points_redeemed = json.loyalty_points_redeemed;
    },
});