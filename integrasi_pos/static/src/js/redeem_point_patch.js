/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { RedeemPointButton } from "./redeem_point";
import { _t } from "@web/core/l10n/translation";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { ConfirmPopup } from "@point_of_sale/app/utils/confirm_popup/confirm_popup";

patch(RedeemPointButton.prototype, {
    
    async redeemPoints(payload) {
        try {
            const order = this.pos.get_order();
            const { points, card_id, program_id, conversion_rate, discount_amount } = payload;

            console.group("🔍 IMMEDIATE REDEMPTION");
            console.log("1. Payload:", payload);

            // ✅ VALIDASI: Order belum pernah redeem
            if (order.loyalty_points_redeemed) {
                console.error("❌ Order already redeemed!");
                console.groupEnd();
                
                await this.popup.add(ErrorPopup, {
                    title: _t("Already Redeemed"),
                    body: _t("Points can only be redeemed once per order."),
                });
                return;
            }

            // Cari program
            const program = this.pos.program_by_id[program_id];
            if (!program) {
                console.error("❌ Program not found!");
                console.groupEnd();
                await this.popup.add(ErrorPopup, {
                    title: _t("Program Not Found"),
                    body: _t("Loyalty program not available in POS."),
                });
                return;
            }

            console.log("2. Program found:", program.name);
            
            const conversionRate = program.vit_konversi_poin || payload.conversion_rate || 1000;
            console.log("3. Using conversion rate:", conversionRate);

            // 🔥 Cari reward dengan discount_mode = 'per_point'
            const perPointReward = program.rewards.find(r => 
                r.reward_type === 'discount' && 
                r.discount_mode === 'per_point'
            );
            
            if (!perPointReward) {
                console.error("❌ No per_point reward found!");
                console.groupEnd();
                
                await this.popup.add(ErrorPopup, {
                    title: _t("Reward Not Configured"),
                    body: _t("Please configure a 'Per Point' discount reward for this loyalty program."),
                });
                return;
            }

            console.log("4. Reward found:", {
                id: perPointReward.id,
                description: perPointReward.description,
                discount_mode: perPointReward.discount_mode,
                discount: perPointReward.discount
            });

            // 🔥 CRITICAL: DEDUCT POINTS IMMEDIATELY VIA BACKEND
            console.log("5. 🔥 Deducting points from backend...");
            
            const deductResult = await this.orm.call(
                'pos.session',
                'pos_redeem_loyalty_points',
                [this.pos.pos_session.id, card_id, points, order.uid]
            );

            console.log("6. Deduct result:", deductResult);

            if (!deductResult.success) {
                console.error("❌ Deduction failed:", deductResult.error);
                console.groupEnd();
                
                await this.popup.add(ErrorPopup, {
                    title: _t("Redemption Failed"),
                    body: _t("Failed to deduct points: ") + deductResult.error,
                });
                return;
            }

            console.log("7. ✅ Points deducted successfully!");
            console.log("   - Old Balance:", deductResult.old_balance);
            console.log("   - Deducted:", deductResult.points_deducted);
            console.log("   - New Balance:", deductResult.new_balance);

            // 🔥 UPDATE LOCAL CACHE untuk update UI
            const loyaltyCard = this.pos.couponCache[card_id];
            if (loyaltyCard) {
                loyaltyCard.balance = deductResult.new_balance;
                console.log("8. ✅ Local cache updated:", loyaltyCard);
            }

            // 🔥 TRACK REDEMPTION di order untuk rollback
            order.loyalty_points_redeemed = {
                card_id: card_id,
                points: points,
                timestamp: new Date().toISOString(),
                old_balance: deductResult.old_balance,
                new_balance: deductResult.new_balance,
            };

            console.log("9. Applying reward to order...");

            // Apply reward menggunakan Odoo core
            const result = order._applyReward(perPointReward, card_id, {
                cost: points,
            });

            console.log("10. _applyReward result:", result);

            if (result !== true) {
                console.error("❌ Apply reward failed! Rolling back points...");
                
                // 🔥 ROLLBACK: Kembalikan points jika apply reward gagal
                await this._rollbackPoints(order);
                
                console.groupEnd();
                await this.popup.add(ErrorPopup, {
                    title: _t("Apply Reward Failed"),
                    body: _t("Error: ") + (typeof result === 'string' ? result : JSON.stringify(result)),
                });
                return;
            }

            console.log("11. Updating rewards...");
            order._updateRewards();

            console.log("12. Checking order lines:");
            const allLines = order.get_orderlines();
            const rewardLines = allLines.filter(l => l.is_reward_line);
            
            console.log("Reward lines:", rewardLines.map(l => ({
                product: l.product.display_name,
                price: l.price,
                quantity: l.quantity,
                total: l.get_price_with_tax()
            })));

            if (rewardLines.length === 0) {
                console.error("❌ No reward line created! Rolling back points...");
                
                // 🔥 ROLLBACK: Kembalikan points
                await this._rollbackPoints(order);
                
                console.groupEnd();
                await this.popup.add(ErrorPopup, {
                    title: _t("Reward Not Applied"),
                    body: _t("Discount line tidak terbuat. Points telah dikembalikan."),
                });
                return;
            }

            console.log("✅ Redemption successful!");
            console.groupEnd();

            // Success notification dengan info balance
            this.env.services.pos_notification.add(
                _t(`Successfully redeemed ${points} points!\n` +
                   `Discount: ${this.env.utils.formatCurrency(discount_amount)}\n` +
                   `New Balance: ${deductResult.new_balance} pts`),
                { type: 'success', duration: 5000 }
            );

        } catch (error) {
            console.error("❌ CRITICAL ERROR:", error);
            console.error("Stack:", error.stack);
            console.groupEnd();
            
            // Try rollback on critical error
            try {
                await this._rollbackPoints(this.pos.get_order());
            } catch (rollbackError) {
                console.error("❌ Rollback also failed:", rollbackError);
            }
            
            await this.popup.add(ErrorPopup, {
                title: _t("Error"),
                body: _t("An error occurred: ") + error.message,
            });
        }
    },

    /**
     * Rollback points ke loyalty card
     */
    async _rollbackPoints(order) {
        if (!order.loyalty_points_redeemed) {
            return;
        }

        console.log("🔄 Rolling back points...");

        try {
            const { card_id, points } = order.loyalty_points_redeemed;
            
            const rollbackResult = await this.orm.call(
                'pos.session',
                'pos_rollback_loyalty_points',
                [this.pos.pos_session.id, card_id, points, order.uid]
            );

            if (rollbackResult.success) {
                console.log("✅ Points rolled back successfully:", rollbackResult);
                
                // Update local cache
                const loyaltyCard = this.pos.couponCache[card_id];
                if (loyaltyCard) {
                    loyaltyCard.balance = rollbackResult.new_balance;
                }
                
                // Clear redemption tracking
                delete order.loyalty_points_redeemed;
            } else {
                console.error("❌ Rollback failed:", rollbackResult.error);
            }
        } catch (error) {
            console.error("❌ Rollback error:", error);
        }
    },
});