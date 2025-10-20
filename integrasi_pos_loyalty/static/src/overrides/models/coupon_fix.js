/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { Order } from "@point_of_sale/app/store/models";
import { _t } from "@web/core/l10n/translation";
import { ConfirmPopup } from "@point_of_sale/app/utils/confirm_popup/confirm_popup";

// ========== PATCH ORDER: FIX COUPON ACTIVATION ==========
patch(Order.prototype, {
    /**
     * 🔑 OVERRIDE: Cek apakah coupon sudah diaktivasi di order SAAT INI
     * Izinkan re-scan jika order belum di-payment
     */
    async _activateCode(code) {
        const rule = this.pos.rules.find((rule) => {
            return rule.mode === "with_code" && (rule.promo_barcode === code || rule.code === code);
        });
        
        let claimableRewards = null;
        let coupon = null;
        
        if (rule) {
            // ========== PROMO CODE RULE (tidak berubah) ==========
            if (
                rule.program_id.date_from &&
                this.date_order < rule.program_id.date_from.startOf("day")
            ) {
                return _t("That promo code program is not yet valid.");
            }
            if (rule.program_id.date_to && this.date_order > rule.program_id.date_to.endOf("day")) {
                return _t("That promo code program is expired.");
            }
            const program_pricelists = rule.program_id.pricelist_ids;
            if (
                program_pricelists.length > 0 &&
                (!this.pricelist || !program_pricelists.includes(this.pricelist.id))
            ) {
                return _t("That promo code program requires a specific pricelist.");
            }
            if (this.codeActivatedProgramRules.includes(rule.id)) {
                return _t("That promo code program has already been activated.");
            }
            this.codeActivatedProgramRules.push(rule.id);
            await this._updateLoyaltyPrograms();
            claimableRewards = this.getClaimableRewards(false, rule.program_id.id);
            
        } else {
            // ========== COUPON CODE (GIFT CARD / VOUCHER) ==========
            
            // 🔑 PERBAIKAN 1: Cek apakah coupon sudah ada di order SAAT INI
            const existingCoupon = this.codeActivatedCoupons.find((c) => c.code === code);
            
            if (existingCoupon) {
                console.log("🔍 Coupon ditemukan di order:", {
                    code: code,
                    coupon_id: existingCoupon.id,
                    order_state: this.state,
                    is_finalized: this.finalized
                });
                
                // 🔑 PERBAIKAN 2: Cek status order
                // Izinkan re-scan hanya jika order belum di-payment/finalized
                const isOrderPending = !this.state || this.state === 'draft' || this.state === 'new';
                const isNotFinalized = !this.finalized;
                
                if (isOrderPending && isNotFinalized) {
                    // ✅ Order masih pending, izinkan re-claim reward
                    console.log("✅ Order masih pending, izinkan re-claim reward dari coupon");
                    
                    // Refresh claimable rewards untuk coupon ini
                    claimableRewards = this.getClaimableRewards(existingCoupon.id);
                    
                    // Auto-apply jika hanya ada 1 reward
                    if (claimableRewards && claimableRewards.length === 1) {
                        const { reward, coupon_id } = claimableRewards[0];
                        if (
                            reward.reward_type !== "product" ||
                            !reward.multi_product
                        ) {
                            console.log("🎁 Auto-applying reward:", reward.description);
                            this._applyReward(reward, coupon_id);
                            this._updateRewards();
                        }
                    }
                    
                    return true;
                    
                } else {
                    // ❌ Order sudah finalized/paid, tolak
                    console.warn("❌ Order sudah finalized, tolak re-scan coupon");
                    return _t("That coupon code has already been scanned and activated.");
                }
            }

            // 🔑 PERBAIKAN 3: Coupon belum ada di order, fetch dari server
            console.log("🆕 Coupon baru, fetch dari server:", code);
            
            const customerId = this.get_partner() ? this.get_partner().id : false;
            const { successful, payload } = await this.env.services.orm.call(
                "pos.config",
                "use_coupon_code",
                [
                    [this.pos.config.id],
                    code,
                    this.date_order,
                    customerId,
                    this.pricelist ? this.pricelist.id : false,
                ]
            );
            
            if (successful) {
                // Gift card validation
                const program = this.pos.program_by_id[payload.program_id];
                if (program && program.program_type === "gift_card" && !payload.has_source_order) {
                    const { confirmed } = await this.env.services.popup.add(ConfirmPopup, {
                        title: _t("Unpaid gift card"),
                        body: _t(
                            "This gift card is not linked to any order. Do you really want to apply its reward?"
                        ),
                    });
                    if (!confirmed) {
                        return _t("Unpaid gift card rejected.");
                    }
                }
                
                // 🔑 PERBAIKAN 4: Buat coupon object (tanpa constructor)
                coupon = {
                    code: code,
                    id: payload.coupon_id,
                    program_id: payload.program_id,
                    partner_id: payload.partner_id,
                    balance: payload.points,
                    expiration_date: payload.expiration_date ? new Date(payload.expiration_date) : false,
                    isExpired() {
                        return this.expiration_date && this.expiration_date < new Date();
                    }
                };
                
                console.log("✅ Coupon berhasil dibuat:", coupon);
                
                this.pos.couponCache[coupon.id] = coupon;
                this.codeActivatedCoupons.push(coupon);
                await this._updateLoyaltyPrograms();
                claimableRewards = this.getClaimableRewards(coupon.id);
                
            } else {
                console.error("❌ Gagal validasi coupon:", payload.error_message);
                return payload.error_message;
            }
        }
        
        // ========== AUTO-APPLY REWARD ==========
        if (claimableRewards && claimableRewards.length === 1) {
            const { reward, coupon_id } = claimableRewards[0];
            if (
                reward.reward_type !== "product" ||
                !reward.multi_product
            ) {
                console.log("🎁 Auto-applying single reward:", reward.description);
                this._applyReward(reward, coupon_id);
                this._updateRewards();
            }
        }
        
        // ========== GIFT CARD BALANCE INFO ==========
        if (!rule && this.orderlines.length === 0 && coupon) {
            return _t(
                "Gift Card: %s\nBalance: %s",
                code,
                this.env.utils.formatCurrency(coupon.balance)
            );
        }
        
        return true;
    },

    /**
     * 🔑 OVERRIDE: activateCode untuk logging yang lebih baik
     */
    async activateCode(code) {
        console.log("🎫 Aktivasi code:", code);
        const res = await this._activateCode(code);
        
        if (res !== true) {
            console.warn("⚠️ Aktivasi gagal:", res);
            this.env.services.pos_notification.add(res, 5000);
        } else {
            console.log("✅ Aktivasi berhasil!");
        }
    },

    /**
     * 🔑 TAMBAHAN: Reset coupon saat order reset
     */
    _resetPrograms() {
        console.log("🔄 Reset programs dipanggil");
        
        // Simpan reference ke parent method jika ada
        const parentReset = super._resetPrograms;
        
        if (parentReset) {
            parentReset.call(this);
        } else {
            // Fallback: reset manual
            this.disabledRewards = new Set();
            this.codeActivatedProgramRules = [];
            this.codeActivatedCoupons = [];
            this.couponPointChanges = {};
            
            const rewardLines = this._get_reward_lines();
            if (rewardLines && rewardLines.length > 0) {
                this.orderlines.remove(rewardLines);
            }
        }
        
        this._updateRewards();
    },
});