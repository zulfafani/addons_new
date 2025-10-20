/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { Order } from "@point_of_sale/app/store/models";
import { RewardButton } from "@pos_loyalty/app/control_buttons/reward_button/reward_button";
import { _t } from "@web/core/l10n/translation";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { SelectionPopup } from "@point_of_sale/app/utils/input_popups/selection_popup";
import { Mutex } from "@web/core/utils/concurrency";

// ========== MUTEX ==========
const updateRewardsMutex = new Mutex();

// ========== UTILS ==========
function getSafeProgramId(programRef) {
    if (typeof programRef === "object" && programRef !== null && "id" in programRef) return programRef.id;
    if (Array.isArray(programRef)) return programRef[0];
    return typeof programRef === "number" ? programRef : -1;
}

function isMemberRewardAllowed(order, reward) {
    if (!reward || !reward.program_id) {
        console.warn("❌ Reward invalid:", reward);
        return false;
    }

    const now = new Date();
    const currentDay = now.toLocaleString("en-US", { weekday: "long" }).toLowerCase();
    const currentHour = now.getHours() + now.getMinutes() / 60;

    const programId = getSafeProgramId(reward.program_id);
    const partner = order.get_partner();

    // Ambil ID kategori partner
    const partnerCategoryIds = Array.isArray(partner?.category_id)
        ? partner.category_id.map((c) => (Array.isArray(c) ? c[0] : c))
        : [];

    // Cari schedules untuk program ini
    const schedules = order.pos.loyalty_schedules.filter(
        (s) => getSafeProgramId(s.program_id) === programId
    );
    const hasDays = schedules.some((s) => !!s.days);
    const hasTimes = schedules.some((s) => s.time_start || s.time_end);

    // Cek schedule valid
    const scheduleValid = schedules.some((s) => {
        const dayOk = !s.days || s.days === currentDay;
        const timeOk = (!s.time_start && !s.time_end) ||
                       (s.time_start <= currentHour && currentHour <= s.time_end);
        return dayOk && timeOk;
    });

    // Cek member valid
    const membersForProgram = order.pos.loyalty_members.filter(
        (m) => getSafeProgramId(m.member_program_id) === programId
    );
    const programHasMembers = membersForProgram.length > 0;
    const isMemberValid = membersForProgram.some((member) => {
        const memberCategoryId = Array.isArray(member.member_pos) ? member.member_pos[0] : member.member_pos;
        return partnerCategoryIds.includes(memberCategoryId);
    });

    let finalResult = false;
    if (!programHasMembers) {
        finalResult = !hasDays && !hasTimes ? true : scheduleValid;
    } else {
        finalResult = isMemberValid && (!hasDays && !hasTimes ? true : scheduleValid);
    }
    return finalResult;
}

// ========== PATCH ORDER ==========
const OriginalApplyReward = Order.prototype._applyReward;
const OriginalUpdateRewards = Order.prototype._updateRewards;
const OriginalUpdateRewardLines = Order.prototype._updateRewardLines;
const OriginalComputeUnclaimedFreeProductQty = Order.prototype._computeUnclaimedFreeProductQty;
const OriginalGetRealCouponPoints = Order.prototype._getRealCouponPoints;

patch(Order.prototype, {
    _applyReward(reward, coupon_id, args) {
        // Validasi points terlebih dahulu
        if (this._getRealCouponPoints(coupon_id) < reward.required_points) {
            return _t("There are not enough points on the coupon to claim this reward.");
        }

        // Jika tidak aktif validasi jadwal/member → panggil core tanpa modifikasi
        if (!this.pos.config.validate_member_schedule) {
            return OriginalApplyReward.call(this, reward, coupon_id, args);
        }

        // Validasi jadwal/member
        if (!isMemberRewardAllowed(this, reward)) {
            return _t("Reward tidak memenuhi syarat jadwal/member");
        }

        // Untuk kasus normal, gunakan logika asli
        const result = OriginalApplyReward.call(this, reward, coupon_id, args);
        return result;
    },

    getClaimableRewards(coupon_id = false, program_id = false, auto = false) {
        // Gunakan logic asli jika validasi dinonaktifkan
        if (!this.pos.config.validate_member_schedule) {
            // Logic asli Odoo
            const allCouponPrograms = Object.values(this.couponPointChanges)
                .map((pe) => {
                    return {
                        program_id: pe.program_id,
                        coupon_id: pe.coupon_id,
                    };
                })
                .concat(
                    this.codeActivatedCoupons.map((coupon) => {
                        return {
                            program_id: coupon.program_id,
                            coupon_id: coupon.id,
                        };
                    })
                );
            const result = [];
            const totalWithTax = this.get_total_with_tax();
            const totalWithoutTax = this.get_total_without_tax();
            const totalIsZero = totalWithTax === 0;
            const globalDiscountLines = this._getGlobalDiscountLines();
            const globalDiscountPercent = globalDiscountLines.length
                ? this.pos.reward_by_id[globalDiscountLines[0].reward_id].discount
                : 0;
            
            for (const couponProgram of allCouponPrograms) {
                const program = this.pos.program_by_id[couponProgram.program_id];
                if (
                    program.pricelist_ids.length > 0 &&
                    (!this.pricelist || !program.pricelist_ids.includes(this.pricelist.id))
                ) {
                    continue;
                }
                if (program.trigger == "with_code") {
                    if (!this._canGenerateRewards(program, totalWithTax, totalWithoutTax)) {
                        continue;
                    }
                }
                if (
                    (coupon_id && couponProgram.coupon_id !== coupon_id) ||
                    (program_id && couponProgram.program_id !== program_id)
                ) {
                    continue;
                }
                const points = this._getRealCouponPoints(couponProgram.coupon_id);
                for (const reward of program.rewards) {
                    if (points < reward.required_points) {
                        continue;
                    }
                    if ((reward.program_id.program_type === 'coupons' && this.orderlines.find(((rewardline) => rewardline.reward_id === reward.id)))) {
                        continue;
                    }
                    if (auto && this.disabledRewards.has(reward.id)) {
                        continue;
                    }
                    if (reward.is_global_discount && reward.discount <= globalDiscountPercent) {
                        continue;
                    }
                    if (reward.reward_type === "discount" && totalIsZero) {
                        continue;
                    }
                    let unclaimedQty;
                    if (reward.reward_type === "product") {
                        if (!reward.multi_product) {
                            const product = this.pos.db.get_product_by_id(reward.reward_product_ids[0]);
                            if (!product) {
                                continue;
                            }
                            unclaimedQty = this._computeUnclaimedFreeProductQty(
                                reward,
                                couponProgram.coupon_id,
                                product,
                                points
                            );
                        }
                        if (!unclaimedQty || unclaimedQty <= 0) {
                            continue;
                        }
                    }
                    result.push({
                        coupon_id: couponProgram.coupon_id,
                        reward: reward,
                        potentialQty: unclaimedQty,
                    });
                }
            }
            return result;
        }

        // 🔑 CUSTOM LOGIC dengan validasi member/schedule
        const allCouponPrograms = Object.values(this.couponPointChanges)
            .map((pe) => ({
                program_id: pe.program_id,
                coupon_id: pe.coupon_id,
            }))
            .concat(
                this.codeActivatedCoupons.map((coupon) => ({
                    program_id: coupon.program_id,
                    coupon_id: coupon.id,
                }))
            );

        const result = [];
        const totalWithTax = this.get_total_with_tax();
        const totalWithoutTax = this.get_total_without_tax();
        const totalIsZero = totalWithTax === 0;
        const globalDiscountLines = this._getGlobalDiscountLines();
        const globalDiscountPercent = globalDiscountLines.length
            ? this.pos.reward_by_id[globalDiscountLines[0].reward_id].discount
            : 0;

        for (const couponProgram of allCouponPrograms) {
            const program = this.pos.program_by_id[couponProgram.program_id];
            
            if (
                program.pricelist_ids.length > 0 &&
                (!this.pricelist || !program.pricelist_ids.includes(this.pricelist.id))
            ) {
                continue;
            }
            
            if (program.trigger == "with_code") {
                if (!this._canGenerateRewards(program, totalWithTax, totalWithoutTax)) {
                    continue;
                }
            }
            
            if (
                (coupon_id && couponProgram.coupon_id !== coupon_id) ||
                (program_id && couponProgram.program_id !== program_id)
            ) {
                continue;
            }
            
            const points = this._getRealCouponPoints(couponProgram.coupon_id);
            
            for (const reward of program.rewards) {
                // 🔑 FILTER: Validasi member/schedule
                if (!isMemberRewardAllowed(this, reward)) {
                    continue;
                }
                
                if (points < reward.required_points) {
                    continue;
                }
                
                if ((reward.program_id.program_type === 'coupons' && this.orderlines.find(((rewardline) => rewardline.reward_id === reward.id)))) {
                    continue;
                }
                
                if (auto && this.disabledRewards.has(reward.id)) {
                    continue;
                }
                
                if (reward.is_global_discount && reward.discount <= globalDiscountPercent) {
                    continue;
                }
                
                if (reward.reward_type === "discount" && totalIsZero) {
                    continue;
                }
                
                let unclaimedQty;
                if (reward.reward_type === "product") {
                    if (!reward.multi_product) {
                        const product = this.pos.db.get_product_by_id(reward.reward_product_ids[0]);
                        if (!product) {
                            continue;
                        }
                        unclaimedQty = this._computeUnclaimedFreeProductQty(
                            reward,
                            couponProgram.coupon_id,
                            product,
                            points
                        );
                    }
                    if (!unclaimedQty || unclaimedQty <= 0) {
                        continue;
                    }
                }
                
                result.push({
                    coupon_id: couponProgram.coupon_id,
                    reward: reward,
                    potentialQty: unclaimedQty,
                });
            }
        }

        return result;
    },

    _updateRewards() {
        // Calls are not expected to take some time besides on the first load + when loyalty programs are made applicable
        if (this.pos.programs.length === 0) {
            return;
        }

        // Jika validasi dinonaktifkan, gunakan logic asli
        if (!this.pos.config.validate_member_schedule) {
            return OriginalUpdateRewards.apply(this, arguments);
        }

        // 🔥 CUSTOM LOGIC: AUTO-CLAIM SEMUA PROGRAM
        updateRewardsMutex.exec(() => {
            return this._updateLoyaltyPrograms().then(async () => {
                const claimableRewards = this.getClaimableRewards(false, false, true);
                
                console.log("🎯 AUTO-CLAIM ALL PROGRAMS:", {
                    totalRewards: claimableRewards.length,
                    rewards: claimableRewards.map(r => ({
                        program: r.reward.program_id.name,
                        reward: r.reward.description,
                        type: r.reward.reward_type
                    }))
                });
                
                // 🔥 AUTO-CLAIM untuk semua reward yang eligible
                for (const { coupon_id, reward } of claimableRewards) {
                    const program = reward.program_id;
                    
                    // Skip jika:
                    // 1. Program punya multiple rewards (user harus pilih)
                    // 2. Reward adalah multi-product (user harus pilih produk)
                    // 3. Program nominative (user harus input nominal)
                    const shouldSkipAutoClaim = 
                        program.rewards.length > 1 || 
                        program.is_nominative ||
                        (reward.reward_type === "product" && reward.multi_product);
                    
                    if (shouldSkipAutoClaim) {
                        console.log("⚠️ Skip auto-claim (requires user selection):", reward.description);
                        continue;
                    }
                    
                    // 🔥 AUTO-CLAIM reward
                    console.log("✅ Auto-claiming:", reward.description);
                    this._applyReward(reward, coupon_id);
                }
                
                this._updateRewardLines();
                await this._updateLoyaltyPrograms();
            });
        });
    },

    _updateRewardLines() {
        if (!this.pos.config.validate_member_schedule) {
            if (typeof OriginalUpdateRewardLines === "function") {
                OriginalUpdateRewardLines.apply(this, arguments);
            }
            return;
        }
        
        if (typeof OriginalUpdateRewardLines === "function") {
            OriginalUpdateRewardLines.apply(this, arguments);
        }
        
        const rewardLines = this._get_reward_lines();
        for (const line of rewardLines) {
            // ✅ Skip jika reward_id undefined atau reward tidak ditemukan
            if (!line.reward_id) {
                console.warn("⚠️ Reward line without reward_id:", line);
                continue;
            }
            
            const reward = this.pos.reward_by_id?.[line.reward_id];
            if (!reward) {
                console.warn("⚠️ Reward not found in pos.reward_by_id:", line.reward_id);
                continue;
            }
            
            // ✅ Hanya remove jika reward tidak memenuhi syarat
            if (!isMemberRewardAllowed(this, reward)) {
                console.log("🗑️ Removing invalid reward line:", reward.description);
                this.orderlines.remove(line);
            }
        }
    },

    _getRealCouponPoints(coupon_id) {
        if (typeof OriginalGetRealCouponPoints === "function") {
            return OriginalGetRealCouponPoints.call(this, coupon_id);
        }
        // Fallback
        let points = 0;
        const dbCoupon = this.pos.couponCache[coupon_id];
        if (dbCoupon) {
            points += dbCoupon.balance;
        }
        Object.values(this.couponPointChanges || {}).some((pe) => {
            if (pe.coupon_id === coupon_id) {
                if (this.pos.program_by_id[pe.program_id].applies_on !== "future") {
                    points += pe.points;
                }
                return true;
            }
            return false;
        });
        for (const line of this.get_orderlines()) {
            if (line.is_reward_line && line.coupon_id === coupon_id) {
                points -= line.points_cost;
            }
        }
        return points;
    },

    _computeUnclaimedFreeProductQty(reward, coupon_id, product, remainingPoints) {
        if (typeof OriginalComputeUnclaimedFreeProductQty === "function") {
            return OriginalComputeUnclaimedFreeProductQty.call(this, reward, coupon_id, product, remainingPoints);
        }
        return 0;
    },
});

// ========== PATCH REWARD BUTTON ==========
patch(RewardButton.prototype, {
    setup() {
        super.setup();
        this.pos = usePos();
    },

    _getPotentialRewards() {
        const order = this.pos.get_order();
        if (!order) return [];

        const claimableRewards = order.getClaimableRewards();
        const rewards = claimableRewards.filter(
            ({ reward }) => reward.program_id.program_type !== "ewallet"
        );

        const discountRewards = rewards.filter(({ reward }) => reward.reward_type == "discount");
        const freeProductRewards = rewards.filter(({ reward }) => reward.reward_type == "product");
        const potentialFreeProductRewards = this.pos.getPotentialFreeProductRewards();
        
        return discountRewards.concat(
            this._mergeFreeProductRewards(freeProductRewards, potentialFreeProductRewards)
        );
    },

    hasClaimableRewards() {
        return this._getPotentialRewards().length > 0;
    },

    async _applyReward(reward, coupon_id, potentialQty) {
        const order = this.pos.get_order();
        
        // Validasi member/schedule jika aktif
        if (this.pos.config.validate_member_schedule && !isMemberRewardAllowed(order, reward)) {
            this.notification.add(_t("Reward tidak memenuhi syarat jadwal/member"));
            return false;
        }

        order.disabledRewards.delete(reward.id);

        const args = {};
        
        // Handle multi-product selection
        if (reward.reward_type === "product" && reward.multi_product) {
            const productsList = reward.reward_product_ids.map((product_id) => ({
                id: product_id,
                label: this.pos.db.get_product_by_id(product_id).display_name,
                item: product_id,
            }));
            const { confirmed, payload: selectedProduct } = await this.popup.add(SelectionPopup, {
                title: _t("Please select a product for this reward"),
                list: productsList,
            });
            if (!confirmed) {
                return false;
            }
            args["product"] = selectedProduct;
        }

        // Untuk product rewards, gunakan logic asli Odoo
        if (
            (reward.reward_type == "product" && reward.program_id.applies_on !== "both") ||
            (reward.program_id.applies_on == "both" && potentialQty)
        ) {
            this.pos.addProductToCurrentOrder(
                args["product"] || reward.reward_product_ids[0],
                { quantity: potentialQty || 1 }
            );
            return true;
        }

        const result = order._applyReward(reward, coupon_id, args);
        
        if (result !== true) {
            this.notification.add(result);
            return false;
        }

        order._updateRewards();
        return true;
    },

    async click() {
        const order = this.pos.get_order();
        const rewards = this._getPotentialRewards();

        if (!rewards.length) {
            this.notification.add(_t("Tidak ada reward yang tersedia."));
            return false;
        }
        
        if (rewards.length >= 1) {
            const rewardsList = rewards.map((reward) => ({
                id: reward.reward.id,
                label: reward.reward.description,
                description: reward.reward.program_id.name,
                item: reward,
            }));
            const { confirmed, payload: selectedReward } = await this.popup.add(SelectionPopup, {
                title: _t("Please select a reward"),
                list: rewardsList,
            });
            if (confirmed) {
                return this._applyReward(
                    selectedReward.reward,
                    selectedReward.coupon_id,
                    selectedReward.potentialQty
                );
            }
        }
        return false;
    },
});