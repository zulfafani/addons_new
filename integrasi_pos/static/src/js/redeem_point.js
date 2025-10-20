/** @odoo-module **/

import { _t } from "@web/core/l10n/translation";
import { Component, useState, useRef, onMounted } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { usePos } from "@point_of_sale/app/store/pos_hook";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";

// ========== POPUP ==========
export class RedeemPointPopUp extends AbstractAwaitablePopup {
    static template = "integrasi_pos.RedeemPointPopUp";

    setup() {
        super.setup();
        this.orm = useService("orm");
        this.pos = usePos();
        this.inputRef = useRef("discountInput");
        
        this.payloadData = null;
        
        this.state = useState({
            inputValue: "",
            availablePoints: 0,
            conversionRate: 1,
            loading: true,
            loyaltyCard: null,
            program: null,
        });

        onMounted(async () => {
            this.inputRef.el?.focus();
            await this.loadLoyaltyInfo();
        });
    }

    getPayload() {
        return this.payloadData;
    }

    async loadLoyaltyInfo() {
        try {
            const partnerId = this.props.partnerId;
            if (!partnerId) {
                this.state.loading = false;
                return;
            }

            const cards = await this.orm.searchRead(
                'loyalty.card',
                [['partner_id', '=', partnerId]],
                ['id', 'points', 'program_id'],
                { order: 'points DESC', limit: 1 }
            );

            if (cards.length > 0) {
                const card = cards[0];
                const programId = Array.isArray(card.program_id) ? card.program_id[0] : card.program_id;
                
                console.log("🔍 Debug - Card data:", card);
                console.log("🔍 Debug - Program ID:", programId);
                
                const program = this.pos.program_by_id[programId];
                
                console.log("🔍 Debug - Found program:", program);
                
                if (program) {
                    this.state.loyaltyCard = card;
                    this.state.program = program;
                    this.state.availablePoints = card.points || 0;
                    
                    // ✅ Load vit_konversi_poin directly from database
                    try {
                        const programData = await this.orm.read(
                            'loyalty.program',
                            [programId],
                            ['vit_konversi_poin']
                        );
                        
                        if (programData && programData.length > 0) {
                            this.state.conversionRate = programData[0].vit_konversi_poin || 1;
                            console.log("💰 Loaded vit_konversi_poin from DB:", programData[0].vit_konversi_poin);
                        } else {
                            this.state.conversionRate = 1;
                            console.warn("⚠️ No vit_konversi_poin in DB, using default: 1");
                        }
                    } catch (error) {
                        console.error("❌ Error loading vit_konversi_poin:", error);
                        this.state.conversionRate = 1;
                    }
                    
                    console.log("💰 Final conversion rate:", this.state.conversionRate);
                }
            }

        } catch (error) {
            console.error("Error loading loyalty info:", error);
            this.state.availablePoints = 0;
        } finally {
            this.state.loading = false;
        }
    }

    get estimatedDiscount() {
        const points = parseInt(this.state.inputValue) || 0;
        return points * this.state.conversionRate;
    }

    handleTyping(event) {
        this.state.inputValue = event.target.value;
    }

    addNumber(num) {
        this.state.inputValue += num.toString();
    }

    removeLastChar() {
        this.state.inputValue = this.state.inputValue.slice(0, -1);
    }

    clearInput() {
        this.state.inputValue = "";
    }

    async confirmInput() {
        const points = parseInt(this.state.inputValue);
        
        if (!this.state.loyaltyCard) {
            await this.env.services.popup.add(ErrorPopup, {
                title: _t("No Loyalty Card"),
                body: _t("No loyalty card found for this customer."),
            });
            return;
        }

        if (isNaN(points) || points <= 0) {
            await this.env.services.popup.add(ErrorPopup, {
                title: _t("Invalid Points"),
                body: _t("Please enter a valid point amount."),
            });
            return;
        }

        if (points > this.state.availablePoints) {
            await this.env.services.popup.add(ErrorPopup, {
                title: _t("Insufficient Points"),
                body: _t(`Customer only has ${this.state.availablePoints} points available.`),
            });
            return;
        }

        const payload = { 
            points: points,
            card_id: this.state.loyaltyCard.id,
            program_id: this.state.program.id,
            conversion_rate: this.state.conversionRate,
            discount_amount: this.estimatedDiscount,
        };
        
        this.payloadData = payload;
        this.confirm();
    }

    cancel() {
        super.cancel();
    }
}

// ========== BUTTON ==========
export class RedeemPointButton extends Component {
    static template = "RedeemPointButton";

    setup() {
        this.popup = useService("popup");
        this.orm = useService("orm");
        this.pos = usePos();
    }

    get label() {
        return _t("Redeem Points");
    }

    async onClick() {
        const order = this.pos.get_order();
        
        if (!order) {
            await this.popup.add(ErrorPopup, {
                title: _t("No Order"),
                body: _t("Please create an order first."),
            });
            return;
        }

        const partner = order.get_partner();
        
        if (!partner) {
            await this.popup.add(ErrorPopup, {
                title: _t("No Customer"),
                body: _t("Please select a customer first to redeem points."),
            });
            return;
        }

        const result = await this.popup.add(RedeemPointPopUp, {
            title: _t("Redeem Loyalty Points"),
            partnerId: partner.id,
            partnerName: partner.name,
        });
        
        const { confirmed, payload } = result;

        if (confirmed && payload) {
            await this.redeemPoints(payload);
        }
    }

    async redeemPoints(payload) {
        try {
            const order = this.pos.get_order();
            const { points, card_id, program_id, conversion_rate, discount_amount } = payload;

            console.group("🔍 REDEMPTION DEBUG");
            console.log("1. Payload:", payload);

            // Cari program & reward
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

            const perPointReward = program.rewards.find(r => 
                r.reward_type === 'discount' && 
                r.discount_mode === 'per_point'
            );
            
            if (!perPointReward) {
                console.error("❌ No per_point reward found!");
                console.groupEnd();
                await this.popup.add(ErrorPopup, {
                    title: _t("Reward Not Configured"),
                    body: _t("Please configure a 'Per Point' discount reward."),
                });
                return;
            }

            if (!perPointReward.discount_line_product_id) {
                console.error("❌ No discount_line_product_id!");
                console.groupEnd();
                await this.popup.add(ErrorPopup, {
                    title: _t("Configuration Error"),
                    body: _t("Reward tidak memiliki Discount Line Product."),
                });
                return;
            }

            // Get discount product
            let discountProduct = null;
            
            if (typeof perPointReward.discount_line_product_id === 'object' && 
                perPointReward.discount_line_product_id.id) {
                discountProduct = perPointReward.discount_line_product_id;
            } else {
                const discountProductId = Array.isArray(perPointReward.discount_line_product_id) 
                    ? perPointReward.discount_line_product_id[0] 
                    : perPointReward.discount_line_product_id;
                    
                discountProduct = this.pos.db.get_product_by_id(discountProductId);
            }
            
            if (!discountProduct) {
                console.error(`❌ Discount product not found!`);
                console.groupEnd();
                await this.popup.add(ErrorPopup, {
                    title: _t("Product Not Available"),
                    body: _t(`Produk discount tidak ditemukan.`),
                });
                return;
            }

            console.log("2. Program:", program.name);
            console.log("3. vit_konversi_poin:", program.vit_konversi_poin);
            console.log("4. Discount product:", discountProduct.display_name);
            console.log("5. Points to redeem:", points);
            console.log("6. Conversion rate:", conversion_rate, "Rp/point");
            console.log("7. Calculated discount:", discount_amount, "Rp");

            // 🔥 Hapus reward line lama jika ada
            const existingRewardLines = order.get_orderlines().filter(line => 
                line.reward_id === perPointReward.id || 
                line.product.id === discountProduct.id
            );
            
            if (existingRewardLines.length > 0) {
                console.log("8. Removing existing reward lines:", existingRewardLines.length);
                existingRewardLines.forEach(line => order.remove_orderline(line));
            }

            // 🔥 Create manual discount line
            console.log("9. Creating manual discount line...");
            
            // Calculate exact discount (negative value)
            const exactDiscount = -Math.abs(discount_amount);
            
            console.log("10. Exact discount to apply:", exactDiscount, "Rp");

            // ✅ Add discount line dengan metadata untuk validation
            const discountLine = await order.add_product(discountProduct, {
                price: exactDiscount,
                quantity: 1,
                merge: false,
                extras: {
                    reward_id: perPointReward.id,
                    reward_identifier_code: `point_redemption_${Date.now()}`,
                    is_reward_line: true,
                    // ✅ CRITICAL: Simpan metadata untuk validation nanti
                    points_cost: points,  // Points yang akan dikurangi saat validate
                    coupon_id: card_id,   // ID loyalty card yang akan dikurangi
                }
            });

            if (!discountLine) {
                console.error("❌ Failed to create discount line!");
                console.groupEnd();
                await this.popup.add(ErrorPopup, {
                    title: _t("Error"),
                    body: _t("Failed to create discount line."),
                });
                return;
            }

            console.log("11. Discount line created:", {
                product: discountLine.product.display_name,
                price: discountLine.price,
                quantity: discountLine.quantity,
                points_cost: discountLine.points_cost,
                coupon_id: discountLine.coupon_id,
                total: discountLine.get_price_with_tax()
            });

            // ✅ PENTING: JANGAN kurangi points di sini!
            // Points akan dikurangi saat validation di payment_screen.js
            console.log("12. ⚠️ Points NOT deducted yet - will be deducted on validation");

            console.log("✅ Redemption prepared successfully!");
            console.log("💡 Points will be deducted when order is validated");
            console.groupEnd();

            // Success notification
            this.env.services.pos_notification.add(
                _t(`Redemption prepared: ${points} points\nDiscount: ${this.env.utils.formatCurrency(Math.abs(exactDiscount))}\n\n⚠️ Points will be deducted after payment validation.`),
                { type: 'info', duration: 5000 }
            );

        } catch (error) {
            console.error("❌ CRITICAL ERROR:", error);
            console.error("Stack:", error.stack);
            console.groupEnd();
            
            await this.popup.add(ErrorPopup, {
                title: _t("Error"),
                body: _t("An error occurred: ") + error.message,
            });
        }
    }
}

// Register button to ProductScreen
ProductScreen.addControlButton({
    component: RedeemPointButton,
    condition: function() {
        return true;
    },
    position: ['before', 'SetPricelistButton'],
});