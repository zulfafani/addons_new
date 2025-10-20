/** @odoo-module **/

import { Order } from "@point_of_sale/app/store/models";
import { patch } from "@web/core/utils/patch";

patch(Order.prototype, {
    /**
     * Override untuk mengecek apakah customer adalah store
     */
    _isStoreCustomer() {
        const partner = this.get_partner();
        return partner && partner.is_store === true;
    },

    /**
     * Override untuk memblokir loyalty points bagi store customer
     */
    isLineValidForLoyaltyPoints(line) {
        // Jika customer adalah store, tidak dapat loyalty points
        if (this._isStoreCustomer()) {
            return false;
        }
        return super.isLineValidForLoyaltyPoints(...arguments);
    },

    /**
     * Override getLoyaltyPoints untuk menampilkan points dengan nilai 0 jika store customer
     */
    getLoyaltyPoints() {
        const loyaltyPoints = super.getLoyaltyPoints(...arguments);
        
        // Jika customer adalah store
        if (this._isStoreCustomer()) {
            // Jika tidak ada loyalty points, buat dummy data dengan nilai 0
            if (!loyaltyPoints || loyaltyPoints.length === 0) {
                // Cari loyalty program yang tersedia
                const loyaltyPrograms = this.pos.programs.filter(p => p.program_type === 'loyalty');
                if (loyaltyPrograms.length > 0) {
                    return loyaltyPrograms.map(program => ({
                        couponId: null,
                        program: program,
                        points: {
                            name: program.name || 'Loyalty Points',
                            balance: 0,
                            won: 0,
                            spent: 0,
                            total: 0
                        }
                    }));
                }
            } else {
                // Jika ada loyalty points, set semua nilai menjadi 0
                return loyaltyPoints.map(stat => ({
                    ...stat,
                    points: {
                        ...stat.points,
                        balance: 0,
                        won: 0,
                        spent: 0,
                        total: 0
                    }
                }));
            }
        }
        
        return loyaltyPoints;
    },

    /**
     * Override _programIsApplicable untuk memblokir program loyalty bagi store customer
     */
    _programIsApplicable(program) {
        // Jika customer adalah store dan program adalah loyalty, skip
        if (this._isStoreCustomer() && program.program_type === 'loyalty') {
            return false;
        }
        return super._programIsApplicable(...arguments);
    },

    /**
     * ✅ Override removeOrderline untuk auto-update loyalty points won
     * Ketika item dihapus, points won akan berkurang secara real-time di UI
     */
    removeOrderline(lineToRemove) {
        // Store customer check
        const isStoreCustomer = this._isStoreCustomer();
        
        // Simpan info apakah line yang dihapus adalah reward line
        const isRewardLine = lineToRemove.is_reward_line;
        
        // Call parent method untuk handle logic default (termasuk reward lines)
        const result = super.removeOrderline(lineToRemove);
        
        // ✅ Hanya update loyalty jika:
        // 1. Bukan store customer
        // 2. Ada program loyalty yang aktif
        // 3. Line yang dihapus BUKAN reward line (reward line sudah di-handle parent)
        if (!isStoreCustomer && this.pos.programs.length > 0 && !isRewardLine) {
            console.log("🔄 Updating loyalty points after removing line:", lineToRemove.product?.display_name);
            
            // Trigger recalculation loyalty programs
            // Ini akan update couponPointChanges dan points won
            this._updateRewards();
            
            // Note: _updateRewards() sudah memanggil _updateLoyaltyPrograms() 
            // yang akan recalculate points berdasarkan orderlines yang tersisa
        }
        
        return result;
    },
});