/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { PaymentScreen } from "@point_of_sale/app/screens/payment_screen/payment_screen";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";
import { useState } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { OrderReceipt } from "@point_of_sale/app/screens/receipt_screen/receipt/order_receipt";
import { ConnectionLostError } from "@web/core/network/rpc_service";
import { Orderline } from "@point_of_sale/app/store/models";
import { Order } from "@point_of_sale/app/store/models";

// ========== PATCH UNTUK HARGA ASLI ==========

/**
 * Patch Orderline untuk menambahkan harga asli sebelum diskon
 */
patch(Orderline.prototype, {
    /**
     * Override getDisplayData untuk menambahkan originalUnitPrice
     */
    getDisplayData() {
        const originalData = super.getDisplayData();
        
        // Calculate harga asli sebelum diskon
        const originalUnitPrice = this.get_taxed_lst_unit_price();
        const formattedOriginalPrice = this.env.utils.formatCurrency(originalUnitPrice);
        
        return {
            ...originalData,
            originalUnitPrice: formattedOriginalPrice,
            lstPrice: this.env.utils.formatCurrency(this.get_lst_price()),
            // Untuk kompatibilitas dengan template yang ada
            unitPrice: formattedOriginalPrice, // Override unitPrice dengan harga asli
        };
    },

    /**
     * Method untuk mendapatkan harga asli (numeric)
     */
    get_original_unit_price() {
        return this.get_taxed_lst_unit_price();
    },

    /**
     * Method untuk mendapatkan total harga asli
     */
    get_original_total_price() {
        return this.get_taxed_lst_unit_price() * this.get_quantity();
    },
});

/**
 * Patch Order untuk memastikan data konsisten di receipt
 */
patch(Order.prototype, {
    /**
     * Override export_for_printing untuk menambahkan data harga asli
     */
    export_for_printing() {
        const result = super.export_for_printing();

        result.is_printed = this.is_printed || false;
        
        // Ensure semua orderlines memiliki originalUnitPrice
        result.orderlines = result.orderlines.map(line => {
            // Cari orderline yang sesuai
            const matchingLine = this.orderlines.find(ol => 
                ol.get_full_product_name() === line.productName && 
                Math.abs(ol.get_quantity() - parseFloat(line.qty)) < 0.001
            );
            
            if (matchingLine) {
                const originalPrice = this.env.utils.formatCurrency(matchingLine.get_taxed_lst_unit_price());
                return {
                    ...line,
                    originalUnitPrice: originalPrice,
                    unitPrice: originalPrice, // Override unitPrice dengan harga asli
                };
            }
            return line;
        });
        
        return result;
    },
});

// ========== LOAD CONFIG ==========
let CONFIG = null;

async function loadConfig() {
    if (CONFIG) return CONFIG;
    
    try {
        const response = await fetch('/integrasi_pos/static/src/config/config.json');
        if (!response.ok) {
            console.error("❌ Failed to load config.json");
            CONFIG = {
                api: {
                    baseURL: "http://pos-mc.visi-intech.com",
                    authorization: "03df61505b28cce167036aac80ab16e5cdc51a3165a11cc1246e09db6c163a53",
                    timeout: 10000,
                    serverName: "MC"
                },
                services: {
                    drawerService: "http://localhost:3001",
                    poleDisplay: "ws://localhost:8765",
                    printService: "http://localhost:3001"
                },
                environment: "production"
            };
            return CONFIG;
        }
        CONFIG = await response.json();
        console.log("✅ Config loaded:", CONFIG);
        return CONFIG;
    } catch (error) {
        console.error("❌ Error loading config:", error);
        CONFIG = {
            api: {
                baseURL: "http://pos-mc.visi-intech.com",
                authorization: "03df61505b28cce167036aac80ab16e5cdc51a3165a11cc1246e09db6c163a53",
                timeout: 10000,
                serverName: "MC"
            },
            services: {
                drawerService: "http://localhost:3001",
                poleDisplay: "ws://localhost:8765",
                printService: "http://localhost:3001"
            },
            environment: "production"
        };
        return CONFIG;
    }
}

// ========== HELPER FUNCTIONS ==========
function formatDisplayLine(label, value) {
    const totalWidth = 20;
    const left = label.padEnd(12, " ");
    const right = value.toString().padStart(totalWidth - left.length, " ");
    return left + right;
}

async function triggerCashDrawer() {
    try {
        const config = await loadConfig();
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 2000);
        
        const response = await fetch(`${config.services.drawerService}/open-drawer`, { 
            method: "POST",
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        if (response.ok) {
            console.log("✅ Cash drawer opened");
            return true;
        } else {
            console.warn("⚠️ Drawer service responded with error:", response.status);
            return false;
        }
    } catch (err) {
        if (err.name === 'AbortError') {
            console.warn("⚠️ Drawer service timeout (service mungkin tidak berjalan)");
        } else {
            console.warn("⚠️ Drawer service tidak tersedia:", err.message);
        }
        return false;
    }
}

async function clearPoleDisplay() {
    try {
        const config = await loadConfig();
        const response = await fetch(`${config.services.drawerService}/clear-pole`, {
            method: "POST"
        });
        
        if (response.ok) {
            console.log("✅ Pole display cleared");
            return true;
        } else {
            console.warn("⚠️ Clear pole display failed:", response.status);
            return false;
        }
    } catch (err) {
        console.warn("⚠️ Clear pole display service tidak tersedia:", err.message);
        return false;
    }
}

async function sendToPoleDisplay(line1, line2) {
    const config = await loadConfig();
    return new Promise((resolve) => {
        try {
            const ws = new WebSocket(config.services.poleDisplay);
            
            ws.onerror = () => {
                console.warn("⚠️ Pole display tidak tersedia");
                resolve(false);
            };
            
            ws.onopen = () => {
                ws.send(`${line1}\n${line2}`);
                console.log(`📤 Sent to pole: "${line1}" | "${line2}"`);
                setTimeout(() => {
                    ws.close();
                    resolve(true);
                }, 500);
            };
            
            setTimeout(() => {
                if (ws.readyState !== WebSocket.OPEN) {
                    ws.close();
                    resolve(false);
                }
            }, 2000);
        } catch (err) {
            console.warn("⚠️ Error connecting to pole display:", err.message);
            resolve(false);
        }
    });
}

// ========== API HELPER FUNCTIONS ==========
async function fetchMasterServerURL() {
    try {
        const config = await loadConfig();
        console.log("🔍 Fetching Master Server URL for MC...");
        
        const apiURL = `${config.api.baseURL}/api/master_server/?vit_config_server_name=${config.api.serverName}`;
        
        const response = await fetch(apiURL, {
            method: 'GET',
            headers: {
                'Authorization': config.api.authorization,
                'Content-Type': 'application/json'
            },
            signal: AbortSignal.timeout(config.api.timeout)
        });

        if (!response.ok) {
            console.error("❌ Failed to fetch master server:", response.status);
            return null;
        }

        const data = await response.json();
        
        if (data.status === 'Success' && data.data && data.data.length > 0) {
            const serverURL = data.data[0].vit_config_url;
            console.log("✅ Master Server URL found:", serverURL);
            return serverURL;
        } else {
            console.error("❌ Master Server MC not found in response");
            return null;
        }
    } catch (error) {
        console.error("❌ Error fetching master server URL:", error);
        return null;
    }
}

async function checkCouponIsUsed(baseURL, couponCode) {
    try {
        const config = await loadConfig();
        console.log(`🔍 Checking coupon is_used status: ${couponCode}`);
        
        const apiURL = `${baseURL}/api/master_coupon/?code=${encodeURIComponent(couponCode)}`;
        
        const response = await fetch(apiURL, {
            method: 'GET',
            headers: {
                'Authorization': config.api.authorization,
                'Content-Type': 'application/json'
            },
            signal: AbortSignal.timeout(config.api.timeout)
        });

        if (!response.ok) {
            console.error("❌ API request failed:", response.status);
            return {
                success: false,
                isUsed: false,
                couponData: null,
                error: `API error: ${response.status}`
            };
        }

        const data = await response.json();
        
        if (data.status === 'Success' && data.data && data.data.length > 0) {
            const couponData = data.data[0];
            console.log("✅ Coupon found:", {
                id: couponData.id,
                code: couponData.code,
                is_used: couponData.is_used
            });
            
            return {
                success: true,
                isUsed: couponData.is_used || false,
                couponData: couponData,
                error: null
            };
        } else {
            console.warn("⚠️ Coupon not found in external system:", couponCode);
            return {
                success: true,
                isUsed: false,
                couponData: null,
                error: null
            };
        }
    } catch (error) {
        console.error("❌ Error checking coupon:", error);
        return {
            success: false,
            isUsed: false,
            couponData: null,
            error: error.message
        };
    }
}

async function updateCouponIsUsed(baseURL, couponCode, isUsed = true) {
    try {
        const config = await loadConfig();
        console.log(`📝 Updating coupon is_used: code=${couponCode}, is_used=${isUsed}`);
        
        const apiURL = `${baseURL}/api/master_coupon`;
        
        const response = await fetch(apiURL, {
            method: 'PATCH',
            headers: {
                'Authorization': config.api.authorization,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                items: {
                    code: couponCode,
                    is_used: isUsed
                }
            }),
            signal: AbortSignal.timeout(config.api.timeout)
        });

        if (!response.ok) {
            console.error("❌ PATCH request failed:", response.status, response.statusText);
            return {
                success: false,
                error: `API error: ${response.status} ${response.statusText}`
            };
        }

        const data = await response.json();
        console.log("📥 PATCH Response:", data);
        
        const responseData = data.result || data;
        
        const isSuccess = (
            responseData.status === 'Success' || 
            responseData.status === 'success' ||
            responseData.code === 200
        );
        
        const updatedData = responseData.updated || responseData.updated_coupons || [];
        
        if (isSuccess && updatedData.length > 0) {
            console.log("✅ Coupon updated successfully:", updatedData[0]);
            return {
                success: true,
                error: null
            };
        } else {
            const errorMsg = responseData.message || 
                           (responseData.errors && responseData.errors.length > 0 ? 
                            responseData.errors[0].error || responseData.errors[0].message : null) ||
                           'Unknown error';
            
            console.error("❌ Failed to update coupon. Response data:", responseData);
            return {
                success: false,
                error: errorMsg
            };
        }
    } catch (error) {
        console.error("❌ Error updating coupon:", error);
        return {
            success: false,
            error: error.message || 'Network error'
        };
    }
}

// ========== NUMERIC KEYBOARD POPUP ==========
export class NumericKeyboardPopup extends AbstractAwaitablePopup {
    static template = "integrasi_pos.NumericKeyboardPopup";
    static defaultProps = {
        confirmText: "OK",
        cancelText: "Batal",
        title: "Input Angka",
        placeholder: "Contoh: 1.2.3.4",
        body: ""
    };

    setup() {
        super.setup();
        this.state = useState({ inputValue: "", error: "" });
    }

    onKeyPress(key) {
        if (key === "clear") {
            this.state.inputValue = "";
        } else {
            this.state.inputValue += key;
        }
        this.state.error = "";
    }

    getPayload() {
        if (!this.state.inputValue.trim()) {
            this.state.error = "Input tidak boleh kosong";
            return null;
        }
        return this.state.inputValue;
    }

    confirm() {
        const payload = this.getPayload();
        if (payload) super.confirm();
    }
}

// ========== PAYMENT SCREEN PATCH ==========
patch(PaymentScreen.prototype, {
    setup() {
        super.setup();
        this._rendererService = useService("renderer");
        this.orm = useService("orm");
        this.popup = useService("popup");
        
        this._masterServerURL = null;
        
        loadConfig();
    },

    /**
     * Get dynamic rounding base based on amount range
     */
    _getRoundingBase(amount) {
        if (amount < 100000) {
            return 10000;
        } else if (amount < 500000) {
            return 50000;
        } else if (amount < 1000000) {
            return 100000;
        } else {
            return 500000;
        }
    },

    /**
     * Generate dynamic shortcut amounts
     */
    _calculateShortcutAmounts(amount) {
        if (!amount || amount <= 0) {
            return [10000, 20000];
        }
        
        const roundingBase = this._getRoundingBase(amount);
        const shortcut1 = Math.floor(amount / roundingBase) * roundingBase;
        let shortcut2 = Math.ceil(amount / roundingBase) * roundingBase;
        
        if (shortcut1 === 0) {
            return [roundingBase, roundingBase * 2];
        }
        
        if (shortcut1 === shortcut2) {
            shortcut2 = shortcut1 + roundingBase;
        }
        
        return [shortcut1, shortcut2];
    },

    /**
     * Format amount to display format
     */
    _formatShortcutLabel(amount) {
        if (amount >= 1000000) {
            const millions = amount / 1000000;
            return millions % 1 === 0 
                ? `${millions}JT` 
                : `${millions.toFixed(1).replace('.', ',')}JT`;
        } else {
            const thousands = amount / 1000;
            return `${thousands}K`;
        }
    },

    /**
     * ✅ NEW: Get shortcut amounts (untuk template)
     */
    getShortcutAmounts() {
        const pos = this?.env?.pos || this?.pos;
        if (!pos) return [10000, 20000]; // fallback aman
        const currentOrder = pos.get_order?.();
        const remainingAmount = currentOrder ? currentOrder.get_due() : 0;
        return this._calculateShortcutAmounts(remainingAmount);
    },


    /**
     * ✅ NEW: Format shortcut label (untuk template)
     */
    formatShortcutLabel(amount) {
        return this._formatShortcutLabel(amount);
    },

    /**
     * ✅ NEW METHOD: Handle shortcut button click
     */
    onShortcutClick(amount) {
        console.log(`🎯 Shortcut clicked: ${amount}`);
        
        // Pastikan ada payment line yang selected
        if (!this.selectedPaymentLine) {
            console.warn("⚠️ No payment line selected");
            this.notification.add("Pilih payment method terlebih dahulu", {
                type: "warning",
            });
            return;
        }
        
        // Set amount langsung
        this.selectedPaymentLine.set_amount(amount);
        
        // Reset number buffer
        this.numberBuffer.reset();
        
        console.log("✅ Payment amount updated via shortcut:", amount);
    },

    /**
     * ✅ OVERRIDE: getNumpadButtons - TANPA SHORTCUT (karena sudah terpisah)
     */
    getNumpadButtons() {
        return [
            { value: "1" },
            { value: "2" },
            { value: "3" },
            { value: "Quantity", text: "Qty" },  // Ganti shortcut dengan Qty
            { value: "4" },
            { value: "5" },
            { value: "6" },
            { value: "Discount", text: "Disc" }, // Ganti shortcut dengan Disc
            { value: "7" },
            { value: "8" },
            { value: "9" },
            { value: "-", text: "+/-" },
            { value: this.env.services.localization.decimalPoint },
            { value: "0" },
            { value: "Backspace", text: "⌫" },
            { value: "", text: "", disabled: true }, // Empty slot
        ];
    },

    // ========== EXISTING METHODS (TIDAK PERLU DIUBAH) ==========

    async _finalizeValidation() {
        console.group("🔐 FINALIZE VALIDATION START");
        
        // ========== CHECK PIC STATUS ==========
        const cashierId = this.pos.get_cashier()?.id;
        if (cashierId) {
            try {
                const employeeData = await this.orm.searchRead(
                    "hr.employee",
                    [["id", "=", cashierId]],
                    ["is_pic"]
                );
                if (employeeData?.[0]?.is_pic) {
                    await this.popup.add(ErrorPopup, {
                        title: "Akses Ditolak",
                        body: "Anda tidak dapat memvalidasi karena status Anda adalah PIC.",
                    });
                    console.groupEnd();
                    return;
                }
            } catch (error) {
                console.error("Error checking employee PIC status:", error);
            }
        }

        // ========== VALIDATE COUPONS IS_USED STATUS ==========
        const activatedCoupons = this.currentOrder.codeActivatedCoupons || [];
        
        if (activatedCoupons.length > 0) {
            console.group("🎫 VALIDATING COUPON IS_USED STATUS");
            console.log("Found", activatedCoupons.length, "activated coupons");

            if (!this._masterServerURL) {
                this._masterServerURL = await fetchMasterServerURL();
            }

            if (!this._masterServerURL) {
                await this.popup.add(ErrorPopup, {
                    title: "Konfigurasi Error",
                    body: "Tidak dapat menemukan konfigurasi Master Server untuk validasi coupon.\n\nSilakan hubungi administrator.",
                });
                console.groupEnd();
                console.groupEnd();
                return;
            }

            for (const coupon of activatedCoupons) {
                console.log(`Checking coupon: ${coupon.code} (ID: ${coupon.id})`);
                
                const checkResult = await checkCouponIsUsed(this._masterServerURL, coupon.code);
                
                if (!checkResult.success) {
                    await this.popup.add(ErrorPopup, {
                        title: "Error Validasi Coupon",
                        body: `Gagal memvalidasi coupon ${coupon.code}.\n\nError: ${checkResult.error}\n\nSilakan coba lagi atau hubungi administrator.`,
                    });
                    console.groupEnd();
                    console.groupEnd();
                    return;
                }

                if (checkResult.isUsed) {
                    console.warn(`❌ Coupon ${coupon.code} sudah digunakan!`);
                    await this.popup.add(ErrorPopup, {
                        title: "Coupon Sudah Digunakan",
                        body: `Coupon ${coupon.code} sudah pernah digunakan dan tidak dapat digunakan lagi.\n\nSilakan batalkan coupon ini dari order.`,
                    });
                    console.groupEnd();
                    console.groupEnd();
                    return;
                }

                console.log(`✅ Coupon ${coupon.code} valid (belum digunakan)`);
            }

            console.log("✅ All coupons validation passed");
            console.groupEnd();
        }

        // ========== VALIDATE POINT REDEMPTIONS ==========
        const rewardLines = this.currentOrder.get_orderlines().filter(line => 
            line.is_reward_line && 
            line.points_cost && 
            line.coupon_id &&
            line.coupon_id > 0
        );

        if (rewardLines.length > 0) {
            console.group("💳 VALIDATING POINT REDEMPTIONS");
            console.log("Found", rewardLines.length, "reward lines to validate");

            const pointsPerCoupon = {};
            for (const line of rewardLines) {
                if (!line.coupon_id || line.coupon_id <= 0) {
                    console.warn(`⚠️ Skipping reward line with invalid coupon_id: ${line.coupon_id}`);
                    continue;
                }
                
                if (!pointsPerCoupon[line.coupon_id]) {
                    pointsPerCoupon[line.coupon_id] = 0;
                }
                pointsPerCoupon[line.coupon_id] += line.points_cost;
                console.log(`- Line: ${line.product.display_name}, Points: ${line.points_cost}, Coupon: ${line.coupon_id}`);
            }

            console.log("Total points per coupon:", pointsPerCoupon);

            if (Object.keys(pointsPerCoupon).length === 0) {
                console.log("ℹ️ No valid loyalty cards to validate (all rewards are system-generated)");
                console.groupEnd();
            } else {
                let validationFailed = false;
                for (const [couponId, requiredPoints] of Object.entries(pointsPerCoupon)) {
                    try {
                        const cardData = await this.orm.read(
                            'loyalty.card',
                            [parseInt(couponId)],
                            ['points', 'code']
                        );

                        if (!cardData || cardData.length === 0) {
                            console.error(`❌ Loyalty card ${couponId} tidak ditemukan!`);
                            await this.popup.add(ErrorPopup, {
                                title: "Loyalty Card Tidak Ditemukan",
                                body: `Loyalty card yang digunakan tidak dapat ditemukan di sistem.\n\nSilakan batalkan redemption dan coba lagi.`,
                            });
                            validationFailed = true;
                            break;
                        }

                        const currentPoints = cardData[0].points || 0;
                        const cardCode = cardData[0].code || couponId;

                        console.log(`Coupon ${cardCode}: Current=${currentPoints}, Required=${requiredPoints}`);

                        if (currentPoints < requiredPoints) {
                            await this.popup.add(ErrorPopup, {
                                title: "Poin Tidak Mencukupi",
                                body: `Loyalty card ${cardCode} tidak memiliki cukup poin.\n\nPoin tersedia: ${currentPoints}\nPoin dibutuhkan: ${requiredPoints}\n\nMohon batalkan redemption atau gunakan poin lebih sedikit.`,
                            });
                            validationFailed = true;
                            break;
                        }
                    } catch (error) {
                        console.error("Error validating loyalty card:", error);
                        await this.popup.add(ErrorPopup, {
                            title: "Validation Error",
                            body: `Gagal validasi loyalty card: ${error.message}\nMohon coba lagi.`,
                        });
                        validationFailed = true;
                        break;
                    }
                }

                if (validationFailed) {
                    console.groupEnd();
                    console.groupEnd();
                    return;
                }

                console.log("✅ All point validations passed");
                console.groupEnd();
            }
        }

        // ========== OPEN CASH DRAWER (if cash payment) ==========
        if (this.currentOrder.is_paid_with_cash() || this.currentOrder.get_change()) {
            this.hardwareProxy.openCashbox();
        }

        // ========== PREPARE ORDER FOR FINALIZATION ==========
        this.currentOrder.date_order = luxon.DateTime.now();
        for (const line of this.paymentLines) {
            if (!line.amount === 0) {
                this.currentOrder.remove_paymentline(line);
            }
        }
        this.currentOrder.finalized = true;

        // ========== SYNC ORDER TO BACKEND ==========
        this.env.services.ui.block();
        let syncOrderResult;
        let syncSuccess = false;

        try {
            syncOrderResult = await this.pos.push_single_order(this.currentOrder);
            if (!syncOrderResult) {
                this.env.services.ui.unblock();
                console.groupEnd();
                return;
            }

            syncSuccess = true;
            console.log("✅ Order synced successfully");

        } catch (error) {
            this.env.services.ui.unblock();
            console.groupEnd();
            if (error instanceof ConnectionLostError) {
                this.pos.showScreen(this.nextScreen);
                return Promise.reject(error);
            } else {
                throw error;
            }
        } finally {
            this.env.services.ui.unblock();
        }

        // ========== POST SYNC PROCESSING ==========
        if (syncOrderResult && syncOrderResult.length > 0 && this.currentOrder.wait_for_push_order()) {
            await this.postPushOrderResolve(syncOrderResult.map((res) => res.id));
        }

        // ========== UPDATE IS_PRINTED STATUS ==========
        if (syncSuccess && syncOrderResult && syncOrderResult.length > 0) {
            console.group("📝 UPDATING IS_PRINTED STATUS");
            
            try {
                const orderId = syncOrderResult[0].id;
                
                await this.orm.write(
                    'pos.order',
                    [orderId],
                    { is_printed: true }
                );
                
                console.log(`✅ Order ${orderId} marked as printed (is_printed=true)`);
                console.groupEnd();
            } catch (error) {
                console.error("❌ Error updating is_printed status:", error);
                console.groupEnd();
            }
        }

        // ========== UPDATE COUPON IS_USED STATUS ==========
        if (syncSuccess && activatedCoupons.length > 0) {
            console.group("🔄 UPDATING COUPON IS_USED STATUS");
            
            for (const coupon of activatedCoupons) {
                console.log(`Updating coupon: ${coupon.code}`);
                
                const updateResult = await updateCouponIsUsed(
                    this._masterServerURL,
                    coupon.code,
                    true
                );
                
                if (updateResult.success) {
                    console.log(`✅ Coupon ${coupon.code} marked as used`);
                } else {
                    console.warn(`⚠️ Failed to update coupon ${coupon.code}: ${updateResult.error}`);
                }
            }
            
            console.groupEnd();
        }

        // ========== POINT DEDUCTION INFO ==========
        if (syncSuccess && rewardLines.length > 0) {
            console.group("💳 POINT DEDUCTION INFO");
            console.log("✅ Order synced successfully");
            console.log("📋 Reward lines found:", rewardLines.length);
            console.log("⚠️  Backend will automatically deduct points via confirm_coupon_programs");
            console.log("🚫 Skipping manual point deduction to avoid double deduction");
            console.groupEnd();
        }

        // ========== DISABLE AUTO PRINT ==========
        const originalPrintAuto = this.pos.config.iface_print_auto;
        this.pos.config.iface_print_auto = false;

        await this.afterOrderValidation(!!syncOrderResult && syncOrderResult.length > 0);

        this.pos.config.iface_print_auto = originalPrintAuto;

        // ========== CASH DRAWER - TRIGGER VIA SERVICE ==========
        if (syncSuccess) {
            await triggerCashDrawer();
        }

        // ========== POLE DISPLAY - UPDATE WITH TOTAL & CHANGE ==========
        if (syncSuccess) {
            const total = this.currentOrder.get_total_with_tax();
            const change = this.currentOrder.get_change();
            
            const formattedTotal = new Intl.NumberFormat('id-ID', {
                minimumFractionDigits: 0,
                maximumFractionDigits: 0
            }).format(total);
            
            const formattedChange = new Intl.NumberFormat('id-ID', {
                minimumFractionDigits: 0,
                maximumFractionDigits: 0
            }).format(change);
            
            const line1 = formatDisplayLine("Total", formattedTotal);
            const line2 = formatDisplayLine("Kembali", formattedChange);

            await sendToPoleDisplay(line1, line2);
        }

        // ========== ✅ OPTIMIZED: AUTO PRINT RECEIPT (NON-BLOCKING) ==========
        if (syncSuccess) {
            // ✅ Fire and forget - don't await
            (async () => {
                try {
                    const config = await loadConfig();
                    
                    console.log("📝 Generating receipt HTML...");
                    const htmlVNode = await this._rendererService.toHtml(OrderReceipt, {
                        data: this.pos.get_order().export_for_printing(),
                        formatCurrency: this.env.utils.formatCurrency,
                    });
                    const html = htmlVNode?.outerHTML || "";
                    
                    console.log("📤 Sending receipt to print service...");
                    
                    // ✅ Set timeout untuk print request
                    const controller = new AbortController();
                    const timeoutId = setTimeout(() => controller.abort(), 5000);
                    
                    const response = await fetch(`${config.services.printService}/print`, {
                        method: "POST",
                        headers: {
                            "Content-Type": "application/json",
                        },
                        body: JSON.stringify({ html }),
                        signal: controller.signal
                    });
                    
                    clearTimeout(timeoutId);
                    
                    if (response.ok) {
                        console.log("✅ Print job queued successfully");
                    } else {
                        console.warn("⚠️ Print service responded with:", response.status);
                    }
                } catch (error) {
                    if (error.name === 'AbortError') {
                        console.warn("⚠️ Print request timeout - printer mungkin lambat");
                    } else {
                        console.error("❌ Print error:", error.message);
                    }
                    // ✅ Don't block the flow even if print fails
                }
            })();
        }

        // ========== ✅ OPTIMIZED: CLEAR POLE DISPLAY (NON-BLOCKING) ==========
        if (syncSuccess) {
            // ✅ Fire and forget
            (async () => {
                console.log("🧹 Clearing pole display...");
                await clearPoleDisplay();
                
                await new Promise(resolve => setTimeout(resolve, 500));
                
                const welcomeLine1 = "Welcome to KemChicks".padEnd(20).substring(0, 20);
                const welcomeLine2 = "Ready!".padStart(20).substring(0, 20);
                await sendToPoleDisplay(welcomeLine1, welcomeLine2);
            })();
        }

        console.log("✅ FINALIZE VALIDATION COMPLETED");
        console.groupEnd();

        // ✅ Navigate immediately - don't wait for print/pole
        this.pos.showScreen(this.nextScreen);
    },

    async addNewPaymentLine(paymentMethod) {
        const result = this.currentOrder.add_paymentline(paymentMethod);
        
        if (!this.pos.get_order().check_paymentlines_rounding()) {
            this._display_popup_error_paymentlines_rounding();
        }

        if (result) {
            const paymentLine = this.currentOrder.selected_paymentline;
            if (paymentLine) {
                paymentLine.set_amount(0);
            }
            
            this.numberBuffer.reset();
            
            if (paymentMethod.type !== "cash") {
                const { confirmed, payload } = await this.popup.add(NumericKeyboardPopup, {
                    title: "Input 4 Digit Terakhir Kartu",
                    maxLength: 4,
                    placeholder: "Contoh: 1234",
                    body: "Masukkan 4 digit terakhir dari nomor kartu"
                });

                if (confirmed && payload) {
                    if (paymentLine) {
                        paymentLine.card_number = payload;
                    }
                } else if (!confirmed) {
                    if (paymentLine) {
                        this.currentOrder.remove_paymentline(paymentLine);
                    }
                    return false;
                }
            }
            
            return true;
        } else {
            await this.popup.add(ErrorPopup, {
                title: "Error",
                body: "Sudah ada pembayaran elektronik dalam proses.",
            });
            return false;
        }
    },
});