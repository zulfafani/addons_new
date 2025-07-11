/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { PosStore } from "@point_of_sale/app/store/pos_store";

patch(PosStore.prototype, {
    async _processData(loadedData) {
        await super._processData(...arguments);

        try {
            // 🔐 Config Settings
            const configSettings = loadedData["res.config.settings"]?.[0];
            if (configSettings) {
                Object.assign(this.config, configSettings);
                console.log("✅ POS Config injected:", configSettings);
            }

            // 🏢 Company Info
            const companies = loadedData["res.company"] || [];
            if (companies.length) {
                this.company = companies[0];
                console.log("✅ Company loaded:", this.company);
            }

            // 📦 Barcode Config
            const barcodeConfig = loadedData["barcode.config"]?.[0];
            if (barcodeConfig) {
                Object.assign(this.config, {
                    digit_awal: parseInt(barcodeConfig.digit_awal || 2),
                    digit_akhir: parseInt(barcodeConfig.digit_akhir || 4),
                    prefix_timbangan: barcodeConfig.prefix_timbangan || "",
                    panjang_barcode: parseInt(barcodeConfig.panjang_barcode || 7),
                });
                console.log("✅ Barcode Config loaded:", barcodeConfig);
            }

            // 🧾 POS Order Lines
            const posOrderLines = loadedData["pos.order.line"] || [];
            this.pos_order_lines = posOrderLines;
            this.order_line_numbers = {};

            for (const line of posOrderLines) {
                try {
                    const orderId = Array.isArray(line.order_id) ? line.order_id[0] : line.order_id;
                    if (orderId) {
                        if (!this.order_line_numbers[orderId]) {
                            this.order_line_numbers[orderId] = {};
                        }
                        this.order_line_numbers[orderId][line.id] = line.line_number || 1;
                    }
                } catch (e) {
                    console.error("❌ Error processing order line:", line, e);
                }
            }

            console.log(`✅ Loaded ${posOrderLines.length} pos.order.line records`);
            console.log("📊 order_line_numbers:", this.order_line_numbers);

            // 🕵️ Session Data
            this.cashier_logs = loadedData["pos.cashier.log"] || [];
            // this.end_shifts = loadedData["end.shift"] || [];
            // this.end_shift_lines = loadedData["end.shift.line"] || [];

            console.log(`✅ Loaded ${this.cashier_logs.length} pos.cashier.log records`);
            // console.log(`✅ Loaded ${this.end_shifts.length} end.shift records`);
            // console.log(`✅ Loaded ${this.end_shift_lines.length} end.shift.line records`);

            // 🗓️ Loyalty Schedules
            this.loyalty_schedules = Array.isArray(loadedData["loyalty.program.schedule"])
                ? loadedData["loyalty.program.schedule"]
                : [];
            
            console.log(`✅ Loaded ${this.loyalty_schedules.length} loyalty schedules`);

            // 👥 Loyalty Members
            this.loyalty_members = Array.isArray(loadedData["loyalty.member"])
                ? loadedData["loyalty.member"]
                : [];
            
            console.log(`✅ Loaded ${this.loyalty_members.length} loyalty members`);

            // 🏷️ Loyalty Programs
            this.programs = Array.isArray(loadedData["loyalty.program"])
                ? loadedData["loyalty.program"]
                : [];

            // Set program active status based on schedules
            const validProgramIds = new Set();
            for (const schedule of this.loyalty_schedules) {
                try {
                    let pid;
                    if (Array.isArray(schedule.program_id)) {
                        pid = schedule.program_id[0];
                    } else if (typeof schedule.program_id === "object" && schedule.program_id !== null) {
                        pid = schedule.program_id.id;
                    } else {
                        pid = schedule.program_id;
                    }
                    
                    if (pid) {
                        validProgramIds.add(Number(pid));
                    }
                } catch (e) {
                    console.error("❌ Error processing schedule:", schedule, e);
                }
            }

            for (const program of this.programs) {
                program.active = validProgramIds.has(Number(program.id));
            }

            console.log(`✅ Loaded ${this.programs.length} loyalty programs`);

            // 👤 HR Employee (Salesperson)
            this.hr_employee = Array.isArray(loadedData["hr.employee"])
                ? loadedData["hr.employee"]
                : [];
            
            console.log(`✅ Loaded ${this.hr_employee.length} HR employees`);

            // 👤 Patch partner.category_id
            const rawPartners = loadedData["res.partner"] || [];
            const partnerCategoryMap = {};
            
            for (const p of rawPartners) {
                try {
                    partnerCategoryMap[p.id] = Array.isArray(p.category_id) ? p.category_id : [];
                } catch (e) {
                    console.error("❌ Error processing partner:", p, e);
                }
            }

            // Apply category_id to partners
            if (this.partners) {
                for (const p of this.partners) {
                    try {
                        if (partnerCategoryMap[p.id]) {
                            p.category_id = partnerCategoryMap[p.id];
                        }
                    } catch (e) {
                        console.error("❌ Error patching partner:", p, e);
                    }
                }

                console.log("✅ Patched Partner Categories");
                const partnersWithCategories = this.partners.filter(
                    p => Array.isArray(p.category_id) && p.category_id.length > 0
                );
                
                console.log(`✅ ${partnersWithCategories.length} partners have categories`);
            }

            // 📦 Multiple Barcodes
            this.multiple_barcodes = Array.isArray(loadedData["multiple.barcode"])
                ? loadedData["multiple.barcode"]
                : [];
            
            console.log(`✅ Loaded ${this.multiple_barcodes.length} multiple barcodes`);

            // 🎯 Loyalty Rules
            this.loyalty_rules = Array.isArray(loadedData["loyalty.rule"])
                ? loadedData["loyalty.rule"]
                : [];
            
            console.log(`✅ Loaded ${this.loyalty_rules.length} loyalty rules`);

            // 🎁 Loyalty Rewards
            this.loyalty_rewards = Array.isArray(loadedData["loyalty.reward"])
                ? loadedData["loyalty.reward"]
                : [];
            
            console.log(`✅ Loaded ${this.loyalty_rewards.length} loyalty rewards`);

            console.log("✅ All POS data loaded successfully!");

        } catch (error) {
            console.error("❌ Error in _processData:", error);
            // Don't throw - allow POS to continue loading
        }
    },

    // getOrderLineNumber(orderId, lineId) {
    //     try {
    //         return this.order_line_numbers[orderId]?.[lineId] || 1;
    //     } catch (e) {
    //         console.error("❌ Error getting order line number:", e);
    //         return 1;
    //     }
    // },

    // setOrderLineNumber(orderId, lineId, lineNumber) {
    //     try {
    //         if (!this.order_line_numbers[orderId]) {
    //             this.order_line_numbers[orderId] = {};
    //         }
    //         this.order_line_numbers[orderId][lineId] = lineNumber;
    //     } catch (e) {
    //         console.error("❌ Error setting order line number:", e);
    //     }
    // },

    // getNextLineNumber(orderId) {
    //     try {
    //         const lines = this.order_line_numbers[orderId] || {};
    //         const numbers = Object.values(lines);
    //         return numbers.length ? Math.max(...numbers) + 1 : 1;
    //     } catch (e) {
    //         console.error("❌ Error getting next line number:", e);
    //         return 1;
    //     }
    // }
});