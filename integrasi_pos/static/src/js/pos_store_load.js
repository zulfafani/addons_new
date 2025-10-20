/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { PosStore } from "@point_of_sale/app/store/pos_store";

patch(PosStore.prototype, {
    async _processData(loadedData) {
        await super._processData(...arguments);

        try {
            console.log("🔄 Starting custom POS data processing...");

            // 🔐 Config Settings
            const configSettings = loadedData["res.config.settings"]?.[0];
            if (configSettings) {
                Object.assign(this.config, configSettings);
                console.log("✅ POS Config injected:", {
                    manager_validation: configSettings.manager_validation,
                    validate_discount: configSettings.validate_discount,
                    validate_payment: configSettings.validate_payment
                });
            } else {
                console.warn("⚠️ No config settings loaded");
            }

            // 🏢 Company Info
            const companies = loadedData["res.company"] || [];
            if (companies.length) {
                this.company = companies[0];
                console.log("✅ Company loaded:", this.company.name);
            } else {
                console.warn("⚠️ No company data loaded");
            }

            // 📦 Barcode Config
            const barcodeConfig = loadedData["barcode.config"]?.[0];
            if (barcodeConfig) {
                Object.assign(this.config, {
                    digit_awal: parseInt(barcodeConfig.digit_awal || 2),
                    digit_akhir: parseInt(barcodeConfig.digit_akhir || 4),
                    prefix_timbangan: barcodeConfig.prefix_timbangan || "",
                    panjang_barcode: parseInt(barcodeConfig.panjang_barcode || 7),
                    multiple_barcode_activate: barcodeConfig.multiple_barcode_activate || false,
                });
                console.log("✅ Barcode Config loaded:", {
                    digit_awal: this.config.digit_awal,
                    digit_akhir: this.config.digit_akhir,
                    prefix_timbangan: this.config.prefix_timbangan
                });
            } else {
                console.warn("⚠️ No barcode config loaded");
            }

            // 🗓️ Loyalty Schedules
            this.loyalty_schedules = [];
            const rawSchedules = loadedData["loyalty.program.schedule"];
            if (Array.isArray(rawSchedules)) {
                this.loyalty_schedules = rawSchedules;
                console.log(`✅ Loaded ${this.loyalty_schedules.length} loyalty schedules`);
            } else {
                console.warn("⚠️ No loyalty schedules loaded");
            }

            // 👥 Loyalty Members
            this.loyalty_members = [];
            const rawMembers = loadedData["loyalty.member"];
            if (Array.isArray(rawMembers)) {
                this.loyalty_members = rawMembers;
                console.log(`✅ Loaded ${this.loyalty_members.length} loyalty members`);
            } else {
                console.warn("⚠️ No loyalty members loaded");
            }

            // 🏷️ Loyalty Programs
            this.programs = [];
            const rawPrograms = loadedData["loyalty.program"];
            if (Array.isArray(rawPrograms)) {
                this.programs = rawPrograms;

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

                // Apply active status to programs
                for (const program of this.programs) {
                    program.active = validProgramIds.has(Number(program.id));
                }

                console.log(`✅ Loaded ${this.programs.length} loyalty programs (${validProgramIds.size} active)`);
            } else {
                console.warn("⚠️ No loyalty programs loaded");
            }

            // 🎯 Loyalty Rules
            this.loyalty_rules = [];
            const rawRules = loadedData["loyalty.rule"];
            if (Array.isArray(rawRules)) {
                this.loyalty_rules = rawRules;
                console.log(`✅ Loaded ${this.loyalty_rules.length} loyalty rules`);
            } else {
                console.warn("⚠️ No loyalty rules loaded");
            }

            // 🎁 Loyalty Rewards
            this.loyalty_rewards = [];
            const rawRewards = loadedData["loyalty.reward"];
            if (Array.isArray(rawRewards)) {
                this.loyalty_rewards = rawRewards;
                console.log(`✅ Loaded ${this.loyalty_rewards.length} loyalty rewards`);
            } else {
                console.warn("⚠️ No loyalty rewards loaded");
            }

            // 👤 HR Employee (Salesperson)
            this.hr_employee = [];
            const rawEmployees = loadedData["hr.employee"];
            if (Array.isArray(rawEmployees)) {
                this.hr_employee = rawEmployees;
                console.log(`✅ Loaded ${this.hr_employee.length} HR employees`);
            } else {
                console.warn("⚠️ No HR employees loaded");
            }

            // 👤 HR Employee Config Settings
            this.hr_employee_config = [];
            const rawEmployeeConfig = loadedData["hr.employee.config.settings"];
            if (Array.isArray(rawEmployeeConfig)) {
                this.hr_employee_config = rawEmployeeConfig;
                console.log(`✅ Loaded ${this.hr_employee_config.length} HR employee configs`);
            } else {
                console.warn("⚠️ No HR employee configs loaded");
            }

            // 👤 Patch Partner Categories
            const rawPartners = loadedData["res.partner"] || [];
            const partnerCategoryMap = {};
            
            for (const p of rawPartners) {
                try {
                    if (p && p.id) {
                        partnerCategoryMap[p.id] = Array.isArray(p.category_id) ? p.category_id : [];
                    }
                } catch (e) {
                    console.error("❌ Error processing partner categories:", p, e);
                }
            }

            // Apply category_id to partners
            if (this.partners) {
                let categorizedCount = 0;
                for (const p of this.partners) {
                    try {
                        if (partnerCategoryMap[p.id]) {
                            p.category_id = partnerCategoryMap[p.id];
                            if (p.category_id.length > 0) {
                                categorizedCount++;
                            }
                        }
                    } catch (e) {
                        console.error("❌ Error patching partner:", p, e);
                    }
                }

                console.log(`✅ Patched ${this.partners.length} partners (${categorizedCount} have categories)`);
            } else {
                console.warn("⚠️ No partners to patch");
            }

            // 📦 Multiple Barcodes
            this.multiple_barcodes = [];
            const rawBarcodes = loadedData["multiple.barcode"];
            if (Array.isArray(rawBarcodes)) {
                this.multiple_barcodes = rawBarcodes;
                console.log(`✅ Loaded ${this.multiple_barcodes.length} multiple barcodes`);
            } else {
                console.warn("⚠️ No multiple barcodes loaded");
            }

            // 💰 POS Cashier Logs
            this.cashier_logs = [];
            const rawLogs = loadedData["pos.cashier.log"];
            if (Array.isArray(rawLogs)) {
                this.cashier_logs = rawLogs;
                console.log(`✅ Loaded ${this.cashier_logs.length} cashier logs`);
            } else {
                console.warn("⚠️ No cashier logs loaded");
            }

            console.log("✅ All custom POS data loaded successfully!");
            console.log("📊 Data Summary:", {
                config: !!configSettings,
                company: !!this.company,
                barcodeConfig: !!barcodeConfig,
                schedules: this.loyalty_schedules.length,
                members: this.loyalty_members.length,
                programs: this.programs.length,
                rules: this.loyalty_rules.length,
                rewards: this.loyalty_rewards.length,
                employees: this.hr_employee.length,
                employeeConfigs: this.hr_employee_config.length,
                partners: this.partners ? this.partners.length : 0,
                multipleBarcodes: this.multiple_barcodes.length,
                cashierLogs: this.cashier_logs.length,
            });

        } catch (error) {
            console.error("❌ Critical error in _processData:", error);
            console.error("❌ Error stack:", error.stack);
            // Don't throw - allow POS to continue loading with partial data
        }
    },

    /**
     * Helper method to get loyalty program by ID
     */
    getLoyaltyProgram(programId) {
        try {
            return this.programs.find(p => p.id === programId);
        } catch (e) {
            console.error("❌ Error getting loyalty program:", e);
            return null;
        }
    },

    /**
     * Helper method to get active loyalty programs
     */
    getActiveLoyaltyPrograms() {
        try {
            return this.programs.filter(p => p.active === true);
        } catch (e) {
            console.error("❌ Error getting active programs:", e);
            return [];
        }
    },

    /**
     * Helper method to get employee by ID
     */
    getEmployee(employeeId) {
        try {
            return this.hr_employee.find(emp => emp.id === employeeId);
        } catch (e) {
            console.error("❌ Error getting employee:", e);
            return null;
        }
    },

    /**
     * Helper method to check if employee is cashier
     */
    isEmployeeCashier(employeeId) {
        try {
            const config = this.hr_employee_config.find(c => {
                const empId = Array.isArray(c.employee_id) ? c.employee_id[0] : c.employee_id;
                return empId === employeeId;
            });
            return config ? config.is_cashier : false;
        } catch (e) {
            console.error("❌ Error checking cashier status:", e);
            return false;
        }
    },

    /**
     * Helper method to check if employee is sales person
     */
    isEmployeeSalesPerson(employeeId) {
        try {
            const config = this.hr_employee_config.find(c => {
                const empId = Array.isArray(c.employee_id) ? c.employee_id[0] : c.employee_id;
                return empId === employeeId;
            });
            return config ? config.is_sales_person : false;
        } catch (e) {
            console.error("❌ Error checking sales person status:", e);
            return false;
        }
    },

    /**
     * Helper method to get product by multiple barcode
     */
    getProductByMultipleBarcode(barcode) {
        try {
            const barcodeRecord = this.multiple_barcodes.find(b => b.barcode === barcode);
            if (barcodeRecord && barcodeRecord.product_id) {
                const productId = Array.isArray(barcodeRecord.product_id) 
                    ? barcodeRecord.product_id[0] 
                    : barcodeRecord.product_id;
                return this.db.get_product_by_id(productId);
            }
            return null;
        } catch (e) {
            console.error("❌ Error getting product by multiple barcode:", e);
            return null;
        }
    },

    /**
     * Helper method to check if manager validation is required
     */
    requiresManagerValidation(action) {
        try {
            if (!this.config.manager_validation) {
                return false;
            }

            const validationMap = {
                'discount': this.config.validate_discount,
                'discount_amount': this.config.validate_discount_amount,
                'price_change': this.config.validate_price_change,
                'payment': this.config.validate_payment,
                'refund': this.config.validate_refund,
                'delete_order': this.config.validate_order_deletion,
                'delete_line': this.config.validate_order_line_deletion,
                'end_shift': this.config.validate_end_shift,
                'close_session': this.config.validate_close_session,
                'void_sales': this.config.validate_void_sales,
                'member_schedule': this.config.validate_member_schedule,
                'cash_drawer': this.config.validate_cash_drawer,
                'reprint': this.config.validate_reprint_receipt,
                'discount_button': this.config.validate_discount_button,
                'pricelist': this.config.validate_pricelist,
                'add_remove_qty': this.config.validate_add_remove_quantity,
            };

            return validationMap[action] || false;
        } catch (e) {
            console.error("❌ Error checking manager validation:", e);
            return false;
        }
    },
});