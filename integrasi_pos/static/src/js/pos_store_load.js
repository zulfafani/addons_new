/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { PosStore } from "@point_of_sale/app/store/pos_store";

patch(PosStore.prototype, {
    async _processData(loadedData) {
        await super._processData(...arguments);

        // Inject Configuration Settings
        const configSettings = loadedData["res.config.settings"]?.[0];
        if (configSettings) {
            Object.assign(this.config, {
                validate_discount_amount: configSettings.validate_discount_amount,
                validate_closing_pos: configSettings.validate_closing_pos,
                validate_order_line_deletion: configSettings.validate_order_line_deletion,
                validate_discount: configSettings.validate_discount,
                validate_price_change: configSettings.validate_price_change,
                validate_order_deletion: configSettings.validate_order_deletion,
                validate_add_remove_quantity: configSettings.validate_add_remove_quantity,
                validate_payment: configSettings.validate_payment,
                validate_end_shift: configSettings.validate_end_shift,
                validate_refund: configSettings.validate_refund,
                validate_close_session: configSettings.validate_close_session,
                validate_void_sales: configSettings.validate_void_sales,
                validate_member_schedule: configSettings.validate_member_schedule,
                one_time_password: configSettings.one_time_password,
                multiple_barcode_activate: configSettings.multiple_barcode_activate,
                manager_validation: configSettings.manager_validation,
                manager_pin: configSettings.manager_pin,
                manager_name: configSettings.manager_name,
            });
            console.log("🔐 POS Config injected from res.config.settings:", configSettings);
        }

        // Barcode Config Injection
        const barcodeConfig = loadedData["barcode.config"]?.[0];
        if (barcodeConfig) {
            Object.assign(this.config, {
                digit_awal: parseInt(barcodeConfig.digit_awal || 2),
                digit_akhir: parseInt(barcodeConfig.digit_akhir || 4),
                prefix_timbangan: barcodeConfig.prefix_timbangan || "",
                panjang_barcode: parseInt(barcodeConfig.panjang_barcode || 7),
            });
            console.log("📦 Barcode Config Loaded Offline:", barcodeConfig);
        }

        // Multiple Barcode Loader & Indexing
        const multiBarcodes = loadedData["multiple.barcode"] || [];
        this.db.multi_barcode_map = {};

        for (const entry of multiBarcodes) {
            const productId = Array.isArray(entry.product_id)
                ? entry.product_id[0]
                : entry.product_id;

            if (!entry.barcode || !productId) continue;

            if (!this.db.multi_barcode_map[entry.barcode]) {
                this.db.multi_barcode_map[entry.barcode] = new Set();
            }

            this.db.multi_barcode_map[entry.barcode].add(productId);
        }

        // Enrich Product Model with multi_barcode_ids
        const allProducts = Object.values(this.db.product_by_id || {});
        for (const product of allProducts) {
            product.multi_barcode_ids = [];

            for (const [barcode, productSet] of Object.entries(this.db.multi_barcode_map)) {
                if (productSet.has(product.id)) {
                    product.multi_barcode_ids.push(barcode);
                }
            }
        }

        // POS Session Data: cashier + shifts
        this.cashier_logs = loadedData["pos.cashier.log"] || [];
        this.end_shifts = loadedData["end.shift"] || [];
        this.end_shift_lines = loadedData["end.shift.line"] || [];

        console.log("📥 pos.cashier.log:", this.cashier_logs.length);
        console.log("📥 end.shift:", this.end_shifts.length);
        console.log("📥 end.shift.line:", this.end_shift_lines.length);

        // Loyalty Data: schedule + member
        this.loyalty_schedules = loadedData["loyalty.program.schedule"] || [];
        this.loyalty_members = loadedData["loyalty.member"] || [];

        this.loyalty_members.forEach(member => {
            const pos = member.member_pos;
            const pos_id = Array.isArray(pos) ? pos[0] : pos;
            const pos_name = Array.isArray(pos) ? pos[1] : '';
        });

        // Loyalty Program Activation by Valid Schedule
        this.programs = loadedData["loyalty.program"] || [];
        if (this.programs && Array.isArray(this.programs)) {
            const validProgramIds = new Set(
                (this.loyalty_schedules || []).map(s => {
                    const pid = Array.isArray(s.program_id)
                        ? s.program_id[0]
                        : typeof s.program_id === "object"
                            ? s.program_id?.id
                            : s.program_id;
                    return Number(pid);
                })
            );
            this.programs.forEach(program => {
                program.active = validProgramIds.has(Number(program.id));
            });
        }

        // Patch partner.category_id
        const rawPartners = loadedData["res.partner"] || [];
        const partnerCategoryMap = Object.fromEntries(
            rawPartners.map(p => [p.id, Array.isArray(p.category_id) ? p.category_id : []])
        );

        this.partners.forEach(p => {
            if (partnerCategoryMap[p.id]) {
                p.category_id = partnerCategoryMap[p.id];
            }
        });
    }
});
