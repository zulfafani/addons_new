// /** @odoo-module **/

// import { patch } from "@web/core/utils/patch";
// import { Order } from "@point_of_sale/app/store/models";
// const { DateTime } = luxon;

// patch(Order.prototype, {
//     add_product(product, options) {
//         const result = super.add_product(...arguments);

//         const lastLine = this.get_last_orderline();
//         if (!lastLine) return result;

//         const productId = lastLine.get_product().id;

//         for (const program of this.pos.programs || []) {
//             const hasReward = program.rules?.some(rule =>
//                 rule.valid_product_ids?.has?.(productId)
//             );

//             if (hasReward && !this._isProgramScheduleValid(program)) {
//                 // Hapus semua reward lines yang muncul karena produk ini
//                 const rewardLines = this.get_orderlines().filter((line) =>
//                     line.is_reward_line &&
//                     line.reward_product_id === productId &&
//                     this.pos.reward_by_id[line.reward_id]?.program_id?.id === program.id
//                 );

//                 for (const rewardLine of rewardLines) {
//                     this.removeOrderline(rewardLine); // ✅ Gunakan cara resmi
//                 }

//                 window.alert(`⛔ Promo "${program.name}" tidak aktif sekarang. Produk tetap ditambahkan tanpa reward.`);
//                 break;
//             }
//         }

//         return result;
//     }
// });
