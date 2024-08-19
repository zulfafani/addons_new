// /** @odoo-module **/

// import { patch } from "@web/core/utils/patch";
// import { Order } from "@point_of_sale/app/store/models";

// patch(Order.prototype, {
//     get_orderlines() {
//         // ✅ Filter out orderlines with missing product or broken structure
//         return super.get_orderlines().filter((line) => {
//             const valid = line && line.product && typeof line.product.id !== 'undefined';
//             if (!valid) {
//                 console.warn("⛔ Found invalid orderline removed from get_orderlines():", line);
//             }
//             return valid;
//         });
//     },

//     _computeUnclaimedFreeProductQty(reward, coupon_id, product, remainingPoints) {
//         let claimed = 0;
//         let available = 0;
//         let shouldCorrectRemainingPoints = false;

//         for (const line of this.get_orderlines()) {
//             const lineProductId = line?.product?.id;
//             const lineRewardProductId = line?.reward_product_id;

//             if (!lineProductId && !lineRewardProductId) continue;

//             if (
//                 reward.reward_product_ids.includes(product.id) &&
//                 reward.reward_product_ids.includes(lineProductId)
//             ) {
//                 if (this._get_reward_lines().length === 0) {
//                     if (lineProductId === product.id) {
//                         available += line.get_quantity();
//                     }
//                 } else {
//                     available += line.get_quantity();
//                 }
//             } else if (reward.reward_product_ids.includes(lineRewardProductId)) {
//                 if (line.reward_id === reward.id) {
//                     remainingPoints += line.points_cost;
//                     claimed += line.get_quantity();
//                 } else {
//                     shouldCorrectRemainingPoints = true;
//                 }
//             }
//         }

//         return {
//             claimed,
//             available,
//             remainingPoints,
//             shouldCorrectRemainingPoints,
//         };
//     }
// });
