// /** @odoo-module **/

// import { patch } from "@web/core/utils/patch";
// import { Order } from "@point_of_sale/app/store/models";

// patch(Order.prototype, {
//     async _updateRewards() {
//         const orm = this.orm || this.env.services.orm;

//         const updateRewardsMutex = this.env.services.concurrency.Mutex.create();
//         const now = new Date();
//         const dayMap = {
//             0: "sunday",
//             1: "monday",
//             2: "tuesday",
//             3: "wednesday",
//             4: "thursday",
//             5: "friday",
//             6: "saturday",
//         };
//         const currentDay = dayMap[now.getDay()];
//         const currentTime = now.getHours() + now.getMinutes() / 60;

//         if (!this.pos.programs.length) return;

//         await updateRewardsMutex.exec(async () => {
//             await this._updateLoyaltyPrograms();

//             // 🔍 Step 1: Filter rewards by valid schedule
//             const schedules = await orm.silent.call(
//                 "loyalty.program.schedule",
//                 "search_read",
//                 [[], ["program_id", "days", "time_start", "time_end"]]
//             );

//             const claimableRewards = this.getClaimableRewards(false, false, true);
//             let changed = false;

//             for (const { coupon_id, reward } of claimableRewards) {
//                 const programSchedules = schedules.filter(
//                     (s) =>
//                         s.program_id[0] === reward.program_id.id &&
//                         s.days === currentDay &&
//                         s.time_start <= currentTime &&
//                         currentTime <= s.time_end
//                 );

//                 if (!programSchedules.length) {
//                     console.log(
//                         `⛔ Skipping reward '${reward.name}' (ID ${reward.id}) due to inactive schedule.`
//                     );
//                     continue;
//                 }

//                 const result = this._applyReward(reward, coupon_id);
//                 if (result === true) {
//                     changed = true;
//                 }
//             }

//             if (changed) {
//                 await this._updateLoyaltyPrograms();
//             }

//             this._updateRewardLines();
//         });
//     },
// });
