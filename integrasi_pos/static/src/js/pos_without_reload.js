// /** @odoo-module **/

// import { onWillStart } from "@odoo/owl";
// import { useBus } from "@web/core/utils/hooks";
// import { patch } from "@web/core/utils/patch";
// import { PosStore } from "@point_of_sale/app/store/pos_store";

// patch(PosStore.prototype, {
//     setup() {
//         super.setup();
//         const busService = useBus();

//         busService.addEventListener("notification", ({ detail }) => {
//             for (const notif of detail) {
//                 const [dbname, channel, payload] = notif;
//                 if (channel === "loyalty.update") {
//                     this._onLoyaltyUpdate(payload);
//                 }
//             }
//         });
//     },

//     async _onLoyaltyUpdate(payload) {
//         if (payload.type === "schedule") {
//             const updatedSchedules = await this.rpc({
//                 model: "loyalty.program.schedule",
//                 method: "search_read",
//                 args: [[["id", "=", payload.id]]],
//                 fields: ["id", "program_id", "days", "time_start", "time_end"]
//             });

//             for (const schedule of updatedSchedules) {
//                 const index = this.loyalty_schedules.findIndex(s => s.id === schedule.id);
//                 if (index !== -1) {
//                     this.loyalty_schedules[index] = schedule;
//                 } else {
//                     this.loyalty_schedules.push(schedule);
//                 }
//             }

//             this.pos.orders.forEach(order => order._updateRewards());
//         }

//         if (payload.type === "member") {
//             const updatedMembers = await this.rpc({
//                 model: "loyalty.member",
//                 method: "search_read",
//                 args: [[["id", "=", payload.id]]],
//                 fields: ["id", "member_program_id", "member_pos"]
//             });

//             for (const member of updatedMembers) {
//                 const index = this.loyalty_members.findIndex(m => m.id === member.id);
//                 if (index !== -1) {
//                     this.loyalty_members[index] = member;
//                 } else {
//                     this.loyalty_members.push(member);
//                 }
//             }

//             this.pos.orders.forEach(order => order._updateRewards());
//         }
//     },
// });
