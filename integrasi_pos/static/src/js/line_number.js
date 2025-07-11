// /** @odoo-module **/

// import { patch } from "@web/core/utils/patch";
// import { Orderline } from "@point_of_sale/app/store/models";

// // Patch Orderline model to include sequence
// patch(Orderline.prototype, {
//     getNextSequence() {
//         if (!this.order) return 10;
//         const orderlines = this.order.get_orderlines();
//         if (!orderlines || orderlines.length === 0) return 10;
//         const sequences = orderlines.map(line => line.sequence || 0).filter(seq => seq > 0);
//         const maxSequence = sequences.length > 0 ? Math.max(...sequences) : 0;
//         return maxSequence + 10;
//     },

//     getDisplaySequence() {
//         if (!this.order) return 1;
//         const orderlines = this.order.get_orderlines();
//         if (!orderlines || orderlines.length === 0) return 1;
        
//         // Sort by sequence, fallback to creation order
//         const sortedLines = orderlines.sort((a, b) => {
//             const seqA = a.sequence || (orderlines.indexOf(a) + 1) * 10;
//             const seqB = b.sequence || (orderlines.indexOf(b) + 1) * 10;
//             return seqA - seqB;
//         });
        
//         const index = sortedLines.findIndex(line => line.cid === this.cid);
//         return index >= 0 ? index + 1 : 1;
//     },

//     export_as_JSON() {
//         const json = this._super?.(...arguments) || {};
//         // Ensure sequence is set
//         if (!this.sequence) {
//             this.sequence = this.getNextSequence();
//         }
//         json.sequence = this.sequence;
//         return json;
//     },

//     init_from_JSON(json) {
//         this._super?.(...arguments);
//         this.sequence = json.sequence || this.getNextSequence();
//     }
// });