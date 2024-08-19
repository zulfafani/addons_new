// /** @odoo-module **/

// import { patch } from "@web/core/utils/patch";
// import { ReceiptScreen } from "@point_of_sale/app/screens/receipt_screen/receipt_screen";
// import { OrderReceipt } from "@point_of_sale/app/screens/receipt_screen/receipt/order_receipt";

// console.log("🔥 Patch ReceiptScreen loaded successfully");

// patch(ReceiptScreen.prototype, {
//     async onMounted() {
//         console.log("🚀 ReceiptScreen onMounted called");
        
//         // Panggil parent method
//         await super.onMounted?.();

//         console.log("🟢 ReceiptScreen onMounted triggered");
//         console.log("📊 Current order:", this.pos.get_order());

//         // Tambah delay untuk memastikan rendering selesai
//         setTimeout(async () => {
//             try {
//                 console.log("⏰ Starting receipt processing after delay...");
                
//                 const order = this.pos.get_order();
//                 if (!order) {
//                     console.error("❌ No order found");
//                     return;
//                 }

//                 const orderData = order.export_for_printing();
//                 console.log("📋 Order data:", orderData);

//                 const htmlContent = await this.renderer.toHTML(OrderReceipt, {
//                     data: orderData,
//                     formatCurrency: this.env.utils.formatCurrency,
//                 });

//                 console.log("📤 HTML Content length:", htmlContent.length);
//                 console.log("📤 HTML Preview:", htmlContent.substring(0, 200) + "...");

//                 // Test koneksi ke server dulu
//                 console.log("🔗 Testing server connection...");
                
//                 const res = await fetch("http://localhost:3001/print-receipt", {
//                     method: "POST",
//                     headers: { 
//                         "Content-Type": "application/json",
//                         "Accept": "application/json"
//                     },
//                     body: JSON.stringify({ html: htmlContent }),
//                 });

//                 console.log("📡 Response status:", res.status);
//                 console.log("📡 Response ok:", res.ok);

//                 if (!res.ok) {
//                     throw new Error(`HTTP error! status: ${res.status}`);
//                 }

//                 const result = await res.text();
//                 console.log("✅ Server response:", result);
                
//                 // Tampilkan notifikasi sukses
//                 this.env.services.notification.add("Receipt sent to printer successfully!", {
//                     type: "success"
//                 });

//             } catch (err) {
//                 console.error("❌ Error details:", err);
//                 console.error("❌ Error message:", err.message);
//                 console.error("❌ Error stack:", err.stack);
                
//                 // Tampilkan notifikasi error
//                 this.env.services.notification.add(`Print error: ${err.message}`, {
//                     type: "danger"
//                 });
//             }
//         }, 1000); // Delay 1 detik
//     },

//     // Override method lain untuk debugging
//     async _printReceipt() {
//         console.log("🖨️ _printReceipt method called");
//         return super._printReceipt?.();
//     }
// });