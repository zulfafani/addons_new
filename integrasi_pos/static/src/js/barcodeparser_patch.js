// /** @odoo-module **/

// import { patch } from "@web/core/utils/patch";
// import { BarcodeParser } from "@barcodes/js/barcode_parser";
// import { jsonrpc } from "@web/core/network/rpc_service";

// let loadedConfig = null;

// // Load config once (make it non-async to avoid Promise issues)
// function getLoadedConfig() {
//     return loadedConfig || {};
// }

// // Load the config separately
// jsonrpc("/pos/get_barcode_settings").then(config => {
//     loadedConfig = config;
//     console.log("🛠️ Barcode Config Loaded:", loadedConfig);
// }).catch(err => {
//     console.error("❌ Failed to load barcode config:", err);
//     loadedConfig = {};
// });

// // Patch Odoo barcode parser - make it non-async
// patch(BarcodeParser.prototype, {
//     parse_barcode(barcode) {
//         try {
//             // Validate barcode is defined
//             if (!barcode) {
//                 console.warn("⚠️ Barcode is undefined, returning empty result");
//                 return { encoding: "error", type: "error", code: "", base_code: "" };
//             }

//             const config = getLoadedConfig();

//             // 💡 Use fallback defaults if fields don't exist
//             const prefixTimbangan = config?.prefix_timbangan || "20";
//             const digitAwal = parseInt(config?.digit_awal ?? "2");
//             const digitAkhir = parseInt(config?.digit_akhir ?? "4");
//             const panjangBarcode = parseInt(config?.panjang_barcode ?? "7");

//             console.log("🔍 Parsed Config Values:", {
//                 prefixTimbangan,
//                 digitAwal,
//                 digitAkhir,
//                 panjangBarcode,
//                 rawBarcode: barcode
//             });

//             if (!barcode || typeof barcode !== 'string' || barcode.length < (panjangBarcode || 0)) {
//                 console.warn("⚠️ Barcode tidak valid atau panjang barcode terlalu pendek.");
//                 return super.parse_barcode(...arguments);
//             }

//             const totalLength = barcode.length;

//             // 🔄 Validate prefix and barcode length
//             if (barcode.startsWith(prefixTimbangan) && totalLength >= panjangBarcode) {
//                 const kode_produk = barcode.slice(0, totalLength - panjangBarcode);
//                 const timbangPart = barcode.slice(-panjangBarcode);
//                 const qty_bulat = timbangPart.slice(0, digitAwal);
//                 const qty_desimal = timbangPart.slice(digitAwal, digitAwal + digitAkhir);
//                 const quantity = parseFloat(`${qty_bulat}.${qty_desimal}`);

//                 if (!kode_produk || isNaN(quantity)) {
//                     console.warn("⚠️ Parsing gagal: Produk atau quantity tidak valid.");
//                     return super.parse_barcode(...arguments);
//                 }

//                 console.log("✔️ Barcode Timbangan Valid:", {
//                     kode_produk,
//                     qty_bulat,
//                     qty_desimal,
//                     quantity
//                 });

//                 return {
//                     encoding: "timbang_barcode",
//                     type: "product",
//                     code: kode_produk,
//                     base_code: kode_produk,
//                     quantity: quantity,
//                 };
//             }

//             // Fall back to original parser
//             return super.parse_barcode(...arguments);

//         } catch (error) {
//             console.error("❌ Barcode parsing exception:", error);
//             // Always return a valid object even in case of error
//             return { encoding: "error", type: "error", code: barcode || "", base_code: barcode || "" };
//         }
//     }
// });
