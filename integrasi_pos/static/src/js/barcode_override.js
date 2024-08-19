// /** @odoo-module **/

// import { patch } from "@web/core/utils/patch";
// import { BarcodeReader } from "@point_of_sale/app/barcode/barcode_reader_service";
// import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
// import { ErrorBarcodePopup } from "@point_of_sale/app/barcode/error_popup/barcode_error_popup";
// import { GS1BarcodeError } from "@barcodes_gs1_nomenclature/js/barcode_parser";
// import { _t } from "@web/core/l10n/translation";
// import { jsonrpc } from "@web/core/network/rpc_service";

// patch(BarcodeReader.prototype, {
//     async _scan(code) {
//         if (!code) return;

//         const cbMaps = this.exclusiveCbMap ? [this.exclusiveCbMap] : [...this.cbMaps];

//         try {
//             let parseBarcode;

//             try {
//                 parseBarcode = this.parser.parse_barcode(code);
//                 console.log("PARSED BARCODE RESULT:", parseBarcode);

//                 if (Array.isArray(parseBarcode) && !parseBarcode.some(el => el.type === 'product')) {
//                     throw new GS1BarcodeError('The GS1 barcode must contain a product.');
//                 }
//             } catch (error) {
//                 if (this.fallbackParser && error instanceof GS1BarcodeError) {
//                     parseBarcode = this.fallbackParser.parse_barcode(code);
//                 } else {
//                     throw error;
//                 }
//             }

//             // Check if timbang_barcode
//             if (
//                 !Array.isArray(parseBarcode) &&
//                 parseBarcode.encoding === "timbang_barcode"
//             ) {
//                 const barcodeValue = parseBarcode.code;

//                 try {
//                     const result = await jsonrpc("/pos/resolve_multiple_barcode", {
//                         barcode: barcodeValue,
//                     });

//                     if (result && result.product_id) {
//                         parseBarcode.product = this.env.pos.db.get_product_by_id(result.product_id);

//                         if (!parseBarcode.product) {
//                             console.warn("Product not found in POS cache, might need preloading");
//                         }
//                     }
//                 } catch (err) {
//                     console.error("❌ Failed to resolve multiple.barcode:", err);
//                 }
//             }

//             if (Array.isArray(parseBarcode)) {
//                 cbMaps.forEach(cb => cb.gs1?.(parseBarcode));
//             } else {
//                 const cbs = cbMaps.map(cbMap => cbMap[parseBarcode.type]).filter(Boolean);
//                 if (cbs.length === 0) {
//                     this.popup.add(ErrorBarcodePopup, { code: this.codeRepr(parseBarcode) });
//                     return;
//                 }

//                 for (const cb of cbs) {
//                     if (parseBarcode.encoding === 'timbang_barcode') {
//                         await cb({
//                             ...parseBarcode,
//                             quantity: parseBarcode.quantity || 1,
//                         });
//                     } else {
//                         await cb(parseBarcode);
//                     }
//                 }
//             }
//         } catch (error) {
//             console.error("Error in barcode scanning:", error);
//             this.popup.add(ErrorPopup, {
//                 title: _t("Invalid Barcode"),
//                 body: _t("Cannot process this barcode: " + error.message),
//             });
//         }
//     },
// });