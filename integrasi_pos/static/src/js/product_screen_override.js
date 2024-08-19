/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { InputNumberPopUp } from "./input_number_popup_multiplebarcode";

patch(ProductScreen.prototype, {
    _parseTimbanganParts(barcode, config) {
        const digitAwal = parseInt(config?.digit_awal ?? "2");
        const digitAkhir = parseInt(config?.digit_akhir ?? "4");
        const panjangBarcode = parseInt(config?.panjang_barcode ?? "7");
        const timbangPart = barcode.slice(-panjangBarcode);

        if ((digitAwal + digitAkhir) > timbangPart.length) {
            console.warn("❌ Konfigurasi digit_awal + digit_akhir lebih besar dari panjang barcode timbang.");
            return 1;
        }

        const qty_bulat = timbangPart.slice(0, digitAwal);
        const qty_desimal = timbangPart.slice(digitAwal, digitAwal + digitAkhir);
        const quantity = parseFloat(`${qty_bulat}.${qty_desimal}`);

        return quantity;
    },

    async _showProductChoicePopup(products, barcode, config) {
        const popup = this.popup || this.env.services.popup;
        const productList = products.map((p, i) => ({
            name: p.display_name || p.name,
            index: i + 1,
        }));

        const result = await popup.add(InputNumberPopUp, {
            title: "Pilih Produk dari Barcode",
            body: "Beberapa produk cocok dengan barcode ini. Pilih yang benar.",
            productList,
        });

        if (result && !isNaN(result.productIndex)) {
            const selectedProduct = products[result.productIndex];
            let quantity = 1;
            if (selectedProduct.to_weight) {
                try {
                    quantity = this._parseTimbanganParts(barcode, config);
                } catch (err) {
                    quantity = 1;
                }
            }
            return { product: selectedProduct, quantity };
        }
        return null;
    },

    async _getMatchingBarcodeProducts(barcode) {
        const config = this.pos.config;
        const panjangBarcode = parseInt(config?.panjang_barcode || "7");
        const kode_produk = barcode.slice(0, barcode.length - panjangBarcode);
        const resultProducts = [];

        const pushIfValid = (product) => {
            if (product && product.available_in_pos) {
                resultProducts.push(product);
            }
        };

        // 1️⃣ Match produk timbang (to_weight)
        const productWeightCandidate = this.pos.db.get_product_by_barcode(kode_produk);
        pushIfValid(productWeightCandidate?.to_weight ? productWeightCandidate : null);

        // 2️⃣ Match full barcode langsung
        const directMatch = this.pos.db.get_product_by_barcode(barcode);
        pushIfValid(directMatch?.to_weight ? null : directMatch);

        // 3️⃣ Multi-barcode mode ✅
        if (config?.multiple_barcode_activate && this.pos.db?.product_by_id) {
            const allProducts = Object.values(this.pos.db.product_by_id);
            for (const product of allProducts) {
                const matches = (product.multi_barcode_ids || []).some(bc =>
                    bc === barcode || bc === kode_produk
                );
                if (matches && product.available_in_pos) {
                    resultProducts.push(product);
                }
            }
        }

        return [...new Set(resultProducts)].filter(Boolean);
    },

    async _barcodeProductAction(parsedBarcode) {
        const barcode = parsedBarcode?.code || parsedBarcode?.base_code || "";
        const config = this.pos.config;

        const matchedProducts = await this._getMatchingBarcodeProducts(barcode, config);

        if (matchedProducts.length > 1) {
            const result = await this._showProductChoicePopup(matchedProducts, barcode, config);
            if (result) {
                const existingLine = this.currentOrder
                    .get_orderlines()
                    .find(line => line.product.id === result.product.id);
                if (existingLine) {
                    existingLine.set_quantity(existingLine.get_quantity() + result.quantity);
                } else {
                    this.currentOrder.add_product(result.product, { quantity: result.quantity });
                }
            }
            return;
        }

        if (matchedProducts.length === 1) {
            const product = matchedProducts[0];
            let quantity = 1;
            if (product.to_weight) {
                quantity = this._parseTimbanganParts(barcode, config);
                if (isNaN(quantity)) quantity = 1;
            }

            const existingLine = this.currentOrder
                .get_orderlines()
                .find(line => line.product.id === product.id);
            if (existingLine) {
                existingLine.set_quantity(existingLine.get_quantity() + quantity);
            } else {
                this.currentOrder.add_product(product, { quantity });
            }
            return;
        }

        const fallback = this._getFallbackProduct(barcode, config);
        if (!fallback) {
            const popup = this.popup || this.env.services.popup;
            await popup.add(ErrorPopup, {
                title: "Produk Tidak Ditemukan",
                body: `Barcode: ${parsedBarcode.base_code}`,
            });
            return;
        }

        const { product, quantity } = fallback;

        const existingLine = this.currentOrder
            .get_orderlines()
            .find(line => line.product.id === product.id);
        if (existingLine) {
            existingLine.set_quantity(existingLine.get_quantity() + quantity);
        } else {
            this.currentOrder.add_product(product, { quantity });
        }
    },

    _getFallbackProduct(barcode, config) {
        const panjangBarcode = parseInt(config?.panjang_barcode || "7");
        const kode_produk = barcode.slice(0, barcode.length - panjangBarcode);

        let product = this.pos.db.get_product_by_barcode(kode_produk);
        if (!product) {
            product = this.pos.db.get_product_by_barcode(barcode);
        }

        if (!product || !product.available_in_pos) {
            return null;
        }

        let quantity = 1;
        if (product.to_weight) {
            try {
                quantity = this._parseTimbanganParts(barcode, config);
            } catch (e) {
                console.warn("❌ Gagal parsing barcode timbang:", e);
            }
        }

        return { product, quantity };
    },
});
