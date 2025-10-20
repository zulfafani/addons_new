/** @odoo-module **/

import { patch } from "@web/core/utils/patch";
import { ProductScreen } from "@point_of_sale/app/screens/product_screen/product_screen";
import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";
import { InputNumberPopUp } from "./input_number_popup_multiplebarcode";

patch(ProductScreen.prototype, {
    _parseTimbanganParts(barcode, config) {
        const digitAwal = parseInt(config?.digit_awal ?? "3");
        const digitAkhir = parseInt(config?.digit_akhir ?? "3");
        const panjangBarcode = parseInt(config?.panjang_barcode ?? "7");
        const timbangPart = barcode.slice(-panjangBarcode);

        console.log("📦 Timbangan Debug - Input:", {
            barcode,
            panjangBarcode,
            timbangPart,
            digitAwal,
            digitAkhir,
            totalDigit: digitAwal + digitAkhir
        });

        if ((digitAwal + digitAkhir) > timbangPart.length) {
            console.warn("❌ Konfigurasi digit_awal + digit_akhir lebih besar dari panjang barcode timbang.");
            return 1;
        }

        // Parse sesuai konfigurasi
        // Contoh: timbangPart = "0003467" dengan digitAwal=3, digitAkhir=3
        // - qty_bulat = "000" (3 digit pertama)
        // - qty_desimal = "346" (3 digit berikutnya)
        // - Sisa 1 digit diabaikan
        const qty_bulat = timbangPart.slice(0, digitAwal);
        const qty_desimal = timbangPart.slice(digitAwal, digitAwal + digitAkhir);
        const quantity = parseFloat(`${qty_bulat}.${qty_desimal}`);

        console.log("📏 Barcode timbang parsed:", {
            barcode,
            timbangPart,
            qty_bulat,
            qty_desimal,
            quantity,
            formatted: `${quantity.toFixed(3)} KG`
        });

        // Validasi: quantity harus masuk akal (0.001 - 999.999)
        if (isNaN(quantity) || quantity < 0 || quantity > 999.999) {
            console.warn("⚠️ Quantity tidak valid:", quantity, "- menggunakan 1 KG");
            return 1;
        }

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

    async _searchProductByBarcode(barcode) {
        // Cari di local DB terlebih dahulu
        let product = this.pos.db.get_product_by_barcode(barcode);
        if (product) {
            console.log("✅ Produk ditemukan di local DB:", product.display_name);
            return product;
        }

        // Jika tidak ada di local DB, search_read ke server
        console.log("🔍 Produk tidak di local DB, mencari ke server dengan search_read...");
        try {
            const products = await this.env.services.orm.searchRead(
                "product.product",
                [["barcode", "=", barcode], ["available_in_pos", "=", true]],
                ["id"],
                { limit: 1 }
            );

            if (products && products.length > 0) {
                const productId = products[0].id;
                console.log("✅ Produk ID ditemukan di server:", productId);
                
                // Load produk secara lengkap - pass empty array untuk lines
                try {
                    await this.pos._loadMissingProducts([productId], []);
                } catch (e) {
                    // Fallback jika method signature berbeda
                    console.warn("⚠️ Fallback loading product...");
                    await this.pos._addProducts([productId], false);
                }
                
                // Sekarang produk sudah lengkap dengan semua method-nya
                product = this.pos.db.get_product_by_id(productId);
                
                if (product) {
                    console.log("✅ Produk berhasil dimuat:", product.display_name);
                    return product;
                }
            }
        } catch (error) {
            console.error("❌ Error search_read product:", error);
        }

        return null;
    },

    async _getMatchingBarcodeProducts(barcode) {
        const config = this.pos.config;
        const panjangBarcode = parseInt(config?.panjang_barcode || "7");
        const prefix = config?.prefix_timbangan || "20";
        const panjangPrefix = prefix.length;
        const resultProducts = [];

        let kode_produk = null;
        let isTimbangan = false;

        // Check if barcode starts with timbangan prefix
        if (barcode.startsWith(prefix)) {
            isTimbangan = true;
            const panjangKodeProduk = barcode.length - panjangBarcode - panjangPrefix;

            if (panjangKodeProduk > 0) {
                kode_produk = barcode.slice(panjangPrefix, panjangPrefix + panjangKodeProduk);
                console.log("🔍 [TIMBANGAN DETECTED] Barcode parsing:", {
                    barcode,
                    prefix,
                    panjangPrefix,
                    panjangBarcode,
                    panjangKodeProduk,
                    kode_produk,
                });

                // Search produk timbangan dari server dengan search_read
                const productWeightCandidate = await this._searchProductByBarcode(kode_produk);
                if (productWeightCandidate?.to_weight) {
                    resultProducts.push(productWeightCandidate);
                }
            } else {
                console.warn("⚠️ Panjang kode produk <= 0. Cek konfigurasi prefix dan panjang barcode.");
            }
        }

        // Match full barcode langsung (search_read ke server jika perlu)
        // Hanya cari barcode penuh jika BUKAN produk timbangan
        if (!isTimbangan) {
            const directMatch = await this._searchProductByBarcode(barcode);
            if (directMatch && !directMatch.to_weight) {
                resultProducts.push(directMatch);
            }
        }

        // Multi-barcode mode
        if (config?.multiple_barcode_activate) {
            console.log("✅ [MULTI-BARCODE MODE ACTIVE] Scanning with multiple_barcode_activate = TRUE");

            // Cek di local DB dulu
            if (this.pos.db?.product_by_id) {
                const allProducts = Object.values(this.pos.db.product_by_id);
                for (const product of allProducts) {
                    // Untuk timbangan, cek dengan kode_produk
                    // Untuk non-timbangan, cek dengan barcode penuh
                    const barcodeToCheck = isTimbangan && kode_produk ? kode_produk : barcode;
                    const matches = (product.multi_barcode_ids || []).some(bc => bc === barcodeToCheck);
                    
                    if (matches) {
                        // Pastikan produk sesuai dengan jenis scan (timbangan/non-timbangan)
                        if ((isTimbangan && product.to_weight) || (!isTimbangan && !product.to_weight)) {
                            resultProducts.push(product);
                            console.log("📡 Multiple Barcode Match Detected (Local):", {
                                barcode_scanned: barcode,
                                barcode_checked: barcodeToCheck,
                                matched_with: product.multi_barcode_ids,
                                matched_product_id: product.id,
                                matched_product_name: product.display_name || product.name,
                                is_timbangan: isTimbangan,
                            });
                        }
                    }
                }
            }

            // Search ke server untuk multi-barcode dengan search_read
            try {
                const barcodeToSearch = isTimbangan && kode_produk ? kode_produk : barcode;
                const serverProducts = await this.env.services.orm.searchRead(
                    "product.product",
                    [["multi_barcode_ids", "in", [barcodeToSearch]], 
                     ["available_in_pos", "=", true]],
                    ["id"]
                );

                if (serverProducts && serverProducts.length > 0) {
                    // Load semua produk secara lengkap
                    const productIds = serverProducts.map(p => p.id);
                    try {
                        await this.pos._loadMissingProducts(productIds, []);
                    } catch (e) {
                        // Fallback jika method signature berbeda
                        console.warn("⚠️ Fallback loading products...");
                        await this.pos._addProducts(productIds, false);
                    }
                    
                    for (const productData of serverProducts) {
                        const product = this.pos.db.get_product_by_id(productData.id);
                        if (product) {
                            // Pastikan produk sesuai dengan jenis scan
                            if ((isTimbangan && product.to_weight) || (!isTimbangan && !product.to_weight)) {
                                resultProducts.push(product);
                                console.log("📡 Multiple Barcode Match Detected (Server):", {
                                    barcode_scanned: barcode,
                                    barcode_checked: barcodeToSearch,
                                    matched_with: product.multi_barcode_ids,
                                    matched_product_id: product.id,
                                    matched_product_name: product.display_name || product.name,
                                    is_timbangan: isTimbangan,
                                });
                            }
                        }
                    }
                }
            } catch (error) {
                console.error("❌ Error search_read multi-barcode products:", error);
            }
        }

        // Remove duplicates berdasarkan product.id
        const uniqueProducts = [];
        const seenIds = new Set();
        for (const product of resultProducts) {
            if (product && !seenIds.has(product.id)) {
                seenIds.add(product.id);
                uniqueProducts.push(product);
            }
        }

        return uniqueProducts;
    },

    async _barcodeProductAction(parsedBarcode) {
        const barcode = parsedBarcode?.code || parsedBarcode?.base_code || "";
        const config = this.pos.config;

        console.log("🔍 Barcode scanned:", barcode);

        const matchedProducts = await this._getMatchingBarcodeProducts(barcode, config);

        if (matchedProducts.length > 1) {
            const result = await this._showProductChoicePopup(matchedProducts, barcode, config);
            if (result) {
                // MODIFIED: Produk timbangan selalu buat orderline baru
                if (result.product.to_weight) {
                    console.log("⚖️ Produk timbangan - membuat orderline baru");
                    this.currentOrder.add_product(result.product, { quantity: result.quantity });
                } else {
                    // Produk non-timbangan: cek existing line
                    const existingLine = this.currentOrder
                        .get_orderlines()
                        .find(line => line.product.id === result.product.id);
                    if (existingLine) {
                        existingLine.set_quantity(existingLine.get_quantity() + result.quantity);
                    } else {
                        this.currentOrder.add_product(result.product, { quantity: result.quantity });
                    }
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

            console.log("✅ Product matched:", product.display_name, "Qty:", quantity);

            // MODIFIED: Produk timbangan selalu buat orderline baru
            if (product.to_weight) {
                console.log("⚖️ Produk timbangan - membuat orderline baru");
                this.currentOrder.add_product(product, { quantity });
            } else {
                // Produk non-timbangan: cek existing line
                const existingLine = this.currentOrder
                    .get_orderlines()
                    .find(line => line.product.id === product.id);
                
                if (existingLine) {
                    existingLine.set_quantity(existingLine.get_quantity() + quantity);
                } else {
                    this.currentOrder.add_product(product, { quantity });
                }
            }
            return;
        }

        // Fallback
        const fallback = await this._getFallbackProduct(barcode, config);
        if (!fallback) {
            const popup = this.popup || this.env.services.popup;
            await popup.add(ErrorPopup, {
                title: "Produk Tidak Ditemukan",
                body: `Barcode: ${parsedBarcode.base_code}`,
            });
            return;
        }

        const { product, quantity } = fallback;

        // MODIFIED: Produk timbangan selalu buat orderline baru
        if (product.to_weight) {
            console.log("⚖️ Produk timbangan - membuat orderline baru");
            this.currentOrder.add_product(product, { quantity });
        } else {
            // Produk non-timbangan: cek existing line
            const existingLine = this.currentOrder
                .get_orderlines()
                .find(line => line.product.id === product.id);
            
            if (existingLine) {
                existingLine.set_quantity(existingLine.get_quantity() + quantity);
            } else {
                this.currentOrder.add_product(product, { quantity });
            }
        }
    },

    async _getFallbackProduct(barcode, config) {
        const panjangBarcode = parseInt(config?.panjang_barcode || "7");
        const kode_produk = barcode.slice(0, barcode.length - panjangBarcode);

        // Search ke server dengan search_read jika perlu
        let product = await this._searchProductByBarcode(kode_produk);
        if (!product) {
            product = await this._searchProductByBarcode(barcode);
        }

        if (!product) return null;

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