/** @odoo-module **/

import { Component, onMounted, useRef, useState } from "@odoo/owl";
import { CharField } from "@web/views/fields/char/char_field";
import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { AlertDialog } from "@web/core/confirmation_dialog/confirmation_dialog";

class BarcodeScannerWidget extends Component {
    setup() {
        this.readerRef = useRef("reader");
        this.orm = useService("orm");
        this.dialog = useService("dialog");

        this.state = useState({
            scanning: false,
            devices: [],
            selectedDeviceId: null,
            waitingForQuantity: false,
            currentBarcode: null,
        });

        onMounted(() => {
            this.initCameraList().then(() => {
                this.startScanning(); // ✅ Auto start on load
            });
        });
    }

    async initCameraList() {
        if (typeof Html5Qrcode === "undefined") {
            this.showModal("html5-qrcode belum dimuat!");
            return;
        }

        try {
            const devices = await Html5Qrcode.getCameras();
            if (!devices.length) throw new Error("Tidak ada kamera ditemukan");

            this.state.devices = devices;
            this.state.selectedDeviceId = devices[0].id;
        } catch (err) {
            this.showModal("Gagal inisialisasi kamera: " + err.message);
        }
    }

    async startScanning() {
        if (!this.state.selectedDeviceId) {
            return this.showModal("Kamera belum dipilih.");
        }

        const readerId = this.readerRef.el.id || "barcode-scanner-reader";
        this.html5QrCode = new Html5Qrcode(readerId);

        try {
            await this.html5QrCode.start(
                { deviceId: { exact: this.state.selectedDeviceId } },
                {
                    fps: 10,
                    qrbox: { width: 250, height: 250 },
                },
                this.onScanSuccess.bind(this),
                err => console.warn("Scan error:", err)
            );

            this.state.scanning = true;
        } catch (err) {
            this.showModal("Gagal memulai scanner: " + err.message);
        }
    }

    async stopScanning() {
        if (this.html5QrCode && this.state.scanning) {
            await this.html5QrCode.stop();
            await this.html5QrCode.clear();
            this.state.scanning = false;
        }
    }

    async onScanSuccess(decodedText, decodedResult) {
        if (this.state.waitingForQuantity) {
            return; // Ignore new scans while waiting for quantity input
        }

        const barcode = decodedText;
        this.state.currentBarcode = barcode;
        this.state.waitingForQuantity = true;

        await this.stopScanning(); // ⏸️ Pause camera while getting quantity

        // Show quantity input dialog
        this.showQuantityDialog(barcode);
    }

    showQuantityDialog(barcode) {
        // Use simple browser prompt - this always works
        const quantityStr = prompt(`Barcode: ${barcode}\n\nMasukkan quantity:`, "1");
        
        if (quantityStr === null) {
            // User clicked Cancel
            this.state.waitingForQuantity = false;
            this.state.currentBarcode = null;
            this.startScanning();
            return;
        }
        
        const quantity = parseFloat(quantityStr) || 1.0;
        
        if (quantity <= 0) {
            alert("Quantity harus lebih besar dari 0!");
            // Show dialog again
            this.showQuantityDialog(barcode);
            return;
        }
        
        this.processBarcode(barcode, quantity);
    }

    async processBarcode(barcode, quantity) {
        const inventoryField = this.props.record?.data?.inventory_stock_id;
        const inventoryId = Array.isArray(inventoryField) ? inventoryField[0] : this.props.record?.resId;

        if (!inventoryId) {
            this.showModal("Inventory ID tidak ditemukan di wizard.");
            this.state.waitingForQuantity = false;
            this.state.currentBarcode = null;
            return;
        }

        try {
            const result = await this.orm.call(
                'inventory.stock',
                'process_barcode_from_wizard',
                [inventoryId, barcode, quantity]
            );

            this.showModal(result.message);

            if (this.props?.record?.load) {
                this.props.record.load();
            }

        } catch (err) {
            this.showModal("❌ Gagal memproses barcode: " + (err.message || err));
        } finally {
            this.state.waitingForQuantity = false;
            this.state.currentBarcode = null;
        }
    }

    onChangeCamera(ev) {
        this.state.selectedDeviceId = ev.target.value;
    }

    showModal(message) {
        this.dialog.add(AlertDialog, {
            title: "Hasil Scan Barcode",
            body: message,
            confirmLabel: "OK",
            onClose: () => {
                this.startScanning(); // ✅ Resume scanning after user clicks OK
            },
        });
    }

    static template = "integrasi_pos.BarcodeScannerCameraSelector";
}

BarcodeScannerWidget.supportedTypes = ["char"];

registry.category("fields").add("barcode_scanner_widget", {
    component: BarcodeScannerWidget,
    ...CharField,
});