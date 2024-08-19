/** @odoo-module **/

import { Component, onMounted, useRef } from "@odoo/owl";
import { CharField } from "@web/views/fields/char/char_field";
import { registry } from "@web/core/registry";

class BarcodeScannerWidget extends Component {
    setup() {
        this.readerRef = useRef("reader");
        this.html5QrCode = null;

        onMounted(() => {
            const container = this.readerRef.el;
            this.html5QrCode = new Html5Qrcode(container.id);

            Html5Qrcode.getCameras().then((devices) => {
                const frontCamera = devices.find((d) =>
                    d.label.toLowerCase().includes("front") || d.label.toLowerCase().includes("user")
                );
                const cameraId = frontCamera ? frontCamera.id : devices[0].id;

                this.html5QrCode.start(
                    cameraId,
                    { fps: 10, qrbox: 250 },
                    async (decodedText) => {
                        console.log("Scanned:", decodedText);
                        await this.html5QrCode.stop();
                    },
                    (err) => {
                        console.warn("QR Scan error:", err);
                    }
                );
            });
        });
    }

    async stopCamera() {
        if (this.html5QrCode && this.html5QrCode._isScanning) {
            await this.html5QrCode.stop();
            console.log("Camera stopped.");
        }
    }

    static template = "integrasi_pos.BarcodeScannerTemplate";
}

BarcodeScannerWidget.supportedTypes = ["char"];

registry.category("fields").add("barcode_scanner_widget", {
    component: BarcodeScannerWidget,
    ...CharField,
});
