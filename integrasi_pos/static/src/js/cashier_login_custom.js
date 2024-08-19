/** @odoo-module **/

import { SelectionPopup } from "@point_of_sale/app/utils/input_popups/selection_popup";
import { patch } from "@web/core/utils/patch";
import { useService } from "@web/core/utils/hooks";
import { usePos } from "@point_of_sale/app/store/pos_hook";

const originalSetup = SelectionPopup.prototype.setup;
const originalSelectItem = SelectionPopup.prototype.selectItem;

patch(SelectionPopup.prototype, {
    setup() {
        originalSetup.call(this);
        this.rpc = useService("rpc");
        this.orm = useService("orm");
        this.pos = usePos();
    },

    async selectItem(itemId) {
        const selectedItem = this.props.list.find((item) => item.id === itemId);
        if (selectedItem && selectedItem.item && selectedItem.item.id) {
            try {
                const sessionId = this.pos.pos_session ? this.pos.pos_session.id : null;
                if (!sessionId) {
                    console.warn("POS session ID is not available");
                    alert("Sesi POS tidak tersedia. Silakan buka sesi terlebih dahulu.");
                    return;
                }

                const response = await this.rpc("/pos/log_cashier", {
                    employee_id: selectedItem.item.id,
                    session_id: sessionId,
                });

                if (response.success) {
                    console.log("Cashier logged successfully with ID:", response.log_id);
                    
                    // Create end.shift record
                    try {
                        const now = new Date();
                        const currentDate = now.toISOString().slice(0, 19).replace('T', ' ');

                        const endShiftData = await this.orm.call(
                            "end.shift",
                            "create",
                            [{
                                cashier_id: selectedItem.item.id,
                                session_id: sessionId,
                                start_date: currentDate,
                                state: 'opened',
                            }]
                        );
                        
                        if (endShiftData) {
                            console.log("End shift record created successfully with ID:", endShiftData);
                            
                            // Call action_start_progress on the newly created end.shift record
                            await this.orm.call(
                                "end.shift",
                                "action_start_progress",
                                [endShiftData]
                            );
                            console.log("action_start_progress called on end.shift");
                        } else {
                            console.warn("Failed to create end.shift record");
                        }
                    } catch (endShiftError) {
                        console.error("Error creating end.shift record:", endShiftError);
                    }
                    
                    return await originalSelectItem.call(this, itemId);
                } else if (response.error === 'cashier_in_use') {
                    alert(`Kasir ${selectedItem.item.name} sedang digunakan di sesi lain.`);
                    return;
                } else {
                    console.error("Failed to log cashier:", response.error);
                    alert(`Gagal memilih kasir: ${response.error || 'Kesalahan tidak diketahui'}`);
                    return;
                }
            } catch (error) {
                console.error("Error logging cashier:", error);
                let errorMessage = "Terjadi kesalahan saat memilih kasir.";
                if (error.message) {
                    errorMessage += ` Detail: ${error.message}`;
                }
                if (error.data && error.data.message) {
                    errorMessage += ` Server: ${error.data.message}`;
                }
                alert(errorMessage);
                return;
            }
        }

        return await originalSelectItem.call(this, itemId);
    },
});