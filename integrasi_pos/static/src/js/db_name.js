/** @odoo-module **/

import { registry } from "@web/core/registry";
import { Component, useState, onWillStart } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";

export class DbNameNavbar extends Component {
    setup() {
        this.rpc = useService("rpc");
        this.state = useState({ dbName: "Loading..." });

        onWillStart(async () => {
            try {
                // ✅ Ambil nama database langsung dari session info
                const sessionInfo = await this.rpc("/web/session/get_session_info");
                this.state.dbName = sessionInfo.db || "Unknown DB";
            } catch (e) {
                this.state.dbName = "Unknown DB";
                console.error("Failed to fetch database name:", e);
            }
        });
    }

    // ✅ Method untuk refresh browser
    refreshBrowser() {
        window.location.reload();
    }

    // ✅ Method untuk reset cache browser dan refresh
    async resetCacheAndRefresh() {
        try {
            // Hapus cache service worker (jika ada)
            if ('caches' in window) {
                const cacheNames = await caches.keys();
                await Promise.all(
                    cacheNames.map(cacheName => caches.delete(cacheName))
                );
                console.log("Browser cache cleared");
            }

            // Hapus localStorage dan sessionStorage
            localStorage.clear();
            sessionStorage.clear();
            console.log("Local storage and session storage cleared");

            // Hapus cache Odoo khusus (jika ada)
            if (window.odoo && window.odoo.debug) {
                window.odoo.debug.clearCache();
            }

            // Refresh browser setelah membersihkan cache
            setTimeout(() => {
                window.location.reload();
            }, 500);
            
        } catch (error) {
            console.error("Error clearing cache:", error);
            // Tetap refresh meskipun ada error
            window.location.reload();
        }
    }
}

DbNameNavbar.template = "integrasi_pos.DbNameNavbar";

registry.category("systray").add("db_name_navbar", {
    Component: DbNameNavbar,
    sequence: 1,
});