/** @odoo-module **/

import { registry } from "@web/core/registry";
import { Component, useState, onWillStart } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";

export class DbNameNavbar extends Component {
    setup() {
        this.orm = useService("orm");
        this.state = useState({ dbName: "Loading..." });

        onWillStart(async () => {
            try {
                const params = await this.orm.searchRead(
                    "ir.config_parameter",
                    [["key", "=", "db_name_display"]],
                    ["value"],
                    { limit: 1 }
                );
                this.state.dbName = params.length ? params[0].value : "Unknown DB";
            } catch (e) {
                this.state.dbName = "Unknown DB";
                console.error("Failed to fetch db_name_display:", e);
            }
        });
    }
}
DbNameNavbar.template = "integrasi_pos.DbNameNavbar";

registry.category("systray").add("db_name_navbar", {
    Component: DbNameNavbar,
    sequence: 1,
});