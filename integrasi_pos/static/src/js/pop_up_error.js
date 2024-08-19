/** @odoo-module **/
import { AbstractAwaitablePopup } from "@point_of_sale/app/popup/abstract_awaitable_popup";
import { _t } from "@web/core/l10n/translation";
import { useService } from "@web/core/utils/hooks";

export class PopUpSuccesError extends AbstractAwaitablePopup {
    static template = "integrasi_pos.CustomPopup";
    static defaultProps = {
        confirmText: _t("Save"),
        // cancelText: _t("Discard"),
        title: _t("Notification"),
    };

    setup() {
        super.setup();
        this.orm = useService("orm");
        this.pos = useService("pos");
    }
}