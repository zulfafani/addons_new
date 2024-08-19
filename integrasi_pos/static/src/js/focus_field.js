// Tambahkan file static/src/js/focus_field.js

/** @odoo-module */

import { registry } from "@web/core/registry";
import { useEffect } from "@odoo/owl";

class FocusFieldController {
    /**
     * @param {Object} env
     * @param {Object} props
     */
    setup(env, props) {
        this.env = env;
        this.props = props;
        
        useEffect(() => {
            this._focusField();
        });
    }

    /**
     * Focus on the barcode field
     * @private
     */
    _focusField() {
        const fieldName = this.props.context.field_name;
        const modelName = this.props.context.model_name;
        const recordId = this.props.context.record_id;
        
        // Wait a short moment for the form to be fully rendered
        setTimeout(() => {
            const formElement = document.querySelector(`.o_form_view[data-model="${modelName}"] input[name="${fieldName}"]`);
            if (formElement) {
                formElement.focus();
            }
            
            // Close this action silently
            this.env.services.action.doAction({ type: "ir.actions.act_window_close" }, { silent: true });
        }, 200);
    }
}

// Register the client action
registry.category("actions").add("focus_field", FocusFieldController);

export default FocusFieldController;