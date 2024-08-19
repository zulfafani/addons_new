/** @odoo-module **/

import { PosGlobalState } from '@point_of_sale/app/store/pos_store';
import { patch } from '@web/core/utils/patch';

patch(PosGlobalState.prototype, {
    async _processData(loadedData) {
        await this._super(...arguments);
        
        // Load config settings
        try {
            const configSettings = await this.env.services.rpc({
                model: 'res.config.settings',
                method: 'get_config_settings',
                args: [],
            });
            
            // Store the config settings in the POS
            this.config_settings = configSettings;
        } catch (error) {
            console.error('Error loading POS config settings:', error);
            this.config_settings = {};
        }
    },
});