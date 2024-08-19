from odoo import http
from odoo.http import request

class PosConfigSettingsController(http.Controller):
    @http.route('/pos/get_config_settings', type='json', auth='user')
    def get_config_settings(self):
        return request.env['res.config.settings'].sudo().get_config_settings()
