from odoo import http
from odoo.http import request

class BarcodeConfigController(http.Controller):

    @http.route('/pos/get_barcode_settings', type='json', auth='public')
    def get_config_settings(self):
        config = request.env['barcode.config'].sudo().search([], limit=1)
        return {
            'digit_awal': config.digit_awal,
            'digit_akhir': config.digit_akhir,
            'prefix_timbangan': config.prefix_timbangan,
            'panjang_barcode': config.panjang_barcode,
            'multiple_barcode_activate': config.multiple_barcode_activate
        }
