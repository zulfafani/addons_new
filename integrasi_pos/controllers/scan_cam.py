from odoo import http
from odoo.http import request
import logging

_logger = logging.getLogger(__name__)

class BarcodeCallbackController(http.Controller):
    @http.route('/barcode-callback', type='json', auth='public', csrf=False)
    def barcode_callback(self, code=None):
        _logger.info(f"📦 Barcode received: {code}")
        if not code:
            return {"status": "error", "message": "Barcode missing"}

        stock = request.env['inventory.stock'].sudo().search([('state', '=', 'in_progress')], limit=1)
        if stock:
            stock.barcode_input = code
            stock._onchange_barcode_input()
            return {"status": "success", "msg": "Barcode processed"}
        
        return {"status": "error", "msg": "No in-progress inventory"}
