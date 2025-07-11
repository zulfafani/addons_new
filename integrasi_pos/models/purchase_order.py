import requests
from datetime import datetime, timedelta
import pytz
from odoo.http import request
import base64
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class PurchaseOrderIntegration(models.Model):
    _inherit = 'purchase.order'

    vit_trxid = fields.Char(string='Transaction ID')
    is_integrated = fields.Boolean(string="Integrated", default=False)

    def button_confirm(self):
        # Panggil method asli untuk tetap menjalankan logika bawaan Odoo
        res = super().button_confirm()

        for order in self:
            # Set scheduled_date pada semua receipts agar ikut backdate
            for picking in order.picking_ids:
                picking.write({'vit_trxid': order.vit_trxid,
                               'is_integrated': True})
                for move in picking.move_ids_without_package:
                    move.write({'vit_line_number_sap': move.vit_line_number_sap})
                
class PurchaseOrderLineIntegration(models.Model):
    _inherit = 'purchase.order.line'

    vit_line_number_sap = fields.Integer(string='Line Number SAP')