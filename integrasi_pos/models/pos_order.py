import requests
from datetime import datetime, timedelta
import pytz
from odoo.http import request
import base64
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class POSIntegration(models.Model):
    _inherit = 'pos.order'

    order_ref = fields.Char(string='Reference')
    is_integrated = fields.Boolean(string="Integrated", default=False)


    def write_orderref(self):
        pos = self.env['pos.order'].search([])
        for i in pos:
            i.write({'is_integrated': True})