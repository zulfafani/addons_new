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