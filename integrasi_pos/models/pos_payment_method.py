import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class PoSPaymentMethodInherit(models.Model):
    _inherit = 'pos.payment.method'

    id_mc = fields.Char(string="ID MC", default=False)
    is_updated = fields.Boolean(string="Updated", tracking=True)