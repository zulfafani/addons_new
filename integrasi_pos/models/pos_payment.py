import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class PoSPaymentInherit(models.Model):
    _inherit = 'pos.payment'

    nomor_kartu = fields.Char(string="Nomor Kartu", tracking=True)