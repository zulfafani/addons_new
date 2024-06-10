import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class POSIntegration(models.Model):
    _inherit = 'pos.order'

    order_ref = fields.Char(string='Reference')