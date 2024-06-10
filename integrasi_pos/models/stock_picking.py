import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class StockPicking(models.Model):
    _inherit = 'stock.picking'

    is_integrated = fields.Boolean(string="Integrated", default=False)