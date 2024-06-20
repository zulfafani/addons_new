import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class StockMoveLine(models.Model):
    _inherit = 'stock.move.line'

    is_integrated = fields.Booelan(string='Is Integrated')