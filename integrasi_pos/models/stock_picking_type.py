import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError
import random

class StockPickingType(models.Model):
    _inherit = 'stock.picking.type'

    id_mc = fields.Char(string="ID MC", default=False)
