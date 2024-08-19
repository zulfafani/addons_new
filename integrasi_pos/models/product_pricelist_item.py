import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class ProductPricelistItem(models.Model):
    _inherit = 'product.pricelist.item'

    id_mc = fields.Char(string="ID MC", default=False, readonly=True, tracking=True)