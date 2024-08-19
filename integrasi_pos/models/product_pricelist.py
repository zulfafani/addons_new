import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class ProductPricelist(models.Model):
    _inherit = 'product.pricelist'

    id_mc = fields.Char(string="ID MC", default=False)