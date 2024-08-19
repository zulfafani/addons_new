import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class PoSCategory(models.Model):
    _inherit = 'pos.category'

    id_mc = fields.Char(string="ID MC", default=False)