import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class UomUom(models.Model):
    _inherit = 'uom.uom'

    id_mc = fields.Char(string="ID MC", default=False)