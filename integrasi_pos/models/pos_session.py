import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class PosSession(models.Model):
    _inherit = 'pos.session'

    is_integrated = fields.Boolean(string="Integrated", default=False)
    name_session_pos = fields.Char(string="Name Session POS (Odoo Store)", readonly=True)