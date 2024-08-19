import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class AccountJournal(models.Model):
    _inherit = 'account.journal'

    id_mc = fields.Char(string="ID MC", default=False, tracking=True)
    is_credit = fields.Boolean(string="Credit", default=False, tracking=True)