import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class AccountMoveLine(models.Model):
    _inherit = 'account.move.line'

    user_id = fields.Many2one('hr.employee', string='Salesperson',
                              help="You can see salesperson here")