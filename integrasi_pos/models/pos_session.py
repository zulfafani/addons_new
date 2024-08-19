import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class PosSession(models.Model):
    _inherit = 'pos.session'

    is_updated = fields.Boolean(string="Updated", default=False, readonly=True, tracking=True)
    name_session_pos = fields.Char(string="Name Session POS (Odoo Store)", readonly=True)
    id_mc = fields.Char(string="ID MC", default=False)

    def update_session(self):
        pos = self.env['pos.session'].search([('state', '=', 'closed')])
        for i in pos:
            if isinstance(i.stop_at, datetime):
                new_stop_at = i.stop_at - timedelta(hours=7)
            else:
                new_stop_at = i.stop_at  # Keep the original value if it's not a datetime
            
            if isinstance(i.start_at, datetime):
                new_start_at = i.start_at - timedelta(hours=7)
            else:
                new_start_at = i.start_at  # Keep the original value if it's not a datetime

            i.write({
                'state': 'closed',
                'start_at': new_start_at,
                'stop_at': new_stop_at,
                'cash_register_balance_start': i.cash_register_balance_start,
            })
