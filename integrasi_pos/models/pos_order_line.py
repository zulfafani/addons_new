import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID, _
from odoo.exceptions import UserError
import random

class POSOrderLineIntegration(models.Model):
    _inherit = 'pos.order.line'
    
    user_id = fields.Many2one('hr.employee', string='Salesperson',
                              help="You can see salesperson here", ondelete='set null')