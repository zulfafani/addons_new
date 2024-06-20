import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class StockPicking(models.Model):
    _inherit = 'stock.picking'

    is_integrated = fields.Boolean(string="Integrated", default=False)
    vit_trxid = fields.Char(string="Transaction ID")
    target_location = fields.Many2one('master.warehouse', string="Target Location")

    def button_validate(self):
        for picking in self:
            # Set the 'origin' field with the value of the 'name' field
            picking.origin = picking.name

        # Call the original button_validate method
        return super(StockPicking, self).button_validate()
