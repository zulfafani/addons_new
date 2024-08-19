from odoo import models, fields, api, _
from odoo.exceptions import ValidationError

class PartnerPinCode(models.Model):
    _name = 'partner.pin.code'
    _description = 'Partner PIN Code'

    partner_id = fields.Many2one('res.partner', string="Customer", ondelete='cascade')
    pin_code = fields.Char(string="PIN Code", required=True)
    active = fields.Boolean(string="Active", default=True)