# Part of Odoo. See LICENSE file for full copyright and licensing details.

from odoo import fields, models


class LoyaltyHistory(models.Model):
    _name = 'loyalty.history'
    _description = "History for Loyalty cards Customer"
    _order = 'id desc'

    card_id = fields.Many2one(comodel_name='loyalty.card', required=True, ondelete='cascade')
    points_before = fields.Float()
    points_after = fields.Float()
    is_integrated = fields.Boolean(string="Integrated", default=False, readonly=True, tracking=True)
