# -*- coding: utf-8 -*-
from odoo import fields, models


class LoyaltyProgramInherit(models.Model):
    _inherit = 'loyalty.program'

    is_integrated = fields.Boolean(string="Integrated", default=False)
    vit_trxid = fields.Char(string="Transaction ID", default=False)
