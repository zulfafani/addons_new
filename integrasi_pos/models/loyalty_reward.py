# -*- coding: utf-8 -*-
from odoo import fields, models


class LoyaltyRewardInherit(models.Model):
    _inherit = 'loyalty.reward'

    vit_trxid = fields.Char(string="Transaction ID", default=False)
    id_mc = fields.Char(string="ID MC", default=False)