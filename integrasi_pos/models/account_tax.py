# -*- coding: utf-8 -*-
from odoo import fields, models


class AccountTaxInherit(models.Model):
    _inherit = 'account.tax'

    id_mc = fields.Char(string="ID MC", default=False)