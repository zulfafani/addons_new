# -*- coding: utf-8 -*-
from odoo import fields, models


class ProductTemplateInherit(models.Model):
    _inherit = 'product.template'

    id_mc = fields.Char(string="ID MC", default=False)
