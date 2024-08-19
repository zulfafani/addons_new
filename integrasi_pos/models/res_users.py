# -*- coding: utf-8 -*-
from odoo import fields, models


class ResUsersInherit(models.Model):
    _inherit = 'res.users'

    id_mc = fields.Char(string="ID MC", default=False)
