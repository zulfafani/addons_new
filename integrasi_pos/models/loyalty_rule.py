# -*- coding: utf-8 -*-
from odoo import fields, models


class LoyaltyRuleInherit(models.Model):
    _inherit = 'loyalty.rule'

    vit_trxid = fields.Char(string="Transaction ID", default=False)
    id_mc = fields.Char(string="ID MC", default=False)
    reward_point_amount = fields.Float(
        digits=(16, 5),  # Total 16 digit, 5 angka di belakang koma
        default=1, 
        string="Reward"
    )

    apply_days = fields.Selection([
        ('monday', 'Monday'),
        ('tuesday', 'Tuesday'),
        ('wednesday', 'Wednesday'),
        ('thursday', 'Thursday'),
        ('friday', 'Friday'),
        ('saturday', 'Saturday'),
        ('sunday', 'Sunday'),
    ], string="Day of Week")

    start_time = fields.Char("Start Hour", help="Format: HH:MM (e.g., 08:00)")
    end_time = fields.Char("End Hour", help="Format: HH:MM (e.g., 22:00)")

    member_category_ids = fields.Many2many(
        "res.partner.category",
        string="Allowed Member Categories"
    )

    def _export_for_loyalty_pos(self):
        res = super()._export_for_loyalty_pos()
        res.update({
            "apply_days": self.apply_days or "",
            "start_time": self.start_time or "",
            "end_time": self.end_time or "",
            "member_category_ids": self.member_category_ids.ids,
        })
        return res