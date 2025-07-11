# -*- coding: utf-8 -*-
from odoo import fields, models


class LoyaltyRewardInherit(models.Model):
    _inherit = 'loyalty.reward'

    vit_trxid = fields.Char(string="Transaction ID", default=False)
    id_mc = fields.Char(string="ID MC", default=False)
    rule_id = fields.Many2one("loyalty.rule", string="Loyalty Rule")  # ✅ tambahkan ini
    vit_reward_trxid = fields.Char(string="Reward Transaction ID", default=False)
    
    def create(self, vals_list):
        """Override create untuk auto-sync description dari vit_reward_trxid"""
        for vals in vals_list:
            # Jika vit_reward_trxid diisi dan description kosong atau tidak ada
            if vals.get('vit_reward_trxid') and not vals.get('description'):
                vals['description'] = vals['vit_reward_trxid']
            # Atau jika vit_reward_trxid diisi, selalu sync ke description
            elif vals.get('vit_reward_trxid'):
                vals['description'] = vals['vit_reward_trxid']
        
        return super(LoyaltyRewardInherit, self).create(vals_list)

    def write(self, vals):
        """Override write untuk auto-sync description dari vit_reward_trxid"""
        # Jika vit_reward_trxid di-update, maka update description juga
        if 'vit_reward_trxid' in vals and vals.get('vit_reward_trxid'):
            vals['description'] = vals['vit_reward_trxid']
        
        return super(LoyaltyRewardInherit, self).write(vals)

    def _export_for_loyalty_pos(self):
        return {
            'id': self.id,
            'name': self.name,
            'reward_type': self.reward_type,
            'program_id': [self.program_id.id, self.program_id.name],
            'discount': self.discount,
        }