from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
  

class LoyaltyCard(models.Model):
    _inherit = 'loyalty.card'

    is_integrated = fields.Boolean(string="User created", default=False, readonly=True, tracking=True)
    history_ids = fields.One2many(comodel_name='loyalty.history', inverse_name='card_id', readonly=True)

    @api.model_create_multi
    def create(self, vals_list):
        # Tandai setiap record sebagai terintegrasi
        for vals in vals_list:
            vals['is_integrated'] = True
        # Buat record loyalty.card
        res = super().create(vals_list)
        # Buat riwayat loyalty.history untuk setiap card
        for card in res:
            self.env['loyalty.history'].create({
                'card_id': card.id,
                'points_before': 0,
                'points_after': card.points
            })
        return res
    
    def write(self, vals):
        # Tangani is_integrated
        if vals:
            if 'is_integrated' in vals and vals['is_integrated'] is False:
                vals['is_integrated'] = False
            else:
                vals['is_integrated'] = True
        # Cek dan simpan nilai points sebelum update
        points_before = {}
        if 'points' in vals:
            points_before = {card.id: card.points for card in self}
        # Lanjutkan proses update
        res = super().write(vals)
        # Setelah update, simpan ke loyalty.history jika ada perubahan points
        if 'points' in vals:
            for card in self:
                old = points_before.get(card.id)
                new = card.points
                if old != new:
                    self.env['loyalty.history'].create({
                        'card_id': card.id,
                        'points_before': old,
                        'points_after': new,
                    })
        return res

    