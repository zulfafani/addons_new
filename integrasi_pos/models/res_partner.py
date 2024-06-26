from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
  

class ResPartner(models.Model):
    _inherit = 'res.partner'

    customer_code = fields.Char(string='Customer Code')
    customer_code_prefix = fields.Many2one('pos.config', string='POS Name')  # Field untuk mereferensikan pos.config
    is_integrated = fields.Boolean(string="User created", default=False)

    @api.model
    def create(self, vals):
        # Jika customer_code tidak ada atau kosong, isi dengan nomor urut dan set is_integrated ke True
        if not vals.get('customer_code') and vals.get('customer_code_prefix'):
            sequence_code = 'res.partner.customer.code'
            customer_code_seq = self.env['ir.sequence'].next_by_code(sequence_code)

            # Mengambil name dari pos.config jika customer_code_prefix ada di vals
            pos_config_name = ''
            pos_config = self.env['pos.config'].browse(vals['customer_code_prefix'])
            pos_config_name = pos_config.name if pos_config else ''

            # Menggabungkan name dari pos.config dengan customer_code_seq
            vals['customer_code'] = f"{pos_config_name}{customer_code_seq}"
            vals['is_integrated'] = True
        
        # Panggil metode create asli untuk membuat record baru
        result = super(ResPartner, self).create(vals)
        return result
