from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
  

class ResPartner(models.Model):
    _inherit = 'res.partner'

    customer_code = fields.Char(string='Customer Code')
    is_integrated = fields.Boolean(string="User created", default=False)

    @api.model
    def create(self, vals):
        # Jika customer_code tidak ada atau kosong, isi dengan nomor urut dan set is_integrated ke True
        if not vals.get('customer_code'):
            sequence_code = 'res.partner.customer.code'
            customer_code_seq = self.env['ir.sequence'].next_by_code(sequence_code)

            # Mengambil short name dari stock.warehouse
            warehouse_name = ''
            warehouses = self.env['stock.warehouse'].search([])
            if warehouses:
                first_warehouse = warehouses[0]
                warehouse_name = first_warehouse.code if first_warehouse else 'VIT'
            else:
                warehouse_name = 'VIT' 

            # Menggabungkan name warehouse dengan customer_code_seq
            vals['customer_code'] = f"{warehouse_name}{customer_code_seq}"
            vals['is_integrated'] = True
        
        # Panggil metode create asli untuk membuat record baru
        result = super(ResPartner, self).create(vals)
        return result