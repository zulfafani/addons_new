from odoo import models, fields, api, _
from odoo.exceptions import ValidationError

class ResPartner(models.Model):
    _inherit = 'res.partner'

    # compute : metode untuk menghitung nilai field secara otomatis
    # inverse : metode untuk menulis kembali perubahan pada field
    # store=True : nilai yang dihitung harus disimpan di database
    # required=True : field menjadi wajib diisi
    customer_code = fields.Char('Customer Code')
