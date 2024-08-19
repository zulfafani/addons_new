from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
  

class ProductTagsInherit(models.Model):
    _inherit = 'product.tag'

    vit_trxid = fields.Char(string="Transaction ID", default=False)
    id_mc = fields.Char(string="ID MC", default=False)