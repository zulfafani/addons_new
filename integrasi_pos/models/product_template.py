from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
  

class ProductTemplate(models.Model):
    _inherit = 'product.template'

    is_integrated = fields.Boolean(string="Integrated", default=False)