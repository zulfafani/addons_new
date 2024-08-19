from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
  

class ResPartnerCategory(models.Model):
    _inherit = 'res.partner.category'

    id_mc = fields.Char(string="ID MC", default=False)