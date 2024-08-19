from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
  

class ResPartnerTitle(models.Model):
    _inherit = 'res.partner.title'

    id_mc = fields.Char(string="ID MC", default=False)