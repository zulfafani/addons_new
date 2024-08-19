from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
  

class ResCurrency(models.Model):
    _inherit = 'res.currency'

    id_mc = fields.Char(string="ID MC", default=False)