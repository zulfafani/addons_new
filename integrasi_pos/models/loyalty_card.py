from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
  

class LoyaltyCard(models.Model):
    _inherit = 'loyalty.card'

    is_integrated = fields.Boolean(string="User created", default=False, readonly=True, tracking=True)

    @api.model
    def create(self, vals):
        if vals:
            vals['is_integrated'] = True

        return super(LoyaltyCard, self).create(vals)
    
    def write(self, vals):
        if vals:
            if 'is_integrated' in vals and vals['is_integrated'] == False:
                vals['is_integrated'] = False
            else:
                vals['is_integrated'] = True
        
        return super(LoyaltyCard, self).write(vals)