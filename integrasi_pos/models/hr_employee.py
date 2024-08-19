from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
  

class HrEmployee(models.Model):
    _inherit = 'hr.employee'

    is_integrated = fields.Boolean(string="User created", default=False, readonly=True, tracking=True)
    id_mc = fields.Char(string='ID MC', readonly=True, tracking=True)
    disable_payment = fields.Boolean(
        string="POS-Disable Payment",
        help="Disable the payment button on the POS", readonly=True)
    pin_code = fields.Char(string="PIN Code Authorization", tracking=True)
    is_sales_person = fields.Boolean(string="Is Sales Person", tracking=True)
    is_cashier = fields.Boolean(string="Is Cashier", tracking=True)
    is_pic = fields.Boolean(string="Is PIC", tracking=True)
    
    @api.model
    def create(self, vals):
        if 'id_mc' not in vals:
            vals['is_integrated'] = True
        
        # Panggil metode create asli untuk membuat record baru
        return super(HrEmployee, self).create(vals)
    
    def write(self, vals):
        if 'id_mc' not in vals:
            if 'is_integrated' in vals and vals['is_integrated'] == False:
                vals['is_integrated'] = False
            else:
                vals['is_integrated'] = True
        
        return super(HrEmployee, self).write(vals)
