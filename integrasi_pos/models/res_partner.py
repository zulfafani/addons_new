from odoo import models, fields, api, _
from odoo.exceptions import ValidationError
  

class ResPartner(models.Model): 
    _inherit = 'res.partner'

    customer_code = fields.Char(string='Customer Code', tracking=True)
    is_integrated = fields.Boolean(string="User created", default=False, readonly=True, tracking=True)
    id_mc = fields.Char(string='ID MC', readonly=True, tracking=True)
    active_credit = fields.Boolean(string="Active Credit Limit", default=False, tracking=True)
    warn_amount = fields.Float(string="Warning Amount", default=0.0, tracking=True)
    block_amount = fields.Float(string="Blocking Amount", default=0.0, tracking=True)
    credit_amount = fields.Float(string="Credit", default=0.0, tracking=True)

    @api.model
    def create(self, vals):
        if not vals.get('customer_code'):
            name = vals.get('name')
            mobile = vals.get('mobile')
            record_employee = self.env['hr.employee'].search([], order="id desc", limit=1)
            employee_name = record_employee.name
            employee_mobile_phone = record_employee.mobile_phone

            # Fetch system config for prefix flag
            validate_prefix = self.env['ir.config_parameter'].sudo().get_param('pos.validate_prefix_customer') == 'True'

            # Fetch warehouse prefix or fallback
            warehouse_name = 'VIT'
            warehouse = self.env['stock.warehouse'].search([], limit=1)
            if warehouse:
                warehouse_name = warehouse.prefix_code if (validate_prefix and warehouse.prefix_code) else warehouse.code or 'VIT'

            # Check different conditions
            if name != employee_name or (name == employee_name and mobile != employee_mobile_phone):
                sequence_code = 'res.partner.customer.code'
                customer_code_seq = self.env['ir.sequence'].next_by_code(sequence_code)
                vals['customer_code'] = f"{warehouse_name}{customer_code_seq}"
                vals['is_integrated'] = True

        return super(ResPartner, self).create(vals)
    
    def write(self, vals):
        if 'id_mc' not in vals:
            if 'is_integrated' in vals and vals['is_integrated'] == False:
                vals['is_integrated'] = False
            else:
                vals['is_integrated'] = True
        
        return super(ResPartner, self).write(vals)