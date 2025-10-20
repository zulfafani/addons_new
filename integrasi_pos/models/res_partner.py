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
    is_store = fields.Boolean(string="Is Store", default=False, tracking=True)
    vit_customer_group = fields.Many2one('customer.group', string="Customer Group", tracking=True)

    @api.onchange('vit_customer_group')
    def _onchange_vit_customer_group(self):
        """Auto fill pricelist when customer group is selected"""
        if self.vit_customer_group and self.vit_customer_group.vit_pricelist_id:
            self.property_product_pricelist = self.vit_customer_group.vit_pricelist_id

    @api.model
    def create(self, vals):
        # if not vals.get('phone'):
        #     raise ValidationError(_("Field 'Phone' is required. Please fill in the phone number."))
        
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

        # Auto fill pricelist when creating partner with customer group
        if 'vit_customer_group' in vals and vals['vit_customer_group']:
            customer_group = self.env['customer.group'].browse(vals['vit_customer_group'])
            if customer_group.vit_pricelist_id and 'property_product_pricelist' not in vals:
                vals['property_product_pricelist'] = customer_group.vit_pricelist_id.id

        return super(ResPartner, self).create(vals)
    
    def write(self, vals):
        # if 'phone' in vals and not vals['phone']:
        #     raise ValidationError(_("Field 'Phone' is required. Please fill in the phone number."))

        # # atau jika phone belum diisi di record dan tidak dikirim via vals
        # for partner in self:
        #     if not partner.phone and 'phone' not in vals:
        #         raise ValidationError(_("Field 'Phone' is required. Please fill in the phone number."))
        
        # Auto fill pricelist when customer group is updated via write
        if 'vit_customer_group' in vals and vals['vit_customer_group']:
            customer_group = self.env['customer.group'].browse(vals['vit_customer_group'])
            if customer_group and customer_group.vit_pricelist_id:
                vals['property_product_pricelist'] = customer_group.vit_pricelist_id.id
        
        # Log perubahan phone dan category_id ke chatter
        for partner in self:
            changes = []
            
            # Track phone changes
            if 'phone' in vals:
                old_phone = partner.phone or 'Not Set'
                new_phone = vals['phone'] or 'Not Set'
                if old_phone != new_phone:
                    changes.append(f"Phone: {old_phone} → {new_phone}")
            
            # Track category_id changes
            if 'category_id' in vals:
                old_categories = ', '.join(partner.category_id.mapped('name')) if partner.category_id else 'Not Set'
                
                # Process new categories
                new_category_ids = vals['category_id']
                if new_category_ids:
                    # Handle different formats of many2many write
                    category_records = self.env['res.partner.category']
                    for command in new_category_ids:
                        if command[0] == 6:  # (6, 0, [ids])
                            category_records = self.env['res.partner.category'].browse(command[2])
                        elif command[0] == 4:  # (4, id)
                            category_records |= self.env['res.partner.category'].browse(command[1])
                    new_categories = ', '.join(category_records.mapped('name')) if category_records else 'Not Set'
                else:
                    new_categories = 'Not Set'
                
                if old_categories != new_categories:
                    changes.append(f"Tags: {old_categories} → {new_categories}")
            
            # Track customer group changes
            if 'vit_customer_group' in vals:
                old_group = partner.vit_customer_group.vit_group_name if partner.vit_customer_group else 'Not Set'
                new_group_id = vals['vit_customer_group']
                if new_group_id:
                    new_group_record = self.env['customer.group'].browse(new_group_id)
                    new_group = new_group_record.vit_group_name if new_group_record else 'Not Set'
                else:
                    new_group = 'Not Set'
                
                if old_group != new_group:
                    changes.append(f"Customer Group: {old_group} → {new_group}")
            
            # Track pricelist changes
            if 'property_product_pricelist' in vals:
                old_pricelist = partner.property_product_pricelist.name if partner.property_product_pricelist else 'Not Set'
                new_pricelist_id = vals['property_product_pricelist']
                if new_pricelist_id:
                    new_pricelist_record = self.env['product.pricelist'].browse(new_pricelist_id)
                    new_pricelist = new_pricelist_record.name if new_pricelist_record else 'Not Set'
                else:
                    new_pricelist = 'Not Set'
                
                if old_pricelist != new_pricelist:
                    changes.append(f"Pricelist: {old_pricelist} → {new_pricelist}")
            
            # Post message to chatter if there are changes
            if changes:
                message = '\n'.join(changes)
                partner.message_post(body=message, subject="Partner Information Updated")
            
        if 'id_mc' not in vals:
            if 'is_integrated' in vals and vals['is_integrated'] == False:
                vals['is_integrated'] = False
            else:
                vals['is_integrated'] = True
        
        return super(ResPartner, self).write(vals)