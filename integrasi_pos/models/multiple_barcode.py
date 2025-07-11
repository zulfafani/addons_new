from odoo import models, fields, api
from odoo.exceptions import ValidationError

class MultipleBarcode(models.Model):
    _name = "multiple.barcode"
    _description = "Multiple Barcode"

    barcode = fields.Char(string="Barcode", required=True)
    product_tmpl_id = fields.Many2one('product.template', string='Product Template', ondelete='cascade')
    product_id = fields.Many2one('product.product', string='Product Variant', ondelete='cascade')
    id_mc = fields.Char(string="ID MC", default=False)

    @api.model
    def create(self, vals):
        # Auto-fill product_id if product_tmpl_id has one variant
        if not vals.get('product_id') and vals.get('product_tmpl_id'):
            tmpl = self.env['product.template'].browse(vals['product_tmpl_id'])
            if tmpl.product_variant_ids and len(tmpl.product_variant_ids) == 1:
                vals['product_id'] = tmpl.product_variant_ids.id
        return super(MultipleBarcode, self).create(vals)

    def write(self, vals):
        for record in self:
            if not record.product_id and (vals.get('product_tmpl_id') or record.product_tmpl_id):
                tmpl = self.env['product.template'].browse(vals.get('product_tmpl_id', record.product_tmpl_id.id))
                if tmpl.product_variant_ids and len(tmpl.product_variant_ids) == 1:
                    vals['product_id'] = tmpl.product_variant_ids.id
        return super(MultipleBarcode, self).write(vals)

class ProductProduct(models.Model):
    _inherit = 'product.product'

    multi_barcode_ids = fields.One2many('multiple.barcode', 'product_id', string='Multiple Barcodes')
    is_fixed_price = fields.Boolean(string="Fixed Price", default=False)
    brand = fields.Char(string="Brand", tracking=True)