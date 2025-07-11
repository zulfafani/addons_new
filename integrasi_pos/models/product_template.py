# -*- coding: utf-8 -*-
from odoo import fields, models, api


class ProductTemplateInherit(models.Model):
    _inherit = 'product.template'

    id_mc = fields.Char(string="ID MC", default=False)
    multi_barcode_ids = fields.One2many('multiple.barcode', 'product_tmpl_id', string='Multiple Barcodes')
    vit_sub_div = fields.Char(string="Sub Category")
    vit_item_kel = fields.Char(string="Kelompok")
    vit_item_type = fields.Char(string="Type")
    is_fixed_price = fields.Boolean(string="Fixed Price", default=False)
    vit_is_discount = fields.Boolean(string="Is Discount", default=False)
    brand = fields.Char(string="Brand")

    @api.model
    def create(self, vals):
        record = super(ProductTemplateInherit, self).create(vals)
        update_vals = {}
        if 'is_fixed_price' in vals:
            update_vals['is_fixed_price'] = vals['is_fixed_price']
        if 'brand' in vals:
            update_vals['brand'] = vals['brand']
        if update_vals:
            record.product_variant_ids.write(update_vals)
        return record

    def write(self, vals):
        res = super(ProductTemplateInherit, self).write(vals)
        update_vals = {}
        if 'is_fixed_price' in vals:
            update_vals['is_fixed_price'] = vals['is_fixed_price']
        if 'brand' in vals:
            update_vals['brand'] = vals['brand']
        if update_vals:
            for template in self:
                template.product_variant_ids.write(update_vals)
        return res



    def _check_barcode_uniqueness(self):
        # override to disable barcode uniqueness constraint
        return True

    @api.model
    def parse_weight_barcode(self, code):
        prefix_timbangan = "21"
        digit_awal = 2
        digit_akhir = 4
        panjang_barcode = 7

        if not code or not code.startswith(prefix_timbangan):
            return {'error': 'Invalid barcode'}

        try:
            product_barcode = code[:-panjang_barcode]
            qty_str = code[-panjang_barcode:][digit_awal:digit_akhir + 1]
            quantity = float(qty_str) / 1000.0

            product = self.search_read(
                [('barcode', '=', product_barcode)],
                ['id', 'to_weight'], limit=1
            )

            if not product:
                return {'error': 'Product not found'}

            if not product[0]['to_weight']:
                return {'error': 'Product is not weighted'}

            return {
                'code': product_barcode,
                'quantity': quantity,
                'product_template_id': product[0]['id'],
            }

        except Exception as e:
            return {'error': str(e)}

class ProductProductInherit(models.Model):
    _inherit = 'product.product'

    vit_is_discount = fields.Boolean(string="Is Discount", default=False)

    def _check_barcode_uniqueness(self):
        # Override untuk mematikan validasi barcode unik
        # Tidak akan pernah raise ValidationError lagi walaupun ada duplikat
        return True