# -*- coding: utf-8 -*-
from odoo import fields, models, api


class ProductTemplateInherit(models.Model):
    _inherit = 'product.template'

    id_mc = fields.Char(string="ID MC", default=False)
    multi_barcode_ids = fields.One2many('multiple.barcode', 'product_tmpl_id', string='Multiple Barcodes')
    vit_sub_div = fields.Char(string="Sub Category")
    vit_item_kel = fields.Char(string="Kelompok")
    vit_item_type = fields.Char(string="Type")

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