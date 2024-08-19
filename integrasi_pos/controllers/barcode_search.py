# # controllers/main.py
# from odoo import http
# from odoo.http import request

# class CustomBarcodeController(http.Controller):

#     @http.route('/custom_pos/parse_barcode', type='json', auth='public')
#     def parse_barcode(self, code):
#         prefix_timbangan = "21"
#         digit_awal = 2
#         digit_akhir = 4
#         panjang_barcode = 7

#         if not code.startswith(prefix_timbangan) or len(code) <= panjang_barcode:
#             return {"error": "Invalid barcode format"}

#         qty_part = code[-panjang_barcode:]
#         qty_str = qty_part[digit_awal:digit_akhir + 1]
#         try:
#             quantity = float(qty_str) / 1000.0
#         except:
#             return {"error": "Invalid quantity parsing"}

#         product_barcode = code[:-panjang_barcode]
#         product = request.env['product.template'].sudo().search([
#             ('barcode', '=', product_barcode),
#             ('to_weight', '=', True)
#         ], limit=1)

#         if not product:
#             return {"error": "Product not found"}

#         return {
#             "product_template_id": product.id,
#             "code": product_barcode,
#             "quantity": quantity,
#         }
