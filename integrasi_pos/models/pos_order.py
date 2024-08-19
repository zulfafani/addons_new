import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID, _
from odoo.exceptions import UserError
import random

class POSIntegration(models.Model):
    _inherit = 'pos.order'

    vit_trxid = fields.Char(string='Transaction ID', tracking=True)
    is_integrated = fields.Boolean(string="Integrated", default=False, readonly=True, tracking=True)
    id_mc = fields.Char(string="ID MC", tracking=True)

    # @api.model
    # def _get_invoice_lines_values(self, line_values, pos_order_line):
    #     # Panggil implementasi dari superclass
    #     res = super()._get_invoice_lines_values(line_values, pos_order_line)

    #     # Tambahkan field user_id jika tersedia di pos_order_line
    #     res['user_id'] = pos_order_line.user_id.id if pos_order_line.user_id else False

    #     return res

    @api.model
    def create_pos_orders(self):
        # Get the active POS session
        pos_session = self.env['pos.session'].search([('state', '=', 'opened')], limit=1)
        if not pos_session:
            raise UserError('No active POS session found.')
        
        # Get all products to be used in orders
        product_codes = ['LBR00001', 'LBR00002', 'LBR00003', 'LBR00088', 'LBR00099', 'LBR00008', 'LBR00007', 'LBR00006', 'LBR00009', 'LBR00004']
        products = self.env['product.product'].search([('default_code', 'in', product_codes)])
        if not products:
            raise UserError('No products found.')

        pos_orders = []
        for i in range(100):  # Create 100 POS orders
            order_lines = []
            payment_lines = []
            total_amount = 0  # Initialize total_amount
            total_tax = 0  # Initialize total_tax

            for product in products:
                qty = random.randint(1, 10)  # Set quantity to a random number between 1 and 10
                taxes = product.taxes_id.compute_all(product.list_price, quantity=qty, product=product)
                line_tax = sum(t['amount'] for t in taxes['taxes'])
                line_total = taxes['total_included']
                line_subtotal = taxes['total_excluded']
                line_subtotal_incl = line_subtotal + line_tax 

                order_line = (0, 0, {
                    'product_id': product.id,
                    'name': product.name,
                    'full_product_name': product.name,
                    'qty': qty,  # Use the random quantity here
                    'price_unit': -product.list_price,  # Set price_unit to negative
                    'price_subtotal': -line_subtotal,  # Set subtotal to negative
                    'price_subtotal_incl': -line_subtotal_incl,  # Set subtotal including tax to negative
                    'tax_ids': [(6, 0, product.taxes_id.ids)],
                })
                order_lines.append(order_line)
                
                total_amount += line_total
                total_tax += line_tax

            payment_method = self.env['pos.payment.method'].search([], limit=1)
            if not payment_method:
                raise UserError('No payment method found.')
            
            payment_line = (0, 0, {
                'payment_method_id': 2,  # Use the found payment method
                'amount': -total_amount,  # Set amount to negative for credit note
            })

            amount_paid = -total_amount  # Set amount paid to negative
            amount_return = 0

            pos_order = self.env['pos.order'].create({
                'session_id': pos_session.id,
                'pos_reference': f"S01-{i+1:05d}",
                'tracking_number': f"{i+1:05d}",
                'lines': order_lines,
                'partner_id': 7,
                'employee_id': 1,
                'payment_ids': [payment_line],
                'amount_total': -total_amount,  # Set total amount to negative
                'amount_tax': -total_tax,  # Set total tax to negative
                'amount_paid': amount_paid,
                'amount_return': amount_return,
                'state': 'invoiced'
            })
            pos_orders.append(pos_order)

        return pos_orders

    def write_orderref(self):
        pos = self.env['pos.order'].search([])
        for i in pos:
            i.write({'is_integrated': True})