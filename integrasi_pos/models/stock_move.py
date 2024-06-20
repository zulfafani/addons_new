from odoo import models, fields

class StockMove(models.Model):
    _inherit = 'stock.move'

    sale_line_id = fields.Many2one('sale.order.line', string='Sales Order Line')
