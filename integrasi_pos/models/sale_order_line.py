from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class SaleOrderLine(models.Model):
    _inherit = 'sale.order.line'

    move_ids = fields.One2many('stock.move', 'sale_line_id', string='Stock Moves')
