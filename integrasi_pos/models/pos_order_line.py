import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID, _
from odoo.exceptions import UserError
import random

class POSOrderLineIntegration(models.Model):
    _inherit = 'pos.order.line'
    
    user_id = fields.Many2one('hr.employee', string='Salesperson',
                              help="You can see salesperson here", ondelete='set null')
    sequence = fields.Integer(string='Sequence', default=10)

    def _export_for_ui(self, orderline):
        """Override untuk menambahkan user_id ke export data"""
        result = super()._export_for_ui(orderline)
        result.update({
            'user_id': orderline.user_id.id if orderline.user_id else False,
            'salesperson': orderline.user_id.name if orderline.user_id else '',
        })
        return result
    
    @api.model
    def create(self, vals):
        # Auto-assign sequence when creating new line
        if 'order_id' in vals and not vals.get('sequence'):
            order = self.env['pos.order'].browse(vals['order_id'])
            if order.exists():
                max_sequence = max([line.sequence for line in order.lines] + [0])
                vals['sequence'] = max_sequence + 10
        return super().create(vals)

class POSOrder(models.Model):
    _inherit = 'pos.order'
    
    def _get_line_sequence_number(self, line):
        """Get line sequence number for receipt display"""
        sorted_lines = self.lines.sorted('sequence')
        for index, sorted_line in enumerate(sorted_lines, 1):
            if sorted_line.id == line.id:
                return index
        return 1