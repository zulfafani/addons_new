from odoo import fields, models


class PosOrderLine(models.Model):
    """ The class PosOrder is used to inherit pos.order.line """
    _inherit = 'pos.order.line'

    user_id = fields.Many2one('hr.employee', string='Salesperson',
                              help="You can see salesperson here")
