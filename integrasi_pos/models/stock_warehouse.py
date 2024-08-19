from odoo import models, fields, api, _
from odoo.exceptions import ValidationError

class StockWarehouseInherit(models.Model):
    _inherit = 'stock.warehouse'

    prefix_code = fields.Char(string="Prefix Customer Code", help="Used when POS is configured to override customer code prefix.")
    
    show_prefix_code = fields.Boolean(string="Show Prefix Code", compute="_compute_show_prefix_code")

    @api.depends('prefix_code')
    def _compute_show_prefix_code(self):
        config = self.env['ir.config_parameter'].sudo()
        show_flag = config.get_param('pos.validate_prefix_customer') == 'True'
        for record in self:
            record.show_prefix_code = show_flag