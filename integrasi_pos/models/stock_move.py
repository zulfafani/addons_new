from odoo import models, fields
from odoo.exceptions import UserError

class StockMove(models.Model):
    _inherit = 'stock.move'

    sale_line_id = fields.Many2one('sale.order.line', string='Sales Order Line')
    vit_line_number_sap = fields.Integer(string='Line Number SAP')
    
    def write(self, vals):
        # Cek jika manufacturing order terkait sudah done
        for record in self:
            if record.raw_material_production_id and record.raw_material_production_id.state == 'done':
                raise UserError(_("Cannot modify stock moves linked to a done manufacturing order."))
        
        return super(StockMove, self).write(vals)
    
    def unlink(self):
        # Cek jika manufacturing order terkait sudah done
        for record in self:
            if record.raw_material_production_id and record.raw_material_production_id.state == 'done':
                raise UserError(_("Cannot delete stock moves linked to a done manufacturing order."))
        
        return super(StockMove, self).unlink()