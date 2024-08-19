from odoo import fields, models, api, _
from odoo.exceptions import UserError

class MrpProductInherit(models.Model):
    _inherit = 'mrp.production'

    is_integrated = fields.Boolean(string="Integrated", default=False)

class MrpBoMInherit(models.Model):
    _inherit = 'mrp.bom'

    is_integrated = fields.Boolean(string="Integrated", default=False)

class MrpUnbuild(models.Model):
    _inherit = 'mrp.unbuild'

    is_integrated = fields.Boolean(string="Integrated", default=False)
    unbuild_line_ids = fields.One2many('mrp.unbuild.line', 'unbuild_id', string='Unbuild Lines')

    def _generate_produce_moves(self):
        StockLocation = self.env['stock.location']
        virtual_production_location = StockLocation.search([('usage', '=', 'production')], limit=1)
        moves = self.env['stock.move']

        for unbuild in self:
            if not unbuild.unbuild_line_ids:
                raise UserError(_("You must provide unbuild line components before proceeding."))

            for line in unbuild.unbuild_line_ids:
                final_qty = line.product_uom_qty
                product = line.product_id
                product_uom = line.product_uom

                move = self.env['stock.move'].create({
                    'name': unbuild.name,
                    'date': unbuild.create_date,
                    'product_id': product.id,
                    'product_uom_qty': final_qty,
                    'product_uom': product_uom.id,
                    'procure_method': 'make_to_stock',
                    'location_id': virtual_production_location.id,        # 🔁 VIRTUAL PRODUCTION
                    'location_dest_id': unbuild.location_dest_id.id,      # 🔁 REAL STOCK
                    'warehouse_id': unbuild.location_dest_id.warehouse_id.id,
                    'unbuild_id': unbuild.id,
                    'company_id': unbuild.company_id.id,
                })
                moves |= move

        return moves

    @api.onchange('product_id')
    def _onchange_product_id(self):
        if not self.product_id:
            return

        # Cari BOM berdasarkan produk
        bom = self.env['mrp.bom'].search([('product_tmpl_id', '=', self.product_id.product_tmpl_id.id)], limit=1)
        if not bom:
            return

        lines = []
        for bom_line in bom.bom_line_ids:
            lines.append((0, 0, {
                'product_id': bom_line.product_id.id,
                'location_id': self.location_id.id if self.location_id else False,
                'product_uom_qty': bom_line.product_qty,
                'product_uom': bom_line.product_uom_id.id,
            }))

        self.unbuild_line_ids = lines


class MrpUnbuildLine(models.Model):
    _name = 'mrp.unbuild.line'

    unbuild_id = fields.Many2one('mrp.unbuild', string='Unbuild')
    product_id = fields.Many2one('product.product', string='Product')
    location_id = fields.Many2one('stock.location', string='Location')
    product_uom_qty = fields.Float(string='To Consume')
    product_uom = fields.Many2one('uom.uom', string='Unit of Measure')