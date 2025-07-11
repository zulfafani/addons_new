from odoo import fields, models, api, _
from odoo.exceptions import UserError
from odoo.exceptions import ValidationError
from odoo.tools.float_utils import float_compare 

class MrpProductInherit(models.Model):
    _inherit = 'mrp.production'

    is_integrated = fields.Boolean(string="Integrated", default=False)

    @api.constrains('move_finished_ids', 'product_qty')
    def _check_total_move_qty_equals_product_qty(self):
        for production in self:
            if not production.move_finished_ids:
                continue  # Lewati jika belum ada move (misalnya saat create)

            total_move_qty = sum(production.move_finished_ids.mapped('product_uom_qty'))
            if float_compare(total_move_qty, production.product_qty, precision_digits=2) != 0:
                raise ValidationError(_(
                    "Total finished move quantity (%s) must equal production quantity (%s)."
                ) % (total_move_qty, production.product_qty))

class MrpBoMInherit(models.Model):
    _inherit = 'mrp.bom'

    is_integrated = fields.Boolean(string="Integrated", default=False)

class MrpUnbuild(models.Model):
    _inherit = 'mrp.unbuild'

    is_integrated = fields.Boolean(string="Integrated", default=False)
    unbuild_line_ids = fields.One2many('mrp.unbuild.line', 'unbuild_id', string='Unbuild Lines')

    @api.constrains('unbuild_line_ids', 'product_qty')
    def _check_total_line_qty_matches_unbuild_qty(self):
        for unbuild in self:
            # Skip validation if lines are empty (e.g. during copy)
            if not unbuild.unbuild_line_ids:
                continue

            # Total qty check with float precision
            total = sum(unbuild.unbuild_line_ids.mapped('product_uom_qty'))
            if float_compare(total, unbuild.product_qty, precision_digits=2) != 0:
                raise ValidationError(_(
                    "Total component quantity (%s) must equal the unbuild quantity (%s)."
                ) % (total, unbuild.product_qty))

    @api.onchange('bom_id')
    def _onchange_bom_id(self):
        """Auto-fill product_id and unbuild_line_ids when BoM is selected"""
        if not self.bom_id:
            self.product_id = False
            self.unbuild_line_ids = [(5, 0, 0)]  # Clear existing lines
            return

        # Set product_id from BoM's product template
        if self.bom_id.product_tmpl_id:
            # Get the first variant of the product template
            product = self.env['product.product'].search([
                ('product_tmpl_id', '=', self.bom_id.product_tmpl_id.id)
            ], limit=1)
            if product:
                self.product_id = product.id

        # Auto-fill unbuild lines with BoM components
        self._fill_unbuild_lines_from_bom()

    def _fill_unbuild_lines_from_bom(self):
        """Fill unbuild lines based on selected BoM"""
        if not self.bom_id:
            return

        lines = []
        for bom_line in self.bom_id.bom_line_ids:
            lines.append((0, 0, {
                'product_id': bom_line.product_id.id,
                'location_id': self.location_id.id if self.location_id else False,
                'product_uom_qty': bom_line.product_qty,
                'product_uom': bom_line.product_uom_id.id,
            }))

        self.unbuild_line_ids = lines

    @api.onchange('location_id')
    def _onchange_location_id(self):
        """Update location in all unbuild lines when main location changes"""
        if self.location_id and self.unbuild_line_ids:
            for line in self.unbuild_line_ids:
                line.location_id = self.location_id.id

    def _generate_produce_moves(self):
        StockLocation = self.env['stock.location']
        virtual_production_location = StockLocation.search([('usage', '=', 'production')], limit=1)
        moves = self.env['stock.move']

        for unbuild in self:
            if not unbuild.unbuild_line_ids:
                raise UserError(_("You must provide unbuild line components before proceeding."))

            for line in unbuild.unbuild_line_ids:
                final_qty = line.product_uom_qty * unbuild.product_qty
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
        """Legacy method - now BoM selection is preferred"""
        if not self.product_id or self.bom_id:
            return  # Skip if BoM is already selected

        # Cari BOM berdasarkan produk
        bom = self.env['mrp.bom'].search([('product_tmpl_id', '=', self.product_id.product_tmpl_id.id)], limit=1)
        if bom:
            self.bom_id = bom.id
            # Lines akan terisi otomatis melalui _onchange_bom_id


class MrpUnbuildLine(models.Model):
    _name = 'mrp.unbuild.line'
    _description = 'Manufacturing Unbuild Line'

    unbuild_id = fields.Many2one('mrp.unbuild', string='Unbuild', required=True, ondelete='cascade')
    product_id = fields.Many2one('product.product', string='Product', required=True)
    location_id = fields.Many2one('stock.location', string='Location')
    product_uom_qty = fields.Float(string='To Consume', required=True, default=1.0)
    product_uom = fields.Many2one('uom.uom', string='Unit of Measure', required=True)

    @api.onchange('product_id')
    def _onchange_product_id(self):
        """Set default UoM when product is selected"""
        if self.product_id:
            self.product_uom = self.product_id.uom_id.id