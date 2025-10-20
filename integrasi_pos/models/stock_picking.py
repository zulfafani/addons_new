from odoo import models, fields, api, _
from odoo.exceptions import UserError, ValidationError
import random

class StockPicking(models.Model):
    _inherit = 'stock.picking'

    is_integrated = fields.Boolean(string="Integrated", default=False, readonly=True, tracking=True)
    is_updated = fields.Boolean(string="Updated", default=False, readonly=True, tracking=True)
    vit_trxid = fields.Char(string="Transaction ID")
    target_location = fields.Many2one('master.warehouse', string="Target Location")
    targets = fields.Char(string="Target Locations")

    def write(self, vals):
        # Cek jika status sudah ready atau done dan mencoba mengubah field yang dibatasi
        restricted_states = ['assigned', 'done']  # ready = assigned, done = done
        
        for record in self:
            if record.state in restricted_states:
                # Field yang tidak boleh diubah ketika ready/done
                restricted_fields = ['target_location', 'location_id', 'location_dest_id']
                for field in restricted_fields:
                    if field in vals:
                        raise UserError(_("Cannot modify field '%s' when transfer is in %s state.") % (field, record.state))
                
                # Cek jika ada perubahan pada move lines (tambah/hapus item)
                if 'move_ids_without_package' in vals:
                    move_operations = vals.get('move_ids_without_package', [])
                    
                    for operation in move_operations:
                        # (0, 0, values) - CREATE new line
                        # (2, id, 0) - DELETE existing line
                        # (1, id, values) - UPDATE existing line (termasuk qty)
                        if operation[0] in (0, 2, 1):
                            if operation[0] == 0:
                                action = "add new items"
                            elif operation[0] == 2:
                                action = "delete items" 
                            else:
                                action = "modify items or quantities"
                            
                            raise UserError(_("Cannot %s when transfer is in %s state.") % (action, record.state))
        
        return super(StockPicking, self).write(vals)

    @api.model
    def create(self, vals):
        picking_type = None

        # Get picking type from vals if available
        if vals.get('picking_type_id'):
            picking_type = self.env['stock.picking.type'].browse(vals['picking_type_id'])

        return super(StockPicking, self).create(vals)

    def button_validate(self):
        # Check if the operation type is 'Internal Transfers'
        if self.picking_type_id.code == 'internal':
            # Check if the source and destination locations are the same
            if self.location_id.id == self.location_dest_id.id:
                raise UserError("Cannot validate this operation: Source and destination locations are the same.")
        
        # Call the super method
        res = super(StockPicking, self).button_validate()
        return res
    
    @api.model
    def create_stock_pickings(self):
        # Temukan ID untuk operation type TS Out
        operation_type_id = self.env['stock.picking.type'].search([('name', 'ilike', 'TS Out')], limit=1)
        if not operation_type_id:
            raise UserError('Operation Type TS Out tidak ditemukan.')
        
        product_codes = ['LBR00001', 'LBR00002', 'LBR00003', 'LBR00088', 'LBR00099', 'LBR00008', 'LBR00007', 'LBR00006', 'LBR00009', 'LBR00004']
        products = self.env['product.product'].search([('default_code', 'in', product_codes)])

        stock_pickings = []
        for i in range(500):
            # Mengatur move_lines untuk setiap `stock.picking`
            move_lines = []
            for j, product in enumerate(products):
                quantity = random.uniform(1, 10)
                move_lines.append((0, 0, {
                    'name': product.name,
                    'product_id': product.id,
                    'product_uom_qty': quantity,
                    'quantity': quantity,
                    'location_id': 4,
                    'location_dest_id': 8,
                }))

            target_location = self.env['master.warehouse'].search([('warehouse_name', '=', "Store 02")], limit=1)
            
            stock_picking = {
                'picking_type_id': operation_type_id.id,
                'location_id': 4,
                'location_dest_id': 8,
                'target_location': target_location.id,
                'move_ids_without_package': move_lines,
            }
            stock_pickings.append(stock_picking)
        
        self.env['stock.picking'].create(stock_pickings)

    def write_tsout(self):
        ts_out = self.env['stock.picking'].search([('state', '=', 'assigned')])

        for res in ts_out:
            res.write({'is_integrated': False})