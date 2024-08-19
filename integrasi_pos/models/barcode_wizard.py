# Create a new file: models/inventory_barcode_wizard.py

from odoo import models, fields, api
from odoo.exceptions import UserError, ValidationError

class InventoryBarcodeWizard(models.TransientModel):
    _name = "inventory.barcode.wizard"
    _description = "Inventory Barcode Scanning Wizard"

    inventory_stock_id = fields.Many2one('inventory.stock', string="Inventory Stock", required=True)
    barcode_input = fields.Char(string="Barcode", help="Scan barcode here to add product")
    product_ids = fields.Many2many('product.product', string="Scanned Products", readonly=True)
    location_id = fields.Many2one('stock.location', string="Location", related="inventory_stock_id.location_id")
    
    @api.onchange('barcode_input')
    def _onchange_barcode_input(self):
        """Auto-create inventory.counting record based on scanned barcode with config slicing."""
        if not self.barcode_input:
            return

        barcode_value = self.barcode_input

        # Get config
        barcode_config = self.env['barcode.config'].search([], limit=1)
        if not barcode_config:
            raise ValidationError("Barcode config belum disetting.")

        # Default: pakai barcode penuh
        search_barcode = barcode_value

        # Cari produk berdasarkan barcode penuh terlebih dahulu
        product = self.env['product.product'].search([
            ('barcode', '=', search_barcode)
        ], limit=1)

        # Jika tidak ditemukan produk dan ada konfigurasi panjang_barcode,
        # cek apakah ini produk dengan to_weight=True
        if not product and barcode_config.panjang_barcode:
            # Potong barcode satu karakter lebih sedikit dari yang dikonfigurasi (panjang_barcode - 1)
            search_barcode = barcode_value[:barcode_config.panjang_barcode - 1]
            
            # Cari produk dengan barcode yang sudah dipotong
            product = self.env['product.product'].search([
                ('barcode', '=', search_barcode),
                ('to_weight', '=', True)  # Hanya untuk produk to_weight=True
            ], limit=1)

        if not product:
            raise ValidationError(f"Produk dengan barcode '{search_barcode}' tidak ditemukan.")

        # Add to product_ids if not already there
        if product.id not in self.product_ids.ids:
            self.product_ids = [(4, product.id)]

        # Create inventory counting line in the parent inventory stock record
        inventory_stock = self.inventory_stock_id
        
        # Check if line with same product already exists
        existing_line = self.env['inventory.counting'].search([
            ('inventory_stock_id', '=', inventory_stock.id),
            ('product_id', '=', product.id)
        ], limit=1)
        
        if not existing_line:
            self.env['inventory.counting'].create({
                'inventory_stock_id': inventory_stock.id,
                'product_id': product.id,
                'location_id': self.location_id.id,
                'inventory_date': inventory_stock.inventory_date,
                'state': 'in_progress',
                'uom_id': product.uom_id.id,
            })

        # Reset input
        self.barcode_input = ''
    
    def action_done(self):
        """Close the wizard and return to inventory stock view"""
        return {
            'type': 'ir.actions.act_window_close'
        }