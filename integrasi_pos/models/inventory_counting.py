from odoo import models, fields, api
from pytz import timezone
from datetime import datetime, timedelta
from odoo.exceptions import ValidationError

class InventoryStock(models.Model):
    _name = "inventory.stock"
    _description = "Inventory Stock"
    _rec_name = 'doc_num'

    doc_num = fields.Char(string="Internal Reference")
    warehouse_id = fields.Many2one('stock.warehouse', string="Warehouse")
    location_id = fields.Many2one('stock.location', string="Location")
    company_id = fields.Many2one('res.company', string="Company")
    create_date = fields.Datetime(string="Created Date")
    from_date = fields.Datetime(string="From Date")
    to_date = fields.Datetime(string="To Date")
    inventory_date = fields.Datetime(string="Inventory Date")
    total_qty = fields.Float(string="Total Product Quantity", _compute='_compute_total_quantity')
    state = fields.Selection([
        ('draft', 'Draft'),
        ('in_progress', 'In Progress'),
        ('counted', 'Counted'),
    ], string='Status', default='draft', required=True, readonly=True, copy=False, tracking=True)
    inventory_counting_ids = fields.One2many('inventory.counting', 'inventory_counting_id', string='Inventory Countings')

    barcode_input = fields.Char(string="Scan Barcode", readonly=False)
    is_integrated = fields.Boolean(string="Integrated", default=False, readonly=True, tracking=True)

    # inventory_count = fields.Integer(string='Count', compute='_compute_stock_count')

    @api.model
    def default_get(self, fields_list):
        """Override default_get to automatically populate certain fields when creating a new record."""
        res = super(InventoryStock, self).default_get(fields_list)
        
        # Set default create_date and inventory_date to current datetime
        current_datetime = fields.Datetime.now()
        if 'create_date' in fields_list:
            res['create_date'] = current_datetime
        if 'inventory_date' in fields_list:
            res['inventory_date'] = current_datetime
        
        # Set default company_id to the user's current company
        if 'company_id' in fields_list:
            res['company_id'] = self.env.company.id
        
        # Set default warehouse - get the first warehouse associated with the current company
        if 'warehouse_id' in fields_list:
            warehouse = self.env['stock.warehouse'].search([
                ('company_id', '=', self.env.company.id)
            ], limit=1)
            if warehouse:
                res['warehouse_id'] = warehouse.id
                
                # Also trigger location setting if warehouse is set and location is in fields_list
                if 'location_id' in fields_list and warehouse.view_location_id:
                    # Find stock location under this warehouse's view_location
                    stock_location = self.env['stock.location'].search([
                        ('location_id', '=', warehouse.view_location_id.id),
                        ('name', '=', 'Stock')
                    ], limit=1)
                    if stock_location:
                        res['location_id'] = stock_location.id
        
        return res
    
    # models/inventory_stock.py
    # models/inventory_stock.py
    def open_barcode_scanner(self):
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'barcode.scanner.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_inventory_id': self.id},
        }


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

        # Cek apakah sudah ada line dengan produk yang sama
        existing_line = None
        for line in self.inventory_counting_ids:
            if line.product_id.id == product.id and line.location_id.id == self.location_id.id:
                existing_line = line
                break

        if existing_line:
            # Jika line sudah ada, akumulasi counted_qty dengan increment 1
            existing_line.counted_qty += 1.0
        else:
            # Jika belum ada line, buat baris counting baru
            self.inventory_counting_ids += self.env['inventory.counting'].new({
                'product_id': product.id,
                'location_id': self.location_id.id,
                'inventory_date': self.inventory_date,
                'state': 'in_progress',
                'uom_id': product.uom_id.id,
                'counted_qty': 1.0,  # Set initial counted_qty to 1
            })

        # Reset input
        self.barcode_input = ''

    @api.onchange('location_id')
    def _onchange_location_id(self):
        """Update location_id in all inventory counting lines when parent location changes"""
        if self.location_id:
            # Update existing inventory counting lines
            for line in self.inventory_counting_ids:
                line.location_id = self.location_id

    @api.model
    def create(self, vals):
        """Override create method to automatically generate doc_num using sequence."""
        sequence_code = 'inventory.stock.doc.num'
        doc_num_seq = self.env['ir.sequence'].next_by_code(sequence_code)

        # Ambil nilai inventory_date dari record yang akan dibuat
        inventory_date = vals.get('inventory_date') or fields.Datetime.now()  # Default ke sekarang jika tidak ada

        # Menggunakan timezone user untuk mengambil waktu sesuai zona waktu pengguna
        user_tz = timezone(self.env.user.tz or 'UTC')
        current_datetime = datetime.strptime(inventory_date, '%Y-%m-%d %H:%M:%S') if isinstance(inventory_date, str) else inventory_date
        current_datetime = current_datetime.astimezone(user_tz)

        # Format untuk string
        date_str = current_datetime.strftime("%Y%m%d")
        time_str = current_datetime.strftime("%H%M%S")

        INC = "INC"

        # Create `doc_num` dengan sequence-generated number
        vals['doc_num'] = f"{INC}/{date_str}/{time_str}/{doc_num_seq}"

        # Memanggil super untuk membuat record dan mengisi detail lainnya
        record = super(InventoryStock, self).create(vals)
        return record
    
    @api.onchange('warehouse_id')
    def _onchange_warehouse_id(self):
        """Isi otomatis location_id berdasarkan warehouse_id dan parent location."""
        if self.warehouse_id:
            # Ambil root lokasi dari warehouse
            root_location = self.warehouse_id.view_location_id
            
            if root_location:
                # Cari semua lokasi dengan parent = root_location.id
                child_locations = self.env['stock.location'].search([
                    ('location_id', '=', root_location.id),
                    ('name', '=', "Stock")
                ])
                self.location_id = child_locations
            else:
                self.location_id = False
        else:
            self.location_id = False


    # def action_validate(self):
    #     for record in self:
    #         for line in record.inventory_counting_ids:
    #             line.write({'state': 'counted'})
                
    #         # Update the state of the inventory.stock record
    #         record.state = 'counted'

    def action_in_progress(self):
        for record in self:
            record.state = 'in_progress'
            record.barcode_input = ''
            for line in record.inventory_counting_ids:
                line.write({'state': 'in_progress'})
        # Kembalikan aksi untuk membuka form dengan default_focus pada barcode_input
        return {
            'name': 'Inventory Counting',
            'type': 'ir.actions.act_window',
            'res_model': 'inventory.stock',
            'view_mode': 'form',
            'res_id': record.id,
            'context': {'default_focus': 1},  # Menambahkan default_focus ke konteks
        }

    # @api.depends('product_id')
    # def _compute_stock_count(self):
    #     for record in self:
    #         count = 0
    #         for product in record.product_id:
    #             product_variant = product.product_variant_id
    #             if not product_variant:
    #                 continue
    #             count += 1
    #         record.inventory_count = count

    # @api.onchange('category_id')
    # def _onchange_category_id(self):
    #     """Filter products based on the selected category."""
    #     if self.category_id:
    #         products = self.env['product.template'].search([('categ_id', '=', self.category_id.id)])
    #         self.product_id = [(6, 0, products.ids)]
    #     else:
    #         self.product_id = [(5, 0)] 

    def action_view_inventory_counting(self):
        """Open inventory.counting records related to the current inventory.stock record."""
        self.ensure_one()
        domain = [('inventory_stock_id', '=', self.id)]  # Filter berdasarkan inventory.stock

        return {
            'name': 'Inventory Counting',
            'type': 'ir.actions.act_window',
            'res_model': 'inventory.counting',
            'view_mode': 'tree',
            'domain': domain,
            'context': {'create': False},
        }

    def action_start_counting(self):
        """Update qty_hand for each inventory.counting line based on the balance_stock table."""
        for record in self:
            record.state = 'counted' 
            for line in record.inventory_counting_ids:
                line.state = 'counted'
                line.is_edit = False
                line.inventory_date = record.inventory_date
                product_variant_id = line.product_id.id
                inventory_datetime = record.inventory_date

                stock_akhir_real = 0.0

                if product_variant_id:
                    # Find the closest date_stock that is less than or equal to inventory_datetime
                    self.env.cr.execute("""
                        SELECT date_stock, stock_akhir
                        FROM balance_stock
                        WHERE product_id = %s
                        AND date_stock <= %s
                        ORDER BY date_stock DESC
                        LIMIT 1
                    """, (product_variant_id, inventory_datetime))
                    
                    result = self.env.cr.fetchone()
                    if result:
                        stock_akhir_real = result[1]

                line.qty_hand = stock_akhir_real

class InventoryCounting(models.Model):
    _name = "inventory.counting"
    _description = "Inventory Counting"

    inventory_counting_id = fields.Many2one('inventory.stock', string="Inventory Counting")
    inventory_stock_id = fields.Many2one('inventory.stock', string="Inventory Stock", ondelete='cascade')
    product_id = fields.Many2one('product.product', string="Product")
    location_id = fields.Many2one('stock.location', string="Location")
    inventory_date = fields.Datetime(string="Inventory Date")
    lot_id = fields.Many2one('stock.lot', string="Lot/Serial Number")
    expiration_date = fields.Datetime(string="Expiration Date")
    qty_hand = fields.Float(string="On Hand", store=True)
    counted_qty = fields.Float(string="Counted", store=True)
    difference_qty = fields.Float(string="Difference", compute='_compute_difference_qty', store=True)
    uom_id = fields.Many2one('uom.uom', string="UOM")
    state = fields.Selection([
        ('draft', 'Draft'),
        ('in_progress', 'In Progress'),
        ('counted', 'Counted'),
    ], string='Status', default='draft', required=True, readonly=True, copy=False, tracking=True)
    is_edit = fields.Boolean(string="Edit")

    @api.onchange('product_id')
    def _onchange_product_id(self):
        """
        When product_id is filled, automatically set location_id 
        from the parent inventory.stock record
        """
        if self.product_id and self.inventory_counting_id:
            if self.inventory_counting_id.location_id:
                self.location_id = self.inventory_counting_id.location_id
                self.uom_id = self.product_id.uom_id.id

    # @api.depends('product_id', 'location_id')
    # def _compute_qty_hand(self):
    #     """Compute qty_hand from stock.quant."""
    #     for record in self:
    #         if record.product_id and record.location_id:
    #             stock_quant = self.env['stock.quant'].search([
    #                 ('product_id', '=', record.product_id.id),
    #                 ('location_id', '=', record.location_id.id)
    #             ], limit=1)
    #             record.qty_hand = stock_quant.inventory_quantity_auto_apply or 0.0
    #         else:
    #             record.qty_hand = 0.0

    @api.depends('qty_hand', 'counted_qty')
    def _compute_difference_qty(self):
        for record in self:
            record.difference_qty = record.counted_qty - record.qty_hand

    # @api.onchange('product_id', 'location_id')
    # def _onchange_product_id(self):
    #     """Set default UOM and qty_hand based on stock.quant."""
    #     if self.product_id:
    #         self.uom_id = self.product_id.uom_id
    #         if self.location_id:
    #             stock_quant = self.env['stock.quant'].search([
    #                 ('product_id', '=', self.product_id.id),
    #                 ('location_id', '=', self.location_id.id)
    #             ], limit=1)
    #             self.qty_hand = stock_quant.inventory_quantity_auto_apply or 0.0
    #         else:
    #             self.qty_hand = 0.0
    #     else:
    #         self.uom_id = False
    #         self.qty_hand = 0.0

class BarcodeScannerWizard(models.TransientModel):
    _name = 'barcode.scanner.wizard'
    _description = 'Barcode Scanner Wizard'

    inventory_stock_id = fields.Many2one('inventory.stock', string="Inventory Record")
    barcode = fields.Char(string="Scanned Barcode")
    scanner_placeholder = fields.Char(string="Scanner Placeholder")  # dummy field

    def action_close(self):
        return {'type': 'ir.actions.act_window_close'}
