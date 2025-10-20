from odoo import models, fields, api
from pytz import timezone
from datetime import datetime, timedelta
from odoo.exceptions import ValidationError

class InventoryAdjustment(models.Model):
    _inherit = 'stock.quant'

    doc_num = fields.Many2one(
        'inventory.stock', 
        string="Inventory Counting",
        domain="[('state', '=', 'closed')]"  # ✅ Filter hanya yang closed
    )

    @api.onchange('doc_num')
    def _onchange_doc_num(self):
        """Auto-fill inventory_quantity ketika doc_num dipilih"""
        if self.doc_num and self.product_id:
            # ✅ Validasi tambahan: pastikan status closed
            if self.doc_num.state != 'closed':
                raise ValidationError("Hanya Inventory Counting dengan status 'Closed' yang bisa dipilih.")
            
            # Cari line yang match
            inventory_counting_line = self.doc_num.inventory_counting_ids.filtered(
                lambda line: line.product_id.id == self.product_id.id 
                and line.location_id.id == self.location_id.id
                and (line.lot_id.id == self.lot_id.id if self.lot_id else not line.lot_id)
            )
            
            if inventory_counting_line:
                line = inventory_counting_line[0]
                self.inventory_quantity = line.difference_qty
                self.inventory_quantity_set = True

    def action_apply_inventory(self):
        """Override untuk mengisi inventory_quantity dari doc_num"""
        for quant in self:
            if quant.doc_num:
                # ✅ Validasi status closed
                if quant.doc_num.state != 'closed':
                    raise ValidationError(
                        f"Inventory Counting '{quant.doc_num.doc_num}' harus berstatus 'Closed' "
                        "sebelum dapat diaplikasikan."
                    )
                
                inventory_stock = quant.doc_num
                
                inventory_counting_line = inventory_stock.inventory_counting_ids.filtered(
                    lambda line: line.product_id.id == quant.product_id.id 
                    and line.location_id.id == quant.location_id.id
                    and (line.lot_id.id == quant.lot_id.id if quant.lot_id else not line.lot_id)
                )
                
                if inventory_counting_line:
                    line = inventory_counting_line[0]
                    quant.inventory_quantity = line.difference_qty
                    quant.inventory_quantity_set = True
        
        return super(InventoryAdjustment, self).action_apply_inventory()

class InventoryStock(models.Model):
    _name = "inventory.stock"
    _description = "Inventory Stock"
    _rec_name = 'doc_num'

    doc_num = fields.Char(string="Internal Reference", readonly=True)
    warehouse_id = fields.Many2one('stock.warehouse', string="Warehouse")
    location_id = fields.Many2one('stock.location', string="Location")
    company_id = fields.Many2one('res.company', string="Company")
    create_date = fields.Datetime(string="Created Date", readonly=True)
    from_date = fields.Datetime(string="From Date")
    to_date = fields.Datetime(string="To Date")
    inventory_date = fields.Datetime(string="Inventory Date")
    total_qty = fields.Float(string="Total Product Quantity", _compute='_compute_total_quantity')
    state = fields.Selection([
        ('draft', 'Draft'),
        ('in_progress', 'In Progress'),
        ('counted', 'Counted'),
        ('closed', 'Closed'),
    ], string='Status', default='draft', required=True, readonly=True, copy=False, tracking=True)
    inventory_counting_ids = fields.One2many('inventory.counting', 'inventory_counting_id', string='Inventory Countings', order='sequence desc, id desc')

    barcode_input = fields.Char(string="Scan Barcode", readonly=False)
    is_integrated = fields.Boolean(string="Integrated", default=False, readonly=True, tracking=True)
    vit_notes = fields.Text(string="Keterangan", readonly=False, tracking=True)

    def action_apply_to_stock_quant(self):
        """
        Method untuk mengaplikasikan inventory counting ke stock.quant
        """
        self.ensure_one()
        
        if self.state != 'counted':
            raise ValidationError("Inventory harus dalam status 'Counted' sebelum diaplikasikan.")
        
        StockQuant = self.env['stock.quant']
        applied_count = 0
        
        for line in self.inventory_counting_ids:
            # Cari stock.quant yang sesuai
            quant = StockQuant.search([
                ('product_id', '=', line.product_id.id),
                ('location_id', '=', line.location_id.id),
                ('lot_id', '=', line.lot_id.id if line.lot_id else False),
                ('package_id', '=', False),
                ('owner_id', '=', False),
            ], limit=1)
            
            if quant:
                # Update quant yang sudah ada
                quant.write({
                    'doc_num': line.inventory_stock_id.id,
                    'inventory_quantity': line.counted_qty,
                    'inventory_quantity_set': True,
                    'inventory_date': line.inventory_date or fields.Date.today(),
                    'user_id': self.env.user.id,
                })
                applied_count += 1
            else:
                # Buat quant baru dalam inventory mode
                StockQuant.with_context(inventory_mode=True).create({
                    'product_id': line.product_id.id,
                    'location_id': line.location_id.id,
                    'lot_id': line.lot_id.id if line.lot_id else False,
                    'doc_num': line.inventory_stock_id.id,
                    'inventory_quantity': line.counted_qty,
                    'inventory_quantity_set': True,
                    'inventory_date': line.inventory_date or fields.Date.today(),
                    'user_id': self.env.user.id,
                })
                applied_count += 1
        
        # Update status
        self.is_integrated = True
        
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Berhasil',
                'message': f'Inventory counting berhasil diaplikasikan ke {applied_count} stock quant(s).',
                'type': 'success',
                'sticky': False,
            }
        }

    def _get_next_sequence(self):
        """Get the next highest sequence number"""
        if self.inventory_counting_ids:
            max_seq = max(self.inventory_counting_ids.mapped('sequence') or [0])
            return max_seq + 1
        return 1
    
    def _reorder_lines(self):
        """Reorder all lines to maintain newest-first order"""
        lines = list(self.inventory_counting_ids)
        lines.reverse()  # Balik urutan untuk yang terbaru di atas
        for idx, line in enumerate(lines):
            line.sequence = idx + 1

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
    
    @api.model
    def process_barcode_from_wizard(self, inventory_id, barcode, quantity=1.0):
        """Process barcode with quantity input from wizard"""
        inventory = self.browse(inventory_id)
        if not inventory.exists():
            return {'status': 'error', 'message': 'Inventory record not found'}

        try:
            # Get config
            barcode_config = self.env['barcode.config'].search([], limit=1)
            if not barcode_config:
                return {'status': 'error', 'message': 'Barcode config belum disetting.'}

            # Default: pakai barcode penuh
            search_barcode = barcode

            # Cari produk berdasarkan barcode penuh terlebih dahulu
            product = self.env['product.product'].search([
                ('barcode', '=', search_barcode)
            ], limit=1)

            # Jika tidak ditemukan produk dan ada konfigurasi panjang_barcode,
            # cek apakah ini produk dengan to_weight=True
            if not product and barcode_config.panjang_barcode:
                # Potong barcode satu karakter lebih sedikit dari yang dikonfigurasi (panjang_barcode - 1)
                search_barcode = barcode[:barcode_config.panjang_barcode - 1]
                
                # Cari produk dengan barcode yang sudah dipotong
                product = self.env['product.product'].search([
                    ('barcode', '=', search_barcode),
                    ('to_weight', '=', True)  # Hanya untuk produk to_weight=True
                ], limit=1)

            if not product:
                return {'status': 'error', 'message': f"❌ Produk dengan barcode '{search_barcode}' tidak ditemukan."}

            # Cari line dengan produk ini
            existing_line = inventory.inventory_counting_ids.filtered(
                lambda l: l.product_id.id == product.id and l.location_id.id == inventory.location_id.id
            )

            if existing_line:
                # Jika line sudah ada, akumulasi counted_qty dengan quantity yang diinput
                # DAN update sequence agar muncul di atas
                existing_line.counted_qty += quantity
                existing_line.sequence = inventory._get_next_sequence()
                total_qty = existing_line.counted_qty
            else:
                # Jika belum ada line, buat baris counting baru dengan sequence tertinggi
                new_line = self.env['inventory.counting'].create({
                    'inventory_counting_id': inventory.id,
                    'inventory_stock_id': inventory.id,
                    'product_id': product.id,
                    'location_id': inventory.location_id.id,
                    'inventory_date': inventory.inventory_date,
                    'state': 'in_progress',
                    'uom_id': product.uom_id.id,
                    'counted_qty': quantity,
                    'sequence': inventory._get_next_sequence(),
                })
                total_qty = quantity

            return {
                'status': 'success',
                'message': f"✅ Barcode {barcode} berhasil.\nProduk: {product.name}\nQty ditambahkan: {quantity}\nTotal Counted: {total_qty}",
                'product_name': product.name,
                'added_qty': quantity,
                'total_qty': total_qty
            }

        except ValidationError as ve:
            return {'status': 'error', 'message': f"❌ {ve.name or str(ve)}"}
        except Exception as e:
            return {'status': 'error', 'message': f"❌ Terjadi error: {str(e)}"}

    def open_barcode_scanner(self):
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'barcode.scanner.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_inventory_stock_id': self.id},
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
        existing_line = self.inventory_counting_ids.filtered(
            lambda l: l.product_id.id == product.id and l.location_id.id == self.location_id.id
        )

        # Dapatkan sequence tertinggi untuk line baru
        next_seq = self._get_next_sequence()

        if existing_line:
            # Jika line sudah ada, perbarui qty dan sequence
            # Gunakan command (1, id, values) untuk update
            new_commands = []
            
            for line in self.inventory_counting_ids:
                if line.id == existing_line.id or (not line.id and line == existing_line):
                    # Update existing line dengan qty baru dan sequence tertinggi
                    if line.id:
                        new_commands.append((1, line.id, {
                            'counted_qty': line.counted_qty + 1.0,
                            'sequence': next_seq,
                        }))
                    else:
                        # Untuk new record (belum disave)
                        line.counted_qty += 1.0
                        line.sequence = next_seq
                        new_commands.append((4, line.id, 0))
                else:
                    # Keep other lines
                    if line.id:
                        new_commands.append((4, line.id, 0))
                    else:
                        new_commands.append((4, line.id, 0))
            
            if new_commands:
                self.inventory_counting_ids = new_commands
        else:
            # Jika belum ada line, buat baris baru di posisi paling atas
            # Gunakan command (0, 0, values) untuk create
            new_commands = [(0, 0, {
                'product_id': product.id,
                'location_id': self.location_id.id,
                'inventory_date': self.inventory_date,
                'state': 'in_progress',
                'uom_id': product.uom_id.id,
                'counted_qty': 1.0,
                'sequence': next_seq,
            })]
            
            # Tambahkan semua existing lines dengan command (4, id)
            for line in self.inventory_counting_ids:
                if line.id:
                    new_commands.append((4, line.id, 0))
            
            self.inventory_counting_ids = new_commands

        # Reset input
        self.barcode_input = ''

    @api.onchange('location_id')
    def _onchange_location_id(self):
        """Update location_id in all inventory counting lines when parent location changes"""
        if self.location_id:
            # Update existing inventory counting lines
            for line in self.inventory_counting_ids:
                line.location_id = self.location_id

    def write(self, vals):
        """Override write untuk update create_date saat ada perubahan data (kecuali status draft)"""
        for record in self:
            # Update create_date jika ada perubahan dan bukan draft
            if record.state != 'draft' and vals:
                # Pastikan tidak mengupdate create_date jika hanya state yang berubah
                if 'state' not in vals or len(vals) > 1:
                    vals['create_date'] = fields.Datetime.now()
        
        return super(InventoryStock, self).write(vals)

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
        
        # Set create_date
        vals['create_date'] = fields.Datetime.now()

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
        """ Jalankan laporan stock akhir sebelum mulai counting """
        # 🚀 Generate balance.stock dulu
        self.env['balance.stock'].get_report_stock_akhir()

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
                    self.env.cr.execute("""
                        SELECT stock_akhir
                        FROM balance_stock
                        WHERE product_id = %s
                        AND date_stock <= %s
                        ORDER BY date_stock DESC
                        LIMIT 1
                    """, (product_variant_id, inventory_datetime))
                    result = self.env.cr.fetchone()
                    if result:
                        stock_akhir_real = result[0]

                line.qty_hand = stock_akhir_real
        return True

    def action_closed(self):
        """Ubah status menjadi closed, data tidak bisa diubah lagi"""
        for record in self:
            if record.state != 'counted':
                raise ValidationError("Hanya inventory dengan status 'Counted' yang bisa di-closed.")
            
            record.state = 'closed'
            for line in record.inventory_counting_ids:
                line.state = 'closed'
        return True


class InventoryCounting(models.Model):
    _name = "inventory.counting"
    _description = "Inventory Counting"
    _order = 'sequence desc, id desc'

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
        ('closed', 'Closed'),
    ], string='Status', default='draft', required=True, readonly=True, copy=False, tracking=True)
    is_edit = fields.Boolean(string="Edit")
    sequence = fields.Integer(string="Sequence", default=0)

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

    @api.depends('qty_hand', 'counted_qty')
    def _compute_difference_qty(self):
        for record in self:
            record.difference_qty = record.counted_qty - record.qty_hand


class BarcodeScannerWizard(models.TransientModel):
    _name = 'barcode.scanner.wizard'
    _description = 'Barcode Scanner Wizard'

    inventory_stock_id = fields.Many2one('inventory.stock', string="Inventory Record")
    barcode = fields.Char(string="Scanned Barcode")
    quantity = fields.Float(string="Quantity", default=1.0)
    scanner_placeholder = fields.Char(string="Scanner Placeholder")  # dummy field

    def action_close(self):
        return {'type': 'ir.actions.act_window_close'}