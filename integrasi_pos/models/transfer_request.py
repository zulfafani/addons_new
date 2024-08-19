# -*- coding: utf-8 -*-
from odoo import fields, models, api
from datetime import datetime
from pytz import timezone

class TransferRequest(models.Model):
    _name = 'transfer.request'
    _rec_name = 'doc_num'
    _description = 'Transfer Request'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    doc_num = fields.Char(string="Nomor Dokumen", tracking=True)
    partner_id = fields.Many2one('res.partner', string='Requested By', required=True)
    location_id = fields.Many2one('stock.location', string='Delivery Stock From', required=True)
    location_dest_id = fields.Many2one('master.warehouse', string='Stock Needed On Location', required=True)
    picking_type_id = fields.Many2one('stock.picking.type', string='Picking Type', required=True)
    request_date = fields.Date(string='Request Date')
    goods_needed = fields.Datetime(string='Goods Needed On', required=True)
    company_id = fields.Many2one('res.company', string='Company', tracking=True)
    state = fields.Selection([
        ('pending', 'Transfer Pending'),
        ('completed', 'Completed'),
    ], string='Status', default='pending', required=True, readonly=True, copy=False, tracking=True)
    transfer_request_ids = fields.One2many('transfer.request.line', 'transfer_request_id', string='Transfer Request Lines')
    ts_out_count = fields.Integer(string='TS Out', compute='_compute_ts_out_count')

    def cancel_request(self):
        self.write({'state': 'pending'})
        self.transfer_request_ids.write({'state': 'pending'})

    def validate_request(self):
        """Membuat dokumen TS Out dan menampilkan dokumen setelah dibuat."""
        stock_picking = self.env['stock.picking']
        stock_move = self.env['stock.move']

        # Cek jika tidak ada barang dalam request
        if not self.transfer_request_ids:
            raise ValueError("Tidak ada barang yang diminta dalam transfer request ini.")

        # Membuat dokumen TS Out (Stock Picking)
        picking_vals = {
            'partner_id': self.partner_id.id,
            'location_id': self.location_id.id,
            'target_location': self.location_dest_id.id,
            'picking_type_id': self.picking_type_id.id,
            'scheduled_date': self.goods_needed,
            'origin': self.doc_num,
            'move_ids_without_package': [],
        }
        picking = stock_picking.create(picking_vals)

        # Membuat Stock Move untuk setiap item dalam transfer request
        for line in self.transfer_request_ids:
            move_vals = {
                'name': line.product_id.name,
                'product_id': line.product_id.id,
                'product_uom_qty': line.quantity,
                'quantity': line.quantity,
                'location_id': self.location_id.id,
                'location_dest_id': self.location_dest_id.id,
                'picking_id': picking.id,
            }
            stock_move.create(move_vals)

        # Ubah status menjadi Completed setelah dokumen TS Out dibuat
        self.write({'state': 'completed'})
        self.transfer_request_ids.write({'state': 'completed'})

        # Tampilkan dokumen TS Out yang baru dibuat
        return {
            'name': 'TS Out',
            'type': 'ir.actions.act_window',
            'res_model': 'stock.picking',
            'view_mode': 'form',
            'res_id': picking.id,
            'context': {'create': False}
        }

    def action_view_ts_out(self):
        """Menampilkan daftar TS Out yang terkait"""
        self.ensure_one()
        
        scheduled_date = fields.Datetime.to_string(self.request_date)
        domain = [
            ('partner_id', '=', self.partner_id.id),
            ('state', 'in', ['confirmed', 'waiting', 'assigned', 'done']),
            ('picking_type_id.name', '=', 'TS Out'),
            ('origin', '=', self.doc_num),
        ]

        return {
            'name': 'TS Out',
            'type': 'ir.actions.act_window',
            'res_model': 'stock.picking',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'create': False}
        }

    @api.depends('request_date', 'partner_id')
    def _compute_ts_out_count(self):
        """Menghitung jumlah TS Out terkait dengan transfer request ini"""
        for record in self:
            count = 0
            if record.request_date and record.partner_id:
                scheduled_date = fields.Datetime.to_string(record.goods_needed)
                count = self.env['stock.picking'].search_count([
                    ('partner_id', '=', record.partner_id.id),
                    ('state', 'in', ['confirmed', 'waiting', 'assigned', 'done']),
                    ('picking_type_id.name', '=', 'TS Out'),
                    ('origin', '=', record.doc_num),])
            record.ts_out_count = count

    @api.model
    def create(self, vals):
        """Override create method to automatically generate doc_num using sequence."""
        sequence_code = 'transfer.request.doc.num'
        doc_num_seq = self.env['ir.sequence'].next_by_code(sequence_code)

        # Ambil nilai inventory_date dari record yang akan dibuat
        inventory_date = vals.get('goods_needed') or fields.Datetime.now()  # Default ke sekarang jika tidak ada

        # Menggunakan timezone user untuk mengambil waktu sesuai zona waktu pengguna
        user_tz = timezone(self.env.user.tz or 'UTC')
        current_datetime = datetime.strptime(inventory_date, '%Y-%m-%d %H:%M:%S') if isinstance(inventory_date, str) else inventory_date
        current_datetime = current_datetime.astimezone(user_tz)

        # Format untuk string
        date_str = current_datetime.strftime("%Y%m%d")
        time_str = current_datetime.strftime("%H%M%S")

        TRQ = "TRQ"

        # Create `doc_num` dengan sequence-generated number
        vals['doc_num'] = f"{TRQ}/{date_str}/{time_str}/{doc_num_seq}"

        # Memanggil super untuk membuat record dan mengisi detail lainnya
        record = super(TransferRequest, self).create(vals)
        return record

class TransferRequestLine(models.Model):
    _name = 'transfer.request.line'
    _description = 'Transfer Request Line'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    
    transfer_request_id = fields.Many2one('transfer.request', string='Transfer Request', required=True, ondelete='cascade')
    product_id = fields.Many2one('product.product', string='Product', required=True)
    description = fields.Char(string='Description', required=True, tracking=True)
    quantity = fields.Float(string='Quantity', required=True)
    uom_id = fields.Many2one('uom.uom', string='Unit of Measure', required=True)
    state = fields.Selection([
        ('pending', 'Transfer Pending'),
        ('completed', 'Completed'),
    ], string='Status', default='pending', required=True, readonly=True, copy=False, tracking=True)

    @api.onchange('product_id')
    def _onchange_product_id(self):
        """Mengisi description dan uom_id berdasarkan product_id yang dipilih."""
        if self.product_id:
            self.description = self.product_id.name 
            self.uom_id = self.product_id.uom_id.id  
