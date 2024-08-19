from datetime import datetime, timedelta
from odoo import models, fields, api
from collections import defaultdict
from odoo.exceptions import UserError, AccessError

class ReportStockAkhir(models.Model):
    _name = 'balance.stock'
    _description = 'Report Stock Akhir Real'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    numbering = fields.Char(string="Nomor", tracking=True)
    date_stock = fields.Datetime(string='Date', tracking=True)
    reference = fields.Char(string='Reference', tracking=True)
    product_id = fields.Many2one('product.product', string='Product', tracking=True)
    location_id = fields.Many2one('stock.location', string="From", tracking=True)
    location_dest_id = fields.Many2one('stock.location', string="To", tracking=True)
    stock_in = fields.Float(string="Stock In", tracking=True, digits=(16,3))
    stock_out = fields.Float(string="Stock Out", tracking=True, digits=(16,3))
    stock_akhir = fields.Float(string="Stock Akhir", tracking=True, digits=(16,3))
    
    def get_report_stock_akhir(self):
        """ Mengambil data dari stock.move.line dan mengisi balance.stock dengan benar """

        self.env['balance.stock'].sudo().search([]).unlink()
        stock_moves = self.env['stock.move.line'].sudo().search([('state', '=', 'done')], order="product_id, date")

        stock_data = defaultdict(lambda: {
            'stock_in': 0, 'stock_out': 0, 'location_id': None, 'location_dest_id': None, 'reference': ''
        })

        for move in stock_moves:
            product = move.product_id
            move_date = move.date
            reference = move.reference or move.picking_id.name or "-"

            stock_in, stock_out = 0, 0
            
            # Jika barang keluar (dari internal ke customer, supplier, inventory, dll.)
            if move.location_dest_id.usage in ['customer', 'supplier', 'inventory', 'view', 'production', 'transit']:
                stock_out = move.quantity  # Warna Merah (Keluar)
                
            # Jika barang masuk (dari supplier, inventory, dll. ke internal)
            elif move.location_id.usage in ['customer', 'supplier', 'inventory', 'view', 'production', 'transit']:
                stock_in = move.quantity  # Warna Hijau (Masuk)

            key = (product.id, move_date)
            stock_data[key]['stock_in'] += stock_in
            stock_data[key]['stock_out'] += stock_out
            stock_data[key]['location_id'] = move.location_id.id
            stock_data[key]['location_dest_id'] = move.location_dest_id.id
            stock_data[key]['reference'] = reference

        result = []
        last_stock_per_product = defaultdict(float)
        numbering_per_product = defaultdict(int)

        for (product_id, date_stock), data in sorted(stock_data.items()):
            last_stock = last_stock_per_product[product_id]
            stock_akhir = last_stock + data['stock_in'] - data['stock_out']
            last_stock_per_product[product_id] = stock_akhir

            numbering_per_product[product_id] += 1

            result.append({
                'numbering': numbering_per_product[product_id],
                'date_stock': date_stock,
                'reference': data['reference'],
                'product_id': product_id,
                'stock_in': data['stock_in'],
                'stock_out': data['stock_out'],
                'stock_akhir': stock_akhir,
                'location_id': data['location_id'],
                'location_dest_id': data['location_dest_id']
            })

        for row in result:
            self.env['balance.stock'].create(row)

    @api.model
    def _scheduler_generate_stock_report(self):
        """ Scheduler untuk menjalankan get_report_stock_akhir secara otomatis """

        self.env['balance.stock'].sudo().search([]).unlink()
        self.get_report_stock_akhir()
