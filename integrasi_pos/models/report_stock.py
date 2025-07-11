from datetime import datetime, timedelta
from odoo import models, fields, api
from collections import defaultdict
from odoo.exceptions import UserError, AccessError

class WizardGenerateStockLedger(models.TransientModel):
    _name = 'wizard.generate.stock.ledger'
    _description = 'Wizard to Generate Stock Ledger Report'

    @api.model
    def default_get(self, fields):
        res = super().default_get(fields)
        # Jalankan langsung jika ingin otomatis saat wizard dibuka
        # self.env['balance.stock'].get_report_stock_akhir()
        return res

    def action_generate(self):
        self.env['balance.stock'].get_report_stock_akhir()
        return {
            'type': 'ir.actions.act_window',
            'name': 'Stock Ledger',
            'res_model': 'balance.stock',
            'view_mode': 'tree',
            'target': 'current',
            'search_view_id': self.env.ref('integrasi_pos.view_balance_stock_search').id,
            'context': {},
        }

class ReportStockAkhir(models.Model):
    _name = 'balance.stock'
    _description = 'Report Stock Akhir Real'

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
        cr = self.env.cr

        # Hapus data lama
        cr.execute("TRUNCATE balance_stock RESTART IDENTITY CASCADE;")

        # Query aggregate langsung di PostgreSQL
        cr.execute("""
            SELECT
                sml.product_id,
                DATE(sml.date) AS date_stock,
                COALESCE(SUM(
                    CASE WHEN dest.usage IN ('customer','supplier','inventory','view','production','transit')
                        THEN sml.quantity ELSE 0 END
                ), 0) AS stock_out,
                COALESCE(SUM(
                    CASE WHEN src.usage IN ('customer','supplier','inventory','view','production','transit')
                        THEN sml.quantity ELSE 0 END
                ), 0) AS stock_in,
                MAX(sml.reference) AS reference,
                MAX(sml.location_id) AS location_id,
                MAX(sml.location_dest_id) AS location_dest_id
            FROM stock_move_line sml
            JOIN stock_location src ON src.id = sml.location_id
            JOIN stock_location dest ON dest.id = sml.location_dest_id
            WHERE sml.state = 'done'
            GROUP BY sml.product_id, DATE(sml.date)
            ORDER BY sml.product_id, date_stock;
        """)

        rows = cr.fetchall()

        # Hitung cumulative stock & bulk insert
        last_stock_per_product = {}
        numbering_per_product = {}
        bulk_data = []

        for product_id, date_stock, stock_out, stock_in, reference, location_id, location_dest_id in rows:
            last_stock = last_stock_per_product.get(product_id, 0.0)
            stock_akhir = last_stock + stock_in - stock_out
            last_stock_per_product[product_id] = stock_akhir

            numbering_per_product[product_id] = numbering_per_product.get(product_id, 0) + 1

            bulk_data.append({
                'numbering': numbering_per_product[product_id],
                'date_stock': date_stock,
                'reference': reference,
                'product_id': product_id,
                'stock_in': stock_in,
                'stock_out': stock_out,
                'stock_akhir': stock_akhir,
                'location_id': location_id,
                'location_dest_id': location_dest_id
            })

        if bulk_data:
            self.env['balance.stock'].create(bulk_data)


    @api.model
    def _scheduler_generate_stock_report(self):
        self.get_report_stock_akhir()
