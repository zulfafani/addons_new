from odoo import models, fields, api, _
import logging

_logger = logging.getLogger(__name__)

class LogNote(models.Model):
    _name = "log.note"
    _description = "Log Note"

    vit_doc_type = fields.Char(string='Document Type')
    vit_trx_key = fields.Char(string='Transaction Key')
    vit_trx_date = fields.Datetime(string='Transaction Date')
    vit_sync_date = fields.Datetime(string='Sync Date')
    vit_sync_status = fields.Char(string='Sync Status')
    vit_sync_desc = fields.Text(string='Sync Description')
    vit_start_sync = fields.Datetime(string='Start Sync')
    vit_end_sync = fields.Datetime(string='End Sync')
    vit_duration = fields.Char(string='Duration')
    
    @api.autovacuum
    def _gc_old_log_notes(self):
        _logger.info("Running autovacuum for log.note...")

        # Ambil tanggal 2 bulan lalu
        try:
            limit_date = fields.Datetime.subtract(fields.Datetime.now(), months=2)
            domain = [('create_date', '<', fields.Datetime.to_string(limit_date))]
            records = self.sudo().search(domain)
            _logger.info(f"Found {len(records)} old log.note records to delete.")
            records.unlink()
        except Exception as e:
            _logger.error(f"Error during autovacuum for log.note: {e}")