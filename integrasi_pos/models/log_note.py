from odoo import models, fields


class LogNote(models.Model):
    _name = "log.note"
    _description = "Log Note"

    vit_doc_type = fields.Char(string='Document Type')
    vit_trx_key = fields.Char(string='Transaction Key')
    vit_trx_date = fields.Datetime(string='Transaction Date')
    vit_sync_date = fields.Datetime(string='Sync Date')
    vit_sync_status = fields.Char(string='Sync Status')
    vit_sync_desc = fields.Text(string='Sync Description')
