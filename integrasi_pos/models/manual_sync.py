from odoo import models, fields, _, api
from odoo.exceptions import UserError

class ManualSyncIntegratiion(models.Model):
    _name = 'manual.sync'
    _description = 'Manual Sync For Integration'

    treshold = fields.Char(string='Treshold')
    by_doc = fields.Char(string='By Doc/Item')
    date_from = fields.Date(string='Date from')
    date_to = fields.Date(string='Date To')
    sync_model = fields.Selection([('pos invoice', 'Invoice')], string='Modules')
    
    special_sync_ids = fields.One2many('manual.sync.line', 'special_sync_id', string='Special Sync Ids', readonly=True)

    def action_start(self):
        pass

    def action_stop(self):
        pass
class ManualSyncIntegrationLine(models.Model):
    _name = 'manual.sync.line'
    _description = 'Manual Sync For Integration'

    special_sync_id = fields.Many2one('special.sync', string='Special Sync Id')
    no_inc = fields.Integer(string='No')
    doc_num = fields.Char(string='Document Number')
    sync_date = fields.Date(string='Created Date')
    sync_status = fields.Char(string='Status')
    sync_desc = fields.Char(string='Status Description')