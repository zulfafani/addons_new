from odoo import models, fields, api
from datetime import datetime, timedelta
from pytz import timezone
from odoo.exceptions import ValidationError

class PosCashierLog(models.Model):
    _name = 'pos.cashier.log'
    _description = 'POS Cashier Log'

    employee_id = fields.Many2one('hr.employee', string='Cashier', required=True)
    timestamp = fields.Datetime(string='Timestamp', required=True, default=lambda self: self._default_jakarta_time())
    session_id = fields.Many2one('pos.session', string='POS Session', required=True)
    state = fields.Selection([
        ('opened', 'Opened'),
        ('closed', 'Closed')
    ], string='Status', default='opened', required=True)

    # def unlink(self):
    #     for record in self:
    #         if record.employee_id.is_cashier:
    #             raise ValidationError("Tidak bisa menghapus kasir aktif. Silakan arsipkan saja.")
    #     return super(PosCashierLog, self).unlink()

    @api.model
    def _default_jakarta_time(self):
        jakarta_tz = timezone('Asia/Jakarta')
        return datetime.now(jakarta_tz).replace(tzinfo=None)

    def action_close(self):
        for record in self:
            record.state = 'closed'

    @api.model
    def create(self, vals):
        # No need to convert timestamp, as it will use server's local time by default
        return super(PosCashierLog, self).create(vals)

    def write(self, vals):
        # No need to convert timestamp, as it will use server's local time by default
        return super(PosCashierLog, self).write(vals)