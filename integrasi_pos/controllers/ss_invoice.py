from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError


class IntegrationInvoice(models.Model):
    _inherit = 'account.move'

    BATCH_SIZE = 100

    def invoice_integration(self):
        data_invoice = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                                 self.target_client.password, 'account.move' , 'search_read', [[]],
                                                 {'fields': fields})