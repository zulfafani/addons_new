from odoo import http
from odoo.http import request

class PosController(http.Controller):

    @http.route('/pos/log_cashier', type='json', auth="user")
    def log_cashier(self, employee_id, session_id):
        CashierLog = request.env['pos.cashier.log']
        
        # Check if there's an open log for this cashier in any session
        existing_open_log = CashierLog.search([
            ('employee_id', '=', employee_id),
            ('state', '=', 'opened'),
        ], limit=1)

        if existing_open_log:
            return {
                'success': False,
                'error': 'cashier_in_use',
            }

        # Check if there's already a log for this session and cashier
        existing_log = CashierLog.search([
            ('employee_id', '=', employee_id),
            ('session_id', '=', session_id),
            ('state', '=', 'opened'),
        ], limit=1)

        if existing_log:
            return {
                'success': True,
                'log_id': existing_log.id,
            }

        # Create a new log
        new_log = CashierLog.create({
            'employee_id': employee_id,
            'session_id': session_id,
            'state': 'opened',
        })

        return {
            'success': True,
            'log_id': new_log.id,
        }