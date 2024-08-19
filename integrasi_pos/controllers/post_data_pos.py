from odoo import http, fields
from odoo.http import request

class PosController(http.Controller):

    @http.route('/pos/log_cashier', type='json', auth="user")
    def log_cashier(self, employee_id, session_id):
        CashierLog = request.env['pos.cashier.log']
        EndShift = request.env['end.shift']
        
        # Check if there's an end.shift for this cashier in this session
        existing_shift = EndShift.search([
            ('cashier_id', '=', employee_id),
            ('session_id', '=', session_id),
            ('state', 'in', ('in_progress', 'closed'))
        ], limit=1)

        if existing_shift:
            return {
                'success': False,
                'error': 'cashier_shift_closed'
            }

        # Check for existing cashier log
        existing_log = CashierLog.search([
            ('employee_id', '=', employee_id),
            ('session_id', '=', session_id),
            ('state', '=', 'opened')
        ], limit=1)

        log_id = None
        if existing_log:
            log_id = existing_log.id
        else:
            # Create a new cashier log only if it doesn't exist
            new_log = CashierLog.create({
                'employee_id': employee_id,
                'session_id': session_id,
                'state': 'opened',
            })
            log_id = new_log.id

        end_shift_created = False
        end_shift_id = None

        # Create new end.shift record only if it doesn't exist
        if not existing_shift:
            new_end_shift = EndShift.create({
                'cashier_id': employee_id,
                'session_id': session_id,
                'start_date': fields.Datetime.now(),
                'state': 'opened',
            })
            new_end_shift.action_start_progress()
            end_shift_created = True
            end_shift_id = new_end_shift.id
        else:
            end_shift_id = existing_shift.id

        return {
            'success': True,
            'log_id': log_id,
            'end_shift_created': end_shift_created,
            'end_shift_id': end_shift_id,
            'is_new_log': not existing_log,
        }