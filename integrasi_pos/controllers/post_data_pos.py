from odoo import http, fields
from odoo.http import request
import subprocess

class PosController(http.Controller):
    @http.route('/pos/log_cashier', type='json', auth="user")
    def log_cashier(self, employee_id, session_id):
        CashierLog = request.env['pos.cashier.log']
        EndShift = request.env['end.shift']

        # Prevent login if shift already closed
        closed_shift = EndShift.search([
            ('cashier_id', '=', employee_id),
            ('session_id', '=', session_id),
            ('state', '=', 'closed')
        ])

        if closed_shift:
            return {
                'success': False,
                'error': 'cashier_shift_closed',
                'message': 'Tidak dapat login. Shift untuk kasir ini sudah ditutup pada sesi ini.'
            }

        # Check if there's already a log
        existing_log = CashierLog.search([
            ('employee_id', '=', employee_id),
            ('session_id', '=', session_id),
            ('state', '=', 'opened')
        ])
        
        log_id = existing_log.id if existing_log else None
        if not existing_log:
            new_log = CashierLog.create({
                'employee_id': employee_id,
                'session_id': session_id,
                'state': 'opened',
            })
            log_id = new_log.id

        # ✅ Check if any EndShift for this cashier is already 'opened' or 'in_progress'
        existing_shift = EndShift.search([
            ('cashier_id', '=', employee_id),
            ('session_id', '=', session_id),
            ('state', 'in', ['opened', 'in_progress']),
        ], limit=1)

        end_shift_created = False
        end_shift_id = existing_shift.id if existing_shift else None

        if not existing_shift:
            new_end_shift = EndShift.create({
                'cashier_id': employee_id,
                'session_id': session_id,
                'start_date': fields.Datetime.now(),  # Only if new
                'state': 'opened',
            })
            new_end_shift.action_start_progress()
            end_shift_created = True
            end_shift_id = new_end_shift.id

        return {
            'success': True,
            'log_id': log_id,
            'end_shift_created': end_shift_created,
            'end_shift_id': end_shift_id,
            'is_new_log': not existing_log,
        }

class InventoryFocusController(http.Controller):

    @http.route('/inventory/trigger_focus', type='json', auth='user')
    def trigger_focus(self, **kw):
        """
        Called after action_in_progress to notify frontend to focus barcode_input.
        """
        record_id = kw.get('record_id')
        return {'focus_barcode': True, 'record_id': record_id}
    
class LoyaltyScheduleController(http.Controller):

    @http.route('/pos/loyalty/schedules', type='json', auth='user')
    def get_loyalty_program_schedules(self):
        schedules = request.env['loyalty.program.schedule'].sudo().search_read(
            [], ['program_id', 'days', 'time_start', 'time_end']
        )
        return schedules
    
class POSVirtualKeyboard(http.Controller):

    @http.route('/pos/open_virtual_keyboard', type='json', auth='user')
    def open_virtual_keyboard(self):
        try:
            subprocess.Popen("osk")
            return {"status": "ok"}
        except Exception as e:
            return {"error": str(e)}

# class MultipleBarcodeController(http.Controller):
#     # In your custom POS controller
#     @http.route('/pos/resolve_barcode', type='json', auth='user')
#     def resolve_barcode(self, barcode):
#         entry = request.env['multiple.barcode'].sudo().search([('barcode', '=', barcode)], limit=1)
#         if entry:
#             return {
#                 'product_id': entry.product_id.id,
#                 'product_barcode': entry.barcode,
#                 'to_weight': entry.to_weight,
#             }
#         return {}

