from odoo import http, fields, api, _
from odoo.http import request
import subprocess
from datetime import datetime
import pytz

class POSLoyaltyRPC(http.Controller):
    @http.route('/pos/loyalty/get_valid_programs', type='json', auth='user')
    def get_valid_loyalty_programs(self):
        """
        Return list of loyalty.program records (in minimal fields) that are valid now
        for current user/partner based on allowed_partner_ids, allowed_days, start_time, end_time.
        Also include rewards for those programs.
        """
        # Ambil partner dari session / request context
        partner = request.env.user.partner_id

        # Waktu sekarang dengan timezone server
        now_utc = datetime.now(pytz.UTC)
        # Kalau ingin pakai timezone server lokal (pastikan configuration tz server)
        now = now_utc

        # Hari sekarang e.g. 'mon', 'tue', ...
        weekday = now.strftime('%a').lower()[:3]

        # Jam sekarang sebagai float
        current_hour = now.hour + now.minute/60.0

        LoyaltyProgram = request.env['loyalty.program'].sudo()
        LoyaltyReward = request.env['loyalty.reward'].sudo()

        # Cari program yang aktif dan memenuhi kondisi
        programs = LoyaltyProgram.search([('active','=',True)])
        valid = []
        for prog in programs:
            # cek partner membership
            if prog.allowed_partner_ids:
                # partner harus punya kategori yang termasuk
                partner_cat_ids = partner.category_id.ids if hasattr(partner, 'category_id') else []
                if not set(partner_cat_ids).intersection(set(prog.allowed_partner_ids.ids)):
                    continue
            # cek hari
            if prog.allowed_days:
                if weekday not in prog.allowed_days:
                    continue
            # cek waktu
            if (prog.start_time is not False and prog.start_time is not None) and (prog.end_time is not False and prog.end_time is not None):
                st = prog.start_time
                et = prog.end_time
                if st <= et:
                    if not (current_hour >= st and current_hour <= et):
                        continue
                else:
                    # wrap around midnight
                    if not (current_hour >= st or current_hour <= et):
                        continue
            # jika lulus semua, ambil reward nya
            rewards = LoyaltyReward.search([('program_id','=',prog.id)])
            # minimal fields
            valid.append({
                'id': prog.id,
                'name': prog.name,
                'program_type': prog.program_type,
                # tambahkan field lain jika perlu
                'allowed_partner_ids': prog.allowed_partner_ids.ids,
                'allowed_days': prog.allowed_days or [],
                'start_time': prog.start_time,
                'end_time': prog.end_time,
                'reward_ids': rewards.ids,
            })

        return {'programs': valid}

class LogNoteController(http.Controller):

    @http.route('/pos/log_note/create', type='json', auth='user')
    def create_log_note(self, note):
        user = request.env.user
        session = request.env['pos.session'].search([('user_id', '=', user.id)], limit=1)

        now = datetime.now()

        request.env['log.note'].sudo().create({
            'vit_doc_type': 'POS Manager Validation',
            'vit_trx_key': session.name if session else '',
            'vit_trx_date': now,
            'vit_sync_date': now,
            'vit_sync_status': 'VALID',
            'vit_sync_desc': note or 'No reason provided',
            'vit_start_sync': now,
            'vit_end_sync': now,
            'vit_duration': '0s',
        })
        return {'status': 'ok'}

class LoyaltyProgramController(http.Controller):

    @http.route('/loyalty/validate_program_access', type='json', auth='user', methods=['POST'], csrf=False)
    def validate_program_access(self, partner_id=None):
        # Ambil data program loyalitas
        program_ids = request.env['loyalty.program'].search([])

        current_time = datetime.now()
        current_day = current_time.strftime('%a').lower()  # mon, tue, etc.
        current_time_float = current_time.hour + current_time.minute / 60

        result = []
        for program in program_ids:
            valid = True
            error = False

            # Cek apakah program memerlukan anggota
            if program.is_member and not partner_id:
                valid = False
                error = _("Program loyalitas ini hanya untuk anggota.")

            # Cek kategori partner
            if program.allowed_partner_ids and partner_id:
                partner = request.env['res.partner'].browse(partner_id)
                category_ids = partner.category_id.ids
                if not any(cid in category_ids for cid in program.allowed_partner_ids.ids):
                    valid = False
                    error = _("Program loyalitas hanya untuk kategori tertentu.")

            # Cek hari yang diizinkan
            if program.allowed_days:
                allowed_days = program.allowed_days.split(',')
                if current_day not in allowed_days:
                    valid = False
                    error = _("Program tidak tersedia pada hari ini.")

            # Cek waktu yang diizinkan
            if program.start_time and program.end_time:
                start = program.start_time
                end = program.end_time
                if start > end:
                    in_time = current_time_float >= start or current_time_float <= end
                else:
                    in_time = start <= current_time_float <= end
                if not in_time:
                    valid = False
                    error = _("Program hanya tersedia pada jam tertentu.")

            result.append({
                'program_id': program.id,
                'valid': valid,
                'error': error,
            })

        return result

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

