# -*- coding: utf-8 -*-
from odoo import fields, models, api
from odoo.exceptions import UserError, ValidationError
from datetime import datetime, time
from pytz import UTC
from pytz import timezone
import logging
_logger = logging.getLogger(__name__)

class EndShiftSession(models.Model):
    _name = 'end.shift'
    _rec_name = 'doc_num'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _description = "End Shift Session per Cashier"

    doc_num = fields.Char(string='Shift Number', tracking=True, readonly=True, copy=False)
    cashier_id = fields.Many2one('hr.employee', string='Cashier', tracking=True, required=True)
    session_id = fields.Many2one('pos.session', string='Session', tracking=True, required=True)
    start_date = fields.Datetime(string='Start Date', tracking=True)
    end_date = fields.Datetime(string='End Date', tracking=True)
    is_integrated = fields.Boolean(string='Integrated', default=False, tracking=True)
    
    modal = fields.Float(string="Modal", tracking=True, default=0.0, help="Modal awal untuk shift ini")
    
    state = fields.Selection([
        ('opened', 'Opened'),
        ('in_progress', 'In Progress'),
        ('closed', 'Closed'),
        ('finished', 'Finished')
    ], string='Status', default='opened', required=True, copy=False, tracking=True)

    line_ids = fields.One2many('end.shift.line', 'end_shift_id', string='Shift Lines')

    pos_order_count = fields.Integer(string='POS Orders', compute='_compute_pos_order_count', store=False)
    
    vit_notes = fields.Text(string="Notes", tracking=True)
    
    @api.model
    def check_unclosed_shifts(self, session_id):
        unclosed_shifts = self.search([
            ('session_id', '=', session_id),
            ('state', 'in', ['opened', 'in_progress', 'closed']),
        ])
        return unclosed_shifts.ids

    @api.depends('cashier_id', 'session_id', 'start_date', 'end_date')
    def _compute_pos_order_count(self):
        for record in self:
            count = 0
            if record.cashier_id and record.session_id and record.start_date and record.end_date:
                count = self.env['pos.order'].search_count([
                    ('session_id', '=', record.session_id.id),
                    ('employee_id', '=', record.cashier_id.id),
                    ('state', '=', 'invoiced'),
                    ('create_date', '>=', record.start_date),
                    ('create_date', '<=', record.end_date)
                ])
            record.pos_order_count = count

    def action_view_pos_orders(self):
        self.ensure_one()
        
        domain = [
            ('session_id', '=', self.session_id.id),
            ('employee_id', '=', self.cashier_id.id),
            ('state', '=', 'invoiced'),
            ('create_date', '>=', self.start_date),
            ('create_date', '<=', self.end_date)
        ]

        return {
            'name': 'POS Orders',
            'type': 'ir.actions.act_window',
            'res_model': 'pos.order',
            'view_mode': 'tree,form',
            'domain': domain,
            'context': {'create': False}
        }
    
    @api.model
    def create(self, vals):
        sequence_code = 'end.shift.doc.num'
        doc_num_seq = self.env['ir.sequence'].next_by_code(sequence_code)

        session = self.env['pos.session'].browse(vals.get('session_id'))
        pos_config = session.config_id

        pos_name = pos_config.name if pos_config else 'UNKNOWN'

        pos_code = ''.join(e for e in pos_name if e.isalnum()).upper()

        user_tz = timezone(self.env.user.tz or 'UTC')

        current_datetime = datetime.now(user_tz)
        date_str = current_datetime.strftime("%Y%m%d")
        time_str = current_datetime.strftime("%H%M%S")

        vals['doc_num'] = f"{pos_code}/{date_str}/{time_str}/{doc_num_seq}"

        vals['state'] = 'opened'

        result = super(EndShiftSession, self).create(vals)
        return result

    def action_start_progress(self):
        for record in self:
            record.with_context(skip_compute=True).write({'state': 'in_progress'})
            if record.line_ids:
                record.line_ids.with_context(skip_compute=True).write({'state': 'in_progress'})

    def action_close(self):
        for record in self:
            current_time = fields.Datetime.now()
            record.with_context(skip_compute=True).write({
                'end_date': current_time,
                'state': 'closed'
            })

            try:
                pos_orders = self.env['pos.order'].search([
                    ('session_id', '=', record.session_id.id),
                    ('employee_id', '=', record.cashier_id.id),
                    ('state', '=', 'invoiced'),
                    ('create_date', '>=', record.start_date),
                    ('create_date', '<=', record.end_date)
                ], limit=1000)

                payment_data = {}
                for order in pos_orders:
                    payments = order.payment_ids
                    for payment in payments:
                        method_id = payment.payment_method_id.id
                        amount = payment.amount
                        payment_date = payment.payment_date

                        if method_id in payment_data:
                            payment_data[method_id]['amount'] += amount
                            if payment_date > payment_data[method_id]['payment_date']:
                                payment_data[method_id]['payment_date'] = payment_date
                        else:
                            payment_data[method_id] = {
                                'payment_method_id': method_id,
                                'amount': amount,
                                'payment_date': payment_date,
                            }

                record.line_ids.unlink()
                for line_data in payment_data.values():
                    self.env['end.shift.line'].with_context(
                        skip_pin_validation=True,
                        skip_compute=True
                    ).create({
                        'end_shift_id': record.id,
                        'payment_method_id': line_data['payment_method_id'],
                        'amount': 0.0,
                        'payment_date': line_data['payment_date'],
                        'state': 'closed',
                    })
            except Exception as e:
                _logger.error(f"❌ Error in action_close: {e}")
                raise UserError(f"Error saat closing shift: {str(e)}")

    def action_finish(self):
        """
        ✅ SIMPLE: Wizard muncul jika ada amount_difference NEGATIF (shortage)
        """
        for record in self:
            # ✅ Step 1: Force compute dulu
            record.line_ids._compute_expected_amount()
            record.line_ids._compute_amount_difference()
            
            # ✅ Step 2: Filter lines dengan amount_difference negatif
            lines_with_shortage = record.line_ids.filtered(
                lambda line: line.amount_difference < 0  # Negatif = KURANG
            )
            
            # ✅ Step 3: Jika ada shortage DAN notes kosong → TAMPILKAN WIZARD
            if lines_with_shortage:
                if not record.vit_notes or record.vit_notes.strip() == '':
                    return {
                        'name': 'Notes Required - Shortage Detected',
                        'type': 'ir.actions.act_window',
                        'res_model': 'end.shift.notes.wizard',
                        'view_mode': 'form',
                        'view_id': self.env.ref('integrasi_pos.view_end_shift_notes_wizard_form').id,
                        'target': 'new',
                        'context': {
                            'default_end_shift_id': record.id,
                            'default_lines_info': '\n'.join([
                                f"{line.payment_method_id.name}: {line.amount_difference:+,.2f}"
                                for line in record.line_ids
                            ])
                        }
                    }
            
            # ✅ Step 4: Lanjutkan finish shift
            record.with_context(skip_compute=True, skip_pin_validation=True).write({
                'state': 'finished'
            })
            
            if record.line_ids:
                record.line_ids.with_context(skip_compute=True).write({
                    'state': 'finished'
                })
                record.line_ids._compute_expected_amount()
                record.line_ids._compute_amount_difference()

            # ✅ Step 5: Close cashier logs
            cashier_logs = self.env['pos.cashier.log'].search([
                ('employee_id', '=', record.cashier_id.id),
                ('session_id', '=', record.session_id.id),
                ('state', '!=', 'closed')
            ])
            if cashier_logs:
                cashier_logs.write({'state': 'closed'})

class EndShiftSessionLine(models.Model):
    _name = 'end.shift.line'
    _description = "End Shift Line Session per Cashier"

    end_shift_id = fields.Many2one('end.shift', string='End Shift Session', required=True, ondelete='cascade')
    payment_date = fields.Datetime(string='Date', tracking=True)
    payment_method_id = fields.Many2one('pos.payment.method', string="Payment Method", tracking=True, required=True)
    
    # ✅ FIXED: Hapus store=True agar tidak ter-trigger computed field saat write
    amount = fields.Float(string="Amount", tracking=True, default=0.0)
    
    temp_amount = fields.Float(string="Temp Amount", default=0.0)
    
    expected_amount = fields.Float(
        string="Expected Amount", 
        compute='_compute_expected_amount', 
        store=True, 
        tracking=True
    )
    
    amount_difference = fields.Float(
        string="Amount Difference", 
        compute='_compute_amount_difference', 
        store=True, 
        tracking=True
    )
    
    session_state = fields.Char(
        string='Session State',
        compute='_compute_session_state',
        store=False
    )

    state = fields.Selection([
        ('opened', 'Opened'),
        ('in_progress', 'In Progress'),
        ('closed', 'Closed'),
        ('finished', 'Finished')
    ], string='Status', default='opened', required=True, copy=False, tracking=True)

    @api.depends('end_shift_id.session_id.state')
    def _compute_session_state(self):
        for record in self:
            if record.end_shift_id and record.end_shift_id.session_id:
                record.session_state = record.end_shift_id.session_id.state
            else:
                record.session_state = False

    @api.model
    def create(self, vals):
        """Override create untuk logging"""
        _logger.info(f"📝 Creating line with amount: {vals.get('amount', 0.0)}")
        result = super(EndShiftSessionLine, self).create(vals)
        _logger.info(f"✅ Created line {result.id}, amount in DB: {result.amount}")
        return result

    @api.depends('end_shift_id.modal', 'payment_method_id', 'end_shift_id.session_id', 
             'end_shift_id.cashier_id', 'end_shift_id.start_date', 'end_shift_id.end_date', 'end_shift_id.state')
    def _compute_expected_amount(self):
        for record in self:
            expected = 0.0
            
            if self.env.context.get('skip_compute'):
                continue
                
            if not record.end_shift_id or not record.payment_method_id:
                record.expected_amount = expected
                continue
                
            if not record.end_shift_id.start_date or not record.end_shift_id.end_date:
                record.expected_amount = expected
                continue
                
            if record.end_shift_id.state not in ['closed', 'finished']:
                record.expected_amount = expected
                continue
            
            try:
                Order = record.env['pos.order']
                domain = [
                    ('session_id', '=', record.end_shift_id.session_id.id),
                    ('employee_id', '=', record.end_shift_id.cashier_id.id),
                    ('state', '=', 'invoiced'),
                    ('create_date', '>=', record.end_shift_id.start_date),
                    ('create_date', '<=', record.end_shift_id.end_date),
                    ('payment_ids.payment_method_id', '=', record.payment_method_id.id),
                ]
                orders = Order.search(domain)

                total = 0.0
                for order in orders:
                    for payment in order.payment_ids.filtered(
                        lambda p: p.payment_method_id.id == record.payment_method_id.id
                    ):
                        total += payment.amount

                if (record.payment_method_id.journal_id and 
                    record.payment_method_id.journal_id.type == 'cash'):
                    expected = total + (record.end_shift_id.modal or 0.0)
                else:
                    expected = total
                    
            except Exception as e:
                _logger.error(f"❌ Error computing expected_amount: {e}")
                expected = 0.0
                
            record.expected_amount = expected

    @api.depends('amount', 'expected_amount')
    def _compute_amount_difference(self):
        for record in self:
            record.amount_difference = (record.amount or 0.0) - (record.expected_amount or 0.0)

    def _need_pin_validation(self):
        """
        ✅ FIXED: Cek apakah perlu validasi PIN
        - Hanya jika state = 'finished'
        - DAN manager_validation aktif
        - DAN validate_end_shift aktif
        """
        self.ensure_one()
        
        # Skip jika ada context skip_pin_validation
        if self.env.context.get('skip_pin_validation'):
            return False
        
        # Jika parent state bukan 'finished', TIDAK perlu validasi
        if not self.end_shift_id or self.end_shift_id.state != 'finished':
            return False
        
        # Jika session sudah closed, TIDAK perlu validasi
        if self.end_shift_id.session_id.state == 'closed':
            return False
            
        # Cek konfigurasi
        try:
            config_settings = self.env['res.config.settings'].sudo().get_config_settings()
            return (config_settings.get('manager_validation') and 
                    config_settings.get('validate_end_shift'))
        except:
            return False

    def action_open_pin_wizard(self):
        """Open PIN wizard untuk validasi amount"""
        self.ensure_one()
        
        return {
            'name': 'Manager PIN Validation',
            'type': 'ir.actions.act_window',
            'res_model': 'end.shift.line.pin.wizard',
            'view_mode': 'form',
            'view_id': self.env.ref('integrasi_pos.view_end_shift_line_pin_wizard_form').id,
            'target': 'new',
            'context': {
                'default_end_shift_line_id': self.id,
                'default_amount': self.temp_amount or self.amount,
            }
        }

    def write(self, vals):
        """
        ✅ FIXED: Override write dengan logic yang lebih sederhana
        
        Strategi:
        1. Jika skip_pin_validation = True → langsung save
        2. Jika bukan perubahan amount → langsung save
        3. Jika state != 'finished' → langsung save
        4. Jika state = 'finished' DAN butuh PIN → simpan ke temp_amount dan raise warning
        """
        
        # ========== CASE 1: Skip PIN Validation ==========
        if self.env.context.get('skip_pin_validation'):
            _logger.info(f"⚡ Skip PIN - Direct save for {len(self)} records")
            return super(EndShiftSessionLine, self).write(vals)
        
        # ========== CASE 2: Bukan Perubahan Amount ==========
        if 'amount' not in vals:
            return super(EndShiftSessionLine, self).write(vals)
        
        # ========== CASE 3 & 4: Perubahan Amount ==========
        _logger.info(f"🔄 Processing amount change for {len(self)} records")
        
        # Pisahkan records berdasarkan kebutuhan validasi
        records_can_save = self.env['end.shift.line']
        records_need_pin = self.env['end.shift.line']
        
        for record in self:
            if record._need_pin_validation():
                records_need_pin |= record
            else:
                records_can_save |= record
        
        # Simpan records yang TIDAK butuh PIN
        if records_can_save:
            _logger.info(f"✅ Saving {len(records_can_save)} records directly")
            super(EndShiftSessionLine, records_can_save).write(vals)
        
        # Handle records yang BUTUH PIN
        if records_need_pin:
            _logger.info(f"🔐 {len(records_need_pin)} records need PIN validation")
            
            # Simpan nilai baru ke temp_amount
            vals_temp = vals.copy()
            vals_temp['temp_amount'] = vals_temp.pop('amount')
            
            super(EndShiftSessionLine, records_need_pin.with_context(
                skip_pin_validation=True
            )).write(vals_temp)
            
            # ✅ CRITICAL: RAISE WARNING untuk memberitahu user
            if len(records_need_pin) == 1:
                raise UserError(
                    "⚠️ Manager Approval Required\n\n"
                    f"This shift is in FINISHED state.\n"
                    f"Please click 'Update with PIN' button to validate your changes.\n\n"
                    f"Your new amount ({vals['amount']}) has been saved temporarily."
                )
            else:
                raise UserError(
                    "⚠️ Manager Approval Required\n\n"
                    f"{len(records_need_pin)} records need PIN validation.\n"
                    f"Please update each record using 'Update with PIN' button."
                )
        
        return True

class EndShiftNotesWizard(models.TransientModel):
    _name = 'end.shift.notes.wizard'
    _description = "Wizard untuk Input Notes saat Ada Amount Difference"

    end_shift_id = fields.Many2one('end.shift', string="End Shift", required=True)
    lines_info = fields.Text(string="Lines with Difference", readonly=True)
    vit_notes = fields.Text(string="Notes", 
                           help="Please explain the reason for the amount difference")
    
    def action_confirm_and_finish(self):
        """
        ✅ Simpan notes dan lanjutkan proses finish
        """
        self.ensure_one()
        
        # Update notes di end.shift
        self.end_shift_id.write({'vit_notes': self.vit_notes})
        
        # Lanjutkan proses finish
        self.end_shift_id.with_context(skip_compute=True, skip_pin_validation=True).write({
            'state': 'finished'
        })
        
        if self.end_shift_id.line_ids:
            self.end_shift_id.line_ids.with_context(skip_compute=True).write({
                'state': 'finished'
            })
            
            self.end_shift_id.line_ids._compute_expected_amount()
            self.end_shift_id.line_ids._compute_amount_difference()

        cashier_logs = self.env['pos.cashier.log'].search([
            ('employee_id', '=', self.end_shift_id.cashier_id.id),
            ('session_id', '=', self.end_shift_id.session_id.id),
            ('state', '!=', 'closed')
        ])
        if cashier_logs:
            cashier_logs.write({'state': 'closed'})
        
        # Return notification
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': '✅ Shift Finished',
                'message': 'Shift has been finished successfully with notes.',
                'type': 'success',
                'sticky': False,
            }
        }
    
class EndShiftLinePINWizard(models.TransientModel):
    _name = 'end.shift.line.pin.wizard'
    _description = "Wizard untuk Validasi PIN Manager saat Mengubah Amount"

    amount = fields.Float(string="New Amount", required=True)
    end_shift_line_id = fields.Many2one('end.shift.line', string="End Shift Line", required=True)
    manager_pin = fields.Char(string="Manager PIN", required=True, size=4)
    
    current_amount = fields.Float(string="Current Amount", readonly=True, compute='_compute_current_amount')
    payment_method = fields.Char(string="Payment Method", readonly=True, compute='_compute_payment_method')
    
    @api.depends('end_shift_line_id')
    def _compute_current_amount(self):
        for record in self:
            record.current_amount = record.end_shift_line_id.amount if record.end_shift_line_id else 0.0
    
    @api.depends('end_shift_line_id')
    def _compute_payment_method(self):
        for record in self:
            if record.end_shift_line_id and record.end_shift_line_id.payment_method_id:
                record.payment_method = record.end_shift_line_id.payment_method_id.name
            else:
                record.payment_method = '-'

    @api.model
    def default_get(self, fields_list):
        res = super(EndShiftLinePINWizard, self).default_get(fields_list)
        
        # Ambil dari temp_amount jika ada
        if 'end_shift_line_id' in res:
            line = self.env['end.shift.line'].browse(res['end_shift_line_id'])
            if line and line.temp_amount:
                res['amount'] = line.temp_amount
        
        return res

    def action_validate_pin(self):
        """
        ✅ FIXED: Validasi PIN dan update amount
        """
        self.ensure_one()
        
        config_settings = self.env['res.config.settings'].sudo().get_config_settings()
        
        # Jika validasi tidak aktif, langsung save
        if not config_settings.get('manager_validation') or not config_settings.get('validate_end_shift'):
            self.end_shift_line_id.with_context(skip_pin_validation=True).write({
                'amount': self.amount,
                'temp_amount': 0.0
            })
            return {'type': 'ir.actions.act_window_close'}

        # Validasi PIN
        manager_id = config_settings.get('manager_id')
        if not manager_id or not manager_id.get('pin'):
            raise ValidationError("❌ Manager PIN belum dikonfigurasi di Settings!")

        if self.manager_pin != manager_id.get('pin'):
            raise ValidationError("❌ PIN Manager tidak valid!")

        # PIN valid, update amount
        _logger.info(f"✅ PIN validated, updating amount from {self.end_shift_line_id.amount} to {self.amount}")
        
        self.end_shift_line_id.with_context(skip_pin_validation=True).write({
            'amount': self.amount,
            'temp_amount': 0.0
        })
        
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': '✅ Success',
                'message': f'Amount updated to {self.amount}',
                'type': 'success',
                'sticky': False,
            }
        }