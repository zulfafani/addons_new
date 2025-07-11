# -*- coding: utf-8 -*-
from odoo import fields, models, api
from datetime import datetime, time
from pytz import UTC
from pytz import timezone

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
    
    # ✅ NEW: Modal field di level end.shift
    modal = fields.Float(string="Modal", tracking=True, default=0.0, help="Modal awal untuk shift ini")
    
    state = fields.Selection([
        ('opened', 'Opened'),
        ('in_progress', 'In Progress'),
        ('closed', 'Closed'),
        ('finished', 'Finished')
    ], string='Status', default='opened', required=True, copy=False, tracking=True)

    line_ids = fields.One2many('end.shift.line', 'end_shift_id', string='Shift Lines')

    # ✅ FIXED: Computed field dengan dependency yang benar
    pos_order_count = fields.Integer(string='POS Orders', compute='_compute_pos_order_count', store=False)

    @api.model
    def check_unclosed_shifts(self, session_id):
        unclosed_shifts = self.search([
            ('session_id', '=', session_id),
            ('state', 'in', ['opened', 'in_progress', 'closed']),
        ])
        return unclosed_shifts.ids

    # ✅ FIXED: Dependency yang benar dan penanganan None values
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
        # Mengambil sequence untuk document number
        sequence_code = 'end.shift.doc.num'
        doc_num_seq = self.env['ir.sequence'].next_by_code(sequence_code)

        # Mengambil informasi POS dari session
        session = self.env['pos.session'].browse(vals.get('session_id'))
        pos_config = session.config_id

        # Mengambil nama POS
        pos_name = pos_config.name if pos_config else 'UNKNOWN'

        # Membersihkan nama POS dari karakter yang tidak diinginkan
        pos_code = ''.join(e for e in pos_name if e.isalnum()).upper()

        user_tz = timezone(self.env.user.tz or 'UTC')

        # Mendapatkan tanggal dan waktu saat ini dalam zona waktu lokal
        current_datetime = datetime.now(user_tz)
        date_str = current_datetime.strftime("%Y%m%d")
        time_str = current_datetime.strftime("%H%M%S")

        # Membuat doc_num dengan format yang diinginkan
        vals['doc_num'] = f"{pos_code}/{date_str}/{time_str}/{doc_num_seq}"

        # Set state to 'opened'
        vals['state'] = 'opened'

        # Panggil metode create asli untuk membuat record baru
        result = super(EndShiftSession, self).create(vals)
        return result

    def action_start_progress(self):
        for record in self:
            # ✅ FIXED: Gunakan context untuk menghindari loop
            record.with_context(skip_compute=True).write({'state': 'in_progress'})
            if record.line_ids:
                record.line_ids.with_context(skip_compute=True).write({'state': 'in_progress'})

    def action_close(self):
        for record in self:
            # Update end_date to current time
            current_time = fields.Datetime.now()
            record.with_context(skip_compute=True).write({
                'end_date': current_time,
                'state': 'closed'
            })

            # Mencari pos.order yang sesuai
            pos_orders = self.env['pos.order'].search([
                ('session_id', '=', record.session_id.id),
                ('employee_id', '=', record.cashier_id.id),
                ('state', '=', 'invoiced'),
                ('create_date', '>=', record.start_date),
                ('create_date', '<=', record.end_date)
            ])

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

            # Hapus line_ids yang ada dan buat yang baru
            record.line_ids.unlink()
            for line_data in payment_data.values():
                # ✅ FIXED: Buat dengan amount, expected_amount akan dihitung otomatis
                self.env['end.shift.line'].create({
                    'end_shift_id': record.id,
                    'payment_method_id': line_data['payment_method_id'],
                    'amount': 0.0,  # kosong, diisi manual nanti oleh kasir
                    'payment_date': line_data['payment_date'],
                    'state': 'closed',
                })

    def action_finish(self):
        for record in self:
            record.with_context(skip_compute=True).write({'state': 'finished'})
            if record.line_ids:
                record.line_ids.with_context(skip_compute=True).write({'state': 'finished'})
                
                # ✅ Force recompute expected_amount setelah modal dipastikan diisi
                record.line_ids._compute_expected_amount()
                record.line_ids._compute_amount_difference()

            # Close related cashier logs
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
    
    # ✅ SAFE: Base field untuk amount yang diterima kasir
    amount = fields.Float(string="Amount", tracking=True, default=0.0)
    
    # ✅ SAFE: Computed field yang bergantung pada modal dari parent + amount
    expected_amount = fields.Float(
        string="Expected Amount", 
        compute='_compute_expected_amount', 
        store=True, 
        tracking=True
    )
    
    # ✅ SAFE: Computed field yang bergantung pada computed field lain (linear dependency)
    amount_difference = fields.Float(
        string="Amount Difference", 
        compute='_compute_amount_difference', 
        store=True, 
        tracking=True
    )

    state = fields.Selection([
        ('opened', 'Opened'),
        ('in_progress', 'In Progress'),
        ('closed', 'Closed'),
        ('finished', 'Finished')
    ], string='Status', default='opened', required=True, copy=False, tracking=True)

    # ✅ SAFE: Computed method - bergantung pada modal dari parent dan amount
    @api.depends('end_shift_id.modal', 'payment_method_id', 'end_shift_id.session_id', 'end_shift_id.cashier_id', 'end_shift_id.start_date', 'end_shift_id.end_date')
    def _compute_expected_amount(self):
        for record in self:
            expected = 0.0
            if record.end_shift_id and record.payment_method_id:
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
                    for payment in order.payment_ids.filtered(lambda p: p.payment_method_id.id == record.payment_method_id.id):
                        total += payment.amount

                # Tambahkan modal hanya untuk payment method "Cash"
                # Benar ✅
                if record.payment_method_id.journal_id and record.payment_method_id.journal_id.type == 'cash':
                    expected = total + (record.end_shift_id.modal or 0.0)
                else:
                    expected = total
            record.expected_amount = expected


    # ✅ SAFE: Computed method - bergantung pada computed field yang sudah dihitung
    @api.depends('amount', 'expected_amount')
    def _compute_amount_difference(self):
        for record in self:
            record.amount_difference = (record.amount or 0.0) - (record.expected_amount or 0.0)

    # ✅ SAFE: Override write tanpa memanggil computed field secara manual
    def write(self, vals):
        result = super(EndShiftSessionLine, self).write(vals)
        # Computed fields akan otomatis dipicu oleh @api.depends
        return result