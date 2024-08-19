# -*- coding: utf-8 -*-
from odoo import fields, models, api

class EndShiftSession(models.Model):
    _name = 'end.shift'
    _inherit = ['mail.thread', 'mail.activity.mixin']
    _description = "End Shift Session per Cashier"

    cashier_id = fields.Many2one('hr.employee', string='Cashier', tracking=True, required=True)
    session_id = fields.Many2one('pos.session', string='Session', tracking=True, required=True)
    start_date = fields.Datetime(string='Start Date', tracking=True)
    end_date = fields.Datetime(string='End Date', tracking=True)
    state = fields.Selection([
        ('opened', 'Opened'),
        ('in_progress', 'In Progress'),
        ('closed', 'Closed'),
        ('finished', 'Finished')
    ], string='Status', default='opened', required=True, readonly=True, copy=False, tracking=True)

    line_ids = fields.One2many('end.shift.line', 'end_shift_id', string='Shift Lines', context={'parent_state': 'state'})

    pos_order_count = fields.Integer(string='POS Orders', compute='_compute_pos_order_count')

    @api.depends('cashier_id', 'session_id')
    def _compute_pos_order_count(self):
        for record in self:
            record.pos_order_count = self.env['pos.order'].search_count([
                ('session_id', '=', record.session_id.id),
                ('employee_id', '=', record.cashier_id.id)
            ])

    def action_view_pos_orders(self):
        self.ensure_one()
        return {
            'name': 'POS Orders',
            'type': 'ir.actions.act_window',
            'res_model': 'pos.order',
            'view_mode': 'tree,form',
            'domain': [
                ('session_id', '=', self.session_id.id),
                ('employee_id', '=', self.cashier_id.id)
            ],
            'context': {'create': False}
        }
    
    @api.depends('end_shift_id.state', 'amount')
    def _compute_expected_amount(self):
        for line in self:
            if line.end_shift_id.state == 'finished':
                line.expected_amount = line.amount
            else:
                line.expected_amount = 0.0

    @api.model
    def create(self, vals):
        # Set state to 'opened' when creating a new record
        vals['state'] = 'opened'
        return super(EndShiftSession, self).create(vals)

    def action_start_progress(self):
        for record in self:
            record.state = 'in_progress'
            for line in record.line_ids:
                line.state = 'in_progress'

    def action_close(self):
        for record in self:
            record.state = 'closed'
            for line in record.line_ids:
                line.state = 'closed'

    def action_finish(self):
        for record in self:
            record.state = 'finished'
            for line in record.line_ids:
                line.state = 'finished'

    def action_reset(self):
        
        for record in self:
            record.state = 'opened'
            for line in record.line_ids:
                line.state = 'opened'

class EndShiftSessionLine(models.Model):
    _name = 'end.shift.line'
    _description = "End Shift Line Session per Cashier"

    end_shift_id = fields.Many2one('end.shift', string='End Shift Session', required=True, ondelete='cascade')
    payment_date = fields.Datetime(string='Date', tracking=True)
    payment_method_id = fields.Many2one('pos.payment.method', string="Payment Method", tracking=True, required=True)
    amount = fields.Float(string="Amount", tracking=True)
    expected_amount = fields.Float(string="Expected Amount", tracking=True)
    amount_difference = fields.Float(string="Amount Difference", tracking=True)
    state = fields.Selection([
        ('opened', 'Opened'),
        ('in_progress', 'In Progress'),
        ('closed', 'Closed'),
        ('finished', 'Finished')
    ], string='Status', default='opened', required=True, readonly=True, copy=False, tracking=True)
            
    @api.depends('amount', 'expected_amount')
    def _compute_amount_difference(self):
        for record in self:
            record.amount_difference = record.amount - record.expected_amount

    @api.model
    def create(self, vals):
        # Jika 'amount' dan 'expected_amount' ada dalam vals, hitung amount_difference
        if 'amount' in vals and 'expected_amount' in vals:
            vals['amount_difference'] = vals['amount'] - vals['expected_amount']
        return super(EndShiftSessionLine, self).create(vals)

    def write(self, vals):
        res = super(EndShiftSessionLine, self).write(vals)
        # Jika 'amount' atau 'expected_amount' diubah, hitung ulang amount_difference
        if 'amount' in vals or 'expected_amount' in vals:
            for record in self:
                record.amount_difference = record.amount - record.expected_amount
        return res