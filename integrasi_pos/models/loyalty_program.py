# -*- coding: utf-8 -*-
from odoo import fields, models, api, _


class LoyaltyProgramInherit(models.Model):
    _inherit = 'loyalty.program'

    is_integrated = fields.Boolean(string="Integrated", default=False)
    vit_trxid = fields.Char(string="Transaction ID", default=False)
    id_mc = fields.Char(string="ID MC", default=False)
    is_member = fields.Boolean(string="Is Member?", default=False)
    schedule_ids = fields.One2many('loyalty.program.schedule','program_id',string='Schedules')
    member_ids = fields.One2many('loyalty.member','member_program_id',string='Members')

class LoyaltyMember(models.Model):
    _name = 'loyalty.member'
    _description = 'Loyalty Member'

    member_program_id = fields.Many2one('loyalty.program', string='Loyalty Program', required=True, ondelete='cascade')
    member_pos = fields.Many2one('res.partner.category',string="Member")

    # models/loyalty_program_schedule.py
    @api.model
    def create(self, vals):
        record = super().create(vals)
        type(self.env['bus.bus'])._sendone(
            self.env['bus.bus'],
            self.env.cr.dbname,
            'loyalty.update',
            {
                'type': 'member',
                'id': record.id,
            }
        )
        return record

    def write(self, vals):
        res = super().write(vals)
        type(self.env['bus.bus'])._sendone(
            self.env['bus.bus'],
            self.env.cr.dbname,
            'loyalty.update',
            {
                'type': 'member',
                'ids': self.ids,
            }
        )
        return res

    
class LoyaltyProgramSchedule(models.Model):
    _name = 'loyalty.program.schedule'
    _description = 'Loyalty Program Schedule'

    program_id = fields.Many2one('loyalty.program', string='Loyalty Program', required=True, ondelete='cascade')

    days = fields.Selection([
        ('monday', 'Monday'),
        ('tuesday', 'Tuesday'), 
        ('wednesday', 'Wednesday'),
        ('thursday', 'Thursday'),
        ('friday', 'Friday'),
        ('saturday', 'Saturday'),
        ('sunday', 'Sunday'),
    ], string='Day')

    time_start = fields.Float(string="Time Start")
    time_end = fields.Float(string="Time End")

    # models/loyalty_program_schedule.py
    @api.model
    def create(self, vals):
        record = super().create(vals)
        type(self.env['bus.bus'])._sendone(
            self.env['bus.bus'],
            self.env.cr.dbname,
            'loyalty.update',
            {
                'type': 'member',
                'id': record.id,
            }
        )
        return record

    def write(self, vals):
        res = super().write(vals)
        type(self.env['bus.bus'])._sendone(
            self.env['bus.bus'],
            self.env.cr.dbname,
            'loyalty.update',
            {
                'type': 'member',
                'ids': self.ids,
            }
        )
        return res