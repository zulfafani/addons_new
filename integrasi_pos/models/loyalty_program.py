# -*- coding: utf-8 -*-
from odoo import fields, models, api, _
from datetime import datetime


class LoyaltyProgramInherit(models.Model):
    _inherit = 'loyalty.program'

    is_integrated = fields.Boolean(string="Integrated", default=False)
    vit_trxid = fields.Char(string="Transaction ID", default=False)
    id_mc = fields.Char(string="ID MC", default=False)
    is_member = fields.Boolean(string="Is Member?", default=False)
    # PERBAIKAN: Hapus multiple=True dan gunakan Char untuk menyimpan multiple values
    allowed_days = fields.Selection(
        selection=[
            ('mon', 'Monday'),
            ('tue', 'Tuesday'),
            ('wed', 'Wednesday'),
            ('thu', 'Thursday'),
            ('fri', 'Friday'),
            ('sat', 'Saturday'),
            ('sun', 'Sunday'),
        ],
        string="Allowed Days",
    )

    start_time = fields.Float(string="Start Time (e.g. 8.0 for 8 AM)")
    end_time = fields.Float(string="End Time (e.g. 17.0 for 5 PM)")

    allowed_partner_ids = fields.Many2many(
        'res.partner.category',
        string="Allowed Members",
        help="Only these members can access this loyalty program."
    )
    schedule_ids = fields.One2many('loyalty.program.schedule','program_id',string='Schedules')
    member_ids = fields.One2many('loyalty.member','member_program_id',string='Members')
    vit_konversi_poin = fields.Float(string="Konversi untuk Penukaran Point 1 Point =")

    def _export_for_loyalty_pos(self):
        return {
            'id': self.id,
            'name': self.name,
            'program_type': self.program_type,
            'available_on': self.available_on,
            'rule_ids': self.rule_ids.ids,
            'reward_ids': self.reward_ids.ids,
        }

class LoyaltyMember(models.Model):
    _name = 'loyalty.member'
    _description = 'Loyalty Member'

    member_program_id = fields.Many2one('loyalty.program', string='Loyalty Program', required=True, ondelete='cascade')
    member_pos = fields.Many2one('res.partner.category',string="Member")

    def _export_for_loyalty_pos(self):
        return {
            'id': self.id,
            'member_program_id': [self.member_program_id.id, self.member_program_id.name],
            'member_pos': [self.member_pos.id, self.member_pos.name] if self.member_pos else False,
        }

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

    def _export_for_loyalty_pos(self):
        return {
            'id': self.id,
            'program_id': [self.program_id.id, self.program_id.name],
            'days': self.days,
            'time_start': self.time_start,
            'time_end': self.time_end,
        }

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