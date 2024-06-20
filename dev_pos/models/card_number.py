from odoo import api, fields, models
from odoo.fields import Command


class CardNumber(models.Model):
    _name = 'card.number'
    _description = "Card Number"

    name = fields.Char(string='Nama')
    card_num = fields.Char(string='Card Number')
