# -*- coding: utf-8 -*-
from odoo import fields, models, api, _


class PosConfigInherit(models.Model):
    _inherit = 'pos.config'

    default_partner_id = fields.Many2one('res.partner', string="Select Customer")
    id_mc = fields.Char(string="ID MC", default=False)
    is_integrated = fields.Boolean(string="Integrated", tracking=True)
    is_updated = fields.Boolean(string="Updated", tracking=True)
    vit_trxid = fields.Char(string="Transaction ID", tracking=True)
    image = fields.Binary(string='Image', help="Set logo image for viewing it"
                                               "in POS Screen and Receipt")
    manager_pin = fields.Char(compute="_compute_manager_pin", store=False)

    def _compute_manager_pin(self):
        """Compute manager PIN from system parameter"""
        config = self.env['ir.config_parameter'].sudo()
        manager_id = config.get_param('pos.manager_id')
        manager = self.env['hr.employee'].browse(int(manager_id)) if manager_id and manager_id.isdigit() else None
        manager_pin = manager.pin if manager and hasattr(manager, 'pin') else ''
        
        for record in self:
            record.manager_pin = manager_pin
    