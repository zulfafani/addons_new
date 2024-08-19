from odoo import models, fields, api
import json

class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    manager_validation = fields.Boolean("Manager Validation", config_parameter="pos.manager_validation", help="Enable manager validation for specific actions.")
    manager_id = fields.Many2one('hr.employee', string="Manager", help="Select a manager for validation.")
    validate_closing_pos = fields.Boolean("Closing Of POS", config_parameter="pos.validate_closing_pos", help="Allow manager to validate closing POS.")
    validate_order_line_deletion = fields.Boolean("Void Item", config_parameter="pos.validate_order_line_deletion", help="Allow manager to validate order line deletions.")
    validate_discount = fields.Boolean("Apply Discount", config_parameter="pos.validate_discount", help="Allow manager to validate discount applications.")
    validate_price_change = fields.Boolean("Price Change", config_parameter="pos.validate_price_change", help="Allow manager to validate price changes.")
    validate_order_deletion = fields.Boolean("Order Deletion", config_parameter="pos.validate_order_deletion", help="Allow manager to validate order deletions.")
    validate_add_remove_quantity = fields.Boolean("Add/Remove Quantity", config_parameter="pos.validate_add_remove_quantity", help="Allow manager to validate adding/removing quantities.")
    validate_payment = fields.Boolean("Payment", config_parameter="pos.validate_payment", help="Allow manager to validate payments.")
    validate_end_shift = fields.Boolean("End Shift", config_parameter="pos.validate_end_shift", help="Allow manager to validate end of shift.")
    validate_refund = fields.Boolean("Refund", config_parameter="pos.validate_refund", help="Allow manager to validate refund.")
    validate_close_session = fields.Boolean("Close Session", config_parameter="pos.validate_close_session", help="Allow manager to close session.")
    validate_discount_amount = fields.Boolean("Discount Amount", config_parameter="pos.validate_discount_amount", help="Allow manager to validate discount amount.")
    validate_void_sales = fields.Boolean("Void Sales", config_parameter="pos.validate_void_sales", help="Allow manager to reset order.")
    validate_member_schedule = fields.Boolean("Member/Schedule", config_parameter="pos.validate_member_schedule", help="Allow manager to validate member schedule.")
    validate_prefix_customer = fields.Boolean("Prefix Customer", config_parameter="pos.validate_prefix_customer", help="Allow manager to change prefix customer.")
    one_time_password = fields.Boolean("One Time Password for an Order", config_parameter="pos.one_time_password", help="Require OTP for every function.")
    multiple_barcode_activate = fields.Boolean("Multiple Barcode Activation", config_parameter="pos.multiple_barcode_activate", help="Enable multiple barcode activation.")

    # Barcode scanner configuration fields
    digit_awal = fields.Integer(string="Digit Awal", help="Starting position for weight extraction", config_parameter="pos.digit_awal")
    digit_akhir = fields.Integer(string="Digit Akhir", help="Ending position for weight extraction", config_parameter="pos.digit_akhir")
    prefix_timbangan = fields.Char(string="Prefix Timbangan", help="Prefix for weight barcode", config_parameter="pos.prefix_timbangan")
    panjang_barcode = fields.Integer(string="Panjang Barcode", help="Length of the barcode weight portion", config_parameter="pos.panjang_barcode")

    def set_values(self):
        super(ResConfigSettings, self).set_values()
        config = self.env['ir.config_parameter'].sudo()
        config.set_param('pos.manager_id', self.manager_id.id if self.manager_id else False)

    @api.model
    def get_values(self):
        res = super(ResConfigSettings, self).get_values()
        config = self.env['ir.config_parameter'].sudo()
        manager_id = config.get_param('pos.manager_id')
        res.update(
            manager_id=int(manager_id) if manager_id and manager_id.isdigit() else False,
        )
        return res
    
    @api.model
    def get_config_settings(self):
        try:
            config = self.env['ir.config_parameter'].sudo()
            manager_id = config.get_param('pos.manager_id')
            manager = self.env['hr.employee'].browse(int(manager_id)) if manager_id and manager_id.isdigit() else None

            return {
                'manager_validation': config.get_param('pos.manager_validation') == 'True',
                'manager_id': {
                    'id': manager.id if manager else None,
                    'name': manager.name if manager else None,
                    'pin': manager.pin if manager and hasattr(manager, 'pin') else None,
                } if manager else None,
                'validate_closing_pos': config.get_param('pos.validate_closing_pos') == 'True',
                'validate_order_line_deletion': config.get_param('pos.validate_order_line_deletion') == 'True',
                'validate_discount': config.get_param('pos.validate_discount') == 'True',
                'validate_price_change': config.get_param('pos.validate_price_change') == 'True',
                'validate_order_deletion': config.get_param('pos.validate_order_deletion') == 'True',
                'validate_add_remove_quantity': config.get_param('pos.validate_add_remove_quantity') == 'True',
                'validate_payment': config.get_param('pos.validate_payment') == 'True',
                'validate_end_shift': config.get_param('pos.validate_end_shift') == 'True',
                'validate_refund': config.get_param('pos.validate_refund') == 'True',
                'validate_close_session': config.get_param('pos.validate_close_session') == 'True',
                'validate_void_sales': config.get_param('pos.validate_void_sales') == 'True',
                'validate_member_schedule': config.get_param('pos.validate_member_schedule') == 'True',
                'validate_prefix_customer': config.get_param('pos.validate_prefix_customer') == 'True',
                'one_time_password': config.get_param('pos.one_time_password') == 'True',
                'validate_discount_amount': config.get_param('pos.validate_discount_amount') == 'True',
                'multiple_barcode_activate': config.get_param('pos.multiple_barcode_activate') == 'True',
            }
        except Exception as e:
            return {'error': str(e)}