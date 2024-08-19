import logging
from datetime import datetime, timedelta
from odoo import models, fields, api
from odoo.exceptions import UserError

logger = logging.getLogger(__name__)

class PosSession(models.Model):
    _inherit = 'pos.session'

    is_updated = fields.Boolean(string="Updated", default=False, readonly=True, tracking=True)
    name_session_pos = fields.Char(string="Name Session POS (Odoo Store)", readonly=True)
    id_mc = fields.Char(string="ID MC", default=False)

    def _pos_ui_models_to_load(self):
        res = super()._pos_ui_models_to_load()
        res += [
            'loyalty.program',
            'loyalty.program.schedule',
            'loyalty.member',
            'res.partner',
            'res.config.settings',
            'product.product',
            'end.shift',
            'end.shift.line',
            'pos.cashier.log',
            'barcode.config',
            'multiple.barcode',
            'hr.employee.config.settings'
        ]
        return res
    
    def _loader_params_hr_employee_config_settings(self):
        return {
            'search_params': {
                'domain': [],
                'fields': ['id', 'employee_id', 'is_cashier', 'is_sales_person'],
            }
        }

    def _get_pos_ui_hr_employee_config_settings(self, params):
        try:
            records = self.env['hr.employee.config.settings'].search_read(
                params['search_params']['domain'],
                params['search_params']['fields']
            )

            # Convert relational fields to [id, name] for POS JS compatibility
            for rec in records:
                if isinstance(rec['employee_id'], list):
                    rec['employee_id'] = [rec['employee_id'][0], str(rec['employee_id'][1])]
            return records
        except Exception:
            return []

    def _pos_ui_hr_employee_config_settings(self, params):
        return self._get_pos_ui_hr_employee_config_settings(params)


    def _loader_params_multiple_barcode(self):
        return {
            'search_params': {
                'domain': [],
                'fields': ['id', 'barcode', 'product_id'],
            }
        }

    def _get_pos_ui_multiple_barcode(self, params):
        try:
            records = self.env['multiple.barcode'].search_read(
                params['search_params']['domain'],
                params['search_params']['fields']
            )
            for rec in records:
                if isinstance(rec['product_id'], list):
                    rec['product_id'] = [int(rec['product_id'][0]), str(rec['product_id'][1])]
            return records
        except Exception as e:
            return []

    def _pos_ui_multiple_barcode(self, params):
        return self._get_pos_ui_multiple_barcode(params)

    
    def _loader_params_barcode_config(self):
        return {
            'search_params': {
                'domain': [],
                'fields': [
                    'digit_awal',
                    'digit_akhir',
                    'prefix_timbangan',
                    'panjang_barcode',
                    'multiple_barcode_activate',
                ],
            }
        }

    def _get_pos_ui_barcode_config(self, params):
        try:
            return self.env['barcode.config'].search_read(params['search_params']['domain'], params['search_params']['fields'])
        except Exception as e:
            return []

    def _pos_ui_barcode_config(self, params):
        return self._get_pos_ui_barcode_config(params)

    
    def _loader_params_end_shift_line(self):
        return {
            'search_params': {
                'domain': [],
                'fields': [
                    'id',
                    'end_shift_id',
                    'payment_method_id',
                    'expected_amount',
                    'payment_date',
                    'amount',
                    'amount_difference',
                    'state',
                ],
            }
        }

    def _get_pos_ui_end_shift_line(self, params):
        try:
            records = self.env['end.shift.line'].search_read(
                params['search_params'].get('domain', []),
                params['search_params']['fields']
            )
            return records
        except Exception as e:
            return []

    def _pos_ui_end_shift_line(self, params):
        return self._get_pos_ui_end_shift_line(params)
    
    def _loader_params_pos_cashier_log(self):
        return {
            'search_params': {
                'domain': [('session_id', '=', self.id)],
                'fields': [
                    'id',
                    'session_id',
                    'employee_id',
                    'state',
                    'timestamp',
                ],
            }
        }

    def _get_pos_ui_pos_cashier_log(self, params):
        try:
            records = self.env['pos.cashier.log'].search_read(
                params['search_params'].get('domain', []),
                params['search_params']['fields']
            )

            for rec in records:
                if isinstance(rec['employee_id'], int):
                    emp = self.env['hr.employee'].browse(rec['employee_id'])
                    rec['employee_id'] = [rec['employee_id'], emp.name if emp else '']

                if isinstance(rec['session_id'], int):
                    session = self.env['pos.session'].browse(rec['session_id'])
                    rec['session_id'] = [rec['session_id'], session.name if session else '']
            return records
        except Exception as e:
            # _logger.error("Error loading pos.cashier.log: %s", e)
            return []

    def _pos_ui_pos_cashier_log(self, params):
        return self._get_pos_ui_pos_cashier_log(params)

    
    def _loader_params_end_shift(self):
        return {
            'search_params': {
                'domain': [],
                'fields': [
                    'id',
                    'session_id',
                    'cashier_id',
                    'start_date',
                    'end_date',
                    'state',
                    'line_ids',
                ],
            }
        }

    def _get_pos_ui_end_shift(self, params):
        try:
            records = self.env['end.shift'].search_read(
                params['search_params'].get('domain', []),
                params['search_params']['fields']
            )
            for rec in records:
                if isinstance(rec['line_ids'], list):
                    rec['line_ids'] = [int(x) for x in rec['line_ids']]
            return records
        except Exception as e:
            # _logger.error("Error loading end.shift: %s", e)
            return []

    def _pos_ui_end_shift(self, params):
        return self._get_pos_ui_end_shift(params)


    def _loader_params_product_product(self):
        res = super()._loader_params_product_product()
        res['search_params']['domain'] = [('available_in_pos', '=', True), ('default_code', '=', 'Discount001')]
        res['search_params']['fields'] += ['default_code']
        return res

    def _loader_params_res_config_settings(self):
        return {'search_params': {'fields': []}}

    def _get_pos_ui_res_config_settings(self, params):
        try:
            config = self.env['ir.config_parameter'].sudo()
            manager_id = config.get_param('pos.manager_id')
            manager = self.env['hr.employee'].browse(int(manager_id)) if manager_id and manager_id.isdigit() else None

            return [{
                'manager_validation': config.get_param('pos.manager_validation') == 'True',
                'validate_discount_amount': config.get_param('pos.validate_discount_amount') == 'True',
                'validate_end_shift': config.get_param('pos.validate_end_shift') == 'True',
                'validate_closing_pos': config.get_param('pos.validate_closing_pos') == 'True',
                'validate_order_line_deletion': config.get_param('pos.validate_order_line_deletion') == 'True',
                'validate_discount': config.get_param('pos.validate_discount') == 'True',
                'validate_price_change': config.get_param('pos.validate_price_change') == 'True',
                'validate_order_deletion': config.get_param('pos.validate_order_deletion') == 'True',
                'validate_add_remove_quantity': config.get_param('pos.validate_add_remove_quantity') == 'True',
                'validate_payment': config.get_param('pos.validate_payment') == 'True',
                'validate_refund': config.get_param('pos.validate_refund') == 'True',
                'validate_close_session': config.get_param('pos.validate_close_session') == 'True',
                'validate_void_sales': config.get_param('pos.validate_void_sales') == 'True',
                'validate_member_schedule': config.get_param('pos.validate_member_schedule') == 'True',
                'one_time_password': config.get_param('pos.one_time_password') == 'True',
                'multiple_barcode_activate': config.get_param('pos.multiple_barcode_activate') == 'True',
                'manager_pin': manager.pin if manager else '',
                'manager_name': manager.name if manager else '',
            }]
        except Exception as e:
            # _logger.error("Error loading res.config.settings: %s", e)
            return []

    def _loader_params_res_partner(self):
        return {
            'search_params': {
                'domain': self._get_partners_domain(),
                'fields': [
                    'name', 'street', 'city', 'state_id', 'country_id',
                    'vat', 'lang', 'phone', 'zip', 'mobile', 'email',
                    'barcode', 'write_date', 'property_account_position_id',
                    'property_product_pricelist', 'parent_name', 'category_id'
                ],
            }
        }

    def _get_pos_ui_res_partner(self, params):
        try:
            partners = self.env['res.partner'].search_read(
                params['search_params'].get('domain', []),
                params['search_params']['fields']
            )
            for partner in partners:
                if isinstance(partner['category_id'], list):
                    partner['category_id'] = [int(cid) for cid in partner['category_id']]
            return partners
        except Exception as e:
            # _logger.error("Error loading res.partner: %s", e)
            return []

    def _pos_ui_res_partner(self, params):
        return self._get_pos_ui_res_partner(params)

    def _loader_params_loyalty_program(self):
        return {
            'search_params': {
                'domain': [('active', '=', True)],
                'fields': [
                    'name', 'active', 'trigger', 'rules',
                    'is_nominative', 'limit_usage', 'total_order_count',
                    'max_usage', 'pricelist_ids'
                ],
            }
        }

    def _get_pos_ui_loyalty_program(self, params):
        try:
            programs = self.env['loyalty.program'].search_read(
                params['search_params'].get('domain', []),
                params['search_params']['fields']
            )
            for program in programs:
                program['active'] = True
            return programs
        except Exception as e:
            # _logger.error("Error loading loyalty.program: %s", e)
            return []

    def _pos_ui_loyalty_program(self, params):
        return self._get_pos_ui_loyalty_program(params)

    def _loader_params_loyalty_member(self):
        return {
            'search_params': {
                'domain': [('member_program_id.active', '=', True)],
                'fields': ['member_program_id', 'member_pos'],
            }
        }

    def _get_pos_ui_loyalty_member(self, params):
        try:
            records = self.env['loyalty.member'].search_read(
                params['search_params'].get('domain', []),
                params['search_params']['fields']
            )
            for rec in records:
                if isinstance(rec['member_program_id'], int):
                    rec['member_program_id'] = [rec['member_program_id'], '']
                if isinstance(rec['member_pos'], int):
                    rec['member_pos'] = [rec['member_pos'], '']
            return records
        except Exception as e:
            # _logger.error("Error loading loyalty.member: %s", e)
            return []

    def _pos_ui_loyalty_member(self, params):
        return self._get_pos_ui_loyalty_member(params)

    def _loader_params_loyalty_program_schedule(self):
        return {
            'search_params': {
                'domain': [('program_id.active', '=', True)],
                'fields': ['days', 'time_start', 'time_end', 'program_id'],
            }
        }

    def _get_pos_ui_loyalty_program_schedule(self, params):
        try:
            records = self.env['loyalty.program.schedule'].search(params['search_params'].get('domain', []))
            result = []
            for rec in records:
                if rec.program_id and rec.program_id.active:
                    result.append({
                        'id': rec.id,
                        'days': rec.days,
                        'time_start': rec.time_start,
                        'time_end': rec.time_end,
                        'program_id': [rec.program_id.id, rec.program_id.name],
                    })
            return result
        except Exception as e:
            # _logger.error("Error loading loyalty.program.schedule: %s", e)
            return []

    def _pos_ui_loyalty_program_schedule(self, params):
        return self._get_pos_ui_loyalty_program_schedule(params)