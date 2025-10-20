import logging
from datetime import datetime, timedelta
from odoo import models, fields, api
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

class ReportSaleDetailsInherit(models.AbstractModel):
    _inherit = "report.point_of_sale.report_saledetails"

    def get_sale_details(self, date_start=False, date_stop=False, config_ids=False, session_ids=False):
        """
        Override untuk:
        1. Menghilangkan SEMUA cash moves dari closing popup
        2. Menambahkan total modal ke expected cash amount
        """
        # Panggil bawaan dulu
        result = super().get_sale_details(date_start, date_stop, config_ids, session_ids)
        
        sessions = self.env["pos.session"].browse(session_ids) if session_ids else []
        
        for session in sessions:
            # Ambil total modal semua shift dalam session
            total_modal = sum(
                self.env["end.shift"].search([
                    ("session_id", "=", session.id)
                ]).mapped("modal")
            )

            # ✅ KOSONGKAN SEMUA CASH MOVES & UPDATE EXPECTED
            for payment in result.get("payments", []):
                if payment.get("session") == session.id:
                    # Kosongkan semua cash moves
                    payment["cash_moves"] = []
                    
                    # Update final_count untuk cash dengan modal
                    if payment.get("cash"):
                        payment["final_count"] = (payment.get("final_count") or 0.0) + total_modal
                        payment["money_difference"] = (payment.get("money_counted") or 0.0) - payment["final_count"]
        
        return result

class PosSession(models.Model):
    _inherit = 'pos.session'

    is_updated = fields.Boolean(string="Updated", default=False, readonly=True, tracking=True)
    name_session_pos = fields.Char(string="Name Session POS (Odoo Store)", readonly=True)
    id_mc = fields.Char(string="ID MC", default=False)
    vit_edit_start_balance = fields.Char(string="Edit Start Balance", tracking=True)
    vit_edit_end_balance = fields.Char(string="Edit End Balance", tracking=True)

    @api.onchange('vit_edit_start_balance')
    def _onchange_vit_edit_start_balance(self):
        """
        Auto-update cash_register_balance_start when vit_edit_start_balance changes
        Jika sudah ada nilai, akan dijumlahkan
        """
        if self.vit_edit_start_balance:
            try:
                # Convert string to float
                new_value = float(self.vit_edit_start_balance.replace(',', '.'))
                
                # Get current value (default 0 if not set)
                current_value = self.cash_register_balance_start or 0.0
                
                # Add new value to existing value
                total_value = current_value + new_value
                
                self.cash_register_balance_start = total_value
                
                _logger.info(
                    f"💰 balance_start updated: {current_value} + {new_value} = {total_value} "
                    f"for session {self.name}"
                )
            except ValueError:
                # Handle invalid input
                _logger.warning(
                    f"⚠️ Invalid value for start balance: {self.vit_edit_start_balance}"
                )
                # Optionally show warning to user
                return {
                    'warning': {
                        'title': 'Invalid Input',
                        'message': 'Please enter a valid number for Start Balance'
                    }
                }

    @api.onchange('vit_edit_end_balance')
    def _onchange_vit_edit_end_balance(self):
        """
        Auto-update cash_register_balance_end_real when vit_edit_end_balance changes
        Jika sudah ada nilai, akan dijumlahkan
        """
        if self.vit_edit_end_balance:
            try:
                # Convert string to float
                new_value = float(self.vit_edit_end_balance.replace(',', '.'))
                
                # Get current value (default 0 if not set)
                current_value = self.cash_register_balance_end_real or 0.0
                
                # Add new value to existing value
                total_value = current_value + new_value
                
                self.cash_register_balance_end_real = total_value
                
                _logger.info(
                    f"💰 balance_end_real updated: {current_value} + {new_value} = {total_value} "
                    f"for session {self.name}"
                )
            except ValueError:
                # Handle invalid input
                _logger.warning(
                    f"⚠️ Invalid value for end balance: {self.vit_edit_end_balance}"
                )
                # Optionally show warning to user
                return {
                    'warning': {
                        'title': 'Invalid Input',
                        'message': 'Please enter a valid number for End Balance'
                    }
                }

    def get_closing_control_data(self):
        """
        Override untuk menambahkan total_modal ke response
        dan update opening balance
        """
        result = super(PosSession, self).get_closing_control_data()
        
        # Calculate total modal from all end.shift in this session
        total_modal = sum(
            self.env['end.shift'].search([
                ('session_id', '=', self.id)
            ]).mapped('modal')
        )
        
        # Add total_modal to result
        result['total_modal'] = total_modal
        
        # Update cash details if cash control is enabled
        if self.config_id.cash_control and result.get('default_cash_details'):
            # IMPORTANT: Update opening to show total modal
            result['default_cash_details']['opening'] = total_modal
            
            # Recalculate expected amount
            cash_payment = result['default_cash_details'].get('payment_amount', 0)
            cash_moves = sum([move['amount'] for move in result['default_cash_details'].get('moves', [])])
            
            # Expected = Total Modal + Cash Payment + Cash Moves
            result['default_cash_details']['amount'] = total_modal + cash_payment + cash_moves
            
            # Add info about modal calculation
            result['default_cash_details']['modal_info'] = {
                'total_modal': total_modal,
                'cash_payments': cash_payment,
                'cash_moves': cash_moves,
                'expected_total': total_modal + cash_payment + cash_moves
            }
        
        return result

    def post_closing_cash_details(self, counted_cash):
        """
        Override untuk update cash_register_balance_start dengan total modal
        saat close session dari UI
        """
        self.ensure_one()
        
        check_closing_session = self._cannot_close_session()
        if check_closing_session:
            return check_closing_session

        if not self.cash_journal_id:
            raise UserError("There is no cash register in this session.")

        # ✅ Hitung total modal dari end.shift
        total_modal = sum(
            self.env['end.shift'].search([
                ('session_id', '=', self.id)
            ]).mapped('modal')
        )

        # ✅ Update balance_start dengan total modal (EDITABLE)
        self.cash_register_balance_start = total_modal
        
        # ✅ Update balance_end_real dengan counted_cash dari UI (EDITABLE)
        self.cash_register_balance_end_real = counted_cash

        _logger.info(
            f"✅ Session {self.name}: balance_start set to {total_modal}, "
            f"balance_end_real set to {counted_cash}"
        )

        return {'successful': True}

    def update_closing_balances(self, balance_start=None, balance_end_real=None):
        """
        Method baru untuk update manual balance_start dan balance_end_real
        Dipanggil dari UI atau backend saat user mengedit nilai
        
        :param balance_start: float - New balance start value
        :param balance_end_real: float - New balance end real value
        :return: True if successful
        """
        self.ensure_one()
        
        if self.state not in ['closing_control', 'opened']:
            raise UserError("Cannot update balances when session is not in closing state.")
        
        values = {}
        if balance_start is not None:
            values['cash_register_balance_start'] = balance_start
            _logger.info(f"💰 Updating balance_start to {balance_start} for session {self.name}")
            
        if balance_end_real is not None:
            values['cash_register_balance_end_real'] = balance_end_real
            _logger.info(f"💰 Updating balance_end_real to {balance_end_real} for session {self.name}")
            
        if values:
            self.write(values)
        
        return True

    def action_pos_session_closing_control(self, balancing_account=False, amount_to_balance=0, bank_payment_method_diffs=None):
        """
        Override untuk otomatis set balance_start dengan total modal
        saat transition ke closing_control state
        """
        bank_payment_method_diffs = bank_payment_method_diffs or {}
        
        for session in self:
            # ✅ Hitung total modal sebelum closing
            total_modal = sum(
                self.env['end.shift'].search([
                    ('session_id', '=', session.id)
                ]).mapped('modal')
            )
            
            # ✅ Update cash_register_balance_start dengan total modal
            if session.config_id.cash_control and total_modal > 0:
                session.cash_register_balance_start = total_modal
                _logger.info(
                    f"🔐 Session {session.name} closing: "
                    f"balance_start auto-set to {total_modal}"
                )
        
        return super(PosSession, self).action_pos_session_closing_control(
            balancing_account, amount_to_balance, bank_payment_method_diffs
        )

    def _pos_ui_models_to_load(self):
        res = super()._pos_ui_models_to_load()
        
        # Only add models that exist and are properly configured
        additional_models = []
        
        # Check if each model exists before adding
        model_checks = [
            'loyalty.program',
            'loyalty.program.schedule', 
            'loyalty.member',
            'loyalty.reward',
            'loyalty.rule',
            'res.partner',
            'res.config.settings',
            'res.company',
            'barcode.config',
            'hr.employee',
            'hr.employee.config.settings',
        ]
        
        for model_name in model_checks:
            try:
                if model_name in self.env:
                    # Test if model is accessible
                    self.env[model_name].check_access_rights('read', raise_exception=False)
                    additional_models.append(model_name)
                    _logger.info(f"✅ Added model {model_name} to POS UI models")
            except Exception as e:
                _logger.warning(f"⚠️ Skipping model {model_name}: {e}")
        
        res += additional_models
        return res

    # def _loader_params_pos_order_line(self):
    #     return {
    #         'search_params': {
    #             'domain': [],
    #             'fields': [
    #                 'id',
    #                 'order_id',
    #                 'product_id', 
    #                 'qty',
    #                 'price_unit',
    #                 'price_subtotal',
    #                 'price_subtotal_incl',
    #                 'discount',
    #                 'line_number',
    #             ],
    #         }
    #     }

    # def _get_pos_ui_pos_order_line(self, params):
    #     try:
    #         records = self.env['pos.order.line'].search_read(
    #             params['search_params'].get('domain', []),
    #             params['search_params']['fields'],
    #             limit=1000  # Add limit to prevent timeout
    #         )
            
    #         # Process relational fields
    #         for rec in records:
    #             # Handle order_id
    #             if rec.get('order_id'):
    #                 if isinstance(rec['order_id'], int):
    #                     order = self.env['pos.order'].browse(rec['order_id'])
    #                     rec['order_id'] = [rec['order_id'], order.name if order.exists() else '']
    #                 elif isinstance(rec['order_id'], list) and len(rec['order_id']) >= 2:
    #                     rec['order_id'] = [int(rec['order_id'][0]), str(rec['order_id'][1])]
                
    #             # Handle product_id  
    #             if rec.get('product_id'):
    #                 if isinstance(rec['product_id'], int):
    #                     product = self.env['product.product'].browse(rec['product_id'])
    #                     rec['product_id'] = [rec['product_id'], product.display_name if product.exists() else '']
    #                 elif isinstance(rec['product_id'], list) and len(rec['product_id']) >= 2:
    #                     rec['product_id'] = [int(rec['product_id'][0]), str(rec['product_id'][1])]
                    
    #             # Ensure line_number is properly set
    #             if 'line_number' not in rec or not rec['line_number']:
    #                 rec['line_number'] = 1
                    
    #         _logger.info(f"✅ Loaded {len(records)} pos.order.line records")
    #         return records
    #     except Exception as e:
    #         _logger.error(f"❌ Error loading pos.order.line: {e}")
    #         return []

    # def _pos_ui_pos_order_line(self, params):
    #     return self._get_pos_ui_pos_order_line(params)
    
    def _loader_params_res_company(self):
        return {
            'search_params': {
                'domain': [('id', '=', self.env.company.id)],
                'fields': [
                    'id', 'logo', 'name', 'street', 'street2', 'city', 'zip', 'country_id', 'vat', 
                ],
            }
        }

    def _get_pos_ui_res_company(self, params):
        try:
            records = self.env['res.company'].search_read(
                params['search_params']['domain'],
                params['search_params']['fields']
            )
            
            # Process relational fields
            for rec in records:
                if rec.get('country_id'):
                    if isinstance(rec['country_id'], int):
                        country = self.env['res.country'].browse(rec['country_id'])
                        rec['country_id'] = [rec['country_id'], country.name if country.exists() else '']
                    elif isinstance(rec['country_id'], list) and len(rec['country_id']) >= 2:
                        rec['country_id'] = [int(rec['country_id'][0]), str(rec['country_id'][1])]
                        
            _logger.info(f"✅ Loaded res.company")
            return records
        except Exception as e:
            _logger.error(f"❌ Error loading res.company: {e}")
            return []

    def _pos_ui_res_company(self, params):
        return self._get_pos_ui_res_company(params)
    
    def _loader_params_hr_employee(self):
        """Load HR employees for salesperson selection"""
        return {
            'search_params': {
                'domain': [],
                'fields': ['id', 'name', 'work_email', 'mobile_phone', 'job_title', 'pin', 'image_128'],
            }
        }

    def _get_pos_ui_hr_employee(self, params):
        try:
            if 'hr.employee' not in self.env:
                _logger.warning("⚠️ Model hr.employee not found")
                return []
                
            records = self.env['hr.employee'].search_read(
                params['search_params']['domain'],
                params['search_params']['fields'],
                limit=500
            )
            _logger.info(f"✅ Loaded {len(records)} hr.employee records")
            return records
        except Exception as e:
            _logger.error(f"❌ Error loading hr.employee: {e}")
            return []

    def _pos_ui_hr_employee(self, params):
        return self._get_pos_ui_hr_employee(params)

    def _loader_params_hr_employee_config_settings(self):
        return {
            'search_params': {
                'domain': [],
                'fields': ['id', 'employee_id', 'is_cashier'],
            }
        }

    def _get_pos_ui_hr_employee_config_settings(self, params):
        try:
            if 'hr.employee.config.settings' not in self.env:
                _logger.warning("⚠️ Model hr.employee.config.settings not found")
                return []
                
            records = self.env['hr.employee.config.settings'].search_read(
                params['search_params']['domain'],
                params['search_params']['fields'],
                limit=500
            )

            # Convert relational fields to [id, name]
            for rec in records:
                if rec.get('employee_id'):
                    if isinstance(rec['employee_id'], list) and len(rec['employee_id']) >= 2:
                        rec['employee_id'] = [int(rec['employee_id'][0]), str(rec['employee_id'][1])]
                    elif isinstance(rec['employee_id'], int):
                        employee = self.env['hr.employee'].browse(rec['employee_id'])
                        rec['employee_id'] = [rec['employee_id'], employee.name if employee.exists() else '']
                        
            _logger.info(f"✅ Loaded {len(records)} hr.employee.config.settings")
            return records
        except Exception as e:
            _logger.error(f"❌ Error loading hr.employee.config.settings: {e}")
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
            if 'multiple.barcode' not in self.env:
                _logger.warning("⚠️ Model multiple.barcode not found")
                return []
                
            records = self.env['multiple.barcode'].search_read(
                params['search_params']['domain'],
                params['search_params']['fields'],
                limit=5000
            )
            
            for rec in records:
                if rec.get('product_id'):
                    if isinstance(rec['product_id'], list) and len(rec['product_id']) >= 2:
                        rec['product_id'] = [int(rec['product_id'][0]), str(rec['product_id'][1])]
                    elif isinstance(rec['product_id'], int):
                        product = self.env['product.product'].browse(rec['product_id'])
                        rec['product_id'] = [rec['product_id'], product.display_name if product.exists() else '']
                        
            _logger.info(f"✅ Loaded {len(records)} multiple.barcode records")
            return records
        except Exception as e:
            _logger.error(f"❌ Error loading multiple.barcode: {e}")
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
            if 'barcode.config' not in self.env:
                _logger.warning("⚠️ Model barcode.config not found")
                return []
                
            records = self.env['barcode.config'].search_read(
                params['search_params']['domain'], 
                params['search_params']['fields'],
                limit=1
            )
            _logger.info(f"✅ Loaded barcode.config")
            return records
        except Exception as e:
            _logger.error(f"❌ Error loading barcode.config: {e}")
            return []

    def _pos_ui_barcode_config(self, params):
        return self._get_pos_ui_barcode_config(params)
    
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
            if 'pos.cashier.log' not in self.env:
                _logger.warning("⚠️ Model pos.cashier.log not found")
                return []
                
            records = self.env['pos.cashier.log'].search_read(
                params['search_params'].get('domain', []),
                params['search_params']['fields']
            )

            for rec in records:
                if rec.get('employee_id'):
                    if isinstance(rec['employee_id'], int):
                        emp = self.env['hr.employee'].browse(rec['employee_id'])
                        rec['employee_id'] = [rec['employee_id'], emp.name if emp.exists() else '']
                    elif isinstance(rec['employee_id'], list) and len(rec['employee_id']) >= 2:
                        rec['employee_id'] = [int(rec['employee_id'][0]), str(rec['employee_id'][1])]

                if rec.get('session_id'):
                    if isinstance(rec['session_id'], int):
                        session = self.env['pos.session'].browse(rec['session_id'])
                        rec['session_id'] = [rec['session_id'], session.name if session.exists() else '']
                    elif isinstance(rec['session_id'], list) and len(rec['session_id']) >= 2:
                        rec['session_id'] = [int(rec['session_id'][0]), str(rec['session_id'][1])]
                        
            _logger.info(f"✅ Loaded {len(records)} pos.cashier.log records")
            return records
        except Exception as e:
            _logger.error(f"❌ Error loading pos.cashier.log: {e}")
            return []

    def _pos_ui_pos_cashier_log(self, params):
        return self._get_pos_ui_pos_cashier_log(params)

    def _loader_params_res_config_settings(self):
        return {'search_params': {'fields': []}}

    def _get_pos_ui_res_config_settings(self, params):
        try:
            config = self.env['ir.config_parameter'].sudo()

            # Safely get manager_id
            manager_id = config.get_param('pos.manager_id')
            manager = None
            if manager_id and str(manager_id).isdigit():
                try:
                    manager = self.env['hr.employee'].browse(int(manager_id))
                    if not manager.exists():
                        manager = None
                except Exception:
                    manager = None

            # Get digits config safely
            total_digits = config.get_param('reward_point_total_digits', '16')
            decimal_digits = config.get_param('reward_point_decimal_digits', '4')

            result = {
                'manager_validation': config.get_param('pos.manager_validation', 'False') == 'True',
                'validate_discount_amount': config.get_param('pos.validate_discount_amount', 'False') == 'True',
                'validate_end_shift': config.get_param('pos.validate_end_shift', 'False') == 'True',
                'validate_closing_pos': config.get_param('pos.validate_closing_pos', 'False') == 'True',
                'validate_order_line_deletion': config.get_param('pos.validate_order_line_deletion', 'False') == 'True',
                'validate_discount': config.get_param('pos.validate_discount', 'False') == 'True',
                'validate_price_change': config.get_param('pos.validate_price_change', 'False') == 'True',
                'validate_order_deletion': config.get_param('pos.validate_order_deletion', 'False') == 'True',
                'validate_add_remove_quantity': config.get_param('pos.validate_add_remove_quantity', 'False') == 'True',
                'validate_payment': config.get_param('pos.validate_payment', 'False') == 'True',
                'validate_refund': config.get_param('pos.validate_refund', 'False') == 'True',
                'validate_close_session': config.get_param('pos.validate_close_session', 'False') == 'True',
                'validate_void_sales': config.get_param('pos.validate_void_sales', 'False') == 'True',
                'validate_member_schedule': config.get_param('pos.validate_member_schedule', 'False') == 'True',
                'validate_cash_drawer': config.get_param('pos.validate_cash_drawer', 'False') == 'True',
                'validate_reprint_receipt': config.get_param('pos.validate_reprint_receipt', 'False') == 'True',
                'validate_discount_button': config.get_param('pos.validate_discount_button', 'False') == 'True',
                'allow_multiple_global_discounts': config.get_param('pos.allow_multiple_global_discounts', 'False') == 'True',
                'one_time_password': config.get_param('pos.one_time_password', 'False') == 'True',
                'multiple_barcode_activate': config.get_param('pos.multiple_barcode_activate', 'False') == 'True',
                'validate_pricelist': config.get_param('pos.validate_pricelist', 'False') == 'True',
                'reward_point_total_digits': int(total_digits) if str(total_digits).isdigit() else 16,
                'reward_point_decimal_digits': int(decimal_digits) if str(decimal_digits).isdigit() else 4,
                'manager_pin': manager.pin if manager else '',
                'manager_name': manager.name if manager else '',
            }
            
            _logger.info("✅ Loaded res.config.settings")
            return [result]
        except Exception as e:
            _logger.error(f"❌ Error loading res.config.settings: {e}")
            return [{
                'manager_validation': False,
                'validate_discount_amount': False,
                'validate_end_shift': False,
                'validate_closing_pos': False,
                'validate_order_line_deletion': False,
                'validate_discount': False,
                'validate_price_change': False,
                'validate_order_deletion': False,
                'validate_add_remove_quantity': False,
                'validate_payment': False,
                'validate_refund': False,
                'validate_close_session': False,
                'validate_void_sales': False,
                'validate_member_schedule': False,
                'validate_cash_drawer': False,
                'validate_reprint_receipt': False,
                'validate_discount_button': False,
                'allow_multiple_global_discounts': False,
                'one_time_password': False,
                'multiple_barcode_activate': False,
                'validate_pricelist': False,
                'reward_point_total_digits': 16,
                'reward_point_decimal_digits': 4,
                'manager_pin': '',
                'manager_name': '',
            }]

    def _pos_ui_res_config_settings(self, params):
        return self._get_pos_ui_res_config_settings(params)

    def _loader_params_res_partner(self):
        try:
            domain = self._get_partners_domain() if hasattr(self, '_get_partners_domain') else []
        except Exception:
            domain = []
        
        # ✅ TAMBAHKAN: Pastikan default customer selalu ter-load
        if self.config_id.default_partner_id:
            default_partner_id = self.config_id.default_partner_id.id
            if domain:
                domain = ['|', ('id', '=', default_partner_id)] + domain
            else:
                domain = [('id', '=', default_partner_id)]
            
        return {
            'search_params': {
                'domain': domain,
                'fields': [
                    'name', 'street', 'city', 'state_id', 'country_id',
                    'vat', 'lang', 'phone', 'zip', 'mobile', 'email',
                    'barcode', 'write_date', 'property_account_position_id',
                    'property_product_pricelist', 'parent_name', 'category_id',
                    'is_store', 'vit_customer_group',
                ],
            }
        }

    def _get_pos_ui_res_partner(self, params):
        try:
            partners = self.env['res.partner'].search_read(
                params['search_params'].get('domain', []),
                params['search_params']['fields'],
                limit=1000,
                order='write_date DESC'
            )
            
            # ✅ VALIDASI: Log jika default customer ter-load
            if self.config_id.default_partner_id:
                default_id = self.config_id.default_partner_id.id
                is_loaded = any(p['id'] == default_id for p in partners)
                if is_loaded:
                    _logger.info(f"✅ Default customer {self.config_id.default_partner_id.name} loaded")
                else:
                    _logger.warning(f"⚠️ Default customer {self.config_id.default_partner_id.name} NOT loaded")
            
            for partner in partners:
                # Handle category_id
                if partner.get('category_id') and isinstance(partner['category_id'], list):
                    partner['category_id'] = [int(cid) for cid in partner['category_id'] if str(cid).isdigit()]
                    
                # Handle other relational fields
                for field in ['state_id', 'country_id', 'property_account_position_id', 'property_product_pricelist']:
                    if partner.get(field):
                        if isinstance(partner[field], int):
                            try:
                                if field == 'state_id':
                                    record = self.env['res.country.state'].browse(partner[field])
                                elif field == 'country_id':
                                    record = self.env['res.country'].browse(partner[field])
                                elif field == 'property_account_position_id':
                                    record = self.env['account.fiscal.position'].browse(partner[field])
                                elif field == 'property_product_pricelist':
                                    record = self.env['product.pricelist'].browse(partner[field])
                                
                                if record.exists():
                                    partner[field] = [partner[field], record.name]
                            except Exception:
                                pass
                        elif isinstance(partner[field], list) and len(partner[field]) >= 2:
                            partner[field] = [int(partner[field][0]), str(partner[field][1])]
                            
            _logger.info(f"✅ Loaded {len(partners)} res.partner records")
            return partners
        except Exception as e:
            _logger.error(f"❌ Error loading res.partner: {e}")
            return []

    def _pos_ui_res_partner(self, params):
        return self._get_pos_ui_res_partner(params)

    def _loader_params_loyalty_program(self):
        return {
            'search_params': {
                'domain': [('active', '=', True)],
                'fields': [
                    'name', 'program_type', 'active', 'trigger', 'rule_ids',
                    'is_nominative', 'limit_usage', 'total_order_count',
                    'max_usage', 'pricelist_ids', 'date_from', 'date_to',
                ],
            }
        }

    def _get_pos_ui_loyalty_program(self, params):
        try:
            if 'loyalty.program' not in self.env:
                _logger.warning("⚠️ Model loyalty.program not found")
                return []
                
            programs = self.env['loyalty.program'].search_read(
                params['search_params'].get('domain', []),
                params['search_params']['fields'],
                limit=100
            )
            
            for program in programs:
                program['active'] = True
                
                # Handle pricelist_ids
                if program.get('pricelist_ids') and isinstance(program['pricelist_ids'], list):
                    program['pricelist_ids'] = [int(pid) for pid in program['pricelist_ids'] if str(pid).isdigit()]
                
                # Handle rule_ids
                if program.get('rule_ids') and isinstance(program['rule_ids'], list):
                    program['rule_ids'] = [int(rid) for rid in program['rule_ids'] if str(rid).isdigit()]
                    
            _logger.info(f"✅ Loaded {len(programs)} loyalty.program records")
            return programs
        except Exception as e:
            _logger.error(f"❌ Error loading loyalty.program: {e}")
            return []

    def _pos_ui_loyalty_program(self, params):
        return self._get_pos_ui_loyalty_program(params)
    
    def _loader_params_loyalty_reward(self):
        return {
            'search_params': {
                'domain': [('program_id.active', '=', True)],
                'fields': ['name', 'reward_type', 'discount', 'program_id', 'reward_product_ids', 'discount_line_product_id'],
            }
        }

    def _get_pos_ui_loyalty_reward(self, params):
        try:
            if 'loyalty.reward' not in self.env:
                _logger.warning("⚠️ Model loyalty.reward not found")
                return []
            
            records = self.env['loyalty.reward'].search_read(
                params['search_params'].get('domain', []),
                params['search_params']['fields'],
                limit=500
            )

            for rec in records:
                # Process program_id
                if rec.get('program_id'):
                    if isinstance(rec['program_id'], int):
                        program = self.env['loyalty.program'].browse(rec['program_id'])
                        rec['program_id'] = [rec['program_id'], program.name if program.exists() else '']
                    elif isinstance(rec['program_id'], list) and len(rec['program_id']) >= 2:
                        rec['program_id'] = [int(rec['program_id'][0]), str(rec['program_id'][1])]

                # Process discount_line_product_id
                if rec.get('discount_line_product_id'):
                    if isinstance(rec['discount_line_product_id'], int):
                        product = self.env['product.product'].browse(rec['discount_line_product_id'])
                        if product.exists():
                            rec['discount_line_product_id'] = [product.id, product.display_name]
                            if not product.available_in_pos:
                                _logger.warning(
                                    f"⚠️ Discount product '{product.display_name}' "
                                    f"(ID {product.id}) not available in POS"
                                )
                        else:
                            rec['discount_line_product_id'] = False
                    elif isinstance(rec['discount_line_product_id'], list) and len(rec['discount_line_product_id']) >= 2:
                        rec['discount_line_product_id'] = [int(rec['discount_line_product_id'][0]), str(rec['discount_line_product_id'][1])]
                
                # Process reward_product_ids
                if rec.get('reward_product_ids') and isinstance(rec['reward_product_ids'], list):
                    rec['reward_product_ids'] = [int(pid) for pid in rec['reward_product_ids'] if str(pid).isdigit()]
                    
            _logger.info(f"✅ Loaded {len(records)} loyalty.reward records")
            return records
        except Exception as e:
            _logger.error(f"❌ Error loading loyalty.reward: {e}")
            return []

    def _pos_ui_loyalty_reward(self, params):
        return self._get_pos_ui_loyalty_reward(params)

    def _loader_params_loyalty_rule(self):
        return {
            'search_params': {
                'domain': [('program_id.active', '=', True)],
                'fields': ['name', 'program_id', 'reward_point_amount', 'reward_point_mode', 
                          'minimum_qty', 'minimum_amount', 'product_ids', 'product_domain'],
            }
        }

    def _get_pos_ui_loyalty_rule(self, params):
        try:
            if 'loyalty.rule' not in self.env:
                _logger.warning("⚠️ Model loyalty.rule not found")
                return []
                
            records = self.env['loyalty.rule'].search_read(
                params['search_params'].get('domain', []),
                params['search_params']['fields'],
                limit=500
            )
            
            for rec in records:
                # Process program_id
                if rec.get('program_id'):
                    if isinstance(rec['program_id'], int):
                        program = self.env['loyalty.program'].browse(rec['program_id'])
                        rec['program_id'] = [rec['program_id'], program.name if program.exists() else '']
                    elif isinstance(rec['program_id'], list) and len(rec['program_id']) >= 2:
                        rec['program_id'] = [int(rec['program_id'][0]), str(rec['program_id'][1])]
                
                # Process product_ids
                if rec.get('product_ids') and isinstance(rec['product_ids'], list):
                    rec['product_ids'] = [int(pid) for pid in rec['product_ids'] if str(pid).isdigit()]
                    
            _logger.info(f"✅ Loaded {len(records)} loyalty.rule records")
            return records
        except Exception as e:
            _logger.error(f"❌ Error loading loyalty.rule: {e}")
            return []

    def _pos_ui_loyalty_rule(self, params):
        return self._get_pos_ui_loyalty_rule(params)

    def _loader_params_loyalty_member(self):
        return {
            'search_params': {
                'domain': [('member_program_id.active', '=', True)],
                'fields': ['member_program_id', 'member_pos'],
            }
        }

    def _get_pos_ui_loyalty_member(self, params):
        try:
            if 'loyalty.member' not in self.env:
                _logger.warning("⚠️ Model loyalty.member not found")
                return []
                
            records = self.env['loyalty.member'].search_read(
                params['search_params'].get('domain', []),
                params['search_params']['fields'],
                limit=5000
            )
            
            for rec in records:
                # Process member_program_id
                if rec.get('member_program_id'):
                    if isinstance(rec['member_program_id'], int):
                        program = self.env['loyalty.program'].browse(rec['member_program_id'])
                        rec['member_program_id'] = [rec['member_program_id'], program.name if program.exists() else '']
                    elif isinstance(rec['member_program_id'], list) and len(rec['member_program_id']) >= 2:
                        rec['member_program_id'] = [int(rec['member_program_id'][0]), str(rec['member_program_id'][1])]
                    
                # Process member_pos
                if rec.get('member_pos'):
                    if isinstance(rec['member_pos'], int):
                        partner = self.env['res.partner'].browse(rec['member_pos'])
                        rec['member_pos'] = [rec['member_pos'], partner.name if partner.exists() else '']
                    elif isinstance(rec['member_pos'], list) and len(rec['member_pos']) >= 2:
                        rec['member_pos'] = [int(rec['member_pos'][0]), str(rec['member_pos'][1])]
                    
            _logger.info(f"✅ Loaded {len(records)} loyalty.member records")
            return records
        except Exception as e:
            _logger.error(f"❌ Error loading loyalty.member: {e}")
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
            if 'loyalty.program.schedule' not in self.env:
                _logger.warning("⚠️ Model loyalty.program.schedule not found")
                return []
                
            records = self.env['loyalty.program.schedule'].search(
                params['search_params'].get('domain', []),
                limit=100
            )
            
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
                    
            _logger.info(f"✅ Loaded {len(result)} loyalty.program.schedule records")
            return result
        except Exception as e:
            _logger.error(f"❌ Error loading loyalty.program.schedule: {e}")
            return []

    def _pos_ui_loyalty_program_schedule(self, params):
        return self._get_pos_ui_loyalty_program_schedule(params)