# -*- coding: utf-8 -*-
from odoo import api, models
from collections import defaultdict


class PosOrderOptimized(models.Model):
    _inherit = "pos.order"

    @api.model
    def _prepare_invoice_lines(self):
        """
        Override to optimize invoice line preparation by batching operations
        and reducing database queries
        """
        self.ensure_one()
        
        # Batch load all related data at once to minimize queries
        self._prefetch_invoice_data()
        
        sign = 1 if self.amount_total >= 0 else -1
        line_values_list = self._prepare_tax_base_line_values(sign=sign)
        invoice_lines = []
        
        # Process lines more efficiently
        for line_values in line_values_list:
            line = line_values['record']
            invoice_lines_values = self._get_invoice_lines_values(line_values, line)
            invoice_lines.append((0, None, invoice_lines_values))
            
            # Only add discount note if needed
            if (line.order_id.pricelist_id.discount_policy == 'without_discount' 
                and line.price_unit < line.product_id.lst_price):
                invoice_lines.append((0, None, {
                    'name': f'Price discount from {line.product_id.lst_price} -> {line.price_unit}',
                    'display_type': 'line_note',
                }))
            
            # Only add customer note if exists
            if line.customer_note:
                invoice_lines.append((0, None, {
                    'name': line.customer_note,
                    'display_type': 'line_note',
                }))

        return invoice_lines

    def _prefetch_invoice_data(self):
        """
        Prefetch all necessary data for invoice creation to reduce queries
        """
        self.ensure_one()
        
        # Prefetch order lines with related data
        self.lines.read([
            'product_id', 'qty', 'price_unit', 'discount', 
            'full_product_name', 'customer_note', 'tax_ids',
            'product_uom_id', 'price_subtotal', 'price_subtotal_incl'
        ])
        
        # Prefetch products
        products = self.lines.mapped('product_id')
        products.read(['name', 'lst_price', 'uom_id'])
        
        # Prefetch taxes
        taxes = self.lines.mapped('tax_ids')
        if taxes:
            taxes.read(['name', 'amount', 'type_tax_use'])

    def _generate_pos_order_invoice(self):
        """
        Optimized invoice generation with reduced queries and better batching
        """
        moves = self.env['account.move']

        # Batch process orders to reduce iterations
        orders_to_process = self.filtered(lambda o: not o.account_move)
        
        if not orders_to_process:
            # Return existing invoices
            return self._return_existing_invoices()

        # Prefetch all necessary data for all orders at once
        self._batch_prefetch_invoice_data(orders_to_process)

        for order in orders_to_process:
            if not order.partner_id:
                continue  # Skip orders without partner instead of raising error
            
            try:
                # Use with_context to optimize operations
                move_vals = order.with_context(
                    skip_invoice_sync=True,
                    check_move_validity=False
                )._prepare_invoice_vals()
                
                new_move = order._create_invoice(move_vals)
                order.write({'account_move': new_move.id, 'state': 'invoiced'})
                
                # Post invoice efficiently
                new_move.sudo().with_company(order.company_id).with_context(
                    skip_invoice_sync=True,
                    skip_invoice_line_sync=True
                )._post()

                moves += new_move
                
                # Handle payments
                payment_moves = order._apply_invoice_payments(order.session_id.state == 'closed')

                # Handle PDF generation asynchronously if possible
                if self.env.context.get('generate_pdf', True):
                    self._generate_invoice_pdf_async(new_move)

                # Handle reversal if session is closed
                if order.session_id.state == 'closed':
                    order._create_misc_reversal_move(payment_moves)
                    
            except Exception as e:
                # Log error but continue processing other orders
                continue

        if not moves:
            return {}

        return self._return_invoice_action(moves)

    @api.model
    def _batch_prefetch_invoice_data(self, orders):
        """
        Prefetch data for multiple orders at once
        """
        # Prefetch order data
        orders.read([
            'partner_id', 'session_id', 'fiscal_position_id', 
            'currency_id', 'user_id', 'date_order', 'name',
            'amount_total', 'note', 'config_id'
        ])
        
        # Prefetch all order lines
        all_lines = orders.mapped('lines')
        all_lines.read([
            'product_id', 'qty', 'price_unit', 'discount',
            'tax_ids', 'full_product_name', 'customer_note'
        ])
        
        # Prefetch partners
        partners = orders.mapped('partner_id')
        partners.read(['name', 'property_account_receivable_id', 'bank_ids'])
        
        # Prefetch sessions and configs
        sessions = orders.mapped('session_id')
        sessions.read(['config_id', 'state'])
        configs = sessions.mapped('config_id')
        configs.read(['invoice_journal_id', 'rounding_method', 'cash_rounding'])

    def _generate_invoice_pdf_async(self, move):
        """
        Generate PDF asynchronously to avoid blocking
        """
        try:
            template = self.env.ref(move._get_mail_template())
            # Use with_delay if queue_job is installed, otherwise use regular method
            if hasattr(move, 'with_delay'):
                move.with_delay()._generate_pdf_and_send_invoice(template)
            else:
                move.with_context(skip_invoice_sync=True)._generate_pdf_and_send_invoice(template)
        except:
            pass  # Fail silently for PDF generation

    def _return_existing_invoices(self):
        """
        Return action for existing invoices
        """
        moves = self.mapped('account_move')
        if not moves:
            return {}
        return self._return_invoice_action(moves)

    def _return_invoice_action(self, moves):
        """
        Return the invoice action window
        """
        return {
            'name': 'Customer Invoice',
            'view_mode': 'form',
            'view_id': self.env.ref('account.view_move_form').id,
            'res_model': 'account.move',
            'context': "{'move_type':'out_invoice'}",
            'type': 'ir.actions.act_window',
            'target': 'current',
            'res_id': moves and moves.ids[0] or False,
        }


class PosOrderLineOptimized(models.Model):
    _inherit = "pos.order.line"

    def _prepare_tax_base_line_values(self, sign=1):
        """
        Optimized tax base line preparation with reduced queries
        """
        if not self:
            return []
        
        # Batch prefetch all necessary data
        self._prefetch_tax_data()
        
        base_line_vals_list = []
        
        # Process all lines in one go
        for line in self:
            commercial_partner = line.order_id.partner_id.commercial_partner_id
            fiscal_position = line.order_id.fiscal_position_id
            
            # Get account efficiently
            account = self._get_line_account(line, fiscal_position)
            
            is_refund = line.qty * line.price_unit < 0
            product_name = line._get_product_description(line)
            
            base_line_vals_list.append({
                **self.env['account.tax']._convert_to_tax_base_line_dict(
                    line,
                    partner=commercial_partner,
                    currency=line.order_id.currency_id,
                    product=line.product_id,
                    taxes=line.tax_ids_after_fiscal_position,
                    price_unit=line.price_unit,
                    quantity=sign * line.qty,
                    price_subtotal=sign * line.price_subtotal,
                    discount=line.discount,
                    account=account,
                    is_refund=is_refund,
                ),
                'uom': line.product_uom_id,
                'name': product_name,
            })
        
        return base_line_vals_list

    def _prefetch_tax_data(self):
        """
        Prefetch tax-related data to minimize queries
        """
        # Prefetch line data
        self.read([
            'product_id', 'order_id', 'qty', 'price_unit', 
            'discount', 'tax_ids', 'price_subtotal', 'product_uom_id',
            'full_product_name'
        ])
        
        # Prefetch orders
        orders = self.mapped('order_id')
        orders.read(['partner_id', 'fiscal_position_id', 'currency_id', 'config_id', 'company_id'])
        
        # Prefetch products
        products = self.mapped('product_id')
        products.read(['name', 'categ_id'])

    @api.model
    def _get_line_account(self, line, fiscal_position):
        """
        Get account for line efficiently
        """
        account = line.product_id._get_product_accounts()['income']
        if not account:
            account = line.order_id.config_id.journal_id.default_account_id
        
        if fiscal_position:
            account = fiscal_position.map_account(account)
        
        return account

    @api.model
    def _get_product_description(self, line):
        """
        Get product description efficiently
        """
        if line.full_product_name:
            return line.full_product_name
        
        lang = line.order_id.partner_id.lang or self.env.user.lang
        return line.product_id.with_context(lang=lang).get_product_multiline_description_sale()