import time
from datetime import datetime, timedelta
import pytz
import re


# kalau ada case store nya beda zona waktu gimana
class DataTransaksiMCtoSS:
    def __init__(self, source_client, target_client):
        self.source_client = source_client
        self.target_client = target_client
        self.set_log_mc = SetLogMC(self.source_client)
        self.set_log_ss = SetLogSS(self.target_client)

    # Master Console --> Store Server
    # Store Server --> Master Console
    def transfer_transaksi(self, model_name, fields, description):
        # Ambil data dari sumber
        transaksi = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                             self.source_client.uid, self.source_client.password,
                                             model_name, 'search_read',
                                             [[
                                                 '|',
                                                 ['move_type', '=', 'out_invoice'],
                                                 ['move_type', '=', 'out_refund'],
                                                 ['state', '=', 'posted'],
                                                 '|',
                                                 ['payment_state', '=', 'paid'],
                                                 ['payment_state', '=', 'reversed']
                                             ]],
                                             {'fields': fields})

        if not transaksi:
            print("Tidak ada transaksi yang ditemukan untuk ditransfer.")
            return

        # Cari ID jurnal 'Customer Invoices' di database target
        journal_name = "Customer Invoices"
        journal_id = self.find_journal_id_by_name(journal_name)
        if not journal_id:
            print(f"Tidak dapat menemukan jurnal dengan nama '{journal_name}' di database target.")
            return

        # Kirim data ke target
        for record in transaksi:
            partner_id = record.get('partner_id')
            if not partner_id:
                print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'partner_id'.")
                continue

            partner_id = partner_id[0] if isinstance(partner_id, list) else partner_id

            # Cari customer di target berdasarkan partner_id dari sumber
            customer_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'res.partner', 'search_read',
                                                        [[['id', '=', partner_id]]],
                                                        {'fields': ['customer_code'], 'limit': 1})

            if not customer_source or 'customer_code' not in customer_source[0]:
                print(f"Tidak dapat menemukan 'customer_code' untuk partner_id {partner_id} di database sumber.")
                continue

            customer_code = customer_source[0]['customer_code']

            customers_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'res.partner', 'search_read',
                                                            [[['customer_code', '=', customer_code]]],
                                                            {'fields': ['id'], 'limit': 1})

            if not customers_target:
                print(f"Tidak dapat menemukan customer dengan 'customer_code' {customer_code} di database target.")
                message = f"Tidak dapat menemukan customer dengan 'customer_code' {customer_code} di database target."
                self.set_log_mc.create_log_note_failed(record, 'Invoice', message)
                self.set_log_ss.create_log_note_failed(record, 'Invoice', message)
                continue

            customer_target_id = customers_target[0]['id']

            existing_invoices = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'account.move', 'search_read',
                                                            [[['payment_reference', '=', record.get('name')], ['move_type', '=', 'out_invoice']]],
                                                            {'fields': ['id'], 'limit': 1})

            if not existing_invoices:
                # Ambil invoice line items dari sumber
                invoice_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'account.move.line', 'search_read',
                                                            [[['move_id', '=', record['id']]]],
                                                            {'fields': ['product_id', 'quantity', 'price_unit', 'name', 'account_id']})

                invoice_line_ids = []
                missing_products = []
                for line in invoice_lines:
                    product_id = line.get('product_id')
                    account_id = line.get('account_id')
                    if product_id:
                        product_id = product_id[0]  # product_id is a list [id, name], we need the id

                        # Get the default_code for the product in the source system
                        product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    'product.product', 'search_read',
                                                                    [[['id', '=', product_id]]],
                                                                    {'fields': ['default_code', 'taxes_id'], 'limit': 1})

                        if product_source and 'default_code' in product_source[0]:
                            default_code = product_source[0]['default_code']
                            taxes_ids = product_source[0].get('taxes_id', [])

                            # Search for the product in the target system using default_code
                            product_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'product.product', 'search_read',
                                                                        [[['default_code', '=', default_code]]],
                                                                        {'fields': ['id'], 'limit': 1})

                            if product_target:
                                product_id = product_target[0]['id']
                            else:
                                print(f"Tidak dapat menemukan product dengan 'default_code' {default_code} di database target.")
                                missing_products.append(default_code)
                                continue
                        else:
                            print(f"Tidak dapat menemukan 'default_code' untuk product_id {product_id} di database sumber.")
                            return
                    else:
                        print(f"Line item tidak memiliki 'product_id'.")
                        continue

                    tax_ids = []
                    for tax_id in taxes_ids:
                        tax_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'account.tax', 'search_read',
                                                                [[['id', '=', tax_id]]],
                                                                {'fields': ['name']})

                        if tax_source:
                            tax_name = tax_source[0]['name']

                            tax_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'account.tax', 'search_read',
                                                                    [[['name', '=', tax_name]]],
                                                                    {'fields': ['id'], 'limit': 1})

                            if tax_target:
                                tax_ids.append((tax_target[0]['id']))

                    if account_id:
                        account_id = account_id[0]  # account_id is a list [id, name], we need the id
                    else:
                        print(f"Line item tidak memiliki 'account_id'.")
                        continue

                    invoice_line_data = {
                        'product_id': product_id,
                        'name': line.get('name'),
                        'quantity': line.get('quantity'),
                        'price_unit': line.get('price_unit'),
                        'tax_ids': tax_ids,
                        # 'account_id': account_id,
                    }
                    invoice_line_ids.append((0, 0, invoice_line_data))

                if missing_products:
                    missing_products_str = ", ".join(missing_products)
                    message = f"Invoice dibatalkan karena produk tidak terdaftar: {missing_products_str}"
                    print(message)
                    self.set_log_mc.create_log_note_failed(record, 'Invoice', message)
                    self.set_log_ss.create_log_note_failed(record, 'Invoice', message)

                invoice_data = {
                    'partner_id': customer_target_id,
                    'journal_id': journal_id,
                    'invoice_date': record.get('invoice_date', False),
                    'invoice_date_due': record.get('invoice_date_due', False),
                    'payment_reference': record.get('name', False),
                    'ref': record.get('ref', False),
                    # 'state': record.get('state', False),
                    'payment_state': record.get('payment_state', False),
                    'move_type': record.get('move_type'),
                    'invoice_line_ids': invoice_line_ids,
                }

                try:
                    new_invoice_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'account.move', 'create',
                                                                [invoice_data])
                    print(f"Invoice baru telah dibuat dengan ID: {new_invoice_id}")

                    # Post the new invoice
                    self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'account.move', 'action_post',
                                                [new_invoice_id])
                    print(f"Invoice dengan ID: {new_invoice_id} telah diposting.")
                except Exception as e:
                    print(f"Error saat membuat atau memposting invoice: {e}")
                    if 'AccessError' in str(e):
                        print("Periksa izin pengguna di database target.")

    def transfer_discount_loyalty(self, model_name, fields, description):
        # Ambil data dari sumber
        discount_loyalty = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        model_name, 'search_read',
                                                        [[]],
                                                        {'fields': fields})
        if not discount_loyalty:
            print("Tidak ada discount/loyalty yang ditemukan untuk ditransfer.")
            return

        for record in discount_loyalty:
            existing_discount_loyalty = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'loyalty.program', 'search_read',
                [[['vit_trxid', '=', record['name']]]],  # Assuming 'name' is a unique identifier
                {'fields': ['id']}
            )

            if not existing_discount_loyalty:
                rules_ids_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                            self.source_client.uid, self.source_client.password,
                                                                            'loyalty.rule', 'search_read',
                                                                            [[['program_id', '=', record['id']]]],
                                                                            {'fields': ['minimum_qty', 'minimum_amount', 'reward_point_amount', 'reward_point_mode', 'product_domain', 'product_ids', 'product_category_id']})
                reward_ids_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                            self.source_client.uid, self.source_client.password,
                                                                            'loyalty.reward', 'search_read',
                                                                            [[['program_id', '=', record['id']]]],
                                                                            {'fields': ['reward_type', 'discount', 'discount_applicability', 'discount_max_amount', 'required_points', 'description', 'discount_product_domain', 'discount_product_ids', 'discount_product_category_id']})           
                discount_loyalty_line_ids = []
                for line in reward_ids_lines:
                    # Get discount_product_ids from source_client and match them in target_client
                    source_product_ids = line.get('discount_product_ids', [])
                    target_product_ids = []
                    if source_product_ids:
                        products_source = self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'product.product', 'search_read',
                            [[['id', 'in', source_product_ids]]],
                            {'fields': ['default_code']}
                        )
                        for product in products_source:
                            target_product = self.target_client.call_odoo(
                                'object', 'execute_kw', self.target_client.db,
                                self.target_client.uid, self.target_client.password,
                                'product.product', 'search',
                                [[['default_code', '=', product['default_code']]]]
                            )
                            if target_product:
                                target_product_ids.append(target_product[0])

                    # Get discount_product_category_id from source_client and match it in target_client
                    source_category_id = line.get('discount_product_category_id')
                    source_category_id = source_category_id[0] if isinstance(source_category_id, list) else source_category_id
                    target_category_id = None
                    if source_category_id:
                        category_source = self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'product.category', 'search_read',
                            [[['id', '=', source_category_id]]],
                            {'fields': ['complete_name'], 'limit': 1}
                        )
                        if category_source:
                            category_name = category_source[0]['complete_name']
                            category_target = self.target_client.call_odoo(
                                'object', 'execute_kw', self.target_client.db,
                                self.target_client.uid, self.target_client.password,
                                'product.category', 'search_read',
                                [[['complete_name', '=', category_name]]],
                                {'fields': ['id'], 'limit': 1}
                            )
                            if category_target:
                                target_category_id = category_target[0]['id']

                    discount_line_data = {
                        'reward_type': line.get('reward_type'),
                        'discount': line.get('discount'),
                        'discount_applicability': line.get('discount_applicability'),
                        'discount_max_amount': line.get('discount_max_amount'),
                        'required_points': line.get('required_points'),
                        'description': line.get('description'),
                        'discount_product_ids': [(6, 0, target_product_ids)],
                        'discount_product_category_id': target_category_id,
                    }
                    discount_loyalty_line_ids.append((0, 0, discount_line_data))

                rule_ids = []
                for rule in rules_ids_lines:
                    rule_product_ids = rule.get('product_ids', [])
                    rule_target_product_ids = []
                    if rule_product_ids:
                        rule_products_source = self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'product.product', 'search_read',
                            [[['id', 'in', rule_product_ids]]],
                            {'fields': ['default_code']}
                        )
                        for rule_product in rule_products_source:
                            rule_target_product = self.target_client.call_odoo(
                                'object', 'execute_kw', self.target_client.db,
                                self.target_client.uid, self.target_client.password,
                                'product.product', 'search',
                                [[['default_code', '=', rule_product['default_code']]]]
                            )
                            if rule_target_product:
                                rule_target_product_ids.append(rule_target_product[0])

                    rule_source_category_id = rule.get('product_category_id')
                    rule_source_category_id = rule_source_category_id[0] if isinstance(rule_source_category_id, list) else rule_source_category_id
                    rule_target_category_id = None
                    if rule_source_category_id:
                        rule_category_source = self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'product.category', 'search_read',
                            [[['id', '=', rule_source_category_id]]],
                            {'fields': ['complete_name'], 'limit': 1}
                        )
                        if rule_category_source:
                            rule_category_name = rule_category_source[0]['complete_name']
                            rule_category_target = self.target_client.call_odoo(
                                'object', 'execute_kw', self.target_client.db,
                                self.target_client.uid, self.target_client.password,
                                'product.category', 'search_read',
                                [[['complete_name', '=', rule_category_name]]],
                                {'fields': ['id'], 'limit': 1}
                            )
                            if rule_category_target:
                                rule_target_category_id = rule_category_target[0]['id']

                    rule_data = {
                        'minimum_qty': rule.get('minimum_qty'),
                        'minimum_amount': rule.get('minimum_amount'),
                        'reward_point_amount': rule.get('reward_point_amount'),
                        'reward_point_mode': rule.get('reward_point_mode'),
                        'product_domain': rule.get('product_domain'),
                        'product_ids': rule_target_product_ids,
                        'product_category_id': rule_target_category_id,
                        
                    }
                    rule_ids.append((0, 0, rule_data))

                currency_id = record.get('currency_id')
                currency_id = currency_id[0] if isinstance(currency_id, list) else currency_id

                currency_source = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'res.currency', 'search_read',
                    [[['id', '=', currency_id]]],
                    {'fields': ['name'], 'limit': 1}
                )

                if not currency_source or 'name' not in currency_source[0]:
                    print(f"Tidak dapat menemukan 'name' untuk currency_id {currency_source} di database sumber.")
                    continue

                currency_name = currency_source[0]['name']

                currency_target = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'res.currency', 'search_read',
                    [[['name', '=', currency_name]]],
                    {'fields': ['id'], 'limit': 1}
                )

                if not currency_target:
                    print(f"Tidak dapat menemukan 'name' {currency_name} di database target.")
                    continue

                currency_target_id = currency_target[0]['id']

                source_pricelist_ids = record.get('pricelist_ids', [])
                target_pricelist_ids = []
                if source_pricelist_ids:
                    pricelists_source = self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'product.pricelist', 'search_read',
                        [[['id', 'in', source_pricelist_ids]]],
                        {'fields': ['name']}
                    )
                    for pricelist in pricelists_source:
                        target_pricelist = self.target_client.call_odoo(
                            'object', 'execute_kw', self.target_client.db,
                            self.target_client.uid, self.target_client.password,
                            'product.pricelist', 'search',
                            [[['name', '=', pricelist['name']]]]
                        )
                        if target_pricelist:
                            target_pricelist_ids.append(target_pricelist[0])

                # Siapkan data untuk loyalty.program di target_client
                discount_data = {
                    'name': record.get('name'),
                    'program_type': record.get('program_type'),
                    'currency_id': currency_target_id,
                    'portal_point_name': record.get('portal_point_name'),
                    'portal_visible': record.get('portal_visible'),
                    'trigger': record.get('trigger'),
                    'applies_on': record.get('applies_on'),
                    'date_from': record.get('date_from'),
                    'date_to': record.get('date_to'),
                    'vit_trxid': record.get('name'),
                    'pricelist_ids': target_pricelist_ids,
                    'limit_usage': record.get('limit_usage'),
                    'is_integrated': True,
                    'pos_ok': record.get('pos_ok'),
                    'sale_ok': record.get('sale_ok'),
                    'reward_ids': discount_loyalty_line_ids,
                    'rule_ids': rule_ids,
                }

                try:
                    # Buat loyalty.program baru di target_client
                    new_discount_data = self.target_client.call_odoo(
                        'object', 'execute_kw', self.target_client.db,
                        self.target_client.uid, self.target_client.password,
                        'loyalty.program', 'create',
                        [discount_data]
                    )

                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'loyalty.program', 'write',
                        [[record['id']], {'is_integrated': True, 'vit_trxid': record['name']}],
                    )
                    print(f"Status is_integrated untuk discount/loyalty dengan ID {record['id']} telah diperbarui.")

                    print(f"Discount baru telah dibuat dengan ID: {new_discount_data}")
                except Exception as e:
                    print(f"Terjadi kesalahan saat membuat discount baru: {e}")
    
    def update_discount_loyalty(self, model_name, fields, description):
        # Ambil data dari sumber
        discount_loyalty = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        model_name, 'search_read',
                                                        [[['is_integrated', '=', True]]],
                                                        {'fields': fields})
        if not discount_loyalty:
            print("Tidak ada discount/loyalty yang ditemukan untuk ditransfer.")
            return

        for record in discount_loyalty:
            existing_record = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'loyalty.program', 'search_read',
                [[['vit_trxid', '=', record['vit_trxid']]]],  # Assuming 'vit_trxid' is a unique identifier
                {'fields': ['id']}
            )

            if existing_record:
                program_id = existing_record[0]['id']
                existing_reward_lines = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'loyalty.reward', 'search_read',
                                                                    [[['program_id', '=', program_id]]],
                                                                    {'fields': ['id', 'reward_type', 'discount', 'discount_applicability', 'discount_max_amount', 'required_points', 'description', 'discount_product_ids', 'discount_product_category_id']})
                existing_rule_lines = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'loyalty.rule', 'search_read',
                                                                [[['program_id', '=', program_id]]],
                                                                {'fields': ['id', 'minimum_qty', 'minimum_amount', 'reward_point_amount', 'reward_point_mode', 'product_domain', 'product_ids', 'product_category_id']})

                reward_ids_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'loyalty.reward', 'search_read',
                                                                [[['program_id', '=', record['id']]]],
                                                                {'fields': ['reward_type', 'discount', 'discount_applicability', 'discount_max_amount', 'required_points', 'description', 'discount_product_domain', 'discount_product_ids', 'discount_product_category_id']})
                rule_ids_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'loyalty.rule', 'search_read',
                                                            [[['program_id', '=', record['id']]]],
                                                            {'fields': ['minimum_qty', 'minimum_amount', 'reward_point_amount', 'reward_point_mode', 'product_domain', 'product_ids', 'product_category_id']})

                discount_loyalty_line_ids = []
                for line in reward_ids_lines:
                    # Get discount_product_ids from source_client and match them in target_client
                    source_product_ids = line.get('discount_product_ids', [])
                    target_product_ids = []
                    if source_product_ids:
                        products_source = self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'product.product', 'search_read',
                            [[['id', 'in', source_product_ids]]],
                            {'fields': ['default_code']}
                        )
                        for product in products_source:
                            target_product = self.target_client.call_odoo(
                                'object', 'execute_kw', self.target_client.db,
                                self.target_client.uid, self.target_client.password,
                                'product.product', 'search',
                                [[['default_code', '=', product['default_code']]]]
                            )
                            if target_product:
                                target_product_ids.append(target_product[0])

                    # Get discount_product_category_id from source_client and match it in target_client
                    source_category_id = line.get('discount_product_category_id')
                    source_category_id = source_category_id[0] if isinstance(source_category_id, list) else source_category_id
                    target_category_id = None
                    if source_category_id:
                        category_source = self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'product.category', 'search_read',
                            [[['id', '=', source_category_id]]],
                            {'fields': ['complete_name'], 'limit': 1}
                        )
                        if category_source:
                            category_name = category_source[0]['complete_name']
                            category_target = self.target_client.call_odoo(
                                'object', 'execute_kw', self.target_client.db,
                                self.target_client.uid, self.target_client.password,
                                'product.category', 'search_read',
                                [[['complete_name', '=', category_name]]],
                                {'fields': ['id'], 'limit': 1}
                            )
                            if category_target:
                                target_category_id = category_target[0]['id']

                    existing_line = next((x for x in existing_reward_lines if x['reward_type'] == line['reward_type']), None)
                    if existing_line:
                        discount_line_data = (1, existing_line['id'], {
                            'reward_type': line.get('reward_type'),
                            'discount': line.get('discount'),
                            'discount_applicability': line.get('discount_applicability'),
                            'discount_max_amount': line.get('discount_max_amount'),
                            'required_points': line.get('required_points'),
                            'description': line.get('description'),
                            'discount_product_ids': [(6, 0, target_product_ids)],
                            'discount_product_category_id': target_category_id,
                        })
                    else:
                        discount_line_data = (0, 0, {
                            'reward_type': line.get('reward_type'),
                            'discount': line.get('discount'),
                            'discount_applicability': line.get('discount_applicability'),
                            'discount_max_amount': line.get('discount_max_amount'),
                            'required_points': line.get('required_points'),
                            'description': line.get('description'),
                            'discount_product_ids': [(6, 0, target_product_ids)],
                            'discount_product_category_id': target_category_id,
                        })
                    discount_loyalty_line_ids.append(discount_line_data)

                rule_ids = []
                for rule in rule_ids_lines:
                    rule_product_ids = rule.get('product_ids', [])
                    rule_target_product_ids = []
                    if rule_product_ids:
                        rule_products_source = self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'product.product', 'search_read',
                            [[['id', 'in', rule_product_ids]]],
                            {'fields': ['default_code']}
                        )
                        for rule_product in rule_products_source:
                            rule_target_product = self.target_client.call_odoo(
                                'object', 'execute_kw', self.target_client.db,
                                self.target_client.uid, self.target_client.password,
                                'product.product', 'search',
                                [[['default_code', '=', rule_product['default_code']]]]
                            )
                            if rule_target_product:
                                rule_target_product_ids.append(rule_target_product[0])

                    rule_source_category_id = rule.get('product_category_id')
                    rule_source_category_id = rule_source_category_id[0] if isinstance(rule_source_category_id, list) else rule_source_category_id
                    rule_target_category_id = None
                    if rule_source_category_id:
                        rule_category_source = self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'product.category', 'search_read',
                            [[['id', '=', rule_source_category_id]]],
                            {'fields': ['complete_name'], 'limit': 1}
                        )
                        if rule_category_source:
                            rule_category_name = rule_category_source[0]['complete_name']
                            rule_category_target = self.target_client.call_odoo(
                                'object', 'execute_kw', self.target_client.db,
                                self.target_client.uid, self.target_client.password,
                                'product.category', 'search_read',
                                [[['complete_name', '=', rule_category_name]]],
                                {'fields': ['id'], 'limit': 1}
                            )
                            if rule_category_target:
                                rule_target_category_id = rule_category_target[0]['id']

                    existing_rule_line = next((x for x in existing_rule_lines if x['minimum_qty'] == rule['minimum_qty'] and x['minimum_amount'] == rule['minimum_amount']), None)
                    if existing_rule_line:
                        rule_data = (1, existing_rule_line['id'], {
                            'minimum_qty': rule.get('minimum_qty'),
                            'minimum_amount': rule.get('minimum_amount'),
                            'reward_point_amount': rule.get('reward_point_amount'),
                            'reward_point_mode': rule.get('reward_point_mode'),
                            'product_domain': rule.get('product_domain'),
                            'product_ids': rule_target_product_ids,
                            'product_category_id': rule_target_category_id,
                        })
                    else:
                        rule_data = (0, 0, {
                            'minimum_qty': rule.get('minimum_qty'),
                            'minimum_amount': rule.get('minimum_amount'),
                            'reward_point_amount': rule.get('reward_point_amount'),
                            'reward_point_mode': rule.get('reward_point_mode'),
                            'product_domain': rule.get('product_domain'),
                            'product_ids': rule_target_product_ids,
                            'product_category_id': rule_target_category_id,
                        })
                    rule_ids.append(rule_data)

                currency_id = record.get('currency_id')
                currency_id = currency_id[0] if isinstance(currency_id, list) else currency_id

                currency_source = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'res.currency', 'search_read',
                    [[['id', '=', currency_id]]],
                    {'fields': ['name'], 'limit': 1}
                )

                if not currency_source or 'name' not in currency_source[0]:
                    print(f"Tidak dapat menemukan 'name' untuk currency_id {currency_source} di database sumber.")
                    continue

                currency_name = currency_source[0]['name']

                currency_target = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'res.currency', 'search_read',
                    [[['name', '=', currency_name]]],
                    {'fields': ['id'], 'limit': 1}
                )

                if not currency_target:
                    print(f"Tidak dapat menemukan 'name' {currency_name} di database target.")
                    continue

                currency_target_id = currency_target[0]['id']

                source_pricelist_ids = record.get('pricelist_ids', [])
                target_pricelist_ids = []
                if source_pricelist_ids:
                    pricelists_source = self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'product.pricelist', 'search_read',
                        [[['id', 'in', source_pricelist_ids]]],
                        {'fields': ['name']}
                    )
                    for pricelist in pricelists_source:
                        target_pricelist = self.target_client.call_odoo(
                            'object', 'execute_kw', self.target_client.db,
                            self.target_client.uid, self.target_client.password,
                            'product.pricelist', 'search',
                            [[['name', '=', pricelist['name']]]]
                        )
                        if target_pricelist:
                            target_pricelist_ids.append(target_pricelist[0])

                # Siapkan data untuk loyalty.program di target_client
                update_values = {
                    'name': record.get('name'),
                    'program_type': record.get('program_type'),
                    'currency_id': currency_target_id,
                    'portal_point_name': record.get('portal_point_name'),
                    'portal_visible': record.get('portal_visible'),
                    'trigger': record.get('trigger'),
                    'applies_on': record.get('applies_on'),
                    'date_from': record.get('date_from'),
                    'date_to': record.get('date_to'),
                    # 'vit_trxid': record.get('name'),
                    'pricelist_ids': target_pricelist_ids,
                    'limit_usage': record.get('limit_usage'),
                    'is_integrated': True,
                    'pos_ok': record.get('pos_ok'),
                    'sale_ok': record.get('sale_ok'),
                    'reward_ids': discount_loyalty_line_ids,
                    'rule_ids': rule_ids,
                }
                
                self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                            self.target_client.uid, self.target_client.password,
                                            model_name, 'write',
                                            [[program_id], update_values])
                print(f"Record dengan ID {record['id']} telah diupdate.")

            # else:
            #     # Jika record tidak ada, buat record baru
            #     new_values = {
            #         'vit_trxid': record.get('name'),
            #         'name': record.get('name'),
            #         'program_type': record.get('program_type'),
            #         # 'currency_id': record.get('currency_id'),
            #         'portal_point_name': record.get('portal_point_name'),
            #         'portal_visible': record.get('portal_visible'),
            #         'trigger': record.get('trigger'),
            #         'applies_on': record.get('applies_on'),
            #         'date_from': record.get('date_from'),
            #         'date_to': record.get('date_to'),
            #         'pricelist_ids': record.get('pricelist_ids'),
            #         'limit_usage': record.get('limit_usage'),
            #         'pos_ok': record.get('pos_ok'),
            #         'sale_ok': record.get('sale_ok'),
            #     }
                
            #     self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
            #                                 self.target_client.uid, self.target_client.password,
            #                                 model_name, 'create',
            #                                 [new_values])
            #     print(f"Record baru dengan ID {record['id']} telah dibuat.")





    def find_journal_id_by_name(self, journal_name):
        # Cari ID jurnal berdasarkan namanya di database target
        journal_ids = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                   self.target_client.uid, self.target_client.password,
                                                   'account.journal', 'search',
                                                   [[('name', '=', journal_name)]])
        return journal_ids[0] if journal_ids else None  # Mengembalikan ID jurnal pertama yang ditemukan, jika ada

    def transfer_pos_order_inventory(self, model_name, fields, description):
        # Ambil data dari sumber
        transaksi_posorder = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        model_name, 'search_read',
                                                        [[['picking_type_id.name', '=', 'PoS Orders'], ['is_integrated', '=', False], ['state', '=', 'done']]],
                                                        {'fields': fields})

        if not transaksi_posorder:
            print("Tidak ada transaksi yang ditemukan untuk ditransfer.")
            return

        # Kirim data ke target
        for record in transaksi_posorder:
            partner_id = record.get('partner_id')
            if not partner_id:
                print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'partner_id'.")
                continue

            partner_id = partner_id[0] if isinstance(partner_id, list) else partner_id

            # Cari customer di target berdasarkan partner_id dari sumber
            customer_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'res.partner', 'search_read',
                                                        [[['id', '=', partner_id]]],
                                                        {'fields': ['customer_code'], 'limit': 1})

            if not customer_source or 'customer_code' not in customer_source[0]:
                print(f"Tidak dapat menemukan 'customer_code' untuk partner_id {partner_id} di database sumber.")
                continue

            customer_code = customer_source[0]['customer_code']

            customers_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'res.partner', 'search_read',
                                                            [[['customer_code', '=', customer_code]]],
                                                            {'fields': ['id'], 'limit': 1})

            if not customers_target:
                print(f"Tidak dapat menemukan customer dengan 'customer_code' {customer_code} di database target.")
                continue

            customer_target_id = customers_target[0]['id']

            existing_pos_order_inventory = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'stock.picking', 'search_read',
                                                                        [[['vit_trxid', '=', record.get('origin')], ['picking_type_id.name', '=', 'PoS Orders']]],
                                                                        {'fields': ['id'], 'limit': 1})

            if not existing_pos_order_inventory:
                # Ambil invoice line items dari sumber
                location_id = record.get('location_id')
                if not location_id:
                    print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'location_id'.")
                    continue

                location_id = location_id[0] if isinstance(location_id, list) else location_id

                location_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.location', 'search_read',
                                                            [[['id', '=', location_id]]],
                                                            {'fields': ['complete_name'], 'limit': 1})
                
                if not location_source or 'complete_name' not in location_source[0]:
                    print(f"Tidak dapat menemukan 'complete_name' untuk location_id {location_id} di database sumber.")
                    continue

                complete_name = location_source[0]['complete_name']

                location_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['complete_name', '=', complete_name]]],
                                                                {'fields': ['id'], 'limit': 1})

                if not location_target:
                    print(f"Tidak dapat menemukan 'complete_name' {complete_name} di database target.")
                    continue

                location_target_id = location_target[0]['id']

                location_dest_id = record.get('location_dest_id')
                if not location_dest_id:
                    print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'location_dest_id'.")
                    continue

                location_dest_id = location_dest_id[0] if isinstance(location_dest_id, list) else location_dest_id

                location_dest_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.location', 'search_read',
                                                            [[['id', '=', location_dest_id]]],
                                                            {'fields': ['complete_name'], 'limit': 1})
                
                if not location_dest_source or 'complete_name' not in location_dest_source[0]:
                    print(f"Tidak dapat menemukan 'complete_name' untuk location_id {location_dest_id} di database sumber.")
                    continue

                complete_name_dest = location_dest_source[0]['complete_name']

                location_dest_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['complete_name', '=', complete_name_dest]]],
                                                                {'fields': ['id'], 'limit': 1})

                if not location_dest_target:
                    print(f"Tidak dapat menemukan 'complete_name' {complete_name_dest} di database target.")
                    continue

                location_dest_target_id = location_dest_target[0]['id']
                
                pos_order_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', '=', record['id']]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

                pos_order_inventory_line_ids = []
                for line in pos_order_inventory_lines:
                    product_id = line.get('product_id')
                    if product_id:
                        product_id = product_id[0]  # product_id is a list [id, name], we need the id

                        # Get the default_code for the product in the source system
                        product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    'product.product', 'search_read',
                                                                    [[['id', '=', product_id]]],
                                                                    {'fields': ['default_code'], 'limit': 1})

                        if product_source and 'default_code' in product_source[0]:
                            default_code = product_source[0]['default_code']

                            # Search for the product in the target system using default_code
                            product_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'product.product', 'search_read',
                                                                        [[['default_code', '=', default_code]]],
                                                                        {'fields': ['id'], 'limit': 1})

                            if product_target:
                                product_id = product_target[0]['id']
                            else:
                                print(f"Tidak dapat menemukan product dengan 'default_code' {default_code} di database target.")
                                continue
                        else:
                            print(f"Tidak dapat menemukan 'default_code' untuk product_id {product_id} di database sumber.")
                            continue
                    else:
                        print(f"Line item tidak memiliki 'product_id'.")
                        continue

                    pos_order_inventory_line_data = {
                        'product_id': product_id,
                        'product_uom_qty': line.get('product_uom_qty'),
                        'name': line.get('name'),
                        'quantity': line.get('quantity'),
                        'location_dest_id': location_dest_target_id,
                        'location_id': location_target_id
                    }
                    pos_order_inventory_line_ids.append((0, 0, pos_order_inventory_line_data))

                # Nama tipe picking yang relevan (misalnya 'Receipts' untuk penerimaan barang)
                picking_type_name = 'PoS Orders'

                # Cari tipe picking di target_client berdasarkan nama
                picking_types = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.picking.type', 'search_read',
                                                            [[['name', '=', picking_type_name]]],
                                                            {'fields': ['id'], 'limit': 1})

                if not picking_types:
                    print(f"Tidak dapat menemukan tipe picking dengan nama '{picking_type_name}' di database target.")
                    continue

                picking_type_id = picking_types[0]['id']

                pos_order_data = {
                    'name': record.get('name', False) + ' - ' + datetime.now(pytz.timezone('Asia/Jakarta')).strftime("%Y-%m-%d %H:%M:%S"),
                    'partner_id': customer_target_id,
                    'scheduled_date': record.get('scheduled_date', False),
                    'date_done': record.get('date_done', False),
                    'vit_trxid': record.get('origin', False),
                    'location_id': location_target_id,
                    'location_dest_id': location_dest_target_id,
                    'picking_type_id': picking_type_id,
                    'move_ids_without_package': pos_order_inventory_line_ids,
                }

                try:
                    new_pos_order_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking', 'create',
                                                                    [pos_order_data])
                    print(f"Pos Order baru telah dibuat dengan ID: {new_pos_order_id}")

                    # Post the new invoice
                    self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'stock.picking', 'button_validate',
                                                [new_pos_order_id])
                    print(f"Invoice dengan ID: {new_pos_order_id} telah diposting.")
                except Exception as e:
                    print(f"Gagal membuat atau memposting Pos Order baru: {e}")

    def transfer_pos_order_invoice(self, model_name, fields, description):
        # Ambil data dari sumber
        transaksi_posorder_invoice = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                model_name, 'search_read',
                                                                [[['state', '=', 'invoiced']]],
                                                                {'fields': fields})

        if not transaksi_posorder_invoice:
            print("Tidak ada transaksi yang ditemukan untuk ditransfer.")
            return

        # Kirim data ke target
        for record in transaksi_posorder_invoice:
            partner_id = record.get('partner_id')
            if not partner_id:
                print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'partner_id'.")
                continue

            partner_id = partner_id[0] if isinstance(partner_id, list) else partner_id

            # Cari customer di target berdasarkan partner_id dari sumber
            customer_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'res.partner', 'search_read',
                                                        [[['id', '=', partner_id]]],
                                                        {'fields': ['customer_code'], 'limit': 1})

            if not customer_source or 'customer_code' not in customer_source[0]:
                print(f"Tidak dapat menemukan 'customer_code' untuk partner_id {partner_id} di database sumber.")
                continue

            customer_code = customer_source[0]['customer_code']

            customers_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'res.partner', 'search_read',
                                                            [[['customer_code', '=', customer_code]]],
                                                            {'fields': ['id'], 'limit': 1})

            if not customers_target:
                print(f"Tidak dapat menemukan customer dengan 'customer_code' {customer_code} di database target.")
                continue

            customer_target_id = customers_target[0]['id']

            # Session ID
            session_id = record.get('session_id')
            if not session_id:
                print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'session_id'.")
                continue

            session_id = session_id[0] if isinstance(session_id, list) else session_id

            # Cari session di target berdasarkan session_id dari sumber
            session_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.session', 'search_read',
                                                        [[['id', '=', session_id]]],
                                                        {'fields': ['name'], 'limit': 1})

            if not session_source or 'name' not in session_source[0]:
                print(f"Tidak dapat menemukan 'name' untuk session_id {session_id} di database sumber.")
                continue

            session_name = session_source[0]['name']

            session_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'pos.session', 'search_read',
                                                        [[['name_session_pos', '=', session_name]]],
                                                        {'fields': ['id'], 'limit': 1})

            if not session_target:
                print(f"Tidak dapat menemukan session dengan 'session_name' {session_name} di database target.")
                continue

            session_target_id = session_target[0]['id']

            existing_pos_order_invoice = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'pos.order', 'search_read',
                                                                    [[['order_ref', '=', record.get('name')]]],
                                                                    {'fields': ['id'], 'limit': 1})

            if not existing_pos_order_invoice:
                pos_order_invoice_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'pos.order.line', 'search_read',
                                                                        [[['order_id', '=', record['id']]]],
                                                                        {'fields': ['product_id', 'full_product_name', 'qty', 'price_unit', 'tax_ids_after_fiscal_position', 'discount', 'price_subtotal', 'price_subtotal_incl']})
                pos_order_invoice_line_ids = []
                pos_order_payment_ids = []
                missing_products = []
                total_tax = 0  # Initialize total tax

                # Check if all products exist in the target database
                for line in pos_order_invoice_lines:
                    product_id_info = line.get('product_id')  # product_id is a list [id, name]

                    if product_id_info:
                        product_id = product_id_info[0]  # Get the id part

                        # Get the default_code for the product in the source system
                        product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    'product.product', 'search_read',
                                                                    [[['id', '=', product_id]]],
                                                                    {'fields': ['default_code', 'taxes_id'], 'limit': 1})

                        if product_source and 'default_code' in product_source[0]:
                            default_code = product_source[0]['default_code']
                            taxes_ids = product_source[0].get('taxes_id', [])

                            # Search for the product in the target system using default_code
                            product_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'product.product', 'search_read',
                                                                        [[['default_code', '=', default_code]]],
                                                                        {'fields': ['id'], 'limit': 1})

                            if product_target:
                                product_target_id = product_target[0]['id']
                            else:
                                print(f"Tidak dapat menemukan product dengan 'default_code' {default_code} di database target.")
                                missing_products.append(default_code)
                                continue
                        else:
                            print(f"Tidak dapat menemukan 'default_code' untuk product_id {product_id} di database sumber.")
                            return
                    else:
                        print(f"Line item tidak memiliki 'product_id'.")
                        continue

                    tax_source = self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'account.tax', 'search_read',
                        [[]],
                        {'fields': ['name']}
                    )

                    tax_ids = []
                    for rec in tax_source:
                        name = rec.get('name')

                        tax_target = self.target_client.call_odoo(
                            'object', 'execute_kw', self.target_client.db,
                            self.target_client.uid, self.target_client.password,
                            'account.tax', 'search_read',
                            [[['name', '=', name]]],
                            {'fields': ['id']}
                        )

                        if tax_target:  # Check if the list is not empty
                            tax_ids.append(tax_target[0]['id'])
                            print("Menemukan Pajak")
                        else:
                            print("Pajak Tidak Ditemukan")

                    pos_order_line_data = {
                        'product_id': product_target_id,
                        'discount': line.get('discount'),
                        'full_product_name': line.get('full_product_name'),
                        'qty': line.get('qty'),
                        'price_unit': line.get('price_unit'),
                        'price_subtotal': line.get('price_subtotal'),
                        'price_subtotal_incl': line.get('price_subtotal_incl'),
                        'tax_ids': [(6, 0, tax_ids)],
                    }
                    pos_order_invoice_line_ids.append((0, 0, pos_order_line_data))

                # If there are missing products, log the error and continue with the next record
                if missing_products:
                    missing_products_str = ", ".join(missing_products)
                    message = f"Terdapat produk tidak aktif dalam invoice: {missing_products_str}"
                    print(message)
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Invoice', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Invoice', message, write_date)

                # Ambil data pembayaran dari sumber
                pos_order_payments = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'pos.payment', 'search_read',
                                                                [[['pos_order_id', '=', record['id']]]],
                                                                {'fields': ['amount', 'payment_date', 'payment_method_id']})

                amount_paid = 0
                for payment in pos_order_payments:
                    amount_paid += payment.get('amount')
                    payment_method_id = payment.get('payment_method_id')
                    if payment_method_id:
                        payment_method_id = payment_method_id[0]  # payment_method_id is a list [id, name], we need the id

                        # Get the name for the payment method in the source system
                        payment_method_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                            self.source_client.uid, self.source_client.password,
                                                                            'pos.payment.method', 'search_read',
                                                                            [[['id', '=', payment_method_id]]],
                                                                            {'fields': ['name'], 'limit': 1})

                        if payment_method_source and 'name' in payment_method_source[0]:
                            payment_method_name = payment_method_source[0]['name']

                            # Search for the payment method in the target system using name
                            payment_method_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                                self.target_client.uid, self.target_client.password,
                                                                                'pos.payment.method', 'search_read',
                                                                                [[['name', '=', payment_method_name]]],
                                                                                {'fields': ['id'], 'limit': 1})

                            if payment_method_target:
                                payment_method_id = payment_method_target[0]['id']
                            else:
                                print(f"Tidak dapat menemukan payment method dengan 'name' {payment_method_name} di database target.")
                                continue
                        else:
                            print(f"Tidak dapat menemukan 'name' untuk payment_method_id {payment_method_id} di database sumber.")
                            continue
                    else:
                        print(f"Payment tidak memiliki 'payment_method_id'.")
                        continue

                    pos_order_payment_data = {
                        'amount': payment.get('amount'),
                        'payment_date': payment.get('payment_date'),
                        'payment_method_id': payment_method_id,
                    }
                    pos_order_payment_ids.append((0, 0, pos_order_payment_data))

                pos_order_data = {
                    'name': record.get('name'),
                    'pos_reference': record.get('pos_reference'),
                    'order_ref': record.get('name'),
                    'partner_id': customer_target_id,
                    'session_id': session_target_id,
                    'date_order': record.get('date_order', False),
                    'amount_tax': record.get('amount_tax'),
                    'amount_total': amount_paid,
                    'amount_paid': amount_paid,
                    'amount_return': record.get('amount_return'),
                    'tracking_number': record.get('tracking_number'),
                    'margin': record.get('margin'),
                    'state': record.get('state'),
                    # 'is_integrated': True,
                    'lines': pos_order_invoice_line_ids,
                    'payment_ids': pos_order_payment_ids,
                }

                # start_time = time.time()
                new_pos_order_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'pos.order', 'create',
                                                                [pos_order_data])

                print(f"Pos Order baru telah dibuat dengan ID: {new_pos_order_id}")

                self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                            self.target_client.uid, self.target_client.password,
                                            'pos.order', 'action_pos_order_invoice',
                                            [[new_pos_order_id]])
                print(f"Tombol action_pos_order_invoice telah dijalankan untuk Pos Order ID: {new_pos_order_id}")
                # end_time = time.time()
                # duration = end_time - start_time

                # write_date = self.get_write_date(model_name, record['id'])
                # self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Invoice', write_date)
                # self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Invoice', write_date)

                # if amount_paid < 0:
                #     self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                #                                 self.target_client.uid, self.target_client.password,
                #                                 'pos.order', 'action_credit_note_invoice',
                #                                 [[new_pos_order_id]])
                #     print(f"Tombol action_credit_note_invoice telah dijalankan untuk Pos Order ID: {new_pos_order_id}")
                # else:

                # # Update the tax IDs in the target database
                # self.target_client.call_odoo(
                #     'object', 'execute_kw', self.target_client.db, self.target_client.uid, self.target_client.password,
                #     'pos.order.line', 'write', [[new_pos_order_id], {'tax_ids_after_fiscal_position': [(6, 0, [3])]}]
                # )

    def transfer_pos_order_session(self, model_name, fields, description):
        # Ambil data dari sumber
        transaksi_posorder_session = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        model_name, 'search_read',
                                                        [[]],
                                                        {'fields': fields})

        if not transaksi_posorder_session:
            print("Tidak ada transaksi yang ditemukan untuk ditransfer.")
            return

        # Kirim data ke target
        for record in transaksi_posorder_session:
            config_id = record.get('config_id')
            if not config_id:
                print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'config_id'.")
                continue

            config_id = config_id[0] if isinstance(config_id, list) else config_id

            # Cari customer di target berdasarkan partner_id dari sumber
            config_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.config', 'search_read',
                                                        [[['id', '=', config_id]]],
                                                        {'fields': ['name'], 'limit': 1})

            if not config_source or 'name' not in config_source[0]:
                print(f"Tidak dapat menemukan 'name' untuk partner_id {config_id} di database sumber.")
                continue

            name_session = config_source[0]['name']

            session_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'pos.config', 'search_read',
                                                            [[['name', '=', name_session]]],
                                                            {'fields': ['id'], 'limit': 1})

            if not session_target:
                print(f"Tidak dapat menemukan session dengan 'namee' {name_session} di database target.")
                continue

            session_target_id = session_target[0]['id']

            #User ID
            user_id = record.get('user_id')
            if not user_id:
                print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'config_id'.")
                continue

            user_id = user_id[0] if isinstance(user_id, list) else user_id

            # Cari customer di target berdasarkan partner_id dari sumber
            user_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'res.users', 'search_read',
                                                        [[['id', '=', user_id]]],
                                                        {'fields': ['name'], 'limit': 1})

            if not user_source or 'name' not in user_source[0]:
                print(f"Tidak dapat menemukan 'name' untuk partner_id {user_id} di database sumber.")
                continue

            user_session = user_source[0]['name']

            user_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'res.users', 'search_read',
                                                            [[['name', '=', user_session]]],
                                                            {'fields': ['id'], 'limit': 1})

            if not user_target:
                print(f"Tidak dapat menemukan session dengan 'name' {user_session} di database target.")
                continue

            user_target_id = user_target[0]['id']

            # #Name Session
            # name = record.get('name')
            # if not name:
            #     print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'name'.")
            #     continue

            # name = name[0] if isinstance(name, list) else name

            # # Cari customer di target berdasarkan partner_id dari sumber
            # name_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
            #                                             self.source_client.uid, self.source_client.password,
            #                                             'pos.session', 'search_read',
            #                                             [[['name', '=', name]]],
            #                                             {'fields': ['name'], 'limit': 1})

            # if not name_source or 'name' not in name_source[0]:
            #     continue

            # names_session = name_source[0]['name']

            existing_pos_order_invoice = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'pos.session', 'search_read',
                                                                        [[['name_session_pos', '=', record.get('name')]]],
                                                                        {'fields': ['id'], 'limit': 1})
            if not existing_pos_order_invoice:
                cash_register_balance_start = record.get('cash_register_balance_start')
                cash_register_balance_end_real = record.get('cash_register_balance_end_real')

                # Debugging prints
                print(f"Cash Register Balance Start: {cash_register_balance_start}")
                print(f"Cash Register Balance End Real: {cash_register_balance_end_real}")

                # Ensure monetary values are properly handled
                cash_register_balance_start = float(cash_register_balance_start) if cash_register_balance_start else 0.0
                cash_register_balance_end_real = float(cash_register_balance_end_real) if cash_register_balance_end_real else 0.0
                
                if not existing_pos_order_invoice:
                    pos_session_data = {
                        'name_session_pos': record.get('name'),
                        'config_id': session_target_id,
                        'user_id': user_target_id,
                        'start_at': record.get('start_at'),
                        'stop_at': record.get('stop_at'),
                        'cash_register_balance_start': cash_register_balance_start,
                        'cash_register_balance_end_real': cash_register_balance_end_real,
                        'state': record.get('state'),
                    }

                try:
                    start_time = time.time()
                    new_session_pos_order_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'pos.session', 'create',
                                                                    [pos_session_data])
                    print(f"Pos Order Session baru telah dibuat dengan ID: {new_session_pos_order_id}")
                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'POS Session', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'POS Session', write_date)
                
                except Exception as e:
                    print(f"Terjadi kesalahan saat membuat pos order baru: {e}")

    def transfer_warehouse_master(self, model_name, fields, description):
        # Ambil data dari sumber
        transaksi_warehouse = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        model_name, 'search_read',
                                                        [[]],
                                                        {'fields': fields})

        if not transaksi_warehouse:
            print("Tidak ada master yang ditemukan untuk ditransfer.")
            return


        master_warehouse_ids = []
        # Kirim data ke target
        for record in transaksi_warehouse:
            warehouse_name = record.get('name', False)
            
            existing_master_warehouse = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                            self.source_client.uid, self.source_client.password,
                                                                            'master.warehouse', 'search_read',
                                                                            [[['warehouse_name', '=', warehouse_name]]],
                                                                            {'fields': ['id'], 'limit': 1})
            if not existing_master_warehouse:
                existing_warehouse = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                                self.source_client.uid, self.source_client.password,
                                                                                'stock.warehouse', 'search_read',
                                                                                [[['name', '=', warehouse_name]]],
                                                                                {'fields': ['id'], 'limit': 1})
                if existing_warehouse:
                    print(f"Warehouse dengan nama {warehouse_name} sudah ada di master.warehouse.")
                    continue

                location_id = record.get('lot_stock_id', False)
                company_ids = record.get('company_id', [])

                if not location_id:
                    print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'location_id'.")
                    continue

                location_id = location_id[0] if isinstance(location_id, (list, tuple)) else location_id

                # Cari location di target berdasarkan location_id dari sumber
                location_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.location', 'search_read',
                                                            [[['id', '=', location_id]]],
                                                            {'fields': ['complete_name'], 'limit': 1})

                if not location_source or 'complete_name' not in location_source[0]:
                    print(f"Tidak dapat menemukan 'complete_name' untuk location_id {location_id} di database sumber.")
                    continue

                complete_name = location_source[0]['complete_name']

                location_transit = record.get('location_transit')
                location_transit = location_transit[0] if isinstance(location_transit, (list, tuple)) else location_transit

                location_transit_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.location', 'search_read',
                                                            [[['id', '=', location_transit]]],
                                                            {'fields': ['complete_name'], 'limit': 1})

                if not location_transit_source or 'complete_name' not in location_transit_source[0]:
                    print(f"Tidak dapat menemukan 'complete_name' untuk location_transit {location_transit} di database sumber.")
                    continue

                location_transit_name = location_transit_source[0]['complete_name']

                print(location_transit_source)


                warehouse_data = {
                    'warehouse_name': warehouse_name,
                    'warehouse_code': complete_name,
                    'warehouse_transit': location_transit_name
                    # 'warehouse_company': [(6, 0, [1])]  # Many2many relation expects a list of IDs
                }
                
                start_time = time.time()
                new_master_warehouse = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    'master.warehouse', 'create',
                                                                    [warehouse_data])
                print(f"Warehouse baru telah dibuat dengan ID: {new_master_warehouse}")

                end_time = time.time()
                duration = end_time - start_time

                write_date = self.get_write_date(model_name, record['id'])
                self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Master Warehouse', write_date)
                self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Master Warehouse', write_date)

    def ts_in_from_mc(self, model_name, fields, description):
        transaksi_goods_receipt = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            model_name, 'search_read',
                                                            [[['picking_type_id.name', '=', 'TS In'], ['is_integrated', '=', True], ['state', '=', 'assigned']]],
                                                            {'fields': fields})
        
        # print(transaksi_goods_receipt)

        if not transaksi_goods_receipt:
            print("Tidak ada transaksi yang ditemukan untuk ditransfer.")
            return

        # Kirim data ke target
        for record in transaksi_goods_receipt:
            existing_goods_receipt_inventory = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'stock.picking', 'search_read',
                                                                        [[['vit_trxid', '=', record.get('name')], ['is_integrated', '=', True], ['picking_type_id.name', '=', 'TS In']]],
                                                                        {'fields': ['id'], 'limit': 1})

            if not existing_goods_receipt_inventory:
                location_name = "Partners/Vendors"
                location_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['complete_name', '=', location_name ]]],
                                                                {'fields': ['id'], 'limit': 1})

                if not location_target:
                    print(f"Tidak dapat menemukan 'complete_name' {location_name} di database target.")
                    continue

                location_target_id = location_target[0]['id']

                location_dest_id = record.get('location_dest_id')
                if not location_dest_id:
                    print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'location_dest_id'.")
                    continue

                location_dest_id = location_dest_id[0] if isinstance(location_dest_id, list) else location_dest_id

                location_dest_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.location', 'search_read',
                                                            [[['id', '=', location_dest_id]]],
                                                            {'fields': ['complete_name'], 'limit': 1})

                if not location_dest_source or 'complete_name' not in location_dest_source[0]:
                    print(f"Tidak dapat menemukan 'complete_name' untuk location_id {location_dest_id} di database sumber.")
                    continue

                complete_name_dest = location_dest_source[0]['complete_name']

                location_dest_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['complete_name', '=', complete_name_dest]]],
                                                                {'fields': ['id', 'complete_name'], 'limit': 1})

                if not location_dest_target:
                    print(f"Tidak dapat menemukan 'complete_name' {complete_name_dest} di database target.")
                    continue

                location_dest_target_id = location_dest_target[0]['id']

                ts_in_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', '=', record['id']]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})
                missing_products = []
                ts_in_inventory_line_ids = []
                for line in ts_in_inventory_lines:
                    product_id = line.get('product_id')
                    if product_id:
                        product_id = product_id[0]  # product_id is a list [id, name], we need the id

                        # Get the default_code for the product in the source system
                        product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    'product.product', 'search_read',
                                                                    [[['id', '=', product_id]]],
                                                                    {'fields': ['default_code'], 'limit': 1})

                        if product_source and 'default_code' in product_source[0]:
                            default_code = product_source[0]['default_code']

                            # Search for the product in the target system using default_code
                            product_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'product.product', 'search_read',
                                                                        [[['default_code', '=', default_code]]],
                                                                        {'fields': ['id'], 'limit': 1})

                            if product_target:
                                product_id = product_target[0]['id']
                            else:
                                print(f"Tidak dapat menemukan product dengan 'default_code' {default_code} di database target.")
                                missing_products.append(default_code)
                                continue
                        else:
                            print(f"Tidak dapat menemukan 'default_code' untuk product_id {product_id} di database sumber.")
                            return
                    else:
                        print(f"Line item tidak memiliki 'product_id'.")
                        continue

                    ts_in_inventory_line_data = {
                        'product_id': product_id,
                        'product_uom_qty': line.get('product_uom_qty'),
                        'name': line.get('name'),
                        'quantity': line.get('quantity'),
                        'location_dest_id': location_dest_target_id,
                        'location_id': location_target_id
                    }
                    ts_in_inventory_line_ids.append((0, 0, ts_in_inventory_line_data))

                    if missing_products:
                        missing_products_str = ", ".join(missing_products)
                        message = f"Terdapat produk tidak aktif dalam Goods Receipt: {missing_products_str}"
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'Goods Receipts', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'Goods Receipts', message, write_date)

                # Nama tipe picking yang relevan (misalnya 'Receipts' untuk penerimaan barang)
                picking_type_id = record.get('picking_type_id', False)

                picking_type_id = picking_type_id[0] if isinstance(picking_type_id, list) else picking_type_id

                picking_types = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.picking.type', 'search_read',
                                                            [[['id', '=', picking_type_id]]],
                                                            {'fields': ['name'], 'limit': 1})

                if not picking_types:
                    print(f"Tidak dapat menemukan tipe picking di database sumber untuk picking_type_id {picking_type_id}.")
                    continue

                picking_types_name = picking_types[0]['name']

                # Cari tipe picking di target_client berdasarkan nama
                picking_types = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.picking.type', 'search_read',
                                                            [[['name', '=', picking_types_name], ['default_location_dest_id', '=', location_dest_target_id]]],
                                                            {'fields': ['id'], 'limit': 1})

                if not picking_types:
                    print(f"Tidak dapat menemukan tipe picking dengan nama '{picking_types_name}' di database target.")
                    continue

                picking_type_id = picking_types[0]['id']

                target_location = record.get('target_location')

                target_location_list = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.warehouse', 'search_read',
                                                            [[['name', '=', target_location]]],
                                                            {'fields': ['lot_stock_id'], 'limit': 1}) 
                
                if target_location_list:
                    lot_stock_id = target_location_list[0].get('lot_stock_id', [])
                    if isinstance(lot_stock_id, list) and len(lot_stock_id) > 1:
                        target_location_code = lot_stock_id[1]
                        print(target_location_code)
                    else:
                        print("lot_stock_id is not in the expected format or is empty")
                else:
                    print("No target location found")


                list_warehouse = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'master.warehouse', 'search_read',
                                                            [[['warehouse_name', '=', target_location]]],
                                                            {'fields': ['lot_stock_id'], 'limit': 1}) 

                # ts_in_transfer_data = {
                #     # 'name': record.get('name', False) + ' - ' + datetime.now(pytz.timezone('Asia/Jakarta')).strftime("%Y-%m-%d %H:%M:%S"),
                #     # 'partner_id': customer_target_id,
                #     'scheduled_date': record.get('scheduled_date', False),
                #     'date_done': record.get('date_done', False),
                #     'vit_trxid': record.get('name', False),
                #     'is_integrated': True,
                #     'location_id': location_target_id,
                #     'location_dest_id': location_dest_target_id,
                #     'target_location': target_location_id,
                #     'picking_type_id': picking_type_id,
                #     'move_ids_without_package': ts_in_inventory_line_ids,
                # }

                # try:
                #     new_pos_order_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                #                                                     self.target_client.uid, self.target_client.password,
                #                                                     'stock.picking', 'create',
                #                                                     [ts_in_transfer_data])
                #     print(f"Goods Receipt baru telah dibuat dengan ID: {new_pos_order_id}")

                #     start_time = time.time()
                #     self.source_client.call_odoo(
                #         'object', 'execute_kw', self.source_client.db,
                #         self.source_client.uid, self.source_client.password,
                #         'stock.picking', 'write',
                #         [[record['id']], {'is_integrated': True, 'vit_trxid': record['name']}]
                #     )
                #     end_time = time.time()
                #     duration = end_time - start_time

                #     write_date = self.get_write_date(model_name, record['id'])
                #     self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'TS Out/TS In', write_date)
                #     self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'TS Out/TS In', write_date)
                # except Exception as e:
                #     print(f"Gagal membuat atau memposting TS In baru: {e}")

    def validate_tsin_tsout(self, model_name, fields, description):
        # Retrieve TS In records that match the specified criteria from the source database
        TS_in_validates = self.source_client.call_odoo(
            'object', 'execute_kw', 
            self.source_client.db, self.source_client.uid, self.source_client.password,
            'stock.picking', 'search_read',
            [[
                ['picking_type_id.name', '=', 'TS In'], 
                ['is_integrated', '=', True], 
                ['state', '=', 'done']
            ]],
            {'fields': ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'vit_trxid', 'move_ids_without_package']}
        )

        # Check if any TS In records are found
        if not TS_in_validates:
            print("Tidak ada TS In yang ditemukan di target.")
        else:
            for ts in TS_in_validates:
                vit_trxid = ts.get('vit_trxid', False)

                # Retrieve TS In records that need validation from the target database
                TS_in_needs_validate = self.target_client.call_odoo(
                    'object', 'execute_kw', 
                    self.target_client.db, self.target_client.uid, self.target_client.password,
                    'stock.picking', 'search_read',
                    [[
                        ['picking_type_id.name', '=', 'TS In'], 
                        ['name', '=', vit_trxid], 
                        ['is_integrated', '=', True], 
                        ['state', '=', 'assigned']
                    ]],
                    {'fields': ['name']}
                )

                # Validate each TS In record
                for ts_in in TS_in_needs_validate:
                    ts_in_id = ts_in['id']
                    try:
                        start_time = time.time()
                        self.target_client.call_odoo(
                            'object', 'execute_kw',
                            self.target_client.db, self.target_client.uid, self.target_client.password,
                            'stock.picking', 'button_validate',
                            [ts_in_id]
                        )
                        print(f"TS In with ID {ts_in_id} has been validated.")
                        end_time = time.time()
                        duration = end_time - start_time

                        write_date = self.get_write_date(model_name, ts_in['id'])
                        self.set_log_mc.create_log_note_success(ts_in, start_time, end_time, duration, 'Invoice', write_date)
                        self.set_log_ss.create_log_note_success(ts_in, start_time, end_time, duration, 'Invoice', write_date)
                    except Exception as e:
                        print(f"Failed to validate TS In with ID {ts_in_id}: {e}")

    def transfer_internal_transfers(self, model_name, fields, description):
        # Ambil data dari sumber
        transaksi_internal_transfers = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        model_name, 'search_read',
                                                        [[['picking_type_id.name', '=', 'Internal Transfers'], ['is_integrated', '=', False], ['state', '=', 'done']]],
                                                        {'fields': fields})

        if not transaksi_internal_transfers:
            print("Tidak ada transaksi yang ditemukan untuk ditransfer.")
            return

        # Kirim data ke target
        for record in transaksi_internal_transfers:
            existing_internal_transfers_inventory = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'stock.picking', 'search_read',
                                                                        [[['vit_trxid', '=', record.get('name')], ['is_integrated', '=', True], ['picking_type_id.name', '=', 'Internal Transfers']]],
                                                                        {'fields': ['id'], 'limit': 1})

            if not existing_internal_transfers_inventory:
                #Ambil invoice line items dari sumber
                location_id = record.get('location_id')
                if not location_id:
                    print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'location_id'.")
                    continue

                location_id = location_id[0] if isinstance(location_id, list) else location_id

                location_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.location', 'search_read',
                                                            [[['id', '=', location_id]]],
                                                            {'fields': ['complete_name'], 'limit': 1})
                
                if not location_source or 'complete_name' not in location_source[0]:
                    print(f"Tidak dapat menemukan 'complete_name' untuk location_id {location_id} di database sumber.")
                    continue

                complete_name = location_source[0]['complete_name']

                location_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['complete_name', '=', complete_name]]],
                                                                {'fields': ['id'], 'limit': 1})

                if not location_target:
                    print(f"Tidak dapat menemukan 'complete_name' {complete_name} di database target.")
                    continue

                location_target_id = location_target[0]['id']

                location_dest_id = record.get('location_dest_id')
                if not location_dest_id:
                    print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'location_dest_id'.")
                    continue

                location_dest_id = location_dest_id[0] if isinstance(location_dest_id, list) else location_dest_id

                location_dest_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.location', 'search_read',
                                                            [[['id', '=', location_dest_id]]],
                                                            {'fields': ['complete_name'], 'limit': 1})
                
                if not location_dest_source or 'complete_name' not in location_dest_source[0]:
                    print(f"Tidak dapat menemukan 'complete_name' untuk location_id {location_dest_id} di database sumber.")
                    continue

                complete_name_dest = location_dest_source[0]['complete_name']

                location_dest_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['complete_name', '=', complete_name_dest]]],
                                                                {'fields': ['id'], 'limit': 1})

                if not location_dest_target:
                    print(f"Tidak dapat menemukan 'complete_name' {complete_name_dest} di database target.")
                    continue

                location_dest_target_id = location_dest_target[0]['id']
                
                internal_transfers_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', '=', record['id']]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})
                missing_products = []
                internal_transfers_inventory_line_ids = []
                for line in internal_transfers_inventory_lines:
                    product_id = line.get('product_id')
                    if product_id:
                        product_id = product_id[0]  # product_id is a list [id, name], we need the id

                        # Get the default_code for the product in the source system
                        product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    'product.product', 'search_read',
                                                                    [[['id', '=', product_id]]],
                                                                    {'fields': ['default_code'], 'limit': 1})

                        if product_source and 'default_code' in product_source[0]:
                            default_code = product_source[0]['default_code']

                            # Search for the product in the target system using default_code
                            product_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'product.product', 'search_read',
                                                                        [[['default_code', '=', default_code]]],
                                                                        {'fields': ['id'], 'limit': 1})

                            if product_target:
                                product_id = product_target[0]['id']
                            else:
                                print(f"Tidak dapat menemukan product dengan 'default_code' {default_code} di database target.")
                                missing_products.append(default_code)
                                continue
                        else:
                            print(f"Tidak dapat menemukan 'default_code' untuk product_id {product_id} di database sumber.")
                            return
                    else:
                        print(f"Line item tidak memiliki 'product_id'.")
                        continue

                    internal_transfers_inventory_line_data = {
                        'product_id': product_id,
                        'product_uom_qty': line.get('product_uom_qty'),
                        'name': line.get('name'),
                        'quantity': line.get('quantity'),
                        'location_dest_id': location_dest_target_id,
                        'location_id': location_target_id
                    }
                    internal_transfers_inventory_line_ids.append((0, 0, internal_transfers_inventory_line_data))

                    if missing_products:
                        missing_products_str = ", ".join(missing_products)
                        message = f"Terdapat produk tidak aktif dalam Internal Transfers: {missing_products_str}"
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'Internal Transfers', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'Internal Transfers', message, write_date)

                picking_type_id = record.get('picking_type_id', False)

                picking_type_id = picking_type_id[0] if isinstance(picking_type_id, list) else picking_type_id

                picking_types = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.picking.type', 'search_read',
                                                            [[['id', '=', picking_type_id]]],
                                                            {'fields': ['name'], 'limit': 1})
                
                picking_types_name = picking_types[0]['name']

                # Cari tipe picking di target_client berdasarkan nama
                picking_types = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.picking.type', 'search_read',
                                                            [[['name', '=', picking_types_name], ['default_location_src_id', '=', location_target_id], ['default_location_dest_id', '=', location_dest_target_id]]],
                                                            {'fields': ['id'], 'limit': 1})

                if not picking_types:
                    print(f"Tidak dapat menemukan tipe picking dengan nama '{picking_types_name}' di database target.")
                    continue

                picking_type_id = picking_types[0]['id']

                internal_transfer_data = {
                    # 'name': record.get('name', False) + ' - ' + datetime.now(pytz.timezone('Asia/Jakarta')).strftime("%Y-%m-%d %H:%M:%S"),
                    # 'partner_id': customer_target_id,
                    'scheduled_date': record.get('scheduled_date', False),
                    'date_done': record.get('date_done', False),
                    'vit_trxid': record.get('name', False),
                    'is_integrated': True,
                    'location_id': location_target_id,
                    'location_dest_id': location_dest_target_id,
                    'picking_type_id': picking_type_id,
                    'move_ids_without_package': internal_transfers_inventory_line_ids,
                }

                try:
                    new_internal_transfers = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking', 'create',
                                                                    [internal_transfer_data])
                    print(f"Internal Transfers baru telah dibuat dengan ID: {new_internal_transfers}")

                    # Post the new invoice
                    self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'stock.picking', 'button_validate',
                                                [new_internal_transfers])
                    print(f"Internal Transfers dengan ID: {new_internal_transfers} telah diposting.")

                    start_time = time.time()
                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'stock.picking', 'write',
                        [[record['id']], {'is_integrated': True, 'vit_trxid': record['name']}]
                    )
                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Internal Transfers', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Internal Transfers', write_date)
                except Exception as e:
                    print(f"Gagal membuat atau memposting Goods Receipt baru: {e}")

    def transfer_goods_receipt(self, model_name, fields, description):
        # Ambil data dari sumber
        transaksi_goods_receipt = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            model_name, 'search_read',
                                                            [[['picking_type_id.name', '=', 'Goods Receipts'], ['is_integrated', '=', False], ['state', '=', 'done']]],
                                                            {'fields': fields})

        if not transaksi_goods_receipt:
            print("Tidak ada transaksi yang ditemukan untuk ditransfer.")
            return

        # Kirim data ke target
        for record in transaksi_goods_receipt:
            existing_goods_receipt_inventory = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'stock.picking', 'search_read',
                                                                        [[['vit_trxid', '=', record.get('name')], ['is_integrated', '=', True], ['picking_type_id.name', '=', 'Goods Receipts']]],
                                                                        {'fields': ['id'], 'limit': 1})

            if not existing_goods_receipt_inventory:
                # Ambil invoice line items dari sumber
                location_id = record.get('location_id')
                if not location_id:
                    print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'location_id'.")
                    continue

                location_id = location_id[0] if isinstance(location_id, list) else location_id

                location_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.location', 'search_read',
                                                            [[['id', '=', location_id]]],
                                                            {'fields': ['complete_name'], 'limit': 1})

                if not location_source or 'complete_name' not in location_source[0]:
                    print(f"Tidak dapat menemukan 'complete_name' untuk location_id {location_id} di database sumber.")
                    continue

                complete_name = location_source[0]['complete_name']

                location_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['complete_name', '=', complete_name]]],
                                                                {'fields': ['id'], 'limit': 1})

                if not location_target:
                    print(f"Tidak dapat menemukan 'complete_name' {complete_name} di database target.")
                    continue

                location_target_id = location_target[0]['id']

                location_dest_id = record.get('location_dest_id')
                if not location_dest_id:
                    print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'location_dest_id'.")
                    continue

                location_dest_id = location_dest_id[0] if isinstance(location_dest_id, list) else location_dest_id

                location_dest_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.location', 'search_read',
                                                            [[['id', '=', location_dest_id]]],
                                                            {'fields': ['complete_name'], 'limit': 1})

                if not location_dest_source or 'complete_name' not in location_dest_source[0]:
                    print(f"Tidak dapat menemukan 'complete_name' untuk location_id {location_dest_id} di database sumber.")
                    continue

                complete_name_dest = location_dest_source[0]['complete_name']

                location_dest_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['complete_name', '=', complete_name_dest]]],
                                                                {'fields': ['id'], 'limit': 1})

                if not location_dest_target:
                    print(f"Tidak dapat menemukan 'complete_name' {complete_name_dest} di database target.")
                    continue

                location_dest_target_id = location_dest_target[0]['id']

                goods_receipt_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', '=', record['id']]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})
                missing_products = []
                goods_receipt_inventory_line_ids = []
                for line in goods_receipt_inventory_lines:
                    product_id = line.get('product_id')
                    if product_id:
                        product_id = product_id[0]  # product_id is a list [id, name], we need the id

                        # Get the default_code for the product in the source system
                        product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    'product.product', 'search_read',
                                                                    [[['id', '=', product_id]]],
                                                                    {'fields': ['default_code'], 'limit': 1})

                        if product_source and 'default_code' in product_source[0]:
                            default_code = product_source[0]['default_code']

                            # Search for the product in the target system using default_code
                            product_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'product.product', 'search_read',
                                                                        [[['default_code', '=', default_code]]],
                                                                        {'fields': ['id'], 'limit': 1})

                            if product_target:
                                product_id = product_target[0]['id']
                            else:
                                print(f"Tidak dapat menemukan product dengan 'default_code' {default_code} di database target.")
                                missing_products.append(default_code)
                                continue
                        else:
                            print(f"Tidak dapat menemukan 'default_code' untuk product_id {product_id} di database sumber.")
                            return
                    else:
                        print(f"Line item tidak memiliki 'product_id'.")
                        continue

                    goods_receipt_inventory_line_data = {
                        'product_id': product_id,
                        'product_uom_qty': line.get('product_uom_qty'),
                        'name': line.get('name'),
                        'quantity': line.get('quantity'),
                        'location_dest_id': location_dest_target_id,
                        'location_id': location_target_id
                    }
                    goods_receipt_inventory_line_ids.append((0, 0, goods_receipt_inventory_line_data))

                    if missing_products:
                        missing_products_str = ", ".join(missing_products)
                        message = f"Terdapat produk tidak aktif dalam Goods Receipt: {missing_products_str}"
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'Goods Receipts', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'Goods Receipts', message, write_date)

                # Nama tipe picking yang relevan (misalnya 'Receipts' untuk penerimaan barang)
                picking_type_id = record.get('picking_type_id', False)

                picking_type_id = picking_type_id[0] if isinstance(picking_type_id, list) else picking_type_id

                picking_types = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.picking.type', 'search_read',
                                                            [[['id', '=', picking_type_id]]],
                                                            {'fields': ['name'], 'limit': 1})

                if not picking_types:
                    print(f"Tidak dapat menemukan tipe picking di database sumber untuk picking_type_id {picking_type_id}.")
                    continue

                picking_types_name = picking_types[0]['name']

                # Cari tipe picking di target_client berdasarkan nama
                picking_types = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.picking.type', 'search_read',
                                                            [[['name', '=', picking_types_name], ['default_location_dest_id', '=', location_dest_target_id]]],
                                                            {'fields': ['id'], 'limit': 1})

                if not picking_types:
                    print(f"Tidak dapat menemukan tipe picking dengan nama '{picking_types_name}' di database target.")
                    continue

                picking_type_id = picking_types[0]['id']

                internal_transfer_data = {
                    # 'name': record.get('name', False) + ' - ' + datetime.now(pytz.timezone('Asia/Jakarta')).strftime("%Y-%m-%d %H:%M:%S"),
                    # 'partner_id': customer_target_id,
                    'scheduled_date': record.get('scheduled_date', False),
                    'date_done': record.get('date_done', False),
                    'vit_trxid': record.get('name', False),
                    'is_integrated': True,
                    'location_id': location_target_id,
                    'location_dest_id': location_dest_target_id,
                    'picking_type_id': picking_type_id,
                    'move_ids_without_package': goods_receipt_inventory_line_ids,
                }

                try:
                    new_pos_order_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking', 'create',
                                                                    [internal_transfer_data])
                    print(f"Goods Receipt baru telah dibuat dengan ID: {new_pos_order_id}")

                    # Post the new invoice
                    self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'stock.picking', 'button_validate',
                                                [new_pos_order_id])
                    print(f"Goods Receipt dengan ID: {new_pos_order_id} telah diposting.")

                    start_time = time.time()
                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'stock.picking', 'write',
                        [[record['id']], {'is_integrated': True, 'vit_trxid': record['name']}]
                    )
                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Goods Receipts', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Goods Receipts', write_date)
                except Exception as e:
                    print(f"Gagal membuat atau memposting Goods Receipt baru: {e}")


    def transfer_goods_issue(self, model_name, fields, description):
        # Ambil data dari sumber
        transaksi_goods_issue = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        model_name, 'search_read',
                                                        [[['picking_type_id.name', '=', 'Goods Issue'], ['is_integrated', '=', False], ['state', '=', 'done']]],
                                                        {'fields': fields})

        if not transaksi_goods_issue:
            print("Tidak ada transaksi yang ditemukan untuk ditransfer.")
            return

        # Kirim data ke target
        for record in transaksi_goods_issue:
            existing_goods_issue_inventory = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'stock.picking', 'search_read',
                                                                        [[['vit_trxid', '=', record.get('name')], ['is_integrated', '=', True], ['picking_type_id.name', '=', 'Goods Issue']]],
                                                                        {'fields': ['id'], 'limit': 1})

            if not existing_goods_issue_inventory:
                #Ambil invoice line items dari sumber
                location_id = record.get('location_id')
                if not location_id:
                    print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'location_id'.")
                    continue

                location_id = location_id[0] if isinstance(location_id, list) else location_id

                location_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.location', 'search_read',
                                                            [[['id', '=', location_id]]],
                                                            {'fields': ['complete_name'], 'limit': 1})
                
                if not location_source or 'complete_name' not in location_source[0]:
                    print(f"Tidak dapat menemukan 'complete_name' untuk location_id {location_id} di database sumber.")
                    continue

                complete_name = location_source[0]['complete_name']

                location_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['complete_name', '=', complete_name]]],
                                                                {'fields': ['id'], 'limit': 1})

                if not location_target:
                    print(f"Tidak dapat menemukan 'complete_name' {complete_name} di database target.")
                    continue

                location_target_id = location_target[0]['id']

                location_dest_id = record.get('location_dest_id')
                if not location_dest_id:
                    print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'location_dest_id'.")
                    continue

                location_dest_id = location_dest_id[0] if isinstance(location_dest_id, list) else location_dest_id

                location_dest_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.location', 'search_read',
                                                            [[['id', '=', location_dest_id]]],
                                                            {'fields': ['complete_name'], 'limit': 1})
                
                if not location_dest_source or 'complete_name' not in location_dest_source[0]:
                    print(f"Tidak dapat menemukan 'complete_name' untuk location_id {location_dest_id} di database sumber.")
                    continue

                complete_name_dest = location_dest_source[0]['complete_name']

                location_dest_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['complete_name', '=', complete_name_dest]]],
                                                                {'fields': ['id'], 'limit': 1})

                if not location_dest_target:
                    print(f"Tidak dapat menemukan 'complete_name' {complete_name_dest} di database target.")
                    continue

                location_dest_target_id = location_dest_target[0]['id']
                
                goods_issue_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', '=', record['id']]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})
                missing_products = []
                goods_issue_inventory_line_ids = []
                for line in goods_issue_inventory_lines:
                    product_id = line.get('product_id')
                    if product_id:
                        product_id = product_id[0]  # product_id is a list [id, name], we need the id

                        # Get the default_code for the product in the source system
                        product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    'product.product', 'search_read',
                                                                    [[['id', '=', product_id]]],
                                                                    {'fields': ['default_code'], 'limit': 1})

                        if product_source and 'default_code' in product_source[0]:
                            default_code = product_source[0]['default_code']

                            # Search for the product in the target system using default_code
                            product_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'product.product', 'search_read',
                                                                        [[['default_code', '=', default_code]]],
                                                                        {'fields': ['id'], 'limit': 1})

                            if product_target:
                                product_id = product_target[0]['id']
                            else:
                                print(f"Tidak dapat menemukan product dengan 'default_code' {default_code} di database target.")
                                missing_products.append(default_code)
                                continue
                        else:
                            print(f"Tidak dapat menemukan 'default_code' untuk product_id {product_id} di database sumber.")
                            return
                    else:
                        print(f"Line item tidak memiliki 'product_id'.")
                        continue

                    goods_issue_inventory_line_data = {
                        'product_id': product_id,
                        'product_uom_qty': line.get('product_uom_qty'),
                        'name': line.get('name'),
                        'quantity': line.get('quantity'),
                        'location_dest_id': location_dest_target_id,
                        'location_id': location_target_id
                    }
                    goods_issue_inventory_line_ids.append((0, 0, goods_issue_inventory_line_data))

                    if missing_products:
                        missing_products_str = ", ".join(missing_products)
                        message = f"Terdapat produk tidak aktif dalam Goods Issue: {missing_products_str}"
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'Goods Issue', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'Goods Issue', message, write_date)

                picking_type_id = record.get('picking_type_id', False)

                picking_type_id = picking_type_id[0] if isinstance(picking_type_id, list) else picking_type_id

                picking_types = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.picking.type', 'search_read',
                                                            [[['id', '=', picking_type_id]]],
                                                            {'fields': ['name'], 'limit': 1})
                
                picking_types_name = picking_types[0]['name']

                # Cari tipe picking di target_client berdasarkan nama
                picking_types = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.picking.type', 'search_read',
                                                            [[['name', '=', picking_types_name], ['default_location_src_id', '=', location_target_id]]],
                                                            {'fields': ['id'], 'limit': 1})

                if not picking_types:
                    print(f"Tidak dapat menemukan tipe picking dengan nama '{picking_types_name}' di database target.")
                    continue

                picking_type_id = picking_types[0]['id']

                internal_transfer_data = {
                    # 'name': record.get('name', False) + ' - ' + datetime.now(pytz.timezone('Asia/Jakarta')).strftime("%Y-%m-%d %H:%M:%S"),
                    # 'partner_id': customer_target_id,
                    'scheduled_date': record.get('scheduled_date', False),
                    'date_done': record.get('date_done', False),
                    'vit_trxid': record.get('name', False),
                    'is_integrated': True,
                    'location_id': location_target_id,
                    'location_dest_id': location_dest_target_id,
                    'picking_type_id': picking_type_id,
                    'move_ids_without_package': goods_issue_inventory_line_ids,
                }

                try:
                    new_pos_order_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking', 'create',
                                                                    [internal_transfer_data])
                    print(f"Pos Order baru telah dibuat dengan ID: {new_pos_order_id}")

                    # Post the new invoice
                    self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'stock.picking', 'button_validate',
                                                [new_pos_order_id])
                    print(f"Goods Issue dengan ID: {new_pos_order_id} telah diposting.")

                    start_time = time.time()
                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'stock.picking', 'write',
                        [[record['id']], {'is_integrated': True, 'vit_trxid': record['name']}]
                    )
                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Goods Issue', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Goods Issue', write_date)
                except Exception as e:
                    print(f"Gagal membuat atau memposting Goods Issue baru: {e}")

    def transfer_stock_adjustment(self, model_name, fields, description):
        try:
            # Mendapatkan data stock adjustment dari sumber
            transaksi_stock_adjustment = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                model_name, 'search_read',
                [[['reference', '=', 'Product Quantity Updated'], ['x_is_integrated', '=', False], ['state', '=', 'done']]],
                {'fields': fields}
            )

            if not transaksi_stock_adjustment:
                print("Tidak ada transaksi yang ditemukan untuk ditransfer.")
                return

            for record in transaksi_stock_adjustment:
                product_id = record.get('product_id')
                if not product_id:
                    print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'product_id'.")
                    continue

                product_id = product_id[0] if isinstance(product_id, list) else product_id

                # Mendapatkan default_code produk dari sumber
                product_source = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'product.product', 'search_read',
                    [[['id', '=', product_id]]],
                    {'fields': ['default_code'], 'limit': 1}
                )

                if not product_source or 'default_code' not in product_source[0]:
                    print(f"Tidak dapat menemukan 'default_code' untuk product_id {product_id} di database sumber.")
                    continue

                product_code = product_source[0]['default_code']

                # Mendapatkan product_id di target berdasarkan default_code
                product_target = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'product.product', 'search_read',
                    [[['default_code', '=', product_code]]],
                    {'fields': ['id'], 'limit': 1}
                )

                if not product_target:
                    print(f"Tidak dapat menemukan product dengan 'default_code' {product_code} di database target.")
                    continue

                product_target_id = product_target[0]['id']

                location_id = record.get('location_id')
                if not location_id:
                    print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'location_id'.")
                    continue

                location_id = location_id[0] if isinstance(location_id, list) else location_id

                location_source = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'stock.location', 'search_read',
                    [[['id', '=', location_id]]],
                    {'fields': ['complete_name'], 'limit': 1}
                )

                if not location_source or 'complete_name' not in location_source[0]:
                    print(f"Tidak dapat menemukan 'complete_name' untuk location_id {location_id} di database sumber.")
                    continue

                complete_name = location_source[0]['complete_name']

                location_target = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'stock.location', 'search_read',
                    [[['complete_name', '=', complete_name]]],
                    {'fields': ['id'], 'limit': 1}
                )

                if not location_target:
                    print(f"Tidak dapat menemukan 'complete_name' {complete_name} di database target.")
                    continue

                location_target_id = location_target[0]['id']

                inventory_quantity = record.get('quantity')

                location_dest_id = record.get('location_dest_id')
                if not location_dest_id:
                    print(f"Transaksi dengan ID {record.get('id')} tidak memiliki 'location_dest_id'.")
                    continue

                location_dest_id = location_dest_id[0] if isinstance(location_dest_id, list) else location_dest_id

                location_dest_source = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'stock.location', 'search_read',
                    [[['id', '=', location_dest_id]]],
                    {'fields': ['complete_name'], 'limit': 1}
                )

                if not location_dest_source or 'complete_name' not in location_dest_source[0]:
                    print(f"Tidak dapat menemukan 'complete_name' untuk location_id {location_dest_id} di database sumber.")
                    continue

                complete_name_dest = location_dest_source[0]['complete_name']

                location_dest_target = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'stock.location', 'search_read',
                    [[['complete_name', '=', complete_name_dest]]],
                    {'fields': ['id'], 'limit': 1}
                )

                if not location_dest_target:
                    print(f"Tidak dapat menemukan 'complete_name' {complete_name_dest} di database target.")
                    continue

                location_dest_target_id = location_dest_target[0]['id']

                # Mencari stock.quant yang sesuai di target
                stock_quant_target = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'stock.quant', 'search_read',
                    [[
                        ['product_id', '=', product_target_id],
                        '|',
                        ['location_id', '=', location_target_id],
                        ['location_id', '=', location_dest_target_id]
                    ]],
                    {'fields': ['id', 'inventory_quantity'], 'limit': 1}
                )

                if not stock_quant_target:
                    new_stock_quant = self.target_client.call_odoo(
                        'object', 'execute_kw', self.target_client.db,
                        self.target_client.uid, self.target_client.password,
                        'stock.quant', 'create',
                        [{'product_id': product_target_id, 'inventory_quantity': inventory_quantity, 'location_id': location_dest_target_id}]
                    )
                    print(f"Produk dengan default_code {product_code} telah ditambahkan ke stock.quant baru dengan ID {new_stock_quant}.")
                    self.target_client.call_odoo(
                        'object', 'execute_kw', self.target_client.db,
                        self.target_client.uid, self.target_client.password,
                        'stock.quant', 'action_apply_inventory',
                        [new_stock_quant]
                    )
                    print(f"Produk dengan default_code {product_code} telah ditambahkan ke stock.quant dengan ID {new_stock_quant}.")
                    continue

                stock_quant_id = stock_quant_target[0]['id']

                # Update inventory_quantity di stock.quant
                self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'stock.quant', 'write',
                    [[stock_quant_id], {'inventory_quantity': inventory_quantity}]
                )

                print(f"Inventory quantity untuk stock.quant ID {stock_quant_id} telah diperbarui menjadi {inventory_quantity}.")

                # Menandai transaksi di sumber sebagai telah diintegrasikan
                self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    model_name, 'write',
                    [[record['id']], {'x_is_integrated': True}]
                )
                print(f"Transaksi dengan ID {record['id']} di database sumber telah ditandai sebagai diintegrasikan.")

                # Menjalankan tombol action_apply_inventory
                try:
                    start_time = time.time()
                    self.target_client.call_odoo(
                        'object', 'execute_kw', self.target_client.db,
                        self.target_client.uid, self.target_client.password,
                        'stock.quant', 'action_apply_inventory',
                        [stock_quant_id]
                    )
                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Inventory Adjustment', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Inventory Adjustment', write_date)

                    print(f"Action apply inventory telah dijalankan untuk stock.quant ID {stock_quant_id}.")
                except Exception as e:
                    print(f"Error: {str(e)}")

        except Exception as e:
            print(f"Terjadi kesalahan: {str(e)}")
            return False  # Atau sesuai dengan kebutuhan Anda

    def update_session_status(self, model_name, fields, description):
    # Fetch the POS sessions to update from the target client
        pos_sessions = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    model_name, 'search_read',
                                                    [[['state', '=', 'closed']]],
                                                    {'fields': fields})

        if not pos_sessions:
            print("Tidak ada sesi yang ditemukan untuk ditransfer.")
            return

        for record in pos_sessions:
            name = record.get('name')
            state = record.get('state')
            start_at = record.get('start_at')
            stop_at = record.get('stop_at')

            cash_register_balance_start = record.get('cash_register_balance_start')
            cash_register_balance_end_real = record.get('cash_register_balance_end_real')

            # Debugging prints
            print(f"Cash Register Balance Start: {cash_register_balance_start}")
            print(f"Cash Register Balance End Real: {cash_register_balance_end_real}")

            # Ensure monetary values are properly handled
            cash_register_balance_start = float(cash_register_balance_start) if cash_register_balance_start else 0.0
            cash_register_balance_end_real = float(cash_register_balance_end_real) if cash_register_balance_end_real else 0.0

            if not state:
                print(f"Status sesi {name} tidak valid.")
                continue

            # Fetch the corresponding session on the source client based on the session name
            source_session = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        model_name, 'search_read',
                                                        [[['name_session_pos', '=', name]]],
                                                        {'fields': ['state'], 'limit': 1})

            if not source_session:
                print(f"Sesi dengan nama {name} tidak ditemukan di sumber.")
                continue

            # Update the state on the target client
            session_id = source_session[0]['id']

            start_time = time.time()
            update_result = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                             self.target_client.uid, self.target_client.password,
                                             model_name, 'write',
                                             [[session_id], {'state': state, 'start_at': start_at, 'stop_at': stop_at, 'cash_register_balance_start': cash_register_balance_start, 'cash_register_balance_end_real': cash_register_balance_end_real}])
            
            end_time = time.time()
            duration = end_time - start_time

            write_date = self.get_write_date(model_name, record['id'])
            self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Update Session Status', write_date)
            self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Update Session Status', write_date)
            
            if update_result:
                print(f"Berhasil mengupdate sesi {name} dengan status {state}.")
            else:
                print(f"Gagal mengupdate sesi {name}.")

    def debug_taxes(self, model_name, fields, description):
        tax_source = self.source_client.call_odoo(
            'object', 'execute_kw', self.source_client.db,
            self.source_client.uid, self.source_client.password,
            model_name, 'search_read',
            [[]],
            {'fields': fields}
        )

        tax_ids = []
        for rec in tax_source:
            name = rec.get('name')
            print(name)
    
            tax_target = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'account.tax', 'search_read',
                [[['name', '=', name]]],
                {'fields': ['id']}
            )

            if tax_target:  # Check if the list is not empty
                tax_ids.append(tax_target[0]['id'])

                print(tax_ids)
        
    def debug_operatin_type(self, model_name, fields, description):
        operatin_type_source = self.source_client.call_odoo(
            'object', 'execute_kw', self.source_client.db,
            self.source_client.uid, self.source_client.password,
            model_name, 'search_read',
            [[['complete_name', '=', "JB/Stock"]]],
            {'fields': fields}
        )

        print(operatin_type_source)

    def update_integrated(self, model_name, fields, description):
        # Mengambil data dari Odoo
        config_source =  self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        model_name, 'search_read',
                                                        [[['is_integrated', '=', False]]],
                                                        {'fields': fields})

        # Mengambil ID dari data yang didapat
        ids_to_update = [record['id'] for record in config_source]

        # Memperbarui is_integrated menjadi True untuk semua ID yang didapat
        if ids_to_update:
            update_result = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.session', 'write',
                                                        [ids_to_update, {'is_integrated': True}])

            if update_result:
                print("Update successful for IDs:", ids_to_update)
            else:
                print("Failed to update records.")
        else:
            print("No records found to update.")

    def update_status_order_pos(self, model_name, fields, description):
        # Mengambil data dari Odoo
        config_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                    self.target_client.uid, self.target_client.password,
                                                    model_name, 'search_read',
                                                    [[['state', '=', 'paid']]],
                                                    {'fields': fields})

        # Mengambil ID dari data yang didapat
        ids_to_update = [record['id'] for record in config_source]

        # Memperbarui is_integrated menjadi True untuk semua ID yang didapat
        if ids_to_update:
            # Loop through each ID to update
            for order_id in ids_to_update:
                try:
                    update_result = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'pos.order', 'action_pos_order_invoice',
                                                                [[order_id]])

                    if update_result:
                        print(f"Update successful for ID: {order_id}")
                    else:
                        print(f"Failed to update record with ID: {order_id}")
                except Exception as e:
                    print(f"Error updating record with ID {order_id}: {e}")
        else:
            print("No records found to update.")

    def create_data_transaksi(self, model, record, modul):
        try:
            record['invoice_line_ids'] = self.transfer_invoice_lines(record['id'], 'account.move.line')
            start_time = time.time()
            self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                         self.source_client.password, model, 'create', [record])
            end_time = time.time()
            duration = end_time - start_time

            self.set_log_mc.create_log_note_success(record, modul)
            self.set_log_mc.create_log_runtime_odoo(start_time, end_time, duration, modul)  # masih ngelooping belum dikeluarin
            self.set_log_ss.create_log_note_success(record, modul)
            self.set_log_ss.create_log_runtime_odoo(start_time, end_time, duration, modul)
        except Exception as e:
            sync_status = f"An error occurred while create data transaksi: {e}"
            print(sync_status)

    def get_write_uid_data(self, model):
        try:
            write_uid_data = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password, model,
                                                        'search_read', [[]], {'fields': ['write_uid']})
            return write_uid_data
        except Exception as e:
            print(f"An error occured while get write uid data: {e}")
            return None

    def get_write_date(self, model, id):
        try:
            write_date = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password, model,
                                                        'search_read', [[['id', '=', id]]], {'fields': ['write_date']})
            if write_date:
                write_date_value = write_date[0]['write_date']
                return write_date_value
        except Exception as e:
            print(f"Error occurred when get write date: {e}")

class SetLogMC:
    def __init__(self, source_client):
        self.source_client = source_client

    def log_record_success(self, record, start_time, end_time, duration, modul, write_date):
        gmt_7_now = datetime.now() - timedelta(hours=7)  # Odoo menggunakan UTC, belum diatur zona waktunya
        gmt_7_start_time = datetime.fromtimestamp(start_time) - timedelta(hours=7)
        gmt_7_end_time = datetime.fromtimestamp(end_time) - timedelta(hours=7)
        record_log_success = {
            'vit_doc_type': modul,
            'vit_trx_key': record.get('name'),
            'vit_trx_date': write_date,
            'vit_sync_date': gmt_7_now.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_sync_status': 'Success',
            'vit_sync_desc': f"Data yang masuk: {record}",
            'vit_start_sync': gmt_7_start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_end_sync': gmt_7_end_time.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_duration' : f"{duration:.2f} second"
        }
        return record_log_success
    
    def log_update_record_success(self, record, record_id, updated_fields, start_time, end_time, duration, modul, write_date):
        gmt_7_now = datetime.now() - timedelta(hours=7)  # Odoo menggunakan UTC, belum diatur zona waktunya
        gmt_7_start_time = datetime.fromtimestamp(start_time) - timedelta(hours=7)
        gmt_7_end_time = datetime.fromtimestamp(end_time) - timedelta(hours=7)
        record_log_success = {
            'vit_doc_type': f"Update: {modul}",
            'vit_trx_key': record.get('name'),
            'vit_trx_date': write_date,
            'vit_sync_date': gmt_7_now.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_sync_status': 'Success',
            'vit_sync_desc': f"Data yang diupdate: id {record_id},  {updated_fields}",
            'vit_start_sync': gmt_7_start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_end_sync': gmt_7_end_time.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_duration' : f"{duration:.2f} second"
        }
        return record_log_success
    
    def log_record_failed(self, record, modul, sync_status, write_date):
        gmt_7_now = datetime.now() - timedelta(hours=7)  # Odoo menggunakan UTC, belum diatur zona waktunya
        
        if isinstance(sync_status, str) is False:
            sync_status = sync_status.args[0]
            sync_status = sync_status['data']['message']

        record_log_failed = {
            'vit_doc_type': modul,
            'vit_trx_key': record.get('name'),
            'vit_trx_date': write_date,
            'vit_sync_date': gmt_7_now.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_sync_status': 'Failed',
            'vit_sync_desc': sync_status
        }
        return record_log_failed

    def delete_data_log(self):
        try:
            filter_domain = [['vit_code_type', '=', 'Master']]
            data_logruntime = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                           self.source_client.uid, self.source_client.password,
                                                           'log.code.runtime', 'search_read', [filter_domain],
                                                           {'fields': ['id'], 'limit': 1})
            for record in data_logruntime:
                record_id = record['id']
                self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                             self.source_client.password, 'log.code.runtime', 'unlink', [[record_id]])
                print(f"Deleted record with ID: {record_id}")
        except Exception as e:
            print(f"An error occurred while deleting data: {e}")

    def create_log_note_success(self, record, start_time, end_time, duration, modul, write_date):
        try:
            log_record = self.log_record_success(record, start_time, end_time, duration, modul, write_date)
            self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                         self.source_client.password, 'log.note', 'create', [log_record])
            print(f"Data log note yang masuk: {log_record}")
        except Exception as e:
            print(f"An error occurred while creating log note: {e}")

    def create_log_note_update_success(self, record, record_id, updated_fields, start_time, end_time, duration, modul, write_date):
        try:
            log_record = self.log_update_record_success(record, record_id, updated_fields, start_time, end_time, duration, modul, write_date)
            self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                         self.source_client.password, 'log.note', 'create', [log_record])
            print(f"Data log note yang masuk: {log_record}")
        except Exception as e:
            print(f"An error occurred while creating log note: {e}")

    def create_log_note_failed(self, record, modul, sync_status, write_date):
        try:
            log_record = self.log_record_failed(record, modul, sync_status, write_date)
            log_record_existing = self.get_log_note_failed(log_record['vit_trx_key'], log_record['vit_sync_desc'])
            if not log_record_existing:
                self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                            self.source_client.password, 'log.note', 'create', [log_record])
                print(f"Data log note yang masuk: {log_record}")
        except Exception as e:
            print(f"An error occurred while creating log note: {e}")

    def get_log_note_failed(self, key, desc):
        log_note_failed = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password, 'log.note',
                                                        'search_read', [[['vit_trx_key', '=', key], ['vit_sync_desc', '=', desc] , ['vit_sync_status', '=', 'Failed']]])
        return log_note_failed

class SetLogSS:
    def __init__(self, target_client):
        self.target_client = target_client

    def log_record_success(self, record, start_time, end_time, duration, modul, write_date):
        gmt_7_now = datetime.now() - timedelta(hours=7)  # Odoo menggunakan UTC, belum diatur zona waktunya
        gmt_7_start_time = datetime.fromtimestamp(start_time) - timedelta(hours=7)
        gmt_7_end_time = datetime.fromtimestamp(end_time) - timedelta(hours=7)
        record_log_success = {
            'vit_doc_type': modul,
            'vit_trx_key': record.get('name'),
            'vit_trx_date': write_date,
            'vit_sync_date': gmt_7_now.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_sync_status': 'Success',
            'vit_sync_desc': f"Data yang masuk: {record}",
            'vit_start_sync': gmt_7_start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_end_sync': gmt_7_end_time.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_duration' : f"{duration:.2f} second"
        }
        return record_log_success
    
    def log_update_record_success(self, record, record_id, updated_fields, start_time, end_time, duration, modul, write_date):
        gmt_7_now = datetime.now() - timedelta(hours=7)  # Odoo menggunakan UTC, belum diatur zona waktunya
        gmt_7_start_time = datetime.fromtimestamp(start_time) - timedelta(hours=7)
        gmt_7_end_time = datetime.fromtimestamp(end_time) - timedelta(hours=7)
        record_log_success = {
            'vit_doc_type': f"Update: {modul}",
            'vit_trx_key': record.get('name'),
            'vit_trx_date': write_date,
            'vit_sync_date': gmt_7_now.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_sync_status': 'Success',
            'vit_sync_desc': f"Data yang diupdate: id {record_id},  {updated_fields}",
            'vit_start_sync': gmt_7_start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_end_sync': gmt_7_end_time.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_duration' : f"{duration:.2f} second"
        }
        return record_log_success
    
    def log_record_failed(self, record, modul, sync_status, write_date):
        gmt_7_now = datetime.now() - timedelta(hours=7)  # Odoo menggunakan UTC, belum diatur zona waktunya
        
        if isinstance(sync_status, str) is False:
            sync_status = sync_status.args[0]
            sync_status = sync_status['data']['message']

        record_log_failed = {
            'vit_doc_type': modul,
            'vit_trx_key': record.get('name'),
            'vit_trx_date': write_date,
            'vit_sync_date': gmt_7_now.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_sync_status': 'Failed',
            'vit_sync_desc': sync_status
        }
        return record_log_failed

    def delete_data_log(self):
        try:
            filter_domain = [['vit_code_type', '=', 'Master']]
            data_logruntime = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                           self.target_client.uid, self.target_client.password,
                                                           'log.code.runtime', 'search_read', [filter_domain],
                                                           {'fields': ['id'], 'limit': 1})
            for record in data_logruntime:
                record_id = record['id']
                self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                             self.target_client.password, 'log.code.runtime', 'unlink', [[record_id]])
                print(f"Deleted record with ID: {record_id}")
        except Exception as e:
            print(f"An error occurred while deleting data: {e}")

    def create_log_note_success(self, record, start_time, end_time, duration, modul, write_date):
        try:
            log_record = self.log_record_success(record, start_time, end_time, duration, modul, write_date)
            self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                         self.target_client.password, 'log.note', 'create', [log_record])
            print(f"Data log note yang masuk: {log_record}")
        except Exception as e:
            print(f"An error occurred while creating log note: {e}")

    def create_log_note_update_success(self, record, record_id, updated_fields, start_time, end_time, duration, modul, write_date):
        try:
            log_record = self.log_update_record_success(record, record_id, updated_fields, start_time, end_time, duration, modul, write_date)
            self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                         self.target_client.password, 'log.note', 'create', [log_record])
            print(f"Data log note yang masuk: {log_record}")
        except Exception as e:
            print(f"An error occurred while creating log note: {e}")

    def create_log_note_failed(self, record, modul, sync_status, write_date):
        try:
            log_record = self.log_record_failed(record, modul, sync_status, write_date)
            log_record_existing = self.get_log_note_failed(log_record['vit_trx_key'], log_record['vit_sync_desc'])
            if not log_record_existing:
                self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                            self.target_client.password, 'log.note', 'create', [log_record])
                print(f"Data log note yang masuk: {log_record}")
        except Exception as e:
            print(f"An error occurred while creating log note: {e}")

    def get_log_note_failed(self, key, desc):
        log_note_failed = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password, 'log.note',
                                                        'search_read', [[['vit_trx_key', '=', key], ['vit_sync_desc', '=', desc] , ['vit_sync_status', '=', 'Failed']]])
        return log_note_failed
