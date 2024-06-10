import time
from datetime import datetime, timedelta


# kalau ada case store nya beda zona waktu gimana
class DataTransaksi:
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
                                                [[['move_type', '=', 'out_invoice'], ['state', '=', 'posted'], ['payment_state', '=', 'paid']]],
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
                        # 'account_id': account_id,
                    }
                    invoice_line_ids.append((0, 0, invoice_line_data))

                if missing_products:
                    missing_products_str = ", ".join(missing_products)
                    message = f"Invoice dibatalkan karena produk tidak terdaftar: {missing_products_str}"
                    print(message)
                    self.set_log_mc.create_log_note_failed(record, 'Invoice', message)
                    self.set_log_ss.create_log_note_failed(record, 'Invoice', message)
                    continue

                invoice_data = {
                    'partner_id': customer_target_id,
                    'journal_id': journal_id,
                    'invoice_date': record.get('invoice_date', False),
                    'invoice_date_due': record.get('invoice_date_due', False),
                    'payment_reference': record.get('name', False),
                    'ref': record.get('ref', False),
                    'move_type': 'out_invoice',
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
                                                                        [[['origin', '=', record.get('origin')], ['picking_type_id.name', '=', 'PoS Orders']]],
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
                    print(f"Tidak dapat menemukan'complete_name' {complete_name} di database target.")
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
                    print(f"Tidak dapat menemukan'complete_name' {complete_name_dest} di database target.")
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
                    # 'name': record.get('name', False),
                    'partner_id': customer_target_id,
                    'scheduled_date': record.get('scheduled_date', False),
                    'date_done': record.get('date_done', False),
                    'origin': record.get('origin', False),
                    'location_id': location_target_id,
                    'location_dest_id': location_dest_target_id,
                    'picking_type_id': picking_type_id,
                    # 'company_id': record.get('company_id', False),
                    'move_ids_without_package': pos_order_inventory_line_ids,
                }

                new_pos_order_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.picking', 'create',
                                                                [pos_order_data])
                print(f"Pos Order baru telah dibuat dengan ID: {new_pos_order_id}")

                #Post the new invoice
                self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                            self.target_client.uid, self.target_client.password,
                                            'stock.picking', 'button_validate',
                                            [new_pos_order_id])
                print(f"Invoice dengan ID: {new_pos_order_id} telah diposting.")

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
                                                                    {'fields': ['product_id', 'full_product_name', 'qty', 'price_unit']})
                pos_order_invoice_line_ids = []
                pos_order_payment_ids = []
                missing_products = []
                total_tax = 0  # Initialize total tax

                # Check if all products exist in the target database
                for line in pos_order_invoice_lines:
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

                            if not product_target:
                                print(f"Tidak dapat menemukan product dengan 'default_code' {default_code} di database target.")
                                missing_products.append(default_code)
                        else:
                            print(f"Tidak dapat menemukan 'default_code' untuk product_id {product_id} di database sumber.")
                            return
                    else:
                        print(f"Line item tidak memiliki 'product_id'.")
                        continue

                # If there are missing products, log the error and continue with the next record
                if missing_products:
                    missing_products_str = ", ".join(missing_products)
                    message = f"Invoice dibatalkan karena produk tidak terdaftar: {missing_products_str}"
                    print(message)
                    self.set_log_mc.create_log_note_failed(record, 'Invoice', message)
                    self.set_log_ss.create_log_note_failed(record, 'Invoice', message)
                    continue

                # Process the products if all are available in the target
                for line in pos_order_invoice_lines:
                    product_id = line.get('product_id')[0]  # product_id is a list [id, name], we need the id

                    product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'product.product', 'search_read',
                                                                [[['id', '=', product_id]]],
                                                                {'fields': ['default_code'], 'limit': 1})

                    default_code = product_source[0]['default_code']

                    product_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'product.product', 'search_read',
                                                                [[['default_code', '=', default_code]]],
                                                                {'fields': ['id'], 'limit': 1})

                    product_id = product_target[0]['id']

                    pos_order_line_data = {
                        'product_id': product_id,
                        'full_product_name': line.get('full_product_name'),
                        'qty': line.get('qty'),
                        'price_unit': line.get('price_unit'),
                        'price_subtotal': line.get('price_unit') * line.get('qty'),
                        'price_subtotal_incl': line.get('price_unit') * line.get('qty'),
                    }
                    pos_order_invoice_line_ids.append((0, 0, pos_order_line_data))


                    # Assume each line has a fixed tax rate of 10% for demonstration purposes
                    # total_tax += line.get('price_unit') * line.get('qty') * 0.10

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
                    # 'is_integrated': True,
                    'lines': pos_order_invoice_line_ids,
                    'payment_ids': pos_order_payment_ids,
                    'state': 'paid'
                }

                new_pos_order_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'pos.order', 'create',
                                                                [pos_order_data])
                
                print(f"Pos Order baru telah dibuat dengan ID: {new_pos_order_id}")
                
                self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'pos.order', 'action_pos_order_invoice',
                                                [new_pos_order_id])
                
                print(f"Pos Order baru telah dibuatkan invoice ID: {new_pos_order_id}")

    def transfer_pos_order_session(self, model_name, fields, description):
        # Ambil data dari sumber
        transaksi_posorder_session = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        model_name, 'search_read',
                                                        [[['state', '=', 'opened']]],
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
                                                                        [[['name', '=', record.get('name')]]],
                                                                        {'fields': ['id'], 'limit': 1})

            if not existing_pos_order_invoice:
                pos_session_data = {
                    'name_session_pos': record.get('name'),
                    'config_id': session_target_id,
                    'user_id': user_target_id,
                    'start_at': record.get('start_at'),
                    'stop_at': record.get('stop_at'),
                    'cash_register_balance_start': record.get('cash_register_balance_start'),
                    'cash_register_balance_end_real': record.get('cash_register_balance_end_real'),
                    'state': record.get('state'),
                }

            try:
                new_session_pos_order_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'pos.session', 'create',
                                                                [pos_session_data])
                print(f"Pos Order baru telah dibuat dengan ID: {new_session_pos_order_id}")
            except Exception as e:
                print(f"Terjadi kesalahan saat membuat pos order baru: {e}")

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
            update_result = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                             self.target_client.uid, self.target_client.password,
                                             model_name, 'write',
                                             [[session_id], {'state': state, 'start_at': start_at, 'stop_at': stop_at}])

            if update_result:
                print(f"Berhasil mengupdate sesi {name} dengan status {state}.")
            else:
                print(f"Gagal mengupdate sesi {name}.")
        
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

class SetLogMC:
    def __init__(self, source_client):
        self.source_client = source_client

    def log_record_success(self, record, modul):
        gmt_7_now = datetime.now() - timedelta(hours=7)  # Odoo menggunakan UTC, belum diatur zona waktunya
        record_log_success = {
            'vit_doc_type': modul,
            'vit_trx_key': record.get('name'),
            'vit_trx_date': record.get('create_date'),
            'vit_sync_date': gmt_7_now.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_sync_status': 'Success',
            'vit_sync_desc': f"Data yang masuk: {record}"
        }
        return record_log_success
    
    def log_record_failed(self, record, modul, sync_status):
        gmt_7_now = datetime.now() - timedelta(hours=7)  # Odoo menggunakan UTC, belum diatur zona waktunya
        record_log_failed = {
            'vit_doc_type': modul,
            'vit_trx_key': record.get('name'),
            'vit_trx_date': record.get('create_date'),
            'vit_sync_date': gmt_7_now.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_sync_status': 'Failed',
            'vit_sync_desc': f"{sync_status}"
        }
        return record_log_failed

    def log_runtime(self, start_time, end_time, duration, modul):
        gmt_7_start_time = datetime.fromtimestamp(start_time) - timedelta(hours=7)
        gmt_7_end_time = datetime.fromtimestamp(end_time) - timedelta(hours=7)
        runtime_log = {
            'vit_code_type': f"{modul}",
            'vit_start_sync': gmt_7_start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_end_sync': gmt_7_end_time.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_duration': f"{duration:.2f} second"
        }
        return runtime_log

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

    def create_log_note_success(self, record, modul):
        try:
            log_record = self.log_record_success(record, modul)
            self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                         self.source_client.password, 'log.note', 'create', [log_record])
            print(f"Data log note yang masuk: {log_record}")
        except Exception as e:
            print(f"An error occurred while creating log note: {e}")

    def create_log_note_failed(self, record, modul, sync_status):
        try:
            log_record = self.log_record_failed(record, modul, sync_status)
            self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                         self.source_client.password, 'log.note', 'create', [log_record])
            print(f"Data log note yang masuk: {log_record}")
        except Exception as e:
            print(f"An error occurred while creating log note: {e}")

    def create_log_runtime_odoo(self, start_time, end_time, duration, modul):
        try:
            runtime_log = self.log_runtime(start_time, end_time, duration, modul)
            self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                         self.source_client.password, 'log.code.runtime', 'create', [runtime_log])
            print(f"Data log runtime yang masuk: {runtime_log}")
        except Exception as e:
            print(f"An error occurred while creating log runtime: {e}")

class SetLogSS:
    def __init__(self, target_client):
        self.target_client = target_client

    def log_record_success(self, record, modul):
        gmt_7_now = datetime.now() - timedelta(hours=7)  # Odoo menggunakan UTC, belum diatur zona waktunya
        record_log_success = {
            'vit_doc_type': modul,
            'vit_trx_key': record.get('name'),
            'vit_trx_date': record.get('create_date'),
            'vit_sync_date': gmt_7_now.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_sync_status': 'Success',
            'vit_sync_desc': f"Data yang masuk: {record}"
        }
        return record_log_success
    
    def log_record_failed(self, record, modul, sync_status):
        gmt_7_now = datetime.now() - timedelta(hours=7)  # Odoo menggunakan UTC, belum diatur zona waktunya
        record_log_failed = {
            'vit_doc_type': modul,
            'vit_trx_key': record.get('name'),
            'vit_trx_date': record.get('create_date'),
            'vit_sync_date': gmt_7_now.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_sync_status': 'Failed',
            'vit_sync_desc': f"{sync_status}"
        }
        return record_log_failed

    def log_runtime(self, start_time, end_time, duration, modul):
        gmt_7_start_time = datetime.fromtimestamp(start_time) - timedelta(hours=7)
        gmt_7_end_time = datetime.fromtimestamp(end_time) - timedelta(hours=7)
        runtime_log = {
            'vit_code_type': f"{modul}",
            'vit_start_sync': gmt_7_start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_end_sync': gmt_7_end_time.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_duration': f"{duration:.2f} second"
        }
        return runtime_log

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

    def create_log_note_success(self, record, modul):
        try:
            log_record = self.log_record_success(record, modul)
            self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                         self.target_client.password, 'log.note', 'create', [log_record])
            print(f"Data log note yang masuk: {log_record}")
        except Exception as e:
            print(f"An error occurred while creating log note: {e}")

    def create_log_note_failed(self, record, modul, sync_status):
        try:
            log_record = self.log_record_failed(record, modul, sync_status)
            self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                         self.target_client.password, 'log.note', 'create', [log_record])
            print(f"Data log note yang masuk: {log_record}")
        except Exception as e:
            print(f"An error occurred while creating log note: {e}")

    def create_log_runtime_odoo(self, start_time, end_time, duration, modul):
        try:
            runtime_log = self.log_runtime(start_time, end_time, duration, modul)
            self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                         self.target_client.password, 'log.code.runtime', 'create', [runtime_log])
            print(f"Data log runtime yang masuk: {runtime_log}")
        except Exception as e:
            print(f"An error occurred while creating log runtime: {e}")
