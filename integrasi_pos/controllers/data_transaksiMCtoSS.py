import time
from datetime import datetime, timedelta
import pytz
import re
import concurrent.futures

# kalau ada case store nya beda zona waktu gimana
class DataTransaksiMCtoSS:
    def __init__(self, source_client, target_client):
        self.source_client = source_client
        self.target_client = target_client
        self.set_log_mc = SetLogMC(self.source_client)
        self.set_log_ss = SetLogSS(self.target_client)

    def transfer_bom_master(self, model_name, fields, description, date_from, date_to):
        try:
            # Step 1: Fetch BoM master from source
            transaksi_bom_master = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                model_name, 'search_read',
                [[['is_integrated', '=', False]]],
                {'fields': fields})

            if not transaksi_bom_master:
                print("Semua transaksi telah diproses.")
                return

            # STEP A - Mapping product.template with default_code
            product_template_ids = [record['product_tmpl_id'][0] if isinstance(record['product_tmpl_id'], list) else record['product_tmpl_id'] for record in transaksi_bom_master]

            template_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'product.template', 'search_read',
                [[['id', 'in', list(set(product_template_ids))]]],
                {'fields': ['id', 'default_code']})

            template_id_to_default_code = {tpl['id']: tpl['default_code'] for tpl in template_source if tpl.get('default_code')}

            template_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'product.template', 'search_read',
                [[['default_code', 'in', list(template_id_to_default_code.values())]]],
                {'fields': ['id', 'default_code']})

            default_code_to_target_template_id = {tpl['default_code']: tpl['id'] for tpl in template_target}

            # STEP B - Mapping product.product with default_code (header)
            product_variant_ids = [record['product_id'][0] if isinstance(record['product_id'], list) else record['product_id'] for record in transaksi_bom_master if record.get('product_id')]

            product_variant_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'product.product', 'search_read',
                [[['id', 'in', list(set(product_variant_ids))]]],
                {'fields': ['id', 'product_tmpl_id', 'default_code']})

            product_id_to_default_code = {p['id']: p['default_code'] for p in product_variant_source if p.get('default_code')}
            default_code_to_product_tmpl_id = {p['default_code']: p['product_tmpl_id'] for p in product_variant_source if p.get('default_code')}

            product_variant_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'product.product', 'search_read',
                [[['default_code', 'in', list(default_code_to_product_tmpl_id.keys())]]],
                {'fields': ['id', 'default_code']})

            default_code_to_target_product_id = {p['default_code']: p['id'] for p in product_variant_target}

            # STEP C - Mapping bom.line (component items)
            bom_ids = [record['id'] for record in transaksi_bom_master]

            bom_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'mrp.bom.line', 'search_read',
                [[['bom_id', 'in', bom_ids]]],
                {'fields': ['bom_id', 'product_id', 'product_qty']})

            bom_component_ids = [line['product_id'][0] for line in bom_lines if line.get('product_id')]

            product_line_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'product.product', 'search_read',
                [[['id', 'in', list(set(bom_component_ids))]]],
                {'fields': ['id', 'product_tmpl_id', 'default_code']})

            product_line_id_to_default_code = {p['id']: p['default_code'] for p in product_line_source if p.get('default_code')}
            default_code_line_to_template_id = {p['default_code']: p['product_tmpl_id'] for p in product_line_source if p.get('default_code')}

            product_line_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'product.product', 'search_read',
                [[['default_code', 'in', list(default_code_line_to_template_id.keys())]]],
                {'fields': ['id', 'default_code']})

            default_code_to_target_line_product_id = {p['default_code']: p['id'] for p in product_line_target}

            # STEP D - Ambil semua BoM yang sudah ada di target (untuk pengecekan update)
            all_bom_codes = [record.get('code') for record in transaksi_bom_master if record.get('code')]
            
            existing_boms = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'mrp.bom', 'search_read',
                [[['code', 'in', all_bom_codes]]],
                {'fields': ['id', 'code']})
            
            code_to_existing_bom_id = {bom['code']: bom['id'] for bom in existing_boms if bom.get('code')}

            # STEP E - Process each BoM
            def process_bom(record):
                src_tmpl_id = record['product_tmpl_id'][0] if isinstance(record['product_tmpl_id'], list) else record['product_tmpl_id']
                src_template_code = template_id_to_default_code.get(src_tmpl_id)
                if not src_template_code:
                    error_message = f"BoM:{record['id']} tidak punya default_code template di source untuk tmpl_id: {src_tmpl_id}"
                    print(error_message)
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Master BOM', error_message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Master BOM', error_message, write_date)
                    return

                product_id = default_code_to_target_template_id.get(src_template_code)

                src_variant_id = record['product_id'][0] if isinstance(record['product_id'], list) else record['product_id']
                src_variant_code = product_id_to_default_code.get(src_variant_id)

                product_variant_id = default_code_to_target_product_id.get(src_variant_code) if src_variant_code else False

                if not src_variant_code:
                    print(f"⚠️ BoM:{record['id']} tidak punya default_code variant → lanjut tanpa variant_id")

                print(f"[BoM:{record['id']}] → product_id={product_id}, variant_id={product_variant_id}")

                # Fetch component lines for this BoM
                bom_line_items = [line for line in bom_lines if line.get('bom_id') and line['bom_id'][0] == record['id']]
                bom_line_vals = []
                missing_products = []

                for line in bom_line_items:
                    src_line_product_id = line['product_id'][0]
                    src_line_default_code = product_line_id_to_default_code.get(src_line_product_id)
                    target_line_product_id = default_code_to_target_line_product_id.get(src_line_default_code)

                    if not target_line_product_id:
                        missing_products.append(src_line_default_code)
                        continue

                    bom_line_vals.append((0, 0, {
                        'product_id': target_line_product_id,
                        'product_qty': line['product_qty'] or 0.0
                    }))

                if missing_products:
                    print(f"⚠️ Produk tidak ditemukan di target: {missing_products}")

                bom_data = {
                    'product_tmpl_id': product_id,
                    'product_id': product_variant_id or False,
                    'product_qty': record.get('product_qty') or 1.0,
                    'code': record.get('code'),
                    'type': record.get('type'),
                    'consumption': record.get('consumption'),
                    'produce_delay': record.get('produce_delay'),
                    'days_to_prepare_mo': record.get('days_to_prepare_mo'),
                }

                # Cek apakah BoM sudah ada berdasarkan code
                bom_code = record.get('code')
                existing_bom_id = code_to_existing_bom_id.get(bom_code) if bom_code else None

                try:
                    if existing_bom_id:
                        # UPDATE: Hapus line lama dan tambah line baru
                        bom_data['bom_line_ids'] = [(5, 0, 0)] + bom_line_vals  # (5,0,0) = delete all lines
                        
                        self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                            self.target_client.uid, self.target_client.password,
                            'mrp.bom', 'write',
                            [[existing_bom_id], bom_data])
                        
                        print(f"🔄 BoM {record['id']} → updated in target (ID {existing_bom_id})")
                    else:
                        # CREATE: Buat baru
                        bom_data['bom_line_ids'] = bom_line_vals
                        
                        new_bom_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                            self.target_client.uid, self.target_client.password,
                            'mrp.bom', 'create', [bom_data])

                        print(f"✅ BoM {record['id']} → created in target with ID {new_bom_id}")

                except Exception as e:
                    print(f"💥 Gagal create/update BoM ID {record['id']}: {e}")
                    return

                try:
                    index_store_ids = record.get('index_store', [])
                    
                    # Ambil setting config yang aktif untuk target integrasi
                    setting_config_ids = self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'setting.config', 'search_read',
                        [[['vit_config_server', '=', 'ss'], ['vit_linked_server', '=', True]]],
                        {'fields': ['id']}
                    )
                    setting_config_ids = [config['id'] for config in setting_config_ids]

                    # Update index_store dengan setting config IDs yang aktif
                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'mrp.bom', 'write',
                        [[record['id']], {
                            'index_store': [(6, 0, setting_config_ids)]
                        }]
                    )
                    print(f"📌 Field index_store diperbarui untuk BoM ID {record['id']}.")

                    # Periksa apakah semua setting config sudah tercakup di index_store
                    if len(index_store_ids) + 1 >= len(setting_config_ids):
                        self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'mrp.bom', 'write',
                            [[record['id']], {
                                'is_integrated': True,
                                'index_store': [(5, 0, 0)]  # clear
                            }]
                        )
                        print(f"✅ BoM ID {record['id']} telah diintegrasi ke semua target.")
                    else:
                        self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'mrp.bom', 'write',
                            [[record['id']], {
                                'is_integrated': False
                            }]
                        )
                        print(f"⌛ BoM ID {record['id']} belum terintegrasi ke semua target.")
                except Exception as e:
                    print(f"⚠️ Gagal update index_store/is_integrated untuk BoM {record['id']}: {e}")

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_bom, record) for record in transaksi_bom_master]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"💣 ERROR di transfer_bom_master: {e}")

    def config_timbangan(self, model_name, fields, description, date_from, date_to):
        config_timbangan = self.source_client.call_odoo(
            'object', 'execute_kw', self.source_client.db,
            self.source_client.uid, self.source_client.password,
            model_name, 'search_read',
            [[
                ['is_integrated', '=', False],
                ['create_date', '>=', date_from],
                ['create_date', '<=', date_to]
            ]],
            {'fields': fields}
        )

        if not config_timbangan:
            print("Semua config timbangan telah diproses.")
            return

        for rec in config_timbangan:
            rec_id = rec.get('id')
            prefix_timbangan = rec.get('prefix_timbangan')
            digit_awal = rec.get('digit_awal')
            digit_akhir = rec.get('digit_akhir')
            panjang_barcode = rec.get('panjang_barcode')

            timbangan_data = {
                'prefix_timbangan': prefix_timbangan,
                'digit_awal': digit_awal,
                'digit_akhir': digit_akhir,
                'panjang_barcode': panjang_barcode,
            }

            try:
                start_time = time.time()

                # Buat timbangan config di target
                new_timbangan_id = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'barcode.config', 'create',
                    [timbangan_data]
                )

                # Ambil index_store saat ini (list of setting.config id yang sudah integrasi)
                index_store_ids = rec.get('index_store', [])

                # Ambil semua setting.config yang aktif
                setting_config_ids = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'setting.config', 'search_read',
                    [[['vit_config_server', '=', 'ss'], ['vit_linked_server', '=', True]]],
                    {'fields': ['id']}
                )
                setting_config_ids = [config['id'] for config in setting_config_ids]

                # Update record source → tambahkan index_store (mengganti keseluruhan isian)
                self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    model_name, 'write',
                    [[rec_id], {
                        'index_store': [(6, 0, setting_config_ids)],
                    }]
                )
                print(f"📌 index_store diperbarui untuk {model_name} ID {rec_id}")

                # Cek apakah semua setting.config sudah tercakup
                if len(index_store_ids) + 1 >= len(setting_config_ids):  # `+1` karena yang sekarang baru diproses
                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        model_name, 'write',
                        [[rec_id], {
                            'is_integrated': True,
                            'index_store': [(5, 0, 0)]  # Clear
                        }]
                    )
                    print(f"✅ {model_name} ID {rec_id} telah berhasil diintegrasi ke semua target.")
                else:
                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        model_name, 'write',
                        [[rec_id], {
                            'is_integrated': False
                        }]
                    )
                    print(f"⌛ {model_name} ID {rec_id} belum terintegrasi ke semua target.")

                end_time = time.time()
                duration = end_time - start_time

                print(f"Inventory Stock baru telah dibuat dengan ID: {new_timbangan_id}")

                write_date = self.get_write_date(model_name, rec_id)
                self.set_log_mc.create_log_note_success(rec, start_time, end_time, duration, 'Timbangan Configuration', write_date)
                self.set_log_ss.create_log_note_success(rec, start_time, end_time, duration, 'Timbangan Configuration', write_date)

            except Exception as e:
                error_message = str(e)
                print(f"Gagal membuat Inventory Stock baru: {error_message}")
                write_date = self.get_write_date(model_name, rec_id)
                self.set_log_mc.create_log_note_failed(rec, 'Timbangan Configuration', error_message, write_date)
                self.set_log_ss.create_log_note_failed(rec, 'Timbangan Configuration', error_message, write_date)

    def get_picking_type_id(self, picking_type_name):
        picking_type = self.target_client.call_odoo(
            'object', 'execute_kw', self.target_client.db,
            self.target_client.uid, self.target_client.password,
            'stock.picking.type', 'search_read',
            [[['name', '=', picking_type_name]]],
            {'fields': ['id'], 'limit': 1}
        )
        return picking_type[0]['id'] if picking_type else None
    
    # Master Console --> Store Server
    # Store Server --> Master Console
    def update_loyalty_point_mc_to_ss(self, model_name, fields, description, date_from, date_to):
        if isinstance(date_from, datetime):
            date_from = date_from.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(date_to, datetime):
            date_to = date_to.strftime('%Y-%m-%d %H:%M:%S')
            
        id_program = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    model_name, 'search_read',
                                                    [[]],
                                                    {'fields': fields})

        for res in id_program:
            programs = res.get('id', False)

            # Ambil data dari sumber
            loyalty_points = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'loyalty.card', 'search_read',
                                                        [[['program_id', '=', int(programs)], ['is_updated', '=', True]]],
                                                        {'fields': ['code', 'points_display', 'expiration_date', 'program_id', 'currency_id', 'partner_id', 'source_pos_order_id', 'points']})

            # Pre-fetch necessary data to reduce API calls
            pos_order_ids = {record.get('source_pos_order_id')[0] for record in loyalty_points if record.get('source_pos_order_id')}
            pos_orders = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    'pos.order', 'search_read',
                                                    [[['id', 'in', list(pos_order_ids)]]],
                                                    {'fields': ['id', 'vit_trxid']})

            pos_order_map = {order['id']: order['vit_trxid'] for order in pos_orders}

            program_ids = {record.get('program_id')[0] for record in loyalty_points if record.get('program_id')}
            programs_data = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'loyalty.program', 'search_read',
                                                        [[['id', 'in', list(program_ids)]]],
                                                        {'fields': ['id', 'name']})

            program_map = {program['id']: program['name'] for program in programs_data}

            partner_ids = {record.get('partner_id')[0] for record in loyalty_points if record.get('partner_id')}
            partners_data = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'res.partner', 'search_read',
                                                        [[['id', 'in', list(partner_ids)]]],
                                                        {'fields': ['id', 'customer_code']})

            partner_map = {partner['id']: partner['customer_code'] for partner in partners_data}

            # Pre-fetch order references, program IDs, and partner IDs
            order_references = {}
            program_id_sets = {}
            partner_id_sets = {}

            for record in loyalty_points:
                order_ref = False
                if record.get('source_pos_order_id'):
                    order_ref = pos_order_map.get(record['source_pos_order_id'][0])

                if order_ref and order_ref not in order_references:
                    order_reference = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'pos.order', 'search_read',
                                                                    [[['name', '=', order_ref]]],
                                                                    {'fields': ['id'], 'limit': 1})
                    order_references[order_ref] = order_reference[0]['id'] if order_reference else False

                program_id = record.get('program_id')
                if program_id and program_id[0] not in program_id_sets:
                    program_id_new = program_map.get(program_id[0])
                    program_id_set = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'loyalty.program', 'search_read',
                                                                    [[['name', '=', program_id_new]]],
                                                                    {'fields': ['id'], 'limit': 1})
                    program_id_sets[program_id[0]] = program_id_set[0]['id'] if program_id_set else False

                partner_id = record.get('partner_id')
                if partner_id and partner_id[0] not in partner_id_sets:
                    partner_id_new = partner_map.get(partner_id[0])
                    partner_id_set = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'res.partner', 'search_read',
                                                                    [[['customer_code', '=', partner_id_new]]],
                                                                    {'fields': ['id'], 'limit': 1})
                    partner_id_sets[partner_id[0]] = partner_id_set[0]['id'] if partner_id_set else False

            def process_loyalty_point(record):
                existing_loyalty_points_mc = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'loyalty.card', 'search_read',
                    [[['code', '=', record['code']]]],
                    {'fields': ['id']}
                )

                if existing_loyalty_points_mc:
                    loyalty_id = existing_loyalty_points_mc[0]['id']
                    code = record.get('code')
                    expiration_date = record.get('expiration_date')
                    points = record.get('points')
                    points_display = record.get('points_display')

                    order_id = order_references.get(record.get('source_pos_order_id')[0], False) if record.get('source_pos_order_id') else False
                    program_id = program_id_sets.get(record.get('program_id')[0], False) if record.get('program_id') else False
                    partner_id = partner_id_sets.get(record.get('partner_id')[0], False) if record.get('partner_id') else False

                    data_loyalty_mc = {
                        'code': code,
                        'expiration_date': expiration_date,
                        'points': points,
                        'points_display': points_display,
                        'source_pos_order_id': order_id,
                        'program_id': program_id,
                        'partner_id': partner_id
                    }

                    try:
                        # Menggunakan `write` untuk memperbarui data yang sudah ada
                        self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                    self.target_client.uid, self.target_client.password,
                                                    'loyalty.card', 'write',
                                                    [[loyalty_id], data_loyalty_mc])
                        print(f"Loyalty dengan ID {loyalty_id} telah diperbarui di target_client.")
                    except Exception as e:
                        print(f"Terjadi kesalahan saat memperbarui loyalty: {e}")

            # Process loyalty points in batches of 100
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                for i in range(0, len(loyalty_points), 100):
                    batch = loyalty_points[i:i + 100]
                    executor.map(process_loyalty_point, batch)

    # Master Console --> Store Server
    # Store Server --> Master Console
    def transfer_discount_loyalty(self, model_name, fields, description, date_from, date_to):
        try:
            discount_loyalty = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    model_name, 'search_read',
                                                    [[['active', '=', True], ['is_integrated', '=', False]]],
                                                    {'fields': fields})
            if not discount_loyalty:
                print("Tidak ada discount/loyalty yang ditemukan untuk ditransfer.")
                return

            print(f"Found {len(discount_loyalty)} discount/loyalty programs to transfer")

            # Fetch existing discount/loyalty programs in target
            discount_names = [record['name'] for record in discount_loyalty]
            existing_discount_loyalty = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'loyalty.program', 'search_read',
                [[['vit_trxid', 'in', discount_names], ['active', '=', True]]],
                {'fields': ['id', 'vit_trxid']}
            )
            existing_discount_dict = {record['vit_trxid']: record['id'] for record in existing_discount_loyalty}

            order_ids = [record['id'] for record in discount_loyalty]
            
            # Fetch all reward and rule lines for all programs at once
            reward_ids_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'loyalty.reward', 'search_read',
                                                            [[['program_id', 'in', order_ids]]],
                                                            {'fields': ['reward_type', 'discount', 'discount_applicability', 'discount_line_product_id', 'discount_max_amount', 'required_points', 'description', 'discount_mode', 'discount_product_domain', 'discount_product_ids', 'discount_product_category_id', 'vit_trxid', 'program_id', 'reward_product_id', 'discount_product_tag_id', 'vit_reward_trxid']})

            rule_ids_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'loyalty.rule', 'search_read',
                                                        [[['program_id', 'in', order_ids]]],
                                                        {'fields': ['minimum_qty', 'code', 'minimum_amount', 'reward_point_amount', 'reward_point_mode', 'product_domain', 'product_ids', 'product_category_id', 'minimum_amount_tax_mode', 'vit_trxid', 'program_id', 'product_tag_id']})
            
            # Fetch schedule and member data
            schedule_ids_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'loyalty.program.schedule', 'search_read',
                                                            [[['program_id', 'in', order_ids]]],
                                                            {'fields': ['program_id', 'days', 'time_start', 'time_end']})

            member_ids_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'loyalty.member', 'search_read',
                                                        [[['member_program_id', 'in', order_ids]]],
                                                        {'fields': ['member_program_id', 'member_pos']})
            
            # Collect all product and category IDs (with null checks)
            product_ids_reward = []
            for product in reward_ids_lines:
                if product.get('discount_product_ids'):
                    product_ids_reward.extend(product['discount_product_ids'])
            
            reward_product_id = []
            for record in reward_ids_lines:
                if record.get('reward_product_id'):
                    if isinstance(record['reward_product_id'], list):
                        reward_product_id.append(record['reward_product_id'][0])
                    else:
                        reward_product_id.append(record['reward_product_id'])
            
            product_ids_rule = []
            for product in rule_ids_lines:
                if product.get('product_ids'):
                    product_ids_rule.extend(product['product_ids'])
            
            category_ids_reward = []
            for record in reward_ids_lines:
                if record.get('discount_product_category_id'):
                    if isinstance(record['discount_product_category_id'], list):
                        category_ids_reward.append(record['discount_product_category_id'][0])
                    else:
                        category_ids_reward.append(record['discount_product_category_id'])
            
            category_ids_rule = []
            for record in rule_ids_lines:
                if record.get('product_category_id'):
                    if isinstance(record['product_category_id'], list):
                        category_ids_rule.append(record['product_category_id'][0])
                    else:
                        category_ids_rule.append(record['product_category_id'])
            
            product_tag_ids_rule = []
            for record in rule_ids_lines:
                if record.get('product_tag_id'):
                    if isinstance(record['product_tag_id'], list):
                        product_tag_ids_rule.append(record['product_tag_id'][0])
                    else:
                        product_tag_ids_rule.append(record['product_tag_id'])
            
            product_tag_ids_reward = []
            for record in reward_ids_lines:
                if record.get('discount_product_tag_id'):
                    if isinstance(record['discount_product_tag_id'], list):
                        product_tag_ids_reward.append(record['discount_product_tag_id'][0])
                    else:
                        product_tag_ids_reward.append(record['discount_product_tag_id'])

            # Collect member category IDs
            member_pos_ids = []
            for record in member_ids_lines:
                if record.get('member_pos'):
                    if isinstance(record['member_pos'], list):
                        member_pos_ids.append(record['member_pos'][0])
                    else:
                        member_pos_ids.append(record['member_pos'])
            
            currency_ids = []
            for record in discount_loyalty:
                if record.get('currency_id'):
                    if isinstance(record['currency_id'], list):
                        currency_ids.append(record['currency_id'][0])
                    else:
                        currency_ids.append(record['currency_id'])
            
            pricelist_ids = []
            for record in discount_loyalty:
                if record.get('pricelist_ids'):
                    pricelist_ids.extend(record['pricelist_ids'])
            
            pos_config_ids = []
            for record in discount_loyalty:
                if record.get('pos_config_ids'):
                    pos_config_ids.extend(record['pos_config_ids'])
            
            # Fetch source data only if IDs exist
            reward_product_id_source = []
            if reward_product_id:
                reward_product_id_source = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'product.product', 'search_read',
                    [[['id', 'in', reward_product_id]]],
                    {'fields': ['id', 'default_code']}
                )
            
            products_source_reward = []
            if product_ids_reward:
                products_source_reward = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'product.product', 'search_read',
                    [[['id', 'in', product_ids_reward]]],
                    {'fields': ['id', 'default_code']}
                )
            
            products_source_rule = []
            if product_ids_rule:
                products_source_rule = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'product.product', 'search_read',
                    [[['id', 'in', product_ids_rule]]],
                    {'fields': ['id', 'default_code']}
                )
            
            categories_source_reward = []
            if category_ids_reward:
                categories_source_reward = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'product.category', 'search_read',
                    [[['id', 'in', category_ids_reward]]],
                    {'fields': ['id', 'complete_name']}
                )
            
            product_tag_source_reward = []
            if product_tag_ids_reward:
                product_tag_source_reward = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'product.tag', 'search_read',
                    [[['id', 'in', product_tag_ids_reward]]],
                    {'fields': ['id', 'name']}
                )
            
            categories_source_rule = []
            if category_ids_rule:
                categories_source_rule = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'product.category', 'search_read',
                    [[['id', 'in', category_ids_rule]]],
                    {'fields': ['id', 'complete_name']}
                )
            
            product_tag_source_rule = []
            if product_tag_ids_rule:
                product_tag_source_rule = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'product.tag', 'search_read',
                    [[['id', 'in', product_tag_ids_rule]]],
                    {'fields': ['id', 'name']}
                )
            
            currencies_source = []
            if currency_ids:
                currencies_source = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'res.currency', 'search_read',
                    [[['id', 'in', currency_ids]]],
                    {'fields': ['id', 'name']}
                )
            
            pricelists_source = []
            if pricelist_ids:
                pricelists_source = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'product.pricelist', 'search_read',
                    [[['id', 'in', pricelist_ids]]],
                    {'fields': ['id', 'name']}
                )
            
            pos_configs_source = []
            if pos_config_ids:
                pos_configs_source = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'pos.config', 'search_read',
                    [[['id', 'in', pos_config_ids]]],
                    {'fields': ['id', 'name']}
                )
            
            # Fetch member categories from source
            member_categories_source = []
            if member_pos_ids:
                member_categories_source = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'res.partner.category', 'search_read',
                    [[['id', 'in', member_pos_ids]]],
                    {'fields': ['id', 'name']}
                )
            
            # Fetch corresponding data from target (with empty list checks)
            reward_product_id_target = []
            if reward_product_id_source:
                reward_product_id_target = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'product.product', 'search_read',
                    [[['default_code', 'in', [product['default_code'] for product in reward_product_id_source if product.get('default_code')]]]],
                    {'fields': ['id', 'default_code']}
                )
            
            products_target_reward = []
            if products_source_reward:
                products_target_reward = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'product.product', 'search_read',
                    [[['default_code', 'in', [product['default_code'] for product in products_source_reward if product.get('default_code')]]]],
                    {'fields': ['id', 'default_code']}
                )
            
            products_target_rule = []
            if products_source_rule:
                products_target_rule = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'product.product', 'search_read',
                    [[['default_code', 'in', [product['default_code'] for product in products_source_rule if product.get('default_code')]]]],
                    {'fields': ['id', 'default_code']}
                )
            
            categories_target_reward = []
            if categories_source_reward:
                categories_target_reward = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'product.category', 'search_read',
                    [[['complete_name', 'in', [category['complete_name'] for category in categories_source_reward if category.get('complete_name')]]]],
                    {'fields': ['id', 'complete_name']}
                )
            
            product_tag_id_target_reward = []
            if product_tag_source_reward:
                product_tag_id_target_reward = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'product.tag', 'search_read',
                    [[['name', 'in', [tag['name'] for tag in product_tag_source_reward if tag.get('name')]]]],
                    {'fields': ['id', 'name']}
                )
            
            categories_target_rule = []
            if categories_source_rule:
                categories_target_rule = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'product.category', 'search_read',
                    [[['complete_name', 'in', [category['complete_name'] for category in categories_source_rule if category.get('complete_name')]]]],
                    {'fields': ['id', 'complete_name']}
                )
            
            product_tag_target_rule = []
            if product_tag_source_rule:
                product_tag_target_rule = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'product.tag', 'search_read',
                    [[['name', 'in', [tags['name'] for tags in product_tag_source_rule if tags.get('name')]]]],
                    {'fields': ['id', 'name']}
                )
            
            currencies_target = []
            if currencies_source:
                currencies_target = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'res.currency', 'search_read',
                    [[['name', 'in', [currency['name'] for currency in currencies_source if currency.get('name')]]]],
                    {'fields': ['id', 'name']}
                )
            
            pricelists_target = []
            if pricelists_source:
                pricelists_target = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'product.pricelist', 'search_read',
                    [[['name', 'in', [pricelist['name'] for pricelist in pricelists_source if pricelist.get('name')]]]],
                    {'fields': ['id', 'name']}
                )
            
            pos_configs_target = []
            if pos_configs_source:
                pos_configs_target = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'pos.config', 'search_read',
                    [[['name', 'in', [pos_config['name'] for pos_config in pos_configs_source if pos_config.get('name')]]]],
                    {'fields': ['id', 'name']}
                )

            # Fetch member categories from target
            member_categories_target = []
            if member_categories_source:
                member_categories_target = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'res.partner.category', 'search_read',
                    [[['name', 'in', [category['name'] for category in member_categories_source if category.get('name')]]]],
                    {'fields': ['id', 'name']}
                )

            # Create mapping dictionaries
            product_dict_reward = {product['default_code']: product['id'] for product in products_target_reward if product.get('default_code')}
            reward_product_id_dict = {product['default_code']: product['id'] for product in reward_product_id_target if product.get('default_code')}
            product_dict_rule = {product['default_code']: product['id'] for product in products_target_rule if product.get('default_code')}
            category_dict_reward = {category['complete_name']: category['id'] for category in categories_target_reward if category.get('complete_name')}
            category_dict_rule = {category['complete_name']: category['id'] for category in categories_target_rule if category.get('complete_name')}
            product_tag_dict_reward = {tag['name']: tag['id'] for tag in product_tag_id_target_reward if tag.get('name')}
            product_tag_dict_rule = {tag['name']: tag['id'] for tag in product_tag_target_rule if tag.get('name')}
            currency_dict = {currency['name']: currency['id'] for currency in currencies_target if currency.get('name')}
            pricelist_dict = {pricelist['name']: pricelist['id'] for pricelist in pricelists_target if pricelist.get('name')}
            pos_config_dict = {pos_config['name']: pos_config['id'] for pos_config in pos_configs_target if pos_config.get('name')}
            member_category_dict = {category['name']: category['id'] for category in member_categories_target if category.get('name')}

            def process_create_discount(record):
                if record['vit_trxid'] in existing_discount_dict:
                    print(f"Program {record.get('name')} already exists, skipping...")
                    return
                
                current_reward_lines = [line for line in reward_ids_lines if line['program_id'][0] == record['id']]
                current_rule_lines = [line for line in rule_ids_lines if line['program_id'][0] == record['id']]
                current_schedule_lines = [line for line in schedule_ids_lines if line['program_id'][0] == record['id']]
                current_member_lines = [line for line in member_ids_lines if line['member_program_id'][0] == record['id']]

                # === SIMPAN MAPPING: description -> default_code ===
                source_discount_product_codes = {}
                for line in current_reward_lines:
                    if line.get('discount_line_product_id'):
                        source_discount_product_id = line['discount_line_product_id'][0] if isinstance(
                            line['discount_line_product_id'], list) else line['discount_line_product_id']
                        
                        try:
                            source_discount_product = self.source_client.call_odoo(
                                'object', 'execute_kw', self.source_client.db,
                                self.source_client.uid, self.source_client.password,
                                'product.product', 'read',
                                [[source_discount_product_id]],
                                {'fields': ['default_code']}
                            )
                            
                            if source_discount_product and source_discount_product[0].get('default_code'):
                                reward_description = line.get('description')
                                if reward_description:
                                    source_discount_product_codes[reward_description] = source_discount_product[0]['default_code']
                                    print(f"Stored default_code: {source_discount_product[0]['default_code']} for reward description: {reward_description}")
                        except Exception as e:
                            print(f"Failed to fetch default_code for discount product {source_discount_product_id}: {e}")
                # === AKHIR MAPPING ===

                discount_loyalty_line_ids = []
                for line in current_reward_lines:
                    if isinstance(line, dict):
                        # Mapping reward products
                        reward_product_ids = line.get('discount_product_ids', [])
                        reward_target_product_ids = [product_dict_reward.get(product['default_code']) 
                                                for product in products_source_reward 
                                                if product['id'] in reward_product_ids and product_dict_reward.get(product['default_code'])]

                        # Mapping reward_product_id
                        reward_product_id_field = line.get('reward_product_id')
                        reward_product_id_field = reward_product_id_field[0] if isinstance(reward_product_id_field, list) else reward_product_id_field

                        reward_product_default_code = None
                        if reward_product_id_field:
                            if isinstance(reward_product_id_field, list) and len(reward_product_id_field) == 2:
                                reward_product_default_code = reward_product_id_field[1]
                            else:
                                reward_product_default_code = next(
                                    (product['default_code'] for product in reward_product_id_source if product['id'] == reward_product_id_field),
                                    None
                                )

                        reward_product_id_id = reward_product_id_dict.get(reward_product_default_code) if reward_product_default_code else None

                        # Mapping reward category
                        reward_source_category_id = line.get('discount_product_category_id')
                        reward_source_category_name = None
                        if reward_source_category_id:
                            if isinstance(reward_source_category_id, list) and len(reward_source_category_id) == 2:
                                reward_source_category_name = reward_source_category_id[1]
                            else:
                                reward_source_category_name = next((category['complete_name'] for category in categories_source_reward if category['id'] == reward_source_category_id), None)

                        reward_target_category_id = category_dict_reward.get(reward_source_category_name) if reward_source_category_name else None

                        # Mapping reward product tag
                        reward_source_product_tag_id = line.get('discount_product_tag_id')
                        reward_source_product_tag_name = None
                        if reward_source_product_tag_id:
                            if isinstance(reward_source_product_tag_id, list) and len(reward_source_product_tag_id) == 2:
                                reward_source_product_tag_name = reward_source_product_tag_id[1]
                            else:
                                reward_source_product_tag_name = next((tag['name'] for tag in product_tag_source_reward if tag['id'] == reward_source_product_tag_id), None)

                        reward_target_product_tag_id = product_tag_dict_reward.get(reward_source_product_tag_name) if reward_source_product_tag_name else None

                        reward_description = line.get('description')
                        
                        discount_line_data = {
                            'reward_type': line.get('reward_type'),
                            'discount': line.get('discount'),
                            'discount_applicability': line.get('discount_applicability'),
                            'discount_max_amount': line.get('discount_max_amount'),
                            'required_points': line.get('required_points'),
                            'description': reward_description,
                            'discount_mode': line.get('discount_mode'),
                            'discount_product_domain': line.get('discount_product_domain'),
                            'vit_reward_trxid': reward_description,
                        }
                        
                        if reward_target_product_ids:
                            discount_line_data['discount_product_ids'] = [(6, 0, reward_target_product_ids)]
                        if reward_product_id_id:
                            discount_line_data['reward_product_id'] = reward_product_id_id
                        if reward_target_category_id:
                            discount_line_data['discount_product_category_id'] = reward_target_category_id
                        if reward_target_product_tag_id:
                            discount_line_data['discount_product_tag_id'] = reward_target_product_tag_id
                        
                        discount_loyalty_line_ids.append((0, 0, discount_line_data))

                rule_ids = []
                for rule in current_rule_lines:
                    if isinstance(rule, dict):
                        # Mapping rule products
                        rule_product_ids = rule.get('product_ids', [])
                        rule_target_product_ids = [product_dict_rule.get(product['default_code']) 
                                                for product in products_source_rule 
                                                if product['id'] in rule_product_ids and product_dict_rule.get(product['default_code'])]

                        # Mapping rule category
                        rule_source_category_id = rule.get('product_category_id')
                        rule_source_category_name = None
                        if rule_source_category_id:
                            if isinstance(rule_source_category_id, list) and len(rule_source_category_id) == 2:
                                rule_source_category_name = rule_source_category_id[1]
                            else:
                                rule_source_category_name = next((category['complete_name'] for category in categories_source_rule if category['id'] == rule_source_category_id), None)

                        rule_target_category_id = category_dict_rule.get(rule_source_category_name) if rule_source_category_name else None

                        # Mapping rule product tag
                        rule_source_product_tag_id = rule.get('product_tag_id')
                        rule_source_product_tag_name = None
                        if rule_source_product_tag_id:
                            if isinstance(rule_source_product_tag_id, list) and len(rule_source_product_tag_id) == 2:
                                rule_source_product_tag_name = rule_source_product_tag_id[1]
                            else:
                                rule_source_product_tag_name = next((tag['name'] for tag in product_tag_source_rule if tag['id'] == rule_source_product_tag_id), None)

                        rule_target_product_tag_id = product_tag_dict_rule.get(rule_source_product_tag_name) if rule_source_product_tag_name else None
                        
                        rule_data = {
                            'minimum_qty': rule.get('minimum_qty'),
                            'minimum_amount': rule.get('minimum_amount'),
                            'reward_point_amount': rule.get('reward_point_amount'),
                            'reward_point_mode': rule.get('reward_point_mode'),
                            'product_domain': rule.get('product_domain'),
                            'code': rule.get('code'),
                            'minimum_amount_tax_mode': rule.get('minimum_amount_tax_mode'),
                            'vit_trxid': record.get('name'),
                        }
                        
                        if rule_target_product_ids:
                            rule_data['product_ids'] = [(6, 0, rule_target_product_ids)]
                        if rule_target_category_id:
                            rule_data['product_category_id'] = rule_target_category_id
                        if rule_target_product_tag_id:
                            rule_data['product_tag_id'] = rule_target_product_tag_id
                        
                        rule_ids.append((0, 0, rule_data))

                # Process schedule data
                schedule_vals = []
                for schedule in current_schedule_lines:
                    schedule_data = {
                        'days': schedule.get('days'),
                        'time_start': schedule.get('time_start'),
                        'time_end': schedule.get('time_end'),
                    }
                    schedule_vals.append((0, 0, schedule_data))

                # Process member data
                member_vals = []
                for member in current_member_lines:
                    member_pos_id = member.get('member_pos')
                    member_pos_name = None
                    if member_pos_id:
                        if isinstance(member_pos_id, list) and len(member_pos_id) == 2:
                            member_pos_name = member_pos_id[1]
                        else:
                            member_pos_name = next((category['name'] for category in member_categories_source if category['id'] == member_pos_id), None)
                    
                    member_target_id = member_category_dict.get(member_pos_name) if member_pos_name else None
                    
                    if member_target_id:
                        member_data = {
                            'member_pos': member_target_id,
                        }
                        member_vals.append((0, 0, member_data))

                # Process program data
                currency_id = record.get('currency_id')
                currency_id = currency_id[0] if isinstance(currency_id, list) else currency_id
                currency_name = next((currency['name'] for currency in currencies_source if currency['id'] == currency_id), None)
                currency_target_id = currency_dict.get(currency_name)

                source_pricelist_ids = record.get('pricelist_ids', [])
                target_pricelist_ids = [pricelist_dict.get(pricelist['name']) for pricelist in pricelists_source if pricelist['id'] in source_pricelist_ids and pricelist_dict.get(pricelist['name'])]

                source_pos_config_ids = record.get('pos_config_ids', [])
                target_pos_config_ids = [pos_config_dict.get(pos_config['name']) for pos_config in pos_configs_source if pos_config['id'] in source_pos_config_ids and pos_config_dict.get(pos_config['name'])]

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
                    'id_mc': record.get('id'),
                    'limit_usage': record.get('limit_usage'),
                    'is_integrated': True,
                    'pos_ok': record.get('pos_ok'),
                    'sale_ok': record.get('sale_ok'),
                    'reward_ids': discount_loyalty_line_ids,
                    'schedule_ids': schedule_vals,
                    'member_ids': member_vals,
                    'rule_ids': rule_ids,
                }
                
                if target_pricelist_ids:
                    discount_data['pricelist_ids'] = [(6, 0, target_pricelist_ids)]
                if target_pos_config_ids:
                    discount_data['pos_config_ids'] = [(6, 0, target_pos_config_ids)]
                
                try:
                    start_time = time.time()
                    
                    # Buat loyalty.program baru di target_client
                    new_discount_data = self.target_client.call_odoo(
                        'object', 'execute_kw', self.target_client.db,
                        self.target_client.uid, self.target_client.password,
                        'loyalty.program', 'create',
                        [discount_data]
                    )

                    rewards_to_sync = self.target_client.call_odoo(
                        'object', 'execute_kw',
                        self.target_client.db, self.target_client.uid, self.target_client.password,
                        'loyalty.reward', 'search_read',
                        [[['program_id', '=', new_discount_data]]],
                        {'fields': ['id', 'vit_reward_trxid', 'description']}
                    )
                    for reward in rewards_to_sync:
                        trxid = reward.get('vit_reward_trxid')
                        if trxid and reward.get('description') != trxid:
                            self.target_client.call_odoo(
                                'object', 'execute_kw',
                                self.target_client.db, self.target_client.uid, self.target_client.password,
                                'loyalty.reward', 'write',
                                [[reward['id']], {'vit_reward_trxid': trxid}]
                            )
                            print(f"→ Synced description reward ID {reward['id']} to {trxid}")
                    
                    print(f"Created loyalty program with ID: {new_discount_data} in target")
                    
                    # === UPDATE DEFAULT_CODE di discount_line_product_id target ===
                    if source_discount_product_codes:
                        try:
                            target_rewards = self.target_client.call_odoo(
                                'object', 'execute_kw', self.target_client.db,
                                self.target_client.uid, self.target_client.password,
                                'loyalty.reward', 'search_read',
                                [[['program_id', '=', new_discount_data], ['discount_line_product_id', '!=', False]]],
                                {'fields': ['id', 'discount_line_product_id', 'vit_reward_trxid']}
                            )
                            
                            print(f"Found {len(target_rewards)} rewards with discount_line_product_id in target")
                            
                            for target_reward in target_rewards:
                                reward_vit_trxid = target_reward.get('vit_reward_trxid')
                                
                                if reward_vit_trxid and reward_vit_trxid in source_discount_product_codes:
                                    discount_product_id = target_reward['discount_line_product_id'][0] if isinstance(
                                        target_reward['discount_line_product_id'], list) else target_reward['discount_line_product_id']
                                    
                                    default_code_from_source = source_discount_product_codes[reward_vit_trxid]
                                    
                                    self.target_client.call_odoo(
                                        'object', 'execute_kw', self.target_client.db,
                                        self.target_client.uid, self.target_client.password,
                                        'product.product', 'write',
                                        [[discount_product_id], {
                                            'default_code': default_code_from_source,
                                            'vit_is_discount': True
                                        }]
                                    )
                                    print(f"✓ Updated target discount product ID {discount_product_id} with default_code: {default_code_from_source} (reward: {reward_vit_trxid})")
                                else:
                                    print(f"⚠ No matching default_code found for reward: {reward_vit_trxid}")
                                    
                        except Exception as e:
                            print(f"✗ Failed to update default_code in target discount products: {e}")
                    # === AKHIR UPDATE DEFAULT_CODE ===
                    
                    # === UPDATE vit_reward_trxid DAN vit_trxid di SOURCE ===
                    try:
                        # Dapatkan setting_config_ids
                        index_store_ids = record.get('index_store', [])
                        setting_config_ids = self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'setting.config', 'search_read',
                            [[['vit_config_server', '=', 'ss'], ['vit_linked_server', '=', True]]],
                            {'fields': ['id']}
                        )
                        setting_config_ids = [config['id'] for config in setting_config_ids]
                        
                        # Update setiap reward line secara individual dengan vit_reward_trxid
                        for line in current_reward_lines:
                            reward_description = line.get('description')
                            if reward_description:
                                try:
                                    self.source_client.call_odoo(
                                        'object', 'execute_kw', self.source_client.db,
                                        self.source_client.uid, self.source_client.password,
                                        'loyalty.reward', 'write',
                                        [[line['id']], {'vit_reward_trxid': reward_description}]
                                    )
                                    print(f"✓ Updated vit_reward_trxid for reward ID {line['id']} with description: {reward_description}")
                                except Exception as e:
                                    print(f"✗ Failed to update vit_reward_trxid for reward ID {line['id']}: {e}")
                        
                        # Update setiap rule line secara individual dengan vit_trxid
                        for rule in current_rule_lines:
                            try:
                                self.source_client.call_odoo(
                                    'object', 'execute_kw', self.source_client.db,
                                    self.source_client.uid, self.source_client.password,
                                    'loyalty.rule', 'write',
                                    [[rule['id']], {'vit_trxid': record['name']}]
                                )
                                print(f"✓ Updated vit_trxid for rule ID {rule['id']}")
                            except Exception as e:
                                print(f"✗ Failed to update vit_trxid for rule ID {rule['id']}: {e}")
                        
                        # Update program dengan vit_trxid dan index_store
                        self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'loyalty.program', 'write',
                            [[record['id']], {
                                'vit_trxid': record['name'],
                                'index_store': [(6, 0, setting_config_ids)]
                            }]
                        )
                        print(f"✓ Updated vit_trxid and index_store in source for program ID {record['id']}")
                        
                    except Exception as e:
                        print(f"✗ Failed to update source program data: {e}")
                    # === AKHIR UPDATE SOURCE ===

                    # Check integration status
                    if len(index_store_ids) == len(setting_config_ids):
                        self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'loyalty.program', 'write',
                            [[record['id']], {'is_integrated': True, 'is_updated': False, 'index_store': [(5, 0, 0)]}]
                        )
                    else:
                        self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'loyalty.program', 'write',
                            [[record['id']], {'is_integrated': False}]
                        )

                    end_time = time.time()
                    duration = end_time - start_time
                    write_date = self.get_write_date(model_name, record['id'])

                    message_success = f"Discount baru telah dibuat dengan ID: {new_discount_data}"
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Discount/Loyalty', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Discount/Loyalty', write_date)
                    print(f"✓ Successfully created discount: {record['name']}")
                    
                except Exception as e:
                    write_date = self.get_write_date(model_name, record['id'])
                    message_exception = f"Terjadi kesalahan saat membuat discount baru: {e}"
                    self.set_log_ss.create_log_note_failed(record, 'Discount/Loyalty', message_exception, write_date)
                    self.set_log_mc.create_log_note_failed(record, 'Discount/Loyalty', message_exception, write_date)
                    print(f"✗ Failed to create discount {record['name']}: {e}")
            
            batch_size = 100
            for i in range(0, len(discount_loyalty), batch_size):
                batch = discount_loyalty[i:i + batch_size]
                with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                    futures = [executor.submit(process_create_discount, record) for record in batch]
                results = concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Error during processing: {e}")
        
    def update_discount_loyalty(self, model_name, fields, description, date_from, date_to):
        try:
            # Ambil data dari sumber
            discount_loyalty = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            model_name, 'search_read',
                                                            [[['is_integrated', '=', False], ['is_updated', '=', True], ['active', 'in', [True, False]]]],
                                                            {'fields': fields})
            if not discount_loyalty:
                print("Tidak ada discount/loyalty yang ditemukan untuk ditransfer.")
                return

            # Fetch existing discount/loyalty programs in target
            discount_names = [record['vit_trxid'] for record in discount_loyalty]
            existing_discount_loyalty = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'loyalty.program', 'search_read',
                [[['vit_trxid', 'in', discount_names], ['active', 'in', [True, False]]]],
                {'fields': ['id', 'vit_trxid']}
            )
            existing_discount_dict = {record['vit_trxid']: record['id'] for record in existing_discount_loyalty}

            order_ids = [record['id'] for record in discount_loyalty]

            # Fetch all reward and rule lines for all programs at once
            reward_ids_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'loyalty.reward', 'search_read',
                                                            [[['program_id', 'in', order_ids]]],
                                                            {'fields': ['reward_type', 'discount', 'discount_applicability', 'discount_line_product_id', 'discount_max_amount', 'required_points', 'description', 'discount_mode', 'discount_product_domain', 'discount_product_ids', 'discount_product_category_id', 'vit_trxid', 'program_id', 'reward_product_id', 'discount_product_tag_id', 'vit_reward_trxid']})

            rule_ids_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'loyalty.rule', 'search_read',
                                                        [[['program_id', 'in', order_ids]]],
                                                        {'fields': ['minimum_qty', 'code', 'minimum_amount', 'reward_point_amount', 'reward_point_mode', 'product_domain', 'product_ids', 'product_category_id', 'minimum_amount_tax_mode', 'vit_trxid', 'program_id', 'product_tag_id']})

            # Fetch schedule and member data for all programs
            schedule_ids_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'loyalty.program.schedule', 'search_read',
                                                            [[['program_id', 'in', order_ids]]],
                                                            {'fields': ['program_id', 'days', 'time_start', 'time_end']})

            member_ids_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'loyalty.member', 'search_read',
                                                        [[['member_program_id', 'in', order_ids]]],
                                                        {'fields': ['member_program_id', 'member_pos']})
            
            # Collect all product and category IDs
            product_ids_reward = [product_id for product in reward_ids_lines for product_id in product.get('discount_product_ids', [])]
            reward_product_id = [record.get('reward_product_id')[0] if isinstance(record.get('reward_product_id'), list) else record.get('reward_product_id') for record in reward_ids_lines if record.get('reward_product_id')]
            product_ids_rule = [product_id for product in rule_ids_lines for product_id in product.get('product_ids', [])]
            category_ids_reward = [record.get('discount_product_category_id')[0] if isinstance(record.get('discount_product_category_id'), list) else record.get('discount_product_category_id') for record in reward_ids_lines if record.get('discount_product_category_id')]
            category_ids_rule = [record.get('product_category_id')[0] if isinstance(record.get('product_category_id'), list) else record.get('product_category_id') for record in rule_ids_lines if record.get('product_category_id')]
            product_tag_ids_rule = [record.get('product_tag_id')[0] if isinstance(record.get('product_tag_id'), list) else record.get('product_tag_id') for record in rule_ids_lines if record.get('product_tag_id')]
            product_tag_ids_reward = [record.get('discount_product_tag_id')[0] if isinstance(record.get('discount_product_tag_id'), list) else record.get('discount_product_tag_id') for record in reward_ids_lines if record.get('discount_product_tag_id')]

            currency_ids = [record.get('currency_id')[0] if isinstance(record.get('currency_id'), list) else record.get('currency_id') for record in discount_loyalty if record.get('currency_id')]
            pricelist_ids = [pricelist_id for record in discount_loyalty for pricelist_id in record.get('pricelist_ids', [])]
            pos_config_ids = [config_id for record in discount_loyalty for config_id in record.get('pos_config_ids', [])]
            member_pos_ids = [record.get('member_pos')[0] if isinstance(record.get('member_pos'), list) else record.get('member_pos') for record in member_ids_lines if record.get('member_pos')]
            
            # Fetch all necessary data from source
            products_source_reward = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'product.product', 'search_read',
                [[['id', 'in', product_ids_reward]]],
                {'fields': ['id', 'default_code']}
            ) if product_ids_reward else []
            
            reward_product_id_source = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'product.product', 'search_read',
                [[['id', 'in', reward_product_id]]],
                {'fields': ['id', 'default_code']}
            ) if reward_product_id else []
            
            products_source_rule = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'product.product', 'search_read',
                [[['id', 'in', product_ids_rule]]],
                {'fields': ['id', 'default_code']}
            ) if product_ids_rule else []

            categories_source_reward = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'product.category', 'search_read',
                [[['id', 'in', category_ids_reward]]],
                {'fields': ['id', 'complete_name']}
            ) if category_ids_reward else []
            
            categories_source_rule = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'product.category', 'search_read',
                [[['id', 'in', category_ids_rule]]],
                {'fields': ['id', 'complete_name']}
            ) if category_ids_rule else []
            
            product_tag_source_rule = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'product.tag', 'search_read',
                [[['id', 'in', product_tag_ids_rule]]],
                {'fields': ['id', 'name']}
            ) if product_tag_ids_rule else []
            
            product_tag_source_reward = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'product.tag', 'search_read',
                [[['id', 'in', product_tag_ids_reward]]],
                {'fields': ['id', 'name']}
            ) if product_tag_ids_reward else []

            currencies_source = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'res.currency', 'search_read',
                [[['id', 'in', currency_ids]]],
                {'fields': ['id', 'name']}
            ) if currency_ids else []
            
            pricelists_source = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'product.pricelist', 'search_read',
                [[['id', 'in', pricelist_ids]]],
                {'fields': ['id', 'name']}
            ) if pricelist_ids else []

            pos_configs_source = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'pos.config', 'search_read',
                [[['id', 'in', pos_config_ids]]],
                {'fields': ['id', 'name']}
            ) if pos_config_ids else []
            
            member_categories_source = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'res.partner.category', 'search_read',
                [[['id', 'in', member_pos_ids]]],
                {'fields': ['id', 'name']}
            ) if member_pos_ids else []
            
            # Fetch corresponding data from target
            product_tag_id_target_reward = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'product.tag', 'search_read',
                [[['name', 'in', [tag['name'] for tag in product_tag_source_reward if tag.get('name')]]]],
                {'fields': ['id', 'name']}
            ) if product_tag_source_reward else []
            
            product_tag_target_rule = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'product.tag', 'search_read',
                [[['name', 'in', [tags['name'] for tags in product_tag_source_rule if tags.get('name')]]]],
                {'fields': ['id', 'name']}
            ) if product_tag_source_rule else []
            
            reward_product_id_target = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'product.product', 'search_read',
                [[['default_code', 'in', [product['default_code'] for product in reward_product_id_source if product.get('default_code')]]]],
                {'fields': ['id', 'default_code']}
            ) if reward_product_id_source else []
            
            products_target_reward = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'product.product', 'search_read',
                [[['default_code', 'in', [product['default_code'] for product in products_source_reward if product.get('default_code')]]]],
                {'fields': ['id', 'default_code']}
            ) if products_source_reward else []

            products_target_rule = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'product.product', 'search_read',
                [[['default_code', 'in', [product['default_code'] for product in products_source_rule if product.get('default_code')]]]],
                {'fields': ['id', 'default_code']}
            ) if products_source_rule else []

            categories_target_reward = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'product.category', 'search_read',
                [[['complete_name', 'in', [category['complete_name'] for category in categories_source_reward if category.get('complete_name')]]]],
                {'fields': ['id', 'complete_name']}
            ) if categories_source_reward else []

            categories_target_rule = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'product.category', 'search_read',
                [[['complete_name', 'in', [category['complete_name'] for category in categories_source_rule if category.get('complete_name')]]]],
                {'fields': ['id', 'complete_name']}
            ) if categories_source_rule else []

            currencies_target = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'res.currency', 'search_read',
                [[['name', 'in', [currency['name'] for currency in currencies_source if currency.get('name')]]]],
                {'fields': ['id', 'name']}
            ) if currencies_source else []

            pricelists_target = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'product.pricelist', 'search_read',
                [[['name', 'in', [pricelist['name'] for pricelist in pricelists_source if pricelist.get('name')]]]],
                {'fields': ['id', 'name']}
            ) if pricelists_source else []

            pos_configs_target = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'pos.config', 'search_read',
                [[['name', 'in', [pos_config['name'] for pos_config in pos_configs_source if pos_config.get('name')]]]],
                {'fields': ['id', 'name']}
            ) if pos_configs_source else []

            member_categories_target = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'res.partner.category', 'search_read',
                [[['name', 'in', [category['name'] for category in member_categories_source if category.get('name')]]]],
                {'fields': ['id', 'name']}
            ) if member_categories_source else []

            # Create mapping dictionaries
            product_dict_reward = {product['default_code']: product['id'] for product in products_target_reward if product.get('default_code')}
            reward_product_id_dict = {product['default_code']: product['id'] for product in reward_product_id_target if product.get('default_code')}
            product_dict_rule = {product['default_code']: product['id'] for product in products_target_rule if product.get('default_code')}
            category_dict_reward = {category['complete_name']: category['id'] for category in categories_target_reward if category.get('complete_name')}
            category_dict_rule = {category['complete_name']: category['id'] for category in categories_target_rule if category.get('complete_name')}
            product_tag_dict_reward = {tag['name']: tag['id'] for tag in product_tag_id_target_reward if tag.get('name')}
            product_tag_dict_rule = {tag['name']: tag['id'] for tag in product_tag_target_rule if tag.get('name')}
            currency_dict = {currency['name']: currency['id'] for currency in currencies_target if currency.get('name')}
            pricelist_dict = {pricelist['name']: pricelist['id'] for pricelist in pricelists_target if pricelist.get('name')}
            pos_config_dict = {pos_config['name']: pos_config['id'] for pos_config in pos_configs_target if pos_config.get('name')}
            member_category_dict = {category['name']: category['id'] for category in member_categories_target if category.get('name')}

            def process_update_discount(record):
                program_id = existing_discount_dict.get(record['vit_trxid'])
                if not program_id:
                    print(f"Program {record.get('name')} not found in target, skipping...")
                    return

                # === SINKRONISASI STATUS ACTIVE ===
                source_active_status = record.get('active')
                
                try:
                    # Update status active di target sesuai dengan source
                    self.target_client.call_odoo(
                        'object', 'execute_kw', self.target_client.db,
                        self.target_client.uid, self.target_client.password,
                        model_name, 'write',
                        [[program_id], {'active': source_active_status}]
                    )
                    
                    if not source_active_status:
                        print(f"✓ Program {record.get('name')} archived in target (following source)")
                        
                        # Update status di source
                        index_store_ids = record.get('index_store', [])
                        setting_config_ids = self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'setting.config', 'search_read',
                            [[['vit_config_server', '=', 'ss'], ['vit_linked_server', '=', True]]],
                            {'fields': ['id']}
                        )
                        setting_config_ids = [config['id'] for config in setting_config_ids]
                        
                        # Check integration status
                        if len(index_store_ids) == len(setting_config_ids):
                            self.source_client.call_odoo(
                                'object', 'execute_kw', self.source_client.db,
                                self.source_client.uid, self.source_client.password,
                                'loyalty.program', 'write',
                                [[record['id']], {'is_integrated': True, 'is_updated': False, 'index_store': [(5, 0, 0)]}]
                            )
                        else:
                            self.source_client.call_odoo(
                                'object', 'execute_kw', self.source_client.db,
                                self.source_client.uid, self.source_client.password,
                                'loyalty.program', 'write',
                                [[record['id']], {'is_updated': False}]
                            )
                        
                        # Log success untuk archive
                        start_time = time.time()
                        end_time = time.time()
                        duration = end_time - start_time
                        write_date = self.get_write_date(model_name, record['id'])
                        
                        self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Discount/Loyalty (Archived)', write_date)
                        self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Discount/Loyalty (Archived)', write_date)
                        
                        return  # Skip update detail jika sudah archived
                        
                except Exception as e:
                    print(f"✗ Failed to sync active status for {record.get('name')}: {e}")
                    write_date = self.get_write_date(model_name, record['id'])
                    message_exception = f"Terjadi kesalahan saat archive discount: {e}"
                    self.set_log_ss.create_log_note_failed(record, 'Discount/Loyalty', message_exception, write_date)
                    self.set_log_mc.create_log_note_failed(record, 'Discount/Loyalty', message_exception, write_date)
                    return
                # === AKHIR SINKRONISASI ACTIVE ===

                # Fetch existing data from target
                existing_reward_lines = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'loyalty.reward', 'search_read',
                                                                    [[['program_id', '=', program_id]]],
                                                                    {'fields': ['id', 'reward_type', 'discount', 'discount_applicability',
                                                                                'discount_line_product_id', 'discount_max_amount', 'required_points',
                                                                                'description', 'discount_product_ids', 'discount_product_category_id',
                                                                                'vit_reward_trxid', 'reward_product_id', 'discount_product_tag_id', 
                                                                                'discount_product_domain', 'discount_mode']})

                existing_rule_lines = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'loyalty.rule', 'search_read',
                                                                [[['program_id', '=', program_id]]],
                                                                {'fields': ['id', 'minimum_qty', 'minimum_amount', 'reward_point_amount',
                                                                            'reward_point_mode', 'product_domain', 'product_ids',
                                                                            'product_category_id', 'vit_trxid', 'product_tag_id']})

                existing_schedule_lines = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'loyalty.program.schedule', 'search_read',
                                                                    [[['program_id', '=', program_id]]],
                                                                    {'fields': ['id', 'days', 'time_start', 'time_end']})

                existing_member_lines = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'loyalty.member', 'search_read',
                                                                [[['member_program_id', '=', program_id]]],
                                                                {'fields': ['id', 'member_pos']})

                current_reward_lines = [line for line in reward_ids_lines if line['program_id'][0] == record['id']]
                current_rule_lines = [line for line in rule_ids_lines if line['program_id'][0] == record['id']]
                current_schedule_lines = [line for line in schedule_ids_lines if line['program_id'][0] == record['id']]
                current_member_lines = [line for line in member_ids_lines if line['member_program_id'][0] == record['id']]

                # === SIMPAN MAPPING default_code ===
                source_discount_product_codes = {}
                for line in current_reward_lines:
                    if line.get('discount_line_product_id'):
                        source_discount_product_id = line['discount_line_product_id'][0] if isinstance(
                            line['discount_line_product_id'], list) else line['discount_line_product_id']
                        
                        try:
                            source_discount_product = self.source_client.call_odoo(
                                'object', 'execute_kw', self.source_client.db,
                                self.source_client.uid, self.source_client.password,
                                'product.product', 'read',
                                [[source_discount_product_id]],
                                {'fields': ['default_code']}
                            )
                            
                            if source_discount_product and source_discount_product[0].get('default_code'):
                                reward_trxid = line.get('vit_reward_trxid')
                                if reward_trxid:
                                    source_discount_product_codes[reward_trxid] = source_discount_product[0]['default_code']
                                else:
                                    reward_description = line.get('description')
                                    if reward_description:
                                        source_discount_product_codes[reward_description] = source_discount_product[0]['default_code']
                        except Exception as e:
                            print(f"Failed to fetch default_code for discount product {source_discount_product_id}: {e}")
                # === AKHIR MAPPING ===
                
                discount_loyalty_line_ids = []
                for line in current_reward_lines:
                    if isinstance(line, dict):
                        reward_product_ids = line.get('discount_product_ids', [])
                        reward_target_product_ids = [product_dict_reward.get(product['default_code']) 
                                                for product in products_source_reward 
                                                if product['id'] in reward_product_ids and product_dict_reward.get(product['default_code'])]

                        reward_product_id_field = line.get('reward_product_id')
                        reward_product_id_field = reward_product_id_field[0] if isinstance(reward_product_id_field, list) else reward_product_id_field

                        reward_product_default_code = None
                        if reward_product_id_field:
                            if isinstance(reward_product_id_field, list) and len(reward_product_id_field) == 2:
                                reward_product_default_code = reward_product_id_field[1]
                            else:
                                reward_product_default_code = next(
                                    (product['default_code'] for product in reward_product_id_source if product['id'] == reward_product_id_field),
                                    None
                                )

                        reward_product_id_id = reward_product_id_dict.get(reward_product_default_code) if reward_product_default_code else None

                        reward_source_category_id = line.get('discount_product_category_id')
                        reward_source_category_name = None
                        if reward_source_category_id:
                            if isinstance(reward_source_category_id, list) and len(reward_source_category_id) == 2:
                                reward_source_category_name = reward_source_category_id[1]
                            else:
                                reward_source_category_name = next((category['complete_name'] for category in categories_source_reward if category['id'] == reward_source_category_id), None)

                        reward_target_category_id = category_dict_reward.get(reward_source_category_name) if reward_source_category_name else None

                        reward_source_product_tag_id = line.get('discount_product_tag_id')
                        reward_source_product_tag_name = None
                        if reward_source_product_tag_id:
                            if isinstance(reward_source_product_tag_id, list) and len(reward_source_product_tag_id) == 2:
                                reward_source_product_tag_name = reward_source_product_tag_id[1]
                            else:
                                reward_source_product_tag_name = next((tag['name'] for tag in product_tag_source_reward if tag['id'] == reward_source_product_tag_id), None)

                        reward_target_product_tag_id = product_tag_dict_reward.get(reward_source_product_tag_name) if reward_source_product_tag_name else None

                        # === PERBAIKAN LOGIKA PENCOCOKAN REWARD ===
                        # Ambil identifier dari source
                        source_vit_reward_trxid = line.get('vit_reward_trxid')
                        source_description = line.get('description')
                        source_reward_id = line.get('id')
                        
                        # Debug logging
                        print(f"\n=== Processing Reward Line ===")
                        print(f"Source ID: {source_reward_id}")
                        print(f"Source vit_reward_trxid: {source_vit_reward_trxid}")
                        print(f"Source description: {source_description}")
                        
                        existing_line = None
                        
                        # Prioritas 1: Cocokkan dengan vit_reward_trxid (paling reliable)
                        if source_vit_reward_trxid:
                            existing_line = next(
                                (x for x in existing_reward_lines 
                                if x.get('vit_reward_trxid') == source_vit_reward_trxid),
                                None
                            )
                            if existing_line:
                                print(f"✓ Match found by vit_reward_trxid: {existing_line['id']}")
                        
                        # Prioritas 2: Cocokkan dengan description
                        if not existing_line and source_description:
                            existing_line = next(
                                (x for x in existing_reward_lines 
                                if x.get('description') == source_description),
                                None
                            )
                            if existing_line:
                                print(f"✓ Match found by description: {existing_line['id']}")
                        
                        # Prioritas 3: Fallback - cocokkan dengan karakteristik reward
                        # (untuk data lama yang belum punya identifier)
                        if not existing_line:
                            existing_line = next(
                                (x for x in existing_reward_lines 
                                if x.get('reward_type') == line.get('reward_type') 
                                and x.get('discount') == line.get('discount')
                                and x.get('discount_applicability') == line.get('discount_applicability')
                                and x.get('discount_mode') == line.get('discount_mode')),
                                None
                            )
                            if existing_line:
                                print(f"✓ Match found by characteristics: {existing_line['id']}")
                        
                        # Tentukan identifier yang akan digunakan
                        # Prioritas: vit_reward_trxid > description > generated ID
                        if source_vit_reward_trxid:
                            final_identifier = source_vit_reward_trxid
                        elif source_description:
                            final_identifier = source_description
                        else:
                            # Generate identifier unik jika tidak ada
                            final_identifier = f"{record.get('name')}_reward_{source_reward_id}"
                        
                        print(f"Final identifier: {final_identifier}")
                        # === AKHIR LOGIKA PENCOCOKAN ===

                        discount_line_data = {
                            'reward_type': line.get('reward_type'),
                            'discount': line.get('discount'),
                            'discount_applicability': line.get('discount_applicability'),
                            'discount_max_amount': line.get('discount_max_amount'),
                            'required_points': line.get('required_points'),
                            'description': line.get('description'),
                            'vit_reward_trxid': final_identifier,
                            'discount_mode': line.get('discount_mode'),
                            'discount_product_domain': line.get('discount_product_domain'),
                        }

                        if reward_target_product_ids:
                            discount_line_data['discount_product_ids'] = [(6, 0, reward_target_product_ids)]
                        if reward_product_id_id:
                            discount_line_data['reward_product_id'] = reward_product_id_id
                        if reward_target_category_id:
                            discount_line_data['discount_product_category_id'] = reward_target_category_id
                        if reward_target_product_tag_id:
                            discount_line_data['discount_product_tag_id'] = reward_target_product_tag_id

                        if existing_line:
                            discount_loyalty_line_ids.append((1, existing_line['id'], discount_line_data))
                            print(f"✓ Updating reward ID {existing_line['id']} in target with identifier='{final_identifier}'")
                        else:
                            discount_loyalty_line_ids.append((0, 0, discount_line_data))
                            print(f"✓ Creating new reward with identifier='{final_identifier}'")

                rule_ids = []
                for rule in current_rule_lines:
                    if isinstance(rule, dict):
                        # Mapping rule products
                        rule_product_ids = rule.get('product_ids', [])
                        rule_target_product_ids = [product_dict_rule.get(product['default_code']) 
                                                for product in products_source_rule 
                                                if product['id'] in rule_product_ids and product_dict_rule.get(product['default_code'])]

                        # Mapping rule category
                        rule_source_category_id = rule.get('product_category_id')
                        rule_source_category_name = None
                        if rule_source_category_id:
                            if isinstance(rule_source_category_id, list) and len(rule_source_category_id) == 2:
                                rule_source_category_name = rule_source_category_id[1]
                            else:
                                rule_source_category_name = next((category['complete_name'] for category in categories_source_rule if category['id'] == rule_source_category_id), None)

                        rule_target_category_id = category_dict_rule.get(rule_source_category_name) if rule_source_category_name else None

                        # Mapping rule product tag
                        rule_source_product_tag_id = rule.get('product_tag_id')
                        rule_source_product_tag_name = None
                        if rule_source_product_tag_id:
                            if isinstance(rule_source_product_tag_id, list) and len(rule_source_product_tag_id) == 2:
                                rule_source_product_tag_name = rule_source_product_tag_id[1]
                            else:
                                rule_source_product_tag_name = next((tag['name'] for tag in product_tag_source_rule if tag['id'] == rule_source_product_tag_id), None)

                        rule_target_product_tag_id = product_tag_dict_rule.get(rule_source_product_tag_name) if rule_source_product_tag_name else None
                    
                        existing_rule_line = next((x for x in existing_rule_lines if x['vit_trxid'] == rule['vit_trxid']), None)
                        
                        rule_data = {
                            'minimum_qty': rule.get('minimum_qty'),
                            'minimum_amount': rule.get('minimum_amount'),
                            'reward_point_amount': rule.get('reward_point_amount'),
                            'reward_point_mode': rule.get('reward_point_mode'),
                            'product_domain': rule.get('product_domain'),
                            'vit_trxid': record.get('name'),
                            'code': rule.get('code'),
                            'minimum_amount_tax_mode': rule.get('minimum_amount_tax_mode'),
                        }
                        
                        if rule_target_product_ids:
                            rule_data['product_ids'] = [(6, 0, rule_target_product_ids)]
                        if rule_target_category_id:
                            rule_data['product_category_id'] = rule_target_category_id
                        if rule_target_product_tag_id:
                            rule_data['product_tag_id'] = rule_target_product_tag_id
                        
                        if existing_rule_line:
                            rule_ids.append((1, existing_rule_line['id'], rule_data))
                        else:
                            rule_ids.append((0, 0, rule_data))

                # Process schedule data
                schedule_vals = []
                existing_schedule_dict = {}
                for existing_schedule in existing_schedule_lines:
                    key = f"{existing_schedule.get('days')}_{existing_schedule.get('time_start')}_{existing_schedule.get('time_end')}"
                    existing_schedule_dict[key] = existing_schedule['id']

                valid_schedule_ids = set()
                for schedule in current_schedule_lines:
                    schedule_key = f"{schedule.get('days')}_{schedule.get('time_start')}_{schedule.get('time_end')}"
                    
                    if schedule_key in existing_schedule_dict:
                        valid_schedule_ids.add(existing_schedule_dict[schedule_key])
                    else:
                        schedule_data = {
                            'days': schedule.get('days'),
                            'time_start': schedule.get('time_start'),
                            'time_end': schedule.get('time_end'),
                        }
                        schedule_vals.append((0, 0, schedule_data))

                # Delete schedules yang tidak ada di source
                for existing_schedule in existing_schedule_lines:
                    if existing_schedule['id'] not in valid_schedule_ids:
                        schedule_vals.append((2, existing_schedule['id']))

                # Process member data
                member_vals = []
                existing_member_dict = {}
                for existing_member in existing_member_lines:
                    member_pos_id = existing_member.get('member_pos')
                    if isinstance(member_pos_id, list) and len(member_pos_id) == 2:
                        member_key = member_pos_id[0]
                    else:
                        member_key = member_pos_id
                    existing_member_dict[member_key] = existing_member['id']

                valid_member_ids = set()
                for member in current_member_lines:
                    member_pos_id = member.get('member_pos')
                    if isinstance(member_pos_id, list) and len(member_pos_id) == 2:
                        member_pos_name = member_pos_id[1]
                        source_member_id = member_pos_id[0]
                    else:
                        member_pos_name = next((category['name'] for category in member_categories_source if category['id'] == member_pos_id), None)
                        source_member_id = member_pos_id
                    
                    member_target_id = member_category_dict.get(member_pos_name)
                    
                    if member_target_id:
                        if member_target_id in existing_member_dict:
                            valid_member_ids.add(existing_member_dict[member_target_id])
                        else:
                            member_data = {
                                'member_pos': member_target_id,
                            }
                            member_vals.append((0, 0, member_data))

                # Delete members yang tidak ada di source
                for existing_member in existing_member_lines:
                    if existing_member['id'] not in valid_member_ids:
                        member_vals.append((2, existing_member['id']))

                # Process program data
                currency_id = record.get('currency_id')
                currency_id = currency_id[0] if isinstance(currency_id, list) else currency_id
                currency_name = next((currency['name'] for currency in currencies_source if currency['id'] == currency_id), None)
                currency_target_id = currency_dict.get(currency_name)

                source_pricelist_ids = record.get('pricelist_ids', [])
                target_pricelist_ids = [pricelist_dict.get(pricelist['name']) for pricelist in pricelists_source if pricelist['id'] in source_pricelist_ids and pricelist_dict.get(pricelist['name'])]

                source_pos_config_ids = record.get('pos_config_ids', [])
                target_pos_config_ids = [pos_config_dict.get(pos_config['name']) for pos_config in pos_configs_source if pos_config['id'] in source_pos_config_ids and pos_config_dict.get(pos_config['name'])]

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
                    'vit_trxid': record.get('vit_trxid'),
                    'id_mc': record.get('id'),
                    'limit_usage': record.get('limit_usage'),
                    'active': record.get('active'),
                    'is_integrated': True,
                    'pos_ok': record.get('pos_ok'),
                    'sale_ok': record.get('sale_ok'),
                    'schedule_ids': schedule_vals,
                    'member_ids': member_vals,
                    'reward_ids': discount_loyalty_line_ids,
                    'rule_ids': rule_ids,
                }
                
                if target_pricelist_ids:
                    update_values['pricelist_ids'] = [(6, 0, target_pricelist_ids)]
                else:
                    update_values['pricelist_ids'] = [(5, 0, 0)]
                
                if target_pos_config_ids:
                    update_values['pos_config_ids'] = [(6, 0, target_pos_config_ids)]
                else:
                    update_values['pos_config_ids'] = [(5, 0, 0)]

                try:
                    start_time = time.time()
                    
                    self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                model_name, 'write',
                                                [[program_id], update_values])
                    print(f"Record dengan ID {record['id']} telah diupdate.")

                    # === UPDATE DESCRIPTION di LOYALTY.REWARD TARGET ===
                    try:
                        target_rewards = self.target_client.call_odoo(
                            'object', 'execute_kw', self.target_client.db,
                            self.target_client.uid, self.target_client.password,
                            'loyalty.reward', 'search_read',
                            [[['program_id', '=', program_id]]],
                            {'fields': ['id', 'vit_reward_trxid', 'description']}
                        )

                        for target_reward in target_rewards:
                            vit_reward_trxid = target_reward.get('vit_reward_trxid')
                            
                            # Cari source reward yang matching berdasarkan vit_reward_trxid
                            source_reward = next(
                                (line for line in current_reward_lines 
                                if line.get('vit_reward_trxid') == vit_reward_trxid or 
                                line.get('description') == vit_reward_trxid),
                                None
                            )
                            
                            if source_reward:
                                source_description = source_reward.get('description')
                                target_description = target_reward.get('description')
                                
                                # Update hanya jika description berbeda
                                if source_description and source_description != target_description:
                                    self.target_client.call_odoo(
                                        'object', 'execute_kw', self.target_client.db,
                                        self.target_client.uid, self.target_client.password,
                                        'loyalty.reward', 'write',
                                        [[target_reward['id']], {'description': source_description}]
                                    )
                                    print(f"✓ Updated description reward ID {target_reward['id']}: '{target_description}' -> '{source_description}'")
                    except Exception as e:
                        print(f"⚠ Gagal update description di loyalty.reward: {e}")
                    # === AKHIR UPDATE DESCRIPTION ===

                    # === UPDATE DEFAULT_CODE di discount_line_product_id target ===
                    if source_discount_product_codes:
                        try:
                            target_rewards = self.target_client.call_odoo(
                                'object', 'execute_kw', self.target_client.db,
                                self.target_client.uid, self.target_client.password,
                                'loyalty.reward', 'search_read',
                                [[['program_id', '=', program_id], ['discount_line_product_id', '!=', False]]],
                                {'fields': ['id', 'discount_line_product_id', 'vit_reward_trxid']}
                            )
                            
                            print(f"Found {len(target_rewards)} rewards with discount_line_product_id in target for update")
                            
                            for target_reward in target_rewards:
                                reward_vit_trxid = target_reward.get('vit_reward_trxid')
                                
                                if reward_vit_trxid and reward_vit_trxid in source_discount_product_codes:
                                    discount_product_id = target_reward['discount_line_product_id'][0] if isinstance(
                                        target_reward['discount_line_product_id'], list) else target_reward['discount_line_product_id']
                                    
                                    default_code_from_source = source_discount_product_codes[reward_vit_trxid]
                                    
                                    self.target_client.call_odoo(
                                        'object', 'execute_kw', self.target_client.db,
                                        self.target_client.uid, self.target_client.password,
                                        'product.product', 'write',
                                        [[discount_product_id], {
                                            'default_code': default_code_from_source,
                                            'vit_is_discount': True
                                        }]
                                    )
                                    print(f"✓ Updated target discount product ID {discount_product_id} with default_code: {default_code_from_source} (reward: {reward_vit_trxid})")
                                else:
                                    print(f"⚠ No matching default_code found for reward: {reward_vit_trxid}")
                                    
                        except Exception as e:
                            print(f"✗ Failed to update default_code in target discount products: {e}")
                    # === AKHIR UPDATE DEFAULT_CODE ===

                    # === UPDATE vit_reward_trxid di SOURCE ===
                    index_store_ids = record.get('index_store', [])
                    setting_config_ids = self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'setting.config', 'search_read',
                        [[['vit_config_server', '=', 'ss'], ['vit_linked_server', '=', True]]],
                        {'fields': ['id']}
                    )
                    setting_config_ids = [config['id'] for config in setting_config_ids]
                    
                    # Update vit_reward_trxid di source rewards dengan description terbaru
                    reward_updates = []
                    for line in current_reward_lines:
                        source_vit_reward_trxid = line.get('vit_reward_trxid')
                        source_description = line.get('description')
                        source_reward_id = line.get('id')
                        
                        # Tentukan identifier yang sama seperti yang digunakan di target
                        if source_vit_reward_trxid:
                            final_identifier = source_vit_reward_trxid
                        elif source_description:
                            final_identifier = source_description
                        else:
                            final_identifier = f"{record.get('name')}_reward_{source_reward_id}"
                        
                        reward_updates.append((1, line['id'], {
                            'vit_reward_trxid': final_identifier,
                            'description': final_identifier
                        }))
                    
                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'loyalty.program', 'write',
                        [[record['id']], {
                            'vit_trxid': record['name'],
                            'index_store': [(6, 0, setting_config_ids)],
                            'reward_ids': reward_updates,
                            'rule_ids': [(1, rule['id'], {'vit_trxid': record['name']}) for rule in current_rule_lines],
                        }]
                    )
                    print(f"✓ Updated vit_reward_trxid in source for program ID {record['id']}")
                    # === AKHIR UPDATE SOURCE ===

                    # Check integration status
                    # Check integration status
                    if len(index_store_ids) == len(setting_config_ids):
                        self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'loyalty.program', 'write',
                            [[record['id']], {'is_integrated': True, 'is_updated': False, 'index_store': [(5, 0, 0)]}]
                        )
                    else:
                        self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'loyalty.program', 'write',
                            [[record['id']], {'is_integrated': False}]
                        )

                    end_time = time.time()
                    duration = end_time - start_time
                    write_date = self.get_write_date(model_name, record['id'])

                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Discount/Loyalty', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Discount/Loyalty', write_date)
                    print(f"✓ Discount telah diupdate: {record['name']}")
                    
                except Exception as e:
                    write_date = self.get_write_date(model_name, record['id'])
                    message_exception = f"Terjadi kesalahan saat update discount: {e}"
                    self.set_log_ss.create_log_note_failed(record, 'Discount/Loyalty', message_exception, write_date)
                    self.set_log_mc.create_log_note_failed(record, 'Discount/Loyalty', message_exception, write_date)
                    print(f"✗ Failed to update discount {record['name']}: {e}")

            batch_size = 100
            for i in range(0, len(discount_loyalty), batch_size):
                batch = discount_loyalty[i:i + batch_size]
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    futures = [executor.submit(process_update_discount, record) for record in batch]
                    concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Error during processing: {e}")
            
    def transfer_loyalty_point_mc_to_ss(self, model_name, fields, description, date_from, date_to):
        if isinstance(date_from, datetime):
            date_from = date_from.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(date_to, datetime):
            date_to = date_to.strftime('%Y-%m-%d %H:%M:%S')
                                   
        id_program = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    model_name, 'search_read',
                                                    [[['active', '=', True], ['is_integrated', '=', False], ['program_type', 'in', ['coupons', 'loyalty']]]],
                                                    {'fields': fields})
        
        if not id_program:
            print("Tidak ada promo")
            return

        for res in id_program:
            programs = res.get('id', False)

            # Ambil data dari sumber
            loyalty_points = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'loyalty.card', 'search_read',
                                                        [[['program_id', '=', programs]]],
                                                        {'fields': ['code', 'points_display', 'expiration_date', 'program_id', 'currency_id', 'partner_id', 'source_pos_order_id', 'points']})
            # if not loyalty_points:
            #     print("Tidak ada discount/loyalty yang ditemukan untuk ditransfer.")
            #     return

            # Pre-fetch necessary data to reduce API calls
            pos_order_ids = {record.get('source_pos_order_id')[0] for record in loyalty_points if record.get('source_pos_order_id')}
            pos_orders = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    'pos.order', 'search_read',
                                                    [[['id', 'in', list(pos_order_ids)]]],
                                                    {'fields': ['id', 'vit_trxid']})

            pos_order_map = {order['id']: order['vit_trxid'] for order in pos_orders}

            program_ids = {record.get('program_id')[0] for record in loyalty_points if record.get('program_id')}
            programs_data = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'loyalty.program', 'search_read',
                                                        [[['id', 'in', list(program_ids)]]],
                                                        {'fields': ['id', 'name']})

            program_map = {program['id']: program['name'] for program in programs_data}

            partner_ids = {record.get('partner_id')[0] for record in loyalty_points if record.get('partner_id')}
            partners_data = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'res.partner', 'search_read',
                                                        [[['id', 'in', list(partner_ids)]]],
                                                        {'fields': ['id', 'customer_code']})

            partner_map = {partner['id']: partner['customer_code'] for partner in partners_data}

            # Pre-fetch order references, program IDs, and partner IDs
            order_references = {}
            program_id_sets = {}
            partner_id_sets = {}

            for record in loyalty_points:
                order_ref = pos_order_map.get(record.get('source_pos_order_id')[0]) if record.get('source_pos_order_id') else False
                if order_ref and order_ref not in order_references:
                    order_reference = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'pos.order', 'search_read',
                                                                    [[['name', '=', order_ref]]],
                                                                    {'fields': ['id'], 'limit': 1})
                    order_references[order_ref] = order_reference[0]['id'] if order_reference else False

                program_id = record.get('program_id')
                if program_id and program_id[0] not in program_id_sets:
                    program_id_new = program_map.get(program_id[0])
                    program_id_set = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'loyalty.program', 'search_read',
                                                                    [[['name', '=', program_id_new]]],
                                                                    {'fields': ['id'], 'limit': 1})
                    program_id_sets[program_id[0]] = program_id_set[0]['id'] if program_id_set else False

                partner_id = record.get('partner_id')
                if partner_id and partner_id[0] not in partner_id_sets:
                    partner_id_new = partner_map.get(partner_id[0])
                    partner_id_set = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'res.partner', 'search_read',
                                                                    [[['customer_code', '=', partner_id_new]]],
                                                                    {'fields': ['id'], 'limit': 1})
                    partner_id_sets[partner_id[0]] = partner_id_set[0]['id'] if partner_id_set else False

            def process_loyalty_point(record):
                existing_loyalty_points = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'loyalty.card', 'search_read',
                    [[['code', '=', record['code']]]],
                    {'fields': ['id']}
                )

                if not existing_loyalty_points:
                    code = record.get('code')
                    expiration_date = record.get('expiration_date')
                    points = record.get('points')

                    source_pos_order_id = record.get('source_pos_order_id')
                    order_id = order_references.get(source_pos_order_id[0]) if source_pos_order_id else False

                    program_id = record.get('program_id')
                    program_id_new = program_id_sets.get(program_id[0]) if program_id else False

                    partner_id = record.get('partner_id')
                    partner_id_list_new = partner_id_sets.get(partner_id[0]) if partner_id else False

                    data_loyalty = {
                        'code': code,
                        'expiration_date': expiration_date,
                        'points': points,
                        'source_pos_order_id': order_id,
                        'program_id': program_id_new,
                        'partner_id': partner_id_list_new
                    }

                    try:
                        new_loyalty_data = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'loyalty.card', 'create',
                                                                        [data_loyalty])
                        print(f"Loyalty telah dibuat dengan ID: {new_loyalty_data}")

                        index_store_ids = record.get('index_store', [])
                        # Set the index_store field with setting.config IDs
                        setting_config_ids = self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'setting.config', 'search_read',
                            [[['vit_config_server', '=', 'ss'], ['vit_linked_server', '=', True]]],
                            {'fields': ['id']}
                        )
                        setting_config_ids = [config['id'] for config in setting_config_ids]
                        self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'loyalty.card', 'write',
                            [[record['id']], {'vit_trxid': record['name'],
                                            'index_store': [(6, 0, setting_config_ids)]}]
                        )
                        print(f"Field is_integrated set to True for loyalty card ID {record['id']}.")

                        if len(index_store_ids) == len(setting_config_ids):
                            self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'loyalty.card', 'write',
                            [[record['id']], {'is_integrated': True, 'is_updated': False, 'index_store': [(5, 0, 0)]}])

                        else:
                            self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'loyalty.card', 'write',
                            [[record['id']], {'is_integrated': False}])
                    except Exception as e:
                        print(f"Terjadi kesalahan saat membuat loyalty baru: {e}")

            # Process loyalty points in batches of 100
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                for i in range(0, len(loyalty_points), 100):
                    batch = loyalty_points[i:i + 100]
                    executor.map(process_loyalty_point, batch)

    def update_loyalty_point_mc_to_ss(self, model_name, fields, description, date_from, date_to):
        id_program = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    model_name, 'search_read',
                                                    [[]],
                                                    {'fields': fields})

        for res in id_program:
            programs = res.get('id', False)

            # Ambil data dari sumber
            loyalty_points = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'loyalty.card', 'search_read',
                                                        [[['program_id', '=', int(programs)], ['is_updated', '=', True]]],
                                                        {'fields': ['code', 'points_display', 'expiration_date', 'program_id', 'currency_id', 'partner_id', 'source_pos_order_id', 'points']})

            # Pre-fetch necessary data to reduce API calls
            pos_order_ids = {record.get('source_pos_order_id')[0] for record in loyalty_points if record.get('source_pos_order_id')}
            pos_orders = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    'pos.order', 'search_read',
                                                    [[['id', 'in', list(pos_order_ids)]]],
                                                    {'fields': ['id', 'vit_trxid']})

            pos_order_map = {order['id']: order['vit_trxid'] for order in pos_orders}

            program_ids = {record.get('program_id')[0] for record in loyalty_points if record.get('program_id')}
            programs_data = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'loyalty.program', 'search_read',
                                                        [[['id', 'in', list(program_ids)]]],
                                                        {'fields': ['id', 'name']})

            program_map = {program['id']: program['name'] for program in programs_data}

            partner_ids = {record.get('partner_id')[0] for record in loyalty_points if record.get('partner_id')}
            partners_data = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'res.partner', 'search_read',
                                                        [[['id', 'in', list(partner_ids)]]],
                                                        {'fields': ['id', 'customer_code']})

            partner_map = {partner['id']: partner['customer_code'] for partner in partners_data}

            # Pre-fetch order references, program IDs, and partner IDs
            order_references = {}
            program_id_sets = {}
            partner_id_sets = {}

            for record in loyalty_points:
                order_ref = False
                if record.get('source_pos_order_id'):
                    order_ref = pos_order_map.get(record['source_pos_order_id'][0])

                if order_ref and order_ref not in order_references:
                    order_reference = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'pos.order', 'search_read',
                                                                    [[['name', '=', order_ref]]],
                                                                    {'fields': ['id'], 'limit': 1})
                    order_references[order_ref] = order_reference[0]['id'] if order_reference else False

                program_id = record.get('program_id')
                if program_id and program_id[0] not in program_id_sets:
                    program_id_new = program_map.get(program_id[0])
                    program_id_set = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'loyalty.program', 'search_read',
                                                                    [[['name', '=', program_id_new]]],
                                                                    {'fields': ['id'], 'limit': 1})
                    program_id_sets[program_id[0]] = program_id_set[0]['id'] if program_id_set else False

                partner_id = record.get('partner_id')
                if partner_id and partner_id[0] not in partner_id_sets:
                    partner_id_new = partner_map.get(partner_id[0])
                    partner_id_set = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'res.partner', 'search_read',
                                                                    [[['customer_code', '=', partner_id_new]]],
                                                                    {'fields': ['id'], 'limit': 1})
                    partner_id_sets[partner_id[0]] = partner_id_set[0]['id'] if partner_id_set else False

            def process_loyalty_point(record):
                existing_loyalty_points_mc = self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    'loyalty.card', 'search_read',
                    [[['code', '=', record['code']]]],
                    {'fields': ['id']}
                )

                if existing_loyalty_points_mc:
                    loyalty_id = existing_loyalty_points_mc[0]['id']
                    code = record.get('code')
                    expiration_date = record.get('expiration_date')
                    points = record.get('points')
                    points_display = record.get('points_display')

                    order_id = order_references.get(record.get('source_pos_order_id')[0], False) if record.get('source_pos_order_id') else False
                    program_id = program_id_sets.get(record.get('program_id')[0], False) if record.get('program_id') else False
                    partner_id = partner_id_sets.get(record.get('partner_id')[0], False) if record.get('partner_id') else False

                    data_loyalty_mc = {
                        'code': code,
                        'expiration_date': expiration_date,
                        'points': points,
                        'points_display': points_display,
                        'source_pos_order_id': order_id,
                        'program_id': program_id,
                        'partner_id': partner_id
                    }

                    try:
                        # Menggunakan `write` untuk memperbarui data yang sudah ada
                        self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                    self.target_client.uid, self.target_client.password,
                                                    'loyalty.card', 'write',
                                                    [[loyalty_id], data_loyalty_mc])
                        print(f"Loyalty dengan ID {loyalty_id} telah diperbarui di target_client.")

                        index_store_ids = record.get('index_store', [])
                        # Set the index_store field with setting.config IDs
                        setting_config_ids = self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'setting.config', 'search_read',
                            [[['vit_config_server', '=', 'ss'], ['vit_linked_server', '=', True]]],
                            {'fields': ['id']}
                        )
                        setting_config_ids = [config['id'] for config in setting_config_ids]
                        self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'loyalty.card', 'write',
                            [[record['id']], {'vit_trxid': record['name'],
                                            'index_store': [(6, 0, setting_config_ids)]}]
                        )
                        print(f"Field is_integrated set to True for loyalty program ID {record['id']}.")

                        if len(index_store_ids) == len(setting_config_ids):
                            self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'loyalty.card', 'write',
                            [[record['id']], {'is_updated': True, 'index_store': [(5, 0, 0)]}])

                        else:
                            self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'loyalty.card', 'write',
                            [[record['id']], {'is_updated': False}])
                    except Exception as e:
                        print(f"Terjadi kesalahan saat memperbarui loyalty: {e}")

            # Process loyalty points in batches of 100
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                for i in range(0, len(loyalty_points), 100):
                    batch = loyalty_points[i:i + 100]
                    executor.map(process_loyalty_point, batch)

    def ts_in_from_mc(self, model_name, fields, description, date_from, date_to):
        try:
            transaksi_ts_in = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                model_name, 'search_read',
                                                                [[['picking_type_id.name', '=', 'TS In'], ['is_integrated', '=', False], ['state', '=', 'assigned'], ['create_date', '>=', date_from], ['create_date', '<=', date_to]]],
                                                                {'fields': fields})
            
            # print(transaksi_goods_receipt)

            if not transaksi_ts_in:
                print("Semua transaksi telah diproses.")
                return

            location_dest_id = [str(record.get('location_dest_id')[0]) if isinstance(record.get('location_dest_id'), list) else str(record.get('location_dest_id')) for record in transaksi_ts_in]
            location_dest_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['id_mc', 'in', location_dest_id]]],
                                                                {'fields': ['id', 'id_mc', 'complete_name'] , 'limit': 1})
            location_dest_source_dict = {location_dest['id_mc']: location_dest['id'] for location_dest in location_dest_source}

            picking_type_ids = [str(record.get('picking_type_id')[0]) if isinstance(record.get('picking_type_id'), list) else str(record.get('picking_type_id')) for record in transaksi_ts_in]

            picking_type_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking.type', 'search_read',
                                                                    [[['id_mc', 'in', picking_type_ids]]],
                                                                    {'fields': ['id', 'id_mc'] , 'limit': 1})
            picking_type_source_dict = {type['id_mc']: type['id'] for type in picking_type_source}

            picking_ids = [record['id'] for record in transaksi_ts_in]
            ts_in_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'stock.move', 'search_read',
                                                                [[['picking_id', 'in', picking_ids]]],
                                                                {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

            tsin_transfer_inventory_lines_dict = {}
            for line in ts_in_inventory_lines:
                if 'picking_id' in line:
                    picking_id = line['picking_id'][0]
                    if picking_id not in tsin_transfer_inventory_lines_dict:
                        tsin_transfer_inventory_lines_dict[picking_id] = []
                    tsin_transfer_inventory_lines_dict[picking_id].append(line)

            product_ids = [line['product_id'][0] for line in ts_in_inventory_lines if line.get('product_id')]
            # Step 1: Fetch product.product data from source_client using product_ids
            product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'product.product', 'search_read',
                                                            [[['id', 'in', product_ids]]],
                                                            {'fields': ['id', 'product_tmpl_id', 'default_code']})
            print(product_source)
            # Step 2: Create a dictionary to map product_id to default_code
            product_source_dict = {product['id']: product['default_code'] for product in product_source if 'default_code' in product}
            print(product_source_dict)
            # Step 3: Create a mapping from default_code to product_tmpl_id
            default_code_to_product_tmpl_id = {product['default_code']: product['product_tmpl_id'] for product in product_source if 'default_code' in product}

            # Step 4: Fetch product.template data from target_client using default_code
            # Step 4 (baru): Fetch product.product dari target_client berdasarkan default_code
            product_target_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'product.product', 'search_read',
                                                                [[['default_code', 'in', list(default_code_to_product_tmpl_id.keys())]]],
                                                                {'fields': ['id', 'default_code']})

            # Step 5 (baru): Mapping dari default_code ke product.product.id
            default_code_to_target_product_id = {product['default_code']: product['id'] for product in product_target_source}
            
            existing_ts_in_dict = {}
            for record in transaksi_ts_in:
                existing_ts_in = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.picking', 'search_read',
                                                            [[['vit_trxid', '=', record.get('name')]]],
                                                            {'fields': ['id'], 'limit': 1})
                if existing_ts_in:
                    existing_ts_in_dict[record['id']] = existing_ts_in[0]['id']

            def process_ts_in_record(record):
                if record['id'] in existing_ts_in_dict:
                    return

                ts_in_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    'stock.move', 'search_read',
                                                                    [[['picking_id', '=', record['id']]]],
                                                                    {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

                location_id = "Partners/Vendors"
                location_id_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['complete_name', '=', location_id]]],
                                                                {'fields': ['id'], 'limit': 1})

                location_id = location_id_source[0]['id']
                location_dest_id = location_dest_source_dict.get(
                    str(record.get('location_dest_id')[0]) if isinstance(record.get('location_dest_id'), list) else str(
                        record.get('location_dest_id')))
                
                picking_type_id = picking_type_source_dict.get(
                    str(record.get('picking_type_id')[0]) if isinstance(record.get('picking_type_id'), list) else str(
                        record.get('picking_type_id')))
                
                print(location_id, location_dest_id, picking_type_id)
                
                missing_products = []
                ts_in_inventory_line_ids = []
                should_skip_create = False
                
                # Process each line in ts_in_inventory_lines
                for line in ts_in_inventory_lines:
                    source_product_code = product_source_dict.get(line.get('product_id')[0])

                    # Step 7: Get the target product ID using the default_code mapping
                    target_product_id = default_code_to_target_product_id.get(source_product_code)

                    if not target_product_id:
                        missing_products.append(source_product_code)
                        should_skip_create = True
                        continue  # Skip this product if not found in target system

                    # Prepare the inventory line data for stock.picking
                    ts_in_inventory_line_data = {
                        'product_id': int(target_product_id),
                        'product_uom_qty': line.get('product_uom_qty'),
                        'name': line.get('name'),
                        'quantity': line.get('quantity'),
                        'location_dest_id': int(location_dest_id),
                        'location_id': int(location_id)
                    }
                    ts_in_inventory_line_ids.append((0, 0, ts_in_inventory_line_data))

                # If any products were missing, log the issue and skip creating stock.picking
                if should_skip_create:
                    missing_products_str = ", ".join(missing_products)
                    message = f"Terdapat produk tidak aktif dalam TS Out/TS In: {missing_products_str}"
                    print(message)
                    
                    # Get the write date for the transaction and log the failure
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'TS Out/TS In', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'TS Out/TS In', message, write_date)
                    return  # CRITICAL FIX: Exit the function here to prevent document creation
                
                # ONLY proceed with document creation if no products are missing
                # Prepare the transfer data for stock.picking creation
                target_location = record.get('target_location')

                ts_in_transfer_data = {
                    'scheduled_date': record.get('scheduled_date', False),
                    'date_done': record.get('date_done', False),
                    'vit_trxid': record.get('name', False),
                    'is_integrated': True,
                    'origin': record.get('origin', False),
                    'location_id': int(location_id),
                    'location_dest_id': int(location_dest_id),
                    'targets': target_location,
                    'picking_type_id': int(picking_type_id),
                    'move_ids_without_package': ts_in_inventory_line_ids,
                }

                print(ts_in_transfer_data)

                try:
                    # Create the new stock.picking entry
                    new_ts_in_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.picking', 'create',
                                                                [ts_in_transfer_data])
                    print(f"Goods Receipt baru telah dibuat dengan ID: {new_ts_in_id}")

                    # Fetch the vit_trxid for the newly created stock.picking entry
                    new_ts_in_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.picking', 'read',
                                                                [new_ts_in_id, ['name']])

                    if new_ts_in_id:
                        vit_trxid = new_ts_in_id[0]['name']

                        start_time = time.time()
                        # Update the source system with the new vit_trxid
                        self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'stock.picking', 'write',
                            [[record['id']], {'vit_trxid': vit_trxid}]
                        )
                        end_time = time.time()
                        duration = end_time - start_time

                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'TS Out/TS In', write_date)
                        self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'TS Out/TS In', write_date)
                    else:
                        print(f"Tidak dapat mengambil 'vit_trxid' untuk stock.picking ID {new_ts_in_id}")

                except Exception as e:
                    print(f"Gagal membuat atau memposting TS In baru: {e}")
                    write_date = self.get_write_date(model_name, record['id'])
                    message_exception = f"Gagal membuat atau memposting TS In baru: {e}"
                    self.set_log_mc.create_log_note_failed(record, 'TS Out/TS In', message_exception, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'TS Out/TS In', message_exception, write_date)

            # Execute the function with a ThreadPoolExecutor for concurrent processing
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_ts_in_record, record) for record in transaksi_ts_in]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Gagal membuat atau memposting TS In di Source: {e}")

    def validate_tsout_mc(self, model_name, fields, description, date_from, date_to):
        # Retrieve TS Out records that match the specified criteria from the source database
        TS_out_validates = self.source_client.call_odoo(
            'object', 'execute_kw', 
            self.source_client.db, self.source_client.uid, self.source_client.password,
            'stock.picking', 'search_read',
            [[
                ['picking_type_id.name', '=', 'TS Out'], 
                ['is_integrated', '=', False], 
                ['state', '=', 'assigned'],
                ['write_date', '>=', date_from], 
                ['write_date', '<=', date_to]
            ]],
            {'fields': ['id', 'name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'vit_trxid', 'move_ids_without_package']}
        )

        # Check if any TS Out records are found
        if not TS_out_validates:
            print("Tidak ada TS Out yang ditemukan di target.")
        else:
            # Process in batches of 100
            for i in range(0, len(TS_out_validates), 100):
                batch = TS_out_validates[i:i + 100]
                for ts in batch:
                    try:
                        start_time = time.time()
                        self.source_client.call_odoo(
                            'object', 'execute_kw',
                            self.source_client.db, self.source_client.uid, self.source_client.password,
                            'stock.picking', 'button_validate',
                            [ts['id']]
                        )
                        print(f"TS Out with ID {ts['id']} has been validated.")
                        end_time = time.time()
                        duration = end_time - start_time

                        write_date = self.get_write_date(model_name, ts['id'])
                        self.set_log_mc.create_log_note_success(ts, start_time, end_time, duration, 'TS Out/TS In', write_date)
                        self.set_log_ss.create_log_note_success(ts, start_time, end_time, duration, 'TS Out/TS In', write_date)
                    except Exception as e:
                        message_exception = f"Failed to validate TS Out with ID {ts['id']}: {e}"
                        self.set_log_ss.create_log_note_failed(ts, 'TS Out/TS In', message_exception, write_date)
                        self.set_log_mc.create_log_note_failed(ts, 'TS Out/TS In', message_exception, write_date)

    def validate_goods_receipts_mc(self, model_name, fields, description, date_from, date_to):
        # Retrieve TS Out records that match the specified criteria from the source database
        goods_receipts_validates = self.source_client.call_odoo(
            'object', 'execute_kw', 
            self.source_client.db, self.source_client.uid, self.source_client.password,
            'stock.picking', 'search_read',
            [[
                ['picking_type_id.name', '=', 'Goods Receipts'], 
                ['state', '=', 'assigned'],
            ]],
            {'fields': ['id', 'name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'vit_trxid', 'move_ids_without_package']}
        )

        # Check if any TS Out records are found
        if not goods_receipts_validates:
            print("Tidak ada Goods Receipts yang ditemukan di source.")
        else:
            # Process in batches of 100
            for i in range(0, len(goods_receipts_validates), 100):
                batch = goods_receipts_validates[i:i + 100]
                for gr in batch:
                    try:
                        start_time = time.time()
                        self.source_client.call_odoo(
                            'object', 'execute_kw',
                            self.source_client.db, self.source_client.uid, self.source_client.password,
                            'stock.picking', 'button_validate',
                            [gr['id']]
                        )
                        print(f"Goods Receipts with ID {gr['id']} has been validated.")
                        end_time = time.time()
                        duration = end_time - start_time

                        write_date = self.get_write_date(model_name, gr['id'])
                        self.set_log_mc.create_log_note_success(gr, start_time, end_time, duration, 'Goods Receipts', write_date)
                        self.set_log_ss.create_log_note_success(gr, start_time, end_time, duration, 'Goods Receipts', write_date)
                    except Exception as e:
                        message_exception = f"Failed to validate Goods Receipts with ID {gr['id']}: {e}"
                        self.set_log_ss.create_log_note_failed(gr, 'Goods Receipts', message_exception, write_date)
                        self.set_log_mc.create_log_note_failed(gr, 'Goods Receipts', message_exception, write_date)

    def validate_goods_issue_mc(self, model_name, fields, description, date_from, date_to):
        # Retrieve TS Out records that match the specified criteria from the source database
        goods_issue_validates = self.source_client.call_odoo(
            'object', 'execute_kw', 
            self.source_client.db, self.source_client.uid, self.source_client.password,
            'stock.picking', 'search_read',
            [[
                ['picking_type_id.name', '=', 'Goods Issue'],
                ['state', '=', 'assigned'],
            ]],
            {'fields': ['id', 'name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'vit_trxid', 'move_ids_without_package']}
        )

        # Check if any TS Out records are found
        if not goods_issue_validates:
            print("Tidak ada Goods Issue yang ditemukan di source.")
        else:
            # Process in batches of 100
            for i in range(0, len(goods_issue_validates), 100):
                batch = goods_issue_validates[i:i + 100]
                for gi in batch:
                    try:
                        start_time = time.time()
                        self.source_client.call_odoo(
                            'object', 'execute_kw',
                            self.source_client.db, self.source_client.uid, self.source_client.password,
                            'stock.picking', 'button_validate',
                            [gi['id']]
                        )
                        print(f"Goods Issue with ID {gi['id']} has been validated.")
                        end_time = time.time()
                        duration = end_time - start_time

                        write_date = self.get_write_date(model_name, gi['id'])
                        self.set_log_mc.create_log_note_success(gi, start_time, end_time, duration, 'Goods Issue', write_date)
                        self.set_log_ss.create_log_note_success(gi, start_time, end_time, duration, 'Goods Issue', write_date)
                    except Exception as e:
                        message_exception = f"Failed to validate Goods Issue with ID {gi['id']}: {e}"
                        self.set_log_ss.create_log_note_failed(gi, 'Goods Issue', message_exception, write_date)
                        self.set_log_mc.create_log_note_failed(gi, 'Goods Issue', message_exception, write_date)

    def validate_invoice(self, model_name, fields, description, date_from, date_to):
        # Retrieve TS In records that match the specified criteria from the source database
        invoice_validates = self.source_client.call_odoo(
            'object', 'execute_kw', 
            self.source_client.db, self.source_client.uid, self.source_client.password,
            'pos.order', 'search_read',
            [[
                ['is_integrated', '=', True], 
                ['state', '=', 'paid']
            ]],
            {'fields': ['id']}
        )
        for record in invoice_validates:
            try:
                start_time = time.time()
                self.source_client.call_odoo(
                    'object', 'execute_kw',
                    self.source_client.db, self.source_client.uid, self.source_client.password,
                    'pos.order', 'action_pos_order_invoice',
                    [[record['id']]]  # Corrected to wrap record['id'] in a list
                )
                print(f"PoS Order with ID {record['id']} has been invoiced.")
                end_time = time.time()
                duration = end_time - start_time

                write_date = self.get_write_date(model_name, record['id'])
                self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Invoice', write_date)  # Added record
                self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Invoice', write_date)  # Added record
            except Exception as e:
                write_date = self.get_write_date(model_name, record['id'])
                message_exception = f"Failed to validate POS Order In with ID {record['id']}: {e}"
                self.set_log_ss.create_log_note_failed(record, 'Invoice', message_exception, write_date)
                self.set_log_mc.create_log_note_failed(record, 'Invoice', message_exception, write_date)

    def validate_GRPO(self, model_name, fields, description, date_from, date_to):
        # Retrieve TS In records that match the specified criteria from the source database
        GRPO_validates = self.source_client.call_odoo(
            'object', 'execute_kw', 
            self.source_client.db, self.source_client.uid, self.source_client.password,
            'stock.picking', 'search_read',
            [[
                ['picking_type_id.name', '=', 'GRPO'], 
                ['is_integrated', '=', True], 
                ['is_updated', '=', False],
                ['state', '=', 'done'],
                ['create_date', '>=', date_from],
                ['create_date', '<=', date_to]
            ]],
            {'fields': ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'vit_trxid', 'move_ids_without_package']}
        )

        # Check if any TS In records are found
        if not GRPO_validates:
            print("Tidak ada GRPO yang ditemukan di target.")
        else:
            for res in GRPO_validates:
                vit_trxid = res.get('vit_trxid', False)

                # Retrieve TS In records that need validation from the target database
                GRPO_needs_validate = self.target_client.call_odoo(
                    'object', 'execute_kw', 
                    self.target_client.db, self.target_client.uid, self.target_client.password,
                    'stock.picking', 'search_read',
                    [[
                        ['picking_type_id.name', '=', 'GRPO'], 
                        ['vit_trxid', '=', vit_trxid], 
                        ['is_integrated', '=', True], 
                        ['state', '=', 'assigned']
                    ]],
                    {'fields': ['name']}
                )
                
                write_date = False
                # Validate each TS In record
                for rec in GRPO_needs_validate:
                    grpo_id = rec['id']
                    try:
                        start_time = time.time()
                        self.target_client.call_odoo(
                            'object', 'execute_kw',
                            self.target_client.db, self.target_client.uid, self.target_client.password,
                            'stock.picking', 'button_validate',
                            [grpo_id]
                        )
                        self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'stock.picking', 'write',
                            [[rec['id']], {'is_updated': True}]
                        )

                        print(f"GRPO In with ID {grpo_id} has been validated.")
                        end_time = time.time()
                        duration = end_time - start_time

                        write_date = self.get_write_date(model_name, rec['id'])
                        self.set_log_mc.create_log_note_success(rec, start_time, end_time, duration, 'GRPO', write_date)
                        self.set_log_ss.create_log_note_success(rec, start_time, end_time, duration, 'GRPO', write_date)
                    except Exception as e:
                        print(f"Failed to validate GRPO with ID {grpo_id}: {e}")
                        # self.set_log_ss.create_log_note_failed(rec, 'GRPO', message_exception, write_date)
                        # self.set_log_mc.create_log_note_failed(rec, 'GRPO', message_exception, write_date)

    def transfer_internal_transfers_mc_to_ss(self, model_name, fields, description, date_from, date_to):
        try:
            # Ambil data dari sumber
            Ts_Out_data_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                model_name, 'search_read',
                                                                [[['picking_type_id.name', '=', 'Internal Transfers'], 
                                                                ['is_integrated', '=', False], 
                                                                ['state', '=', 'done'], 
                                                                ['create_date', '>=', date_from], 
                                                                ['create_date', '<=', date_to]]],
                                                                {'fields': fields})

            if not Ts_Out_data_source:
                print("Semua transaksi telah diproses.")
                return

            # Prepare data for processing
            target_location_ids = [record.get('target_location')[0] if isinstance(record.get('target_location'), list) else record.get('target_location') for record in Ts_Out_data_source]
            location_ids = [str(record.get('location_id')[0]) if isinstance(record.get('location_id'), list) else str(record.get('location_id')) for record in Ts_Out_data_source]
            location_dest_id = [str(record.get('location_dest_id')[0]) if isinstance(record.get('location_dest_id'), list) else str(record.get('location_dest_id')) for record in Ts_Out_data_source]

            # Fetch locations from Server A
            location_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.location', 'search_read',
                                                            [[['id_mc', 'in', location_ids]]],
                                                            {'fields': ['id', 'id_mc', 'complete_name'], 'limit': 1})
            location_source_dict = {location['id_mc']: location['id'] for location in location_source}

            # Fetch destination locations from Server B
            location_dest_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['id_mc', 'in', location_dest_id]]],
                                                                {'fields': ['id', 'id_mc', 'complete_name'], 'limit': 1})
            location_dest_source_dict = {location_dest['id_mc']: location_dest['id'] for location_dest in location_dest_source}

            # Fetch internal transfers inventory lines
            picking_ids = [record['id'] for record in Ts_Out_data_source]
            internal_transfers_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                                self.source_client.uid, self.source_client.password,
                                                                                'stock.move', 'search_read',
                                                                                [[['picking_id', 'in', picking_ids]]],
                                                                                {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

            product_ids = [line['product_id'][0] for line in internal_transfers_inventory_lines if line.get('product_id')]
            # Step 1: Fetch product.product data from source_client using product_ids
            product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'product.product', 'search_read',
                                                            [[['id', 'in', product_ids]]],
                                                            {'fields': ['id', 'product_tmpl_id', 'default_code']})

            # Step 2: Create a dictionary to map product_id to default_code
            product_source_dict = {product['id']: product['default_code'] for product in product_source if 'default_code' in product}

            # Step 3: Create a mapping from default_code to product_tmpl_id
            default_code_to_product_tmpl_id = {product['default_code']: product['product_tmpl_id'] for product in product_source if 'default_code' in product}

            # Step 4: Fetch product.template data from target_client using default_code
            product_template_target_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                            self.target_client.uid, self.target_client.password,
                                                                            'product.template', 'search_read',
                                                                            [[['default_code', 'in', list(default_code_to_product_tmpl_id.keys())]]],
                                                                            {'fields': ['id', 'default_code']})

            # Step 5: Create a mapping from default_code to id in target_client
            default_code_to_target_id = {template['default_code']: template['id'] for template in product_template_target_source}

            # Process each record for Goods Issue
            for record in Ts_Out_data_source:
                try:
                    # Create Goods Issue on Server A
                    location_id = location_source_dict.get(str(record.get('location_id')[0]) if isinstance(record.get('location_id'), list) else str(record.get('location_id')))
                    if not location_id:
                        print(f"Missing location_id for record ID {record['id']}. Skipping.")
                        continue

                    tsout_transfer_inventory_line_ids = []
                    tsout_transfer_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                                self.source_client.uid, self.source_client.password,
                                                                                'stock.move', 'search_read',
                                                                                [[['picking_id', '=', record['id']]]],
                                                                                {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

                    missing_products = []
                    should_skip_create = False
                    for line in tsout_transfer_inventory_lines:
                        source_product_code = product_source_dict.get(line.get('product_id')[0])
                        # Step 7: Get the target product ID using the default_code mapping
                        target_product_template_id = default_code_to_target_id.get(source_product_code)

                        if not target_product_template_id:
                            missing_products.append(source_product_code)
                            should_skip_create = True
                            continue

                        tsout_transfer_inventory_line_data = {
                            'product_id': int(target_product_template_id),
                            'product_uom_qty': line.get('product_uom_qty'),
                            'name': line.get('name'),
                            'quantity': line.get('quantity'),
                            'location_dest_id': 5,  # Assuming this is the correct destination for Goods Issue
                            'location_id': int(location_id)
                        }
                        tsout_transfer_inventory_line_ids.append((0, 0, tsout_transfer_inventory_line_data))

                    tsout_transfer_data = {
                        'scheduled_date': record.get('scheduled_date', False),
                        'date_done': record.get('date_done', False),
                        'location_id': int(location_id),
                        'location_dest_id': 5,  # Destination for Goods Issue
                        'picking_type_id': self.get_picking_type_id('Goods Issue'),  # Implement this method to get the picking type ID
                        'is_integrated': True,
                        'vit_trxid': record.get('name', False),
                        'move_ids_without_package': tsout_transfer_inventory_line_ids,
                    }

                    new_tsout_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.picking', 'create',
                                                                [tsout_transfer_data])
                    
                    print(f"Internal Transfers baru telah dibuat di target dengan ID: {new_tsout_id}")
                    
                except Exception as e:
                    print(f"Gagal membuat Internal Transfers untuk record ID {record['id']}: {e}")

            # Now process each record for Goods Receipts on Server B
            for record in Ts_Out_data_source:
                try:
                    # Create Goods Receipts on Server B
                    location_dest_id = location_dest_source_dict.get(str(record.get('location_dest_id')[0]) if isinstance(record.get('location_dest_id'), list) else str(record.get('location_dest_id')))
                    if not location_dest_id:
                        print(f"Missing location_dest_id for record ID {record['id']}. Skipping.")
                        continue

                    tsin_transfer_inventory_line_ids = []
                    tsin_transfer_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                                self.source_client.uid, self.source_client.password,
                                                                                'stock.move', 'search_read',
                                                                                [[['picking_id', '=', record['id']]]],
                                                                                {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

                    missing_products = []
                    for line in tsin_transfer_inventory_lines:
                        source_product_code = product_source_dict.get(line.get('product_id')[0])
                        # Step 7: Get the target product ID using the default_code mapping
                        target_product_template_id = default_code_to_target_id.get(source_product_code)

                        if not target_product_template_id:
                            missing_products.append(source_product_code)
                            continue

                        tsin_transfer_inventory_line_data = {
                            'product_id': int(target_product_template_id),
                            'product_uom_qty': line.get('product_uom_qty'),
                            'name': line.get('name'),
                            'quantity': line.get('quantity'),
                            'location_dest_id': int(location_dest_id),  # Destination for Goods Receipts
                            'location_id': 4,  # Assuming this is the correct source for Goods Receipts
                        }
                        tsin_transfer_inventory_line_ids.append((0, 0, tsin_transfer_inventory_line_data))

                    ts_in_transfer_data = {
                        'scheduled_date': record.get('scheduled_date', False),
                        'date_done': record.get('date_done', False),
                        'location_id': 4,  # Assuming this is the correct source for Goods Receipts
                        'location_dest_id': int(location_dest_id),
                        'origin': record.get('name', False),
                        'picking_type_id': self.get_picking_type_id('Goods Receipts'),  # Implement this method to get the picking type ID
                        'move_ids_without_package': tsin_transfer_inventory_line_ids,
                    }

                    new_ts_in_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.picking', 'create',
                                                                [ts_in_transfer_data])
                    print(f"Internal Transfers baru telah dibuat di target dengan ID: {new_ts_in_id}")

                    self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                self.source_client.uid, self.source_client.password,
                                                model_name, 'write', [[record['id']], {'is_integrated': True}])
                    print(f"Record ID {record['id']} berhasil diperbarui sebagai is_integrated = True di source.")

                except Exception as e:
                    print(f"Gagal membuat Internal Transfers untuk record ID {record['id']}: {e}")

        except Exception as e:
            print(f"Gagal memproses Internal Transfers: {e}")

    def transfer_goods_receipt(self, model_name, fields, description, date_from, date_to):
        try:
            # Ambil data dari sumber
            transaksi_goods_receipt = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                model_name, 'search_read',
                                                                [[['picking_type_id.name', '=', 'Goods Receipts'], ['is_integrated', '=', False], ['state', '=', 'done'], ['create_date', '>=', date_from], ['create_date', '<=', date_to]]],
                                                                {'fields': fields})

            if not transaksi_goods_receipt:
                print("Semua transaksi telah diproses.")
                return
            
            location_ids = [str(record.get('location_id')[0]) if isinstance(record.get('location_id'), list) else str(record.get('location_id')) for record in transaksi_goods_receipt]
            location_dest_ids = [str(record.get('location_dest_id')[0]) if isinstance(record.get('location_dest_id'), list) else str(record.get('location_dest_id')) for record in transaksi_goods_receipt]
            picking_type_ids = [str(record.get('picking_type_id')[0]) if isinstance(record.get('picking_type_id'), list) else str(record.get('picking_type_id')) for record in transaksi_goods_receipt]


            location_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                              self.target_client.uid, self.target_client.password,
                                              'stock.location', 'search_read',
                                              [[['id_mc', 'in', location_ids]]],
                                              {'fields': ['id', 'id_mc', 'complete_name']})
            location_source_dict = {location['id_mc']: location['id'] for location in location_source}
            print("Location Source Dict:", location_source_dict)

            # Ambil data location_dest dari target_client
            location_dest_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['id_mc', 'in', location_dest_ids]]],
                                                                {'fields': ['id', 'id_mc', 'complete_name']})
            location_dest_source_dict = {location_dest['id_mc']: location_dest['id'] for location_dest in location_dest_source}
            print("Location Dest Source Dict:", location_dest_source_dict)

            picking_type_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking.type', 'search_read',
                                                                    [[['id_mc', 'in', picking_type_ids]]],
                                                                    {'fields': ['id', 'id_mc'] , 'limit': 1})
            picking_type_source_dict = {type['id_mc']: type['id'] for type in picking_type_source}

            picking_ids = [record['id'] for record in transaksi_goods_receipt]
            goods_receipt_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', 'in', picking_ids]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})
            
            existing_goods_receipts_dict = {}
            for record in transaksi_goods_receipt:
                existing_gr = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.picking', 'search_read',
                                                            [[['vit_trxid', '=', record.get('name')]]],
                                                            {'fields': ['id'], 'limit': 1})
                if existing_gr:
                    existing_goods_receipts_dict[record['id']] = existing_gr[0]['id']

            goods_receipts_lines_dict = {}
            for line in goods_receipt_inventory_lines:
                if 'picking_id' in line:
                    picking_id = line['picking_id'][0]
                    if picking_id not in goods_receipts_lines_dict:
                        goods_receipts_lines_dict[picking_id] = []
                    goods_receipts_lines_dict[picking_id].append(line)

            product_ids = [line['product_id'][0] for line in goods_receipt_inventory_lines if line.get('product_id')]
            print(product_ids)
            # Step 1: Fetch product.product data from source_client using product_ids
            product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'product.product', 'search_read',
                                                            [[['id', 'in', product_ids]]],
                                                            {'fields': ['id', 'product_tmpl_id', 'default_code']})
            print(product_source)
            # Step 2: Create a dictionary to map product_id to default_code
            product_source_dict = {product['id']: product['default_code'] for product in product_source if 'default_code' in product}
            print(product_source_dict)
            # Step 3: Create a mapping from default_code to product_tmpl_id
            default_code_to_product_tmpl_id = {product['default_code']: product['product_tmpl_id'] for product in product_source if 'default_code' in product}

            # Step 4: Fetch product.template data from target_client using default_code
            # Step 4 (baru): Fetch product.product dari target_client berdasarkan default_code
            product_target_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'product.product', 'search_read',
                                                                [[['default_code', 'in', list(default_code_to_product_tmpl_id.keys())]]],
                                                                {'fields': ['id', 'default_code']})

            # Step 5 (baru): Mapping dari default_code ke product.product.id
            default_code_to_target_product_id = {product['default_code']: product['id'] for product in product_target_source}

            # print(default_code_to_target_id)
            new_goods_receipts_ids = []
            def proces_goods_receipts_record_from_mc(record):
                if record['id'] in existing_goods_receipts_dict:
                    return
                
                # # Ambil invoice line items dari sumber
                goods_receipt_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                                self.source_client.uid, self.source_client.password,
                                                                                'stock.move', 'search_read',
                                                                                [[['picking_id', '=', record['id']]]],
                                                                                {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})
                
                # print(goods_receipt_inventory_lines)
                location_id = location_source_dict.get(str(record.get('location_id')[0]) if isinstance(record.get('location_id'), list) else str(record.get('location_id')))
                location_dest_id = location_dest_source_dict.get(str(record.get('location_dest_id')[0]) if isinstance(record.get('location_dest_id'), list) else str(record.get('location_dest_id')))
                picking_type_id = picking_type_source_dict.get(str(record.get('picking_type_id')[0]) if isinstance(record.get('picking_type_id'), list) else str(record.get('picking_type_id')))
                
                if location_id is None or location_dest_id is None or picking_type_id is None:
                    # print(f"Data tidak lengkap untuk transaksi dengan ID {record.get('id')}. Tidak membuat dokumen.")
                    return

                missing_products = []
                goods_receipt_inventory_line_ids = []
                should_skip_create = False
                for line in goods_receipt_inventory_lines:
                    source_product_code = product_source_dict.get(line.get('product_id')[0])
                    print(source_product_code)
                    # Step 7: Get the target product ID using the default_code mapping
                    target_product_id = default_code_to_target_product_id.get(source_product_code)
                    if not target_product_id:
                        missing_products.append(source_product_code)
                        should_skip_create = True
                        continue

                    goods_receipt_inventory_line_data = {
                        'product_id': int(target_product_id),  # perbaikan di sini
                        'product_uom_qty': line.get('product_uom_qty'),
                        'name': line.get('name'),
                        'quantity': line.get('quantity'),
                        'location_dest_id': int(location_id),
                        'location_id': int(location_dest_id)
                    }
                    goods_receipt_inventory_line_ids.append((0, 0, goods_receipt_inventory_line_data))
                    # print(goods_receipt_inventory_line_ids)
                if should_skip_create:
                    missing_products_str = ", ".join(missing_products)
                    message = f"Terdapat produk tidak aktif dalam Goods Receipt: {missing_products_str}"
                    print(message)
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Goods Receipts', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Goods Receipts', message, write_date)

                goods_receipts_transfer_data = {
                    # 'name': record.get('name', False) + ' - ' + datetime.now(pytz.timezone('Asia/Jakarta')).strftime("%Y-%m-%d %H:%M:%S"),
                    # 'partner_id': customer_target_id,
                    'scheduled_date': record.get('scheduled_date', False),
                    'date_done': record.get('date_done', False),
                    'vit_trxid': record.get('name', False),
                    'is_integrated': True,
                    'origin': record.get('vit_trxid', False),
                    'location_id': int(location_dest_id),
                    'location_dest_id': int(location_id),
                    'picking_type_id': int(picking_type_id),
                    'move_ids_without_package': goods_receipt_inventory_line_ids,
                }
                try:
                    new_gr_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking', 'create',
                                                                    [goods_receipts_transfer_data])
                    print(f"Goods Receipt baru telah dibuat dengan ID: {new_gr_id}")

                    self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'stock.picking', 'button_validate',
                                                [[new_gr_id]])
                    print(f"Goods Receipts dengan ID {new_gr_id} telah divalidasi.")

                    start_time = time.time()
                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'stock.picking', 'write',
                        [[record['id']], {'is_integrated': True}]
                    )
                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Goods Receipts', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Goods Receipts', write_date)
                except Exception as e:
                    message_exception = f"Gagal membuat atau memposting Goods Receipt baru: {e}"
                    self.set_log_ss.create_log_note_failed(record, 'Goods Receipts', message_exception, write_date)
                    self.set_log_mc.create_log_note_failed(record, 'Goods Receipts', message_exception, write_date)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(proces_goods_receipts_record_from_mc, record) for record in transaksi_goods_receipt]
                concurrent.futures.wait(futures)
        except Exception as e:
            print(f"Gagal membuat atau memposting Goods Receipts di Source baru: {e}")

    def transfer_receipts(self, model_name, fields, description, date_from, date_to):
        try:
            # Ambil data dari sumber
            transaksi_receipt = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                model_name, 'search_read',
                                                                [[['picking_type_id.name', '=', 'GRPO'], ['is_integrated', '=', False], ['state', '=', 'assigned'], ['create_date', '>=', date_from], ['create_date', '<=', date_to]]],
                                                                {'fields': fields})

            if not transaksi_receipt:
                print("Semua transaksi telah diproses.")
                return

            location_ids = [str(record.get('location_id')[0]) if isinstance(record.get('location_id'), list) else str(record.get('location_id')) for record in transaksi_goods_receipt]
            location_dest_ids = [str(record.get('location_dest_id')[0]) if isinstance(record.get('location_dest_id'), list) else str(record.get('location_dest_id')) for record in transaksi_goods_receipt]
            picking_type_ids = [str(record.get('picking_type_id')[0]) if isinstance(record.get('picking_type_id'), list) else str(record.get('picking_type_id')) for record in transaksi_goods_receipt]


            location_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                              self.target_client.uid, self.target_client.password,
                                              'stock.location', 'search_read',
                                              [[['id_mc', 'in', location_ids]]],
                                              {'fields': ['id', 'id_mc', 'complete_name']})
            location_source_dict = {location['id_mc']: location['id'] for location in location_source}
            print("Location Source Dict:", location_source_dict)

            # Ambil data location_dest dari target_client
            location_dest_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['id_mc', 'in', location_dest_ids]]],
                                                                {'fields': ['id', 'id_mc', 'complete_name']})
            location_dest_source_dict = {location_dest['id_mc']: location_dest['id'] for location_dest in location_dest_source}
            print("Location Dest Source Dict:", location_dest_source_dict)

            picking_type_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking.type', 'search_read',
                                                                    [[['id_mc', 'in', picking_type_ids]]],
                                                                    {'fields': ['id', 'id_mc'] , 'limit': 1})
            picking_type_source_dict = {type['id_mc']: type['id'] for type in picking_type_source}

            picking_ids = [record['id'] for record in transaksi_receipt]

            receipt_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    'stock.move', 'search_read',
                                                                    [[['picking_id', '=', picking_ids]]],
                                                                    {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

            existing_grpo_dict = {}
            for record in transaksi_receipt:
                existing_grpo = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.picking', 'search_read',
                                                            [[['vit_trxid', '=', record.get('name')]]],
                                                            {'fields': ['id'], 'limit': 1})
                if existing_grpo:
                    existing_grpo_dict[record['id']] = existing_grpo[0]['id']

            grpo_lines_dict = {}
            for line in receipt_inventory_lines:
                if 'picking_id' in line:
                    picking_id = line['picking_id'][0]
                    if picking_id not in grpo_lines_dict:
                        grpo_lines_dict[picking_id] = []
                    grpo_lines_dict[picking_id].append(line)

            product_ids = [line['product_id'][0] for line in receipt_inventory_lines if line.get('product_id')]
            # Step 1: Fetch product.product data from source_client using product_ids
            # Step 1: Fetch product.product data from source_client using product_ids
            product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'product.product', 'search_read',
                                                            [[['id', 'in', product_ids]]],
                                                            {'fields': ['id', 'product_tmpl_id', 'default_code']})
            print(product_source)
            # Step 2: Create a dictionary to map product_id to default_code
            product_source_dict = {product['id']: product['default_code'] for product in product_source if 'default_code' in product}
            print(product_source_dict)
            # Step 3: Create a mapping from default_code to product_tmpl_id
            default_code_to_product_tmpl_id = {product['default_code']: product['product_tmpl_id'] for product in product_source if 'default_code' in product}

            # Step 4: Fetch product.template data from target_client using default_code
            # Step 4 (baru): Fetch product.product dari target_client berdasarkan default_code
            product_target_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'product.product', 'search_read',
                                                                [[['default_code', 'in', list(default_code_to_product_tmpl_id.keys())]]],
                                                                {'fields': ['id', 'default_code']})

            # Step 5 (baru): Mapping dari default_code ke product.product.id
            default_code_to_target_product_id = {product['default_code']: product['id'] for product in product_target_source}
            
            new_grpo_ids = []
            def proces_receipts_record_from_mc(record):
                if record['id'] in existing_grpo_dict:
                    return
                receipt_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', '=', record['id']]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})
                
                location_id = location_source_dict.get(record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id'))
                location_dest_id = location_dest_source_dict.get(record.get('location_dest_id')[0] if isinstance(record.get('location_dest_id'), list) else record.get('location_dest_id'))
                picking_type_id = picking_type_source_dict.get(record.get('picking_type_id')[0] if isinstance(record.get('picking_type_id'), list) else record.get('picking_type_id'))
                
                if location_id is None or location_dest_id is None or picking_type_id is None:
                    print(f"Data tidak lengkap untuk transaksi dengan ID {record.get('id')}. Tidak membuat dokumen.")
                    return
                
                missing_products = []
                receipt_inventory_line_ids = []
                should_skip_create = False
                for line in receipt_inventory_lines:
                    source_product_code = product_source_dict.get(line.get('product_id')[0])

                    # Step 7: Get the target product ID using the default_code mapping
                    target_product_id = default_code_to_target_product_id.get(source_product_code)
                    if not target_product_id:
                        missing_products.append(source_product_code)
                        should_skip_create = True
                        continue

                    receipt_inventory_line_data = {
                        'product_id': int(target_product_id),
                        'product_uom_qty': line.get('product_uom_qty'),
                        'name': line.get('name'),
                        'quantity': line.get('quantity'),
                        'location_dest_id': int(location_dest_id),
                        'location_id': int(location_id)
                    }
                    receipt_inventory_line_ids.append((0, 0, receipt_inventory_line_data))

                if should_skip_create:
                    missing_products_str = ", ".join(missing_products)
                    message = f"Terdapat produk tidak aktif dalam Receipt: {missing_products_str}"
                    print(message)
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Receipts', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Receipts', message, write_date)

                receipt_transfer_data = {
                    # 'name': record.get('name', False) + ' - ' + datetime.now(pytz.timezone('Asia/Jakarta')).strftime("%Y-%m-%d %H:%M:%S"),
                    # 'partner_id': customer_target_id,
                    'scheduled_date': record.get('scheduled_date', False),
                    'date_done': record.get('date_done', False),
                    'vit_trxid': record.get('name', False),
                    'origin': record.get('origin', False),
                    'is_integrated': True,
                    'location_id': int(location_id),
                    'location_dest_id': int(location_dest_id),
                    'picking_type_id': int(picking_type_id),
                    'move_ids_without_package': receipt_inventory_line_ids,
                }

                try:
                    new_receipt_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking', 'create',
                                                                    [receipt_transfer_data])
                    print(f"GRPO baru telah dibuat dengan ID: {new_receipt_id}")

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
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'GRPO', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'GRPO', write_date)
                except Exception as e:
                    message_exception = f"Gagal membuat atau memposting GRPO baru: {e}"
                    self.set_log_ss.create_log_note_failed(record, 'GRPO', message_exception, write_date)
                    self.set_log_mc.create_log_note_failed(record, 'GRPO', message_exception, write_date)

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(proces_receipts_record_from_mc, record) for record in transaksi_receipt]
                concurrent.futures.wait(futures)
        except Exception as e:
            print(f"Gagal membuat atau memposting GRPO di Source baru: {e}")

    def transfer_goods_issue(self, model_name, fields, description, date_from, date_to):
        try:
            # Ambil data dari sumber
            transaksi_goods_issue = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            model_name, 'search_read',
                                                            [[['picking_type_id.name', '=', 'Goods Issue'], ['is_integrated', '=', False], ['state', '=', 'done'], ['create_date', '>=', date_from], ['create_date', '<=', date_to]]],
                                                            {'fields': fields})

            if not transaksi_goods_issue:
                print("Semua transaksi telah diproses.")
                return
            
            location_ids = [str(record.get('location_id')[0]) if isinstance(record.get('location_id'), list) else str(record.get('location_id')) for record in transaksi_goods_issue]
            location_dest_ids = [str(record.get('location_dest_id')[0]) if isinstance(record.get('location_dest_id'), list) else str(record.get('location_dest_id')) for record in transaksi_goods_issue]
            picking_type_ids = [str(record.get('picking_type_id')[0]) if isinstance(record.get('picking_type_id'), list) else str(record.get('picking_type_id')) for record in transaksi_goods_issue]


            location_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                              self.target_client.uid, self.target_client.password,
                                              'stock.location', 'search_read',
                                              [[['id_mc', 'in', location_ids]]],
                                              {'fields': ['id', 'id_mc', 'complete_name']})
            location_source_dict = {location['id_mc']: location['id'] for location in location_source}
            print("Location Source Dict:", location_source_dict)

            # Ambil data location_dest dari target_client
            location_dest_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['id_mc', 'in', location_dest_ids]]],
                                                                {'fields': ['id', 'id_mc', 'complete_name']})
            location_dest_source_dict = {location_dest['id_mc']: location_dest['id'] for location_dest in location_dest_source}
            print("Location Dest Source Dict:", location_dest_source_dict)

            picking_type_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking.type', 'search_read',
                                                                    [[['id_mc', 'in', picking_type_ids]]],
                                                                    {'fields': ['id', 'id_mc', 'name'] , 'limit': 1})
            picking_type_source_dict = {type['id_mc']: type['id'] for type in picking_type_source}

            # print(location_source_dict)

            picking_ids = [record['id'] for record in transaksi_goods_issue]

            goods_issue_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', 'in', picking_ids]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

            existing_goods_issue_dict = {}
            for record in transaksi_goods_issue:
                existing_gi = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.picking', 'search_read',
                                                            [[['vit_trxid', '=', record.get('name')]]],
                                                            {'fields': ['id'], 'limit': 1})
                if existing_gi:
                    existing_goods_issue_dict[record['id']] = existing_gi[0]['id']

            goods_issue_lines_dict = {}
            for line in goods_issue_inventory_lines:
                if 'picking_id' in line:
                    picking_id = line['picking_id'][0]
                    if picking_id not in goods_issue_lines_dict:
                        goods_issue_lines_dict[picking_id] = []
                    goods_issue_lines_dict[picking_id].append(line)

            # Pre-fetch product and tax data
            product_ids = [line['product_id'][0] for line in goods_issue_inventory_lines if line.get('product_id')]
            print(product_ids)
            # Step 1: Fetch product.product data from source_client using product_ids
            product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'product.product', 'search_read',
                                                            [[['id', 'in', product_ids]]],
                                                            {'fields': ['id', 'product_tmpl_id', 'default_code']})
            print(product_source)
            # Step 2: Create a dictionary to map product_id to default_code
            product_source_dict = {product['id']: product['default_code'] for product in product_source if 'default_code' in product}
            print(product_source_dict)
            # Step 3: Create a mapping from default_code to product_tmpl_id
            default_code_to_product_tmpl_id = {product['default_code']: product['product_tmpl_id'] for product in product_source if 'default_code' in product}

            # Step 4: Fetch product.template data from target_client using default_code
            # Step 4 (baru): Fetch product.product dari target_client berdasarkan default_code
            product_target_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'product.product', 'search_read',
                                                                [[['default_code', 'in', list(default_code_to_product_tmpl_id.keys())]]],
                                                                {'fields': ['id', 'default_code']})

            # Step 5 (baru): Mapping dari default_code ke product.product.id
            default_code_to_target_product_id = {product['default_code']: product['id'] for product in product_target_source}

            new_goods_issue_ids = []
            def proces_goods_issue_record_from_mc(record):
                if record['id'] in existing_goods_issue_dict:
                    return
                
                goods_issue_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', '=', record['id']]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

                location_id = location_source_dict.get(str(record.get('location_id')[0]) if isinstance(record.get('location_id'), list) else str(record.get('location_id')))
                location_dest_id = location_dest_source_dict.get(str(record.get('location_dest_id')[0]) if isinstance(record.get('location_dest_id'), list) else str(record.get('location_dest_id')))
                picking_type_id = picking_type_source_dict.get(str(record.get('picking_type_id')[0]) if isinstance(record.get('picking_type_id'), list) else str(record.get('picking_type_id')))
                
                if location_id is None or location_dest_id is None or picking_type_id is None:
                    print(f"Data tidak lengkap untuk transaksi dengan ID {record.get('name')}. Tidak membuat dokumen.")
                    return
                
                missing_products = []
                goods_issue_inventory_line_ids = []
                should_skip_create = False
                for line in goods_issue_inventory_lines:
                    source_product_code = product_source_dict.get(line.get('product_id')[0])

                    # Step 7: Get the target product ID using the default_code mapping
                    target_product_id = default_code_to_target_product_id.get(source_product_code)

                    if not target_product_id:
                        missing_products.append(source_product_code)
                        should_skip_create = True
                        continue

                    goods_issue_inventory_line_data = {
                        'product_id': int(target_product_id),
                        'product_uom_qty': line.get('product_uom_qty'),
                        'name': line.get('name'),
                        'quantity': line.get('quantity'),
                        'location_dest_id': int(location_dest_id),
                        'location_id': int(location_id)
                    }
                    goods_issue_inventory_line_ids.append((0, 0, goods_issue_inventory_line_data))

                if should_skip_create:
                    missing_products_str = ", ".join(missing_products)
                    message = f"Terdapat produk tidak aktif dalam invoice: {missing_products_str}"
                    print(message)
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Goods Issue', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Goods Issue', message, write_date)
                    return

                goods_issue_transfer_data = {
                    # 'name': record.get('name', False) + ' - ' + datetime.now(pytz.timezone('Asia/Jakarta')).strftime("%Y-%m-%d %H:%M:%S"),
                    # 'partner_id': customer_target_id,
                    'scheduled_date': record.get('scheduled_date', False),
                    'date_done': record.get('date_done', False),
                    'vit_trxid': record.get('name', False),
                    'origin': record.get('vit_trxid', False),
                    'is_integrated': True,
                    'location_id': int(location_id),
                    'location_dest_id': int(location_dest_id),
                    'picking_type_id': int(picking_type_id),
                    'move_ids_without_package': goods_issue_inventory_line_ids,
                }

                try:
                    new_goods_issue_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking', 'create',
                                                                    [goods_issue_transfer_data])
                    print(f"Goods Issue baru telah dibuat dengan ID: {new_goods_issue_id}")

                    self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'stock.picking', 'button_validate',
                                                [[new_goods_issue_id]])
                    print(f"Goods Issue dengan ID {new_goods_issue_id} telah divalidasi.")

                    start_time = time.time()
                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'stock.picking', 'write',
                        [[record['id']], {'is_integrated': True}]
                    )
                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Goods Issue', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Goods Issue', write_date)
                except Exception as e:
                    message_exception = f"Gagal membuat atau memposting Goods Issue baru: {e}"
                    self.set_log_ss.create_log_note_failed(record, 'Goods Issue', message_exception, write_date)
                    self.set_log_mc.create_log_note_failed(record, 'Goods Issue', message_exception, write_date)

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(proces_goods_issue_record_from_mc, record) for record in transaksi_goods_issue]
                concurrent.futures.wait(futures)
        except Exception as e:
            print(f"Gagal membuat atau memposting Goods Issue di Source baru: {e}")

    def update_location_id_mc(self, model_name, fields, description, date_from, date_to):
        # Step 1: Find all POS orders eligible for invoicing.
        stock_location = self.source_client.call_odoo(
            'object', 'execute_kw',
            self.source_client.db, self.source_client.uid, self.source_client.password,
            model_name, 'search_read',
            [[]],
            {'fields': fields}
        )

        if not stock_location:
            print("No location to transfer.")
            return
        
        for rec in stock_location:
            complete_name = rec.get('complete_name')
            id_mc = rec.get('id')

            find_loc_target = self.target_client.call_odoo(
                'object', 'execute_kw',
                self.target_client.db, self.target_client.uid, self.target_client.password,
                model_name, 'search_read',
                [[['complete_name', '=', complete_name]]],
                {'fields': fields}
            )

            for res in find_loc_target:
                self.target_client.call_odoo(
                    'object', 'execute_kw', self.target_client.db,
                    self.target_client.uid, self.target_client.password,
                    model_name, 'write',
                    [[res['id']], {'id_mc': id_mc}]
                )

                print(f"Succes update id_mc in store {res['id']}")
                
    def purchase_order_from_mc(self, model_name, fields, description, date_from, date_to):
        try:
            purchase_order = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    model_name, 'search_read',
                                                                    [[['state', '=', 'purchase'], ['is_integrated', '=', False]]],
                                                                    {'fields': fields})

            if not purchase_order:
                print("Tidak ada transaksi yang ditemukan untuk ditransfer.")
                return
            
            partner_ids = [str(record.get('partner_id')[0]) if isinstance(record.get('partner_id'), list) else str(record.get('partner_id')) for record in purchase_order]
            currency_ids = [str(record.get('currency_id')[0]) if isinstance(record.get('currency_id'), list) else str(record.get('currency_id')) for record in purchase_order]
            picking_type_ids = [str(record.get('picking_type_id')[0]) if isinstance(record.get('picking_type_id'), list) else str(record.get('picking_type_id')) for record in purchase_order]

            partner_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'res.partner', 'search_read',
                                                                [[['id_mc', 'in', partner_ids]]],
                                                                {'fields': ['id', 'id_mc'] , 'limit': 1})
            partner_source_dict = {partner['id_mc']: partner['id'] for partner in partner_source}

            currency_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'res.currency', 'search_read',
                                                                [[['id_mc', 'in', currency_ids]]],
                                                                {'fields': ['id', 'id_mc'] , 'limit': 1})
            currency_source_dict = {currency['id']: currency['id'] for currency in currency_source}

            picking_type_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking.type', 'search_read',
                                                                    [[['id_mc', 'in', picking_type_ids]]],
                                                                    {'fields': ['name', 'id_mc', 'default_location_dest_id'] , 'limit': 1})
            picking_type_source_dict = {type['id_mc']: type['id'] for type in picking_type_source} 

            order_ids = [record['id'] for record in purchase_order]

            purchase_order_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'purchase.order.line', 'search_read',
                                                                [[['order_id', 'in', order_ids]]],
                                                                {'fields': ['product_id', 'name', 'product_qty', 'qty_received', 'qty_invoiced', 'product_uom', 'price_unit', 'taxes_id']})
            
            existing_purchase_order_dict = {}
            for record in purchase_order:
                existing_pr = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'purchase.order', 'search_read',
                                                            [[['vit_trxid', '=', record.get('name')]]],
                                                            {'fields': ['id'], 'limit': 1})
                if existing_pr:
                    existing_purchase_order_dict[record['id']] = existing_pr[0]['id']

            purchase_order_lines_dict = {}
            for line in purchase_order_lines:
                if 'order_id' in line:
                    order_id = line['order_id'][0]
                    if order_id not in purchase_order_lines_dict:
                        purchase_order_lines_dict[order_id] = []
                    purchase_order_lines_dict[order_id].append(line)

            product_uom_ids = [line['product_uom'][0] for line in purchase_order_lines if line.get('product_uom')]
            product_uom_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'uom.uom', 'search_read',
                                                        [[['id_mc', 'in', product_uom_ids]]],
                                                        {'fields': ['id', 'id_mc']})
            product_uom_source_dict = {uom['id']: uom['id'] for uom in product_uom_source}

            # Pre-fetch product and tax data
            product_ids = [line['product_id'][0] for line in purchase_order_lines if line.get('product_id')]
            # Step 1: Fetch product.product data from source_client using product_ids
            product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'product.product', 'search_read',
                                                            [[['id', 'in', product_ids]]],
                                                            {'fields': ['id', 'product_tmpl_id', 'default_code']})
            print(product_source)
            # Step 2: Create a dictionary to map product_id to default_code
            # Hapus pemakaian product_tmpl_id, langsung pakai default_code → product_id
            product_source_dict = {
                product['id']: product['default_code']
                for product in product_source if 'default_code' in product
            }
            source_default_codes = list(set(product_source_dict.values()))

            # Step 3: Create a mapping from default_code to product_tmpl_id
            default_code_to_product_tmpl_id = {product['default_code']: product['product_tmpl_id'] for product in product_source if 'default_code' in product}

            default_codes = list(default_code_to_product_tmpl_id.keys())
            # Step 4: Fetch product.template data from target_client using default_code
            # Step 4 (baru): Fetch product.product dari target_client berdasarkan default_code
            product_target_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'product.product', 'search_read',
                                                                [[['default_code', 'in', list(default_code_to_product_tmpl_id.keys())]]],
                                                                {'fields': ['id', 'default_code']})

            # Step 5 (baru): Mapping dari default_code ke product.product.id
            default_code_to_target_product_id = {
                product['default_code']: product['id']
                for product in product_target_source
            }
            
            all_tax_ids = set()
            for line in purchase_order_lines:
                all_tax_ids.update(line.get('taxes_id', []))

            # Create a mapping for tax IDs from source to target
            tax_id_mapping = {}
            for tax_id in all_tax_ids:
                # Fetch the tax from source_client
                tax_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'account.tax', 'search_read',
                                                        [[['id', '=', tax_id]]],
                                                        {'fields': ['id']})
                if tax_source:
                    # Get the id_mc from source_client
                    tax_id_mc = tax_source[0]['id']
                    # Now fetch the corresponding id from target_client
                    tax_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'account.tax', 'search_read',
                                                            [[['id_mc', '=', tax_id_mc]]],
                                                            {'fields': ['id']})
                    if tax_target:
                        tax_id_mapping[tax_id] = tax_target[0]['id']

            def process_purchase_order_record(record):
                if record['id'] in existing_purchase_order_dict:
                    return
                purchase_order_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'purchase.order.line', 'search_read',
                                                                        [[['order_id', '=', record['id']]]],
                                                                        {'fields': ['product_id', 'name', 'product_qty', 'qty_received', 'qty_invoiced', 'product_uom', 'price_unit', 'taxes_id']})
                
                partner_id = partner_source_dict.get(str(record.get('partner_id')[0]) if isinstance(record.get('partner_id'), list) else str(record.get('partner_id')))
                picking_type_id = picking_type_source_dict.get(str(record.get('picking_type_id')[0]) if isinstance(record.get('picking_type_id'), list) else str(record.get('picking_type_id')))
                
                purchase_order_line_ids = []
                missing_products = []
                should_skip_create = False
                total_tax = 0  # Initialize total tax

                # Check if all products exist in the target database
                for line in purchase_order_lines:
                    source_product_code = product_source_dict.get(line.get('product_id')[0])

                    # Step 7: Get the target product ID using the default_code mapping
                    target_product_id = default_code_to_target_product_id.get(source_product_code)

                    if not target_product_id:
                        missing_products.append(source_product_code)
                        should_skip_create = True
                        continue  # Append missing product name

                    tax_ids = line.get('taxes_id', [])
                    target_tax_ids = [tax_id_mapping.get(tax_id) for tax_id in tax_ids if tax_id in tax_id_mapping]

                    purchase_order_line_data = {
                        'product_id': int(target_product_id),
                        'product_qty': line.get('product_qty'),
                        'qty_received': line.get('qty_received'),
                        'qty_invoiced': line.get('qty_invoiced'),
                        # 'product_uom': product_uom,
                        'price_unit': line.get('price_unit'),
                        'taxes_id': [(6, 0, target_tax_ids)]
                    }
                    purchase_order_line_ids.append((0, 0, purchase_order_line_data))

                # Check for missing products after processing all lines
                if should_skip_create:
                    missing_products_str = ", ".join(missing_products)
                    message = f"Terdapat produk tidak aktif dalam invoice: {missing_products_str}"
                    print(message)
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Purchase Order', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Purchase Order', message, write_date)
                    return

                purchase_order_data = {
                    'partner_id': int(partner_id),
                    'partner_ref': record.get('partner_ref'),
                    'date_approve': record.get('date_approve'),
                    'date_planned': record.get('date_planned'),
                    'picking_type_id': int(picking_type_id),
                    'vit_trxid': record.get('vit_trxid'),
                    'is_integrated': True,
                    'order_line': purchase_order_line_ids
                }

                # print(f"Purchase Order Data: {purchase_order_data}")
                try:
                    start_time = time.time()
                    new_purchase_order_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'purchase.order', 'create',
                                                                    [purchase_order_data])

                    print(f"Purchase Order baru telah dibuat dengan ID: {new_purchase_order_id}")

                    self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'purchase.order', 'button_confirm',
                                                [[new_purchase_order_id]])
                    print(f"Tombol button_confirm telah dijalankan untuk PO ID: {new_purchase_order_id}")

                    self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'purchase.order', 'action_view_picking',
                                                [[new_purchase_order_id]])
                    print(f"Tombol receive telah dijalankan untuk PO ID: {new_purchase_order_id}")

                    picking_ids = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                            self.target_client.uid, self.target_client.password,
                                            'stock.picking', 'search_read',
                                            [[['purchase_id', '=', [new_purchase_order_id]]]],
                                            {'fields': ['id', 'move_ids_without_package']})
                    
                    purchase_order_data = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'purchase.order', 'read',
                                                        [[new_purchase_order_id]], {'fields': ['name', 'vit_trxid']})
                    
                    purchase_order_name = purchase_order_data[0]['name']

                    for picking in picking_ids:    
                        self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.picking', 'write',
                                                            [[picking['id']], {'origin': purchase_order_name, 'vit_trxid': record.get('vit_trxid'), 'is_integrated': True}])                    
                        for move_id in picking['move_ids_without_package']:
                            # Baca stock.move untuk mendapatkan quantity
                            move_data = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.move', 'read',
                                                                    [[move_id]], {'fields': ['product_uom_qty']})

                            if move_data:
                                quantity_done = move_data[0]['product_uom_qty']
                                
                                # Update product_uom_qty dengan quantity_done
                                self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.move', 'write',
                                                            [[move_id], {'quantity': quantity_done, 'origin': purchase_order_name}])
                    
                    self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'purchase.order', 'write',
                            [[record['id']], {'is_integrated': True}]
                    )
                    
                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Purchase Order', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Purchase Order', write_date)
                except Exception as e:
                    message_exception = f"Terjadi kesalahan saat membuat invoice: {e}"
                    self.set_log_ss.create_log_note_failed(record, 'Purchase Order', message_exception, write_date)
                    self.set_log_mc.create_log_note_failed(record, 'Purchase Order', message_exception, write_date)

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_purchase_order_record, record) for record in purchase_order]
                concurrent.futures.wait(futures)

        except Exception as e:
                print(f"Gagal membuat atau memposting Purchase Order di Source baru: {e}")

    def payment_method_from_mc(self, model_name, fields, description, date_from, date_to):
        try:
            payment_method = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        model_name, 'search_read',
                                                        [[['is_integrated', '=', False], ['config_ids', '!=', False]]],
                                                        {'fields': fields})

            if not payment_method:
                print("Tidak ada method yang ditemukan untuk ditransfer")
                return
            
            # print(payment_method)

            journal_ids = [str(record.get('journal_id')[0]) if isinstance(record.get('journal_id'), list) else str(record.get('journal_id')) for record in payment_method]


            journal_id_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'account.journal', 'search_read',
                                                                [[['id_mc', 'in', journal_ids]]],
                                                                {'fields': ['id', 'id_mc']})
            journal_id_source_dict = {location['id_mc']: location['id'] for location in journal_id_source}


            pos_config_ids = [config_id for record in payment_method for config_id in record.get('config_ids', [])]

            pos_configs_source = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'pos.config', 'search_read',
                [[['id', 'in', pos_config_ids]]],
                {'fields': ['id', 'name']}
            )

            pos_configs_target = self.target_client.call_odoo(
                'object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'pos.config', 'search_read',
                [[['name', 'in', [pos_config['name'] for pos_config in pos_configs_source]]]],
                {'fields': ['id', 'name']}
            )

            pos_config_dict = {pos_config['name']: pos_config['id'] for pos_config in pos_configs_target}

            existing_payment_method_dict = {}
            for record in payment_method:
                existing_payment = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'pos.payment.method', 'search_read',
                                                                [[['name', '=', record.get('vit_trxid')]]],
                                                                {'fields': ['id']})
                if existing_payment:
                    existing_payment_method_dict[record['id']] = existing_payment[0]['id']

            setting_config_ids = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'setting.config', 'search_read',
                [[['vit_config_server', '=', 'ss'], ['vit_linked_server', '=', True]]],
                {'fields': ['id']}
            )

            if not setting_config_ids:
                print("Tidak ada Setting Config yang valid untuk ditransfer")
                return

            setting_config_id = setting_config_ids[0]['id']

            def process_payment_method_record_from_mc(record):
                # Debug record journal_id
                # print("Processing Record:", record)
                # print("Record Journal ID:", record.get('journal_id'))
                
                journal_id = journal_id_source_dict.get(
                    str(record.get('journal_id')[0]) if isinstance(record.get('journal_id'), list) else str(record.get('journal_id'))
                )

                source_pos_config_ids = record.get('config_ids', [])
                target_pos_config_ids = [pos_config_dict.get(pos_config['name']) for pos_config in pos_configs_source if pos_config['id'] in source_pos_config_ids]

                # No need to check if journal_id or config_ids_mc is None; just leave them empty if they are not found
                if record['id'] in existing_payment_method_dict:
                    # Update id_mc on target_client if payment method exists
                    try:
                        self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                    self.target_client.uid, self.target_client.password,
                                                    'pos.payment.method', 'write',
                                                    [[existing_payment_method_dict[record['id']]],
                                                    {'id_mc': record.get('id'), 'is_updated': True, 
                                                     'journal_id': int(journal_id), 'config_ids': target_pos_config_ids,
                                                     'is_online_payment': record.get('is_online_payment') if record.get('is_online_payment') else False,
                                                    'split_transactions': record.get('split_transactions') if record.get('split_transactions') else False,}])
                        print(f"id_mc updated for existing Payment Method with ID: {existing_payment_method_dict[record['id']]}")
                    except Exception as e:
                        print(f"Gagal memperbarui id_mc untuk Payment Method yang ada: {e}")
                    return
                else:
                    is_store = record.get('is_store')
                    is_store_id = is_store[0] if isinstance(is_store, list) else is_store
                    
                    if is_store_id == setting_config_id:
                        payment_method_transfer_data = {
                            'name': record.get('name') if record.get('name') else False,
                            'is_online_payment': record.get('is_online_payment') if record.get('is_online_payment') else False,
                            'split_transactions': record.get('split_transactions') if record.get('split_transactions') else False,
                            'journal_id': int(journal_id),
                            'config_ids': target_pos_config_ids,
                            'id_mc': record.get('id')
                        }

                        try:
                            new_payment = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'pos.payment.method', 'create',
                                                                    [payment_method_transfer_data])
                            print(f"Payment Method baru telah dibuat dengan ID: {new_payment}")

                            start_time = time.time()
                            self.source_client.call_odoo(
                                'object', 'execute_kw', self.source_client.db,
                                self.source_client.uid, self.source_client.password,
                                'pos.payment.method', 'write',
                                [[record['id']], {'is_integrated': True, 'vit_trxid': record.get('name')}]
                            )
                            end_time = time.time()
                            duration = end_time - start_time

                            write_date = self.get_write_date(model_name, record['id'])
                            self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Payment Method', write_date)
                            self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Payment Method', write_date)
                        except Exception as e:
                            message_exception = f"Gagal memperbarui atau membuat Payment Method: {e}"
                            write_date = self.get_write_date(model_name, record['id'])
                            self.set_log_mc.create_log_note_failed(record, 'Payment Method', message_exception, write_date)    
                            self.set_log_ss.create_log_note_failed(record, 'Payment Method', message_exception, write_date)

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_payment_method_record_from_mc, record) for record in payment_method]
                concurrent.futures.wait(futures)
        except Exception as e:
            print(f"Gagal membuat atau memposting Payment Method di Source baru: {e}")

    def pos_config_from_mc(self, model_name, fields, description, date_from, date_to):
        try:
            # Fetch POS Config data from source client
            target_client_server_name = self.target_client.server_name
            pos_config = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                model_name, 'search_read', # [[['is_integrated', '=', False]]], {'fields': fields}
                [[['is_store.vit_config_server_name', '=', target_client_server_name], ['is_integrated', '=', False]]],
                {'fields': fields}
            )

            if not pos_config:
                print("Tidak ada Config yang ditemukan untuk ditransfer")
                return

            # Fetch Setting Config IDs
            # setting_config_ids = self.source_client.call_odoo(
            #     'object', 'execute_kw', self.source_client.db,
            #     self.source_client.uid, self.source_client.password,
            #     'setting.config', 'search_read',
            #     [[['vit_config_server', '=', 'ss'], ['vit_linked_server', '=', True]]],
            #     {'fields': ['id']}
            # )

            # if not setting_config_ids:
            #     print("Tidak ada Setting Config yang valid untuk ditransfer")
            #     return

            # setting_config_id = setting_config_ids[0]['id']

            existing_pos_config_dict = {}
            for record in pos_config:
                existing_pos_config = self.target_client.call_odoo(
                    'object', 'execute_kw', 
                    self.target_client.db,
                    self.target_client.uid, 
                    self.target_client.password,
                    'pos.config', 'search_read',
                    [[['name', '=', record.get('vit_trxid')]]],
                    {'fields': ['id'], 'limit': 1}
                )
                if existing_pos_config:
                    existing_pos_config_dict[record['id']] = existing_pos_config[0]['id']

            def process_pos_config_record_from_mc(record):
                try:
                    if record['id'] in existing_pos_config_dict:
                        # Update id_mc on target_client if payment method exists
                        try:
                            updated = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'pos.config', 'write',
                                                        [[existing_pos_config_dict[record['id']]],
                                                        {'name': record.get('name', False),
                                                        'id_mc': record.get('id', False),
                                                        'module_pos_hr': record.get('module_pos_hr', False)}])
                            
                            if updated: 
                                self.source_client.call_odoo(
                                    'object', 'execute_kw', self.source_client.db,
                                    self.source_client.uid, self.source_client.password,
                                    'pos.config', 'write', 
                                    [[record['id']], {'is_integrated': True, 'is_updated': True}] # 
                                )
                                print(f"id_mc and is_integrated updated for existing PoS Config with ID: {existing_pos_config_dict[record['id']]}")
                        except Exception as e:
                            print(f"Gagal memperbarui id_mc untuk PoS Config yang ada: {e}")
                        return
                    else:
                        # Validate if the record's is_store matches the setting_config_id
                        # is_store = record.get('is_store')
                        # is_store_id = is_store[0] if isinstance(is_store, list) else is_store
                        
                        # if is_store_id == setting_config_id:
                        pos_config_data = {
                            'name': record.get('name', False),
                            'id_mc': record.get('id', False),
                            'module_pos_hr': record.get('module_pos_hr', False)
                        }
                        start_time = time.time()

                        # Create new POS Config in target client
                        new_pos_config = self.target_client.call_odoo(
                            'object', 'execute_kw', self.target_client.db,
                            self.target_client.uid, self.target_client.password,
                            'pos.config', 'create',
                            [pos_config_data]
                        )
                        print(f"PoS Config baru telah dibuat dengan ID: {new_pos_config}")
                        end_time = time.time()
                        duration = end_time - start_time

                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'TS Out/TS In', write_date)
                        self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'TS Out/TS In', write_date)

                        # Fixed: Corrected the write method call - removed extra list nesting
                        self.source_client.call_odoo(
                                'object', 'execute_kw', self.source_client.db,
                                self.source_client.uid, self.source_client.password,
                                'pos.config', 'write',
                                [[record['id']], {'is_integrated': True, 'vit_trxid': record['name']}]  # Fixed here
                        )
                        # print(f"Record dengan ID {record['id']} tidak cocok dengan setting_config_id")
                except Exception as e:
                    message_exception = f"Gagal memperbarui atau membuat PoS Config: {e}"
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'PoS Config', message_exception, write_date)    
                    self.set_log_ss.create_log_note_failed(record, 'PoS Config', message_exception, write_date)

            # Process records in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_pos_config_record_from_mc, record) 
                        for record in pos_config]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Gagal membuat atau memposting Payment Method di Source baru: {e}")

    def journal_account_from_mc(self, model_name, fields, description, date_from, date_to):
        try:
            target_client_server_name = self.target_client.server_name
            # Retrieve journal accounts from the source
            journal_account = self.source_client.call_odoo(
                'object', 'execute_kw',
                self.source_client.db,
                self.source_client.uid,
                self.source_client.password,
                model_name, 'search_read',
                [[  '&',  # Mulai grouping dengan AND untuk semua kondisi
                        '|',  # OR untuk kondisi tertentu
                            ['is_store.vit_config_server_name', '=', target_client_server_name], ['is_store', '=', False],
                        ['is_integrated', '=', False]]],
                {'fields': fields} # 
            )

            if not journal_account:
                print("Tidak ada journal yang ditransfer")
                return

            # Prepare mapping dictionaries
            def map_accounts(account_ids, model):
                if not account_ids:
                    return {}
                account_source = self.target_client.call_odoo(
                    'object', 'execute_kw',
                    self.target_client.db,
                    self.target_client.uid,
                    self.target_client.password,
                    model, 'search_read',
                    [[['id_mc', 'in', account_ids]]],
                    {'fields': ['id', 'id_mc']}
                )
                return {record['id_mc']: record['id'] for record in account_source}

            default_account_ids = [
                str(record.get('default_account_id')[0]) if isinstance(record.get('default_account_id'), list)
                else str(record.get('default_account_id')) for record in journal_account
            ]
            default_account_source_dict = map_accounts(default_account_ids, 'account.account')

            suspense_account_ids = [
                str(record.get('suspense_account_id')[0]) if isinstance(record.get('suspense_account_id'), list)
                else str(record.get('suspense_account_id')) for record in journal_account
            ]
            suspense_account_source_dict = map_accounts(suspense_account_ids, 'account.account')

            profit_account_ids = [
                str(record.get('profit_account_id')[0]) if isinstance(record.get('profit_account_id'), list)
                else str(record.get('profit_account_id')) for record in journal_account
            ]
            profit_account_source_dict = map_accounts(profit_account_ids, 'account.account')

            loss_account_ids = [
                str(record.get('loss_account_id')[0]) if isinstance(record.get('loss_account_id'), list)
                else str(record.get('loss_account_id')) for record in journal_account
            ]
            loss_account_source_dict = map_accounts(loss_account_ids, 'account.account')

            existing_journal_dict = {}
            for record in journal_account:
                existing_journal = self.target_client.call_odoo(
                    'object', 'execute_kw', 
                    self.target_client.db,
                    self.target_client.uid, 
                    self.target_client.password,
                    'account.journal', 'search_read',
                    [[['name', '=', record.get('name')]]],
                    {'fields': ['id'], 'limit': 1}
                )
                if existing_journal:
                    existing_journal_dict[record['id']] = existing_journal[0]['id']

            # Process each journal account
            def process_journal_account(record):
                try:
                    default_account_id = default_account_source_dict.get(
                        str(record.get('default_account_id')[0]) if isinstance(record.get('default_account_id'), list)
                        else str(record.get('default_account_id'))
                    )
                    suspense_account_id = suspense_account_source_dict.get(
                        str(record.get('suspense_account_id')[0]) if isinstance(record.get('suspense_account_id'), list)
                        else str(record.get('suspense_account_id'))
                    )
                    profit_account_id = profit_account_source_dict.get(
                        str(record.get('profit_account_id')[0]) if isinstance(record.get('profit_account_id'), list)
                        else str(record.get('profit_account_id'))
                    )
                    loss_account_id = loss_account_source_dict.get(
                        str(record.get('loss_account_id')[0]) if isinstance(record.get('loss_account_id'), list)
                        else str(record.get('loss_account_id'))
                    )

                    if record['id'] in existing_journal_dict:
                        # Update id_mc on target_client if payment method exists
                        try:
                            self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'account.journal', 'write',
                                                        [[existing_journal_dict[record['id']]],
                                                        {'id_mc': record.get('id'), 'name': record.get('name', False),
                                                            'type': record.get('type', False),
                                                            'refund_sequence': record.get('refund_sequence', False),
                                                            'code': record.get('code'),
                                                            'id_mc': record.get('id', False),
                                                            'default_account_id': int(default_account_id) if default_account_id else None,
                                                            'suspense_account_id': int(suspense_account_id) if suspense_account_id else None,
                                                            'profit_account_id': int(profit_account_id) if profit_account_id else None,
                                                            'loss_account_id': int(loss_account_id) if loss_account_id else None,}])
                            
                            self.source_client.call_odoo(
                                'object', 'execute_kw', self.source_client.db,
                                self.source_client.uid, self.source_client.password,
                                'account.journal', 'write',
                                [[record['id']], {'is_integrated': True}] # , 'is_updated': True
                            )
                            print(f"id_mc and is_integrated updated for existing Journal with ID: {existing_journal_dict[record['id']]}")
                        except Exception as e:
                            print(f"Gagal memperbarui id_mc untuk Journal yang ada: {e}")
                        return
                    else:
                        # Get vit_config_server_name from is_store
                        # is_store_id = record.get('is_store')
                        # is_store_id = is_store_id[0] if isinstance(is_store_id, list) else is_store_id
                        # vit_config_server_name = None
                        # setting_config_id = self.source_client.call_odoo(
                        #     'object', 'execute_kw',
                        #     self.source_client.db,
                        #     self.source_client.uid,
                        #     self.source_client.password,
                        #     'setting.config', 'search_read',
                        #     [[['vit_config_server', '=', 'ss'], ['vit_linked_server', '=', True]]],
                        #     {'fields': ['id']}
                        # )

                        # setting_config_id = setting_config_id[0]['id']
                        # if is_store_id == setting_config_id:
                        #     is_store = self.source_client.call_odoo(
                        #         'object', 'execute_kw',
                        #         self.source_client.db,
                        #         self.source_client.uid,
                        #         self.source_client.password,
                        #         'setting.config', 'search_read',
                        #         [[['id', '=', is_store_id], ['vit_config_server', '=', 'ss'], ['vit_linked_server', '=', True]]],
                        #         {'fields': ['vit_config_server_name'], 'limit': 1}
                        #     )
                        #     if is_store:
                        #         vit_config_server_name = is_store[0]['vit_config_server_name']
                        #         print(vit_config_server_name)

                        #     if not vit_config_server_name:
                        #         vit_config_server_name = "WH"

                        # # Combine record.get('code') and vit_config_server_name
                        # new_code = f"{record.get('code')}_{vit_config_server_name}"
                        # print(new_code)
                        # menjadi record.get('code')

                        # Check if the journal code already exists
                        existing_code = self.target_client.call_odoo(
                            'object', 'execute_kw',
                            self.target_client.db,
                            self.target_client.uid,
                            self.target_client.password,
                            'account.journal', 'search_read',
                            [[['code', '=', record.get('code')]]],
                            {'fields': ['id']}
                        )
                        if existing_code:
                            print(f"Journal code {record.get('code')} already exists, skipping creation.")
                            return
                        
                        # Prepare journal data
                        journal_account_data = {
                            'name': record.get('name', False),
                            'type': record.get('type', False),
                            'refund_sequence': record.get('refund_sequence', False),
                            'code': record.get('code'),
                            'id_mc': record.get('id', False),
                            'default_account_id': int(default_account_id) if default_account_id else None,
                            'suspense_account_id': int(suspense_account_id) if suspense_account_id else None,
                            'profit_account_id': int(profit_account_id) if profit_account_id else None,
                            'loss_account_id': int(loss_account_id) if loss_account_id else None,
                        }

                        start_time = time.time()
                        # Create new journal account
                        new_journal_id = self.target_client.call_odoo(
                            'object', 'execute_kw',
                            self.target_client.db,
                            self.target_client.uid,
                            self.target_client.password,
                            'account.journal', 'create',
                            [journal_account_data]
                        )

                        self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'account.journal', 'write',
                            [[record['id']], {'is_integrated': True, 'vit_trxid': record['name']}]
                        )
                        print(f"Journal baru telah dibuat dengan kode: {record.get('code')} dan ID: {new_journal_id}")
                        end_time = time.time()
                        duration = end_time - start_time

                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Journal', write_date)
                        self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Journal', write_date)
                except Exception as e:
                    message_exception = f"Gagal memperbarui atau membuat Journal: {e}"
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Journal', message_exception, write_date)    
                    self.set_log_ss.create_log_note_failed(record, 'Journal', message_exception, write_date)

            # Use ThreadPoolExecutor to process records concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_journal_account, record) for record in journal_account]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Gagal membuat atau memposting Payment Method di Source baru: {e}")

    def account_account_from_mc(self, model_name, fields, description, date_from, date_to):
        try:
            # Retrieve journal accounts from the source
            target_client_server_name = self.target_client.server_name
            chart_account = self.source_client.call_odoo(
                'object', 'execute_kw', 
                self.source_client.db,
                self.source_client.uid, 
                self.source_client.password,
                model_name, 'search_read',
                [[  '&',  # Mulai grouping dengan AND untuk semua kondisi
                        '|',  # OR untuk kondisi tertentu
                            ['is_store.vit_config_server_name', '=', target_client_server_name], ['is_store', '=', False],
                        ['is_integrated', '=', False]]],
                {'fields': fields}
            )

            if not chart_account:
                print("Tidak ada coa yang ditransfer")
                return

            # Prepare a dictionary of existing journals in the target system
            existing_chart_dict = {}
            for record in chart_account:
                existing_chart = self.target_client.call_odoo(
                    'object', 'execute_kw', 
                    self.target_client.db,
                    self.target_client.uid, 
                    self.target_client.password,
                    'account.account', 'search_read',
                    [[['code', '=', record.get('code')]]],
                    {'fields': ['id'], 'limit': 1}
                )
                if existing_chart:
                    existing_chart_dict[record['id']] = existing_chart[0]['id']

            # Function to process each journal account
            def process_chart_account(record):
                try:
                    if record['id'] in existing_chart_dict:
                        # Update id_mc on target_client if payment method exists
                        try:
                            updated = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'account.account', 'write',
                                                        [[existing_chart_dict[record['id']]],
                                                        {'id_mc': record.get('id'), 'name': record.get('name'), 'account_type': record.get('account_type'), 'reconcile': record.get('reconcile')}])
                            print(f"id_mc updated for existing COA with ID: {existing_chart_dict[record['id']]}")
                            # tambah update integrated dan log note update
                            if updated:
                                self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'account.account', 'write',
                                                        [[record.get('id')],
                                                        {'is_integrated': True}])
                        except Exception as e:
                            print(f"Gagal memperbarui id_mc untuk COA yang ada: {e}")
                        return
                    else:
                        # Create new journal account
                        chart_account_data = {
                            'name': record.get('name', False),
                            'code': record.get('code', False),
                            'account_type': record.get('account_type', False),
                            'reconcile': record.get('reconcile', False)
                        }

                        new_account_id = self.target_client.call_odoo(
                            'object', 'execute_kw', 
                            self.target_client.db,
                            self.target_client.uid, 
                            self.target_client.password,
                            'account.account', 'create',
                            [chart_account_data]
                        )
                        print(f"COA baru telah dibuat dengan ID: {new_account_id}")

                        # Mark the source journal as integrated
                        start_time = time.time()
                        self.source_client.call_odoo(
                            'object', 'execute_kw', 
                            self.source_client.db,
                            self.source_client.uid, 
                            self.source_client.password,
                            'account.account', 'write',
                            [[record['id']], {'is_integrated': True}]
                        )
                        end_time = time.time()
                        duration = end_time - start_time

                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Chart of Account', write_date)
                        self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Chart of Account', write_date)
                except Exception as e:
                    message_exception = f"Gagal memperbarui atau membuat Chart Account: {e}"
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Chart of Account', message_exception, write_date)    
                    self.set_log_ss.create_log_note_failed(record, 'Chart of Account', message_exception, write_date)

            # Use ThreadPoolExecutor to process records concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_chart_account, record) for record in chart_account]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Gagal membuat atau memposting Chart Account di Source baru: {e}")

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
