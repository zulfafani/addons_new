import time
from datetime import datetime, timedelta
import pytz
import re
import concurrent.futures

# kalau ada case store nya beda zona waktu gimana
class DataTransaksi:
    def __init__(self, source_client, target_client):
        self.source_client = source_client
        self.target_client = target_client
        self.set_log_mc = SetLogMC(self.source_client)
        self.set_log_ss = SetLogSS(self.target_client)

    # Master Console --> Store Server
    # Store Server --> Master Console

    def transfer_unbuild_order(self, model_name, fields, description, date_from, date_to):
        try:
            transaksi_unbuild_order = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                model_name, 'search_read',
                [[['state', '=', 'done'], ['is_integrated', '=', False]]],
                {'fields': fields})

            if not transaksi_unbuild_order:
                print("Semua transaksi telah diproses.")
                return

            # Extract IDs
            location_ids = [record['location_id'][0] if isinstance(record['location_id'], list) else record['location_id'] for record in transaksi_unbuild_order]
            location_dest_ids = [record['location_dest_id'][0] if isinstance(record['location_dest_id'], list) else record['location_dest_id'] for record in transaksi_unbuild_order]
            bom_ids = [record['bom_id'][0] if isinstance(record['bom_id'], list) else record['bom_id'] for record in transaksi_unbuild_order]
            mo_ids = [record['mo_id'][0] if isinstance(record['mo_id'], list) else record['mo_id'] for record in transaksi_unbuild_order]

            # Product Mapping
            product_variant_ids = [record['product_id'][0] if isinstance(record['product_id'], list) else record['product_id'] for record in transaksi_unbuild_order if record.get('product_id')]

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

            mo_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'mrp.production', 'search_read',
                        [[['id', 'in', mo_ids]]],
                        {'fields': ['id', 'name']})

            mo_source_dict = {str(mo['id']): mo['name'] for mo in mo_source}

            mo_trxids = list(mo_source_dict.values())

            mo_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'mrp.production', 'search_read',
                [[['vit_trxid', 'in', mo_trxids]]],
                {'fields': ['id', 'vit_trxid']})

            mo_target_dict = {mo['vit_trxid']: mo['id'] for mo in mo_target}

            # Location Mapping
            location_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'stock.location', 'search_read',
                [[['id', 'in', location_ids]]],
                {'fields': ['id', 'id_mc']})

            location_dest_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'stock.location', 'search_read',
                [[['id', 'in', location_dest_ids]]],
                {'fields': ['id', 'id_mc']})

            location_source_dict = {str(loc['id']): loc['id_mc'] for loc in location_source}
            location_dest_source_dict = {str(loc['id']): loc['id_mc'] for loc in location_dest_source}

            # BoM
            bom_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'mrp.bom', 'search_read',
                [[['id_mc', 'in', [str(bid) for bid in bom_ids]]]],
                {'fields': ['id', 'id_mc']})
            
            bom_dict = {str(rec['id_mc']): rec['id'] for rec in bom_target}

            # Manufacture Order Lines
            picking_ids = [record['id'] for record in transaksi_unbuild_order if record.get('id')]
            unbuild_order_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'mrp.unbuild.line', 'search_read',
                [[['unbuild_id', 'in', picking_ids]]],
                {'fields': ['product_id', 'location_id', 'product_uom_qty', 'product_uom', 'unbuild_id']})

            unbuild_order_lines_dict = {}
            for line in unbuild_order_lines:
                picking_id = line['unbuild_id'][0] if isinstance(line['unbuild_id'], list) else line['unbuild_id']
                unbuild_order_lines_dict.setdefault(picking_id, []).append(line)

            uom_ids = [line['product_uom'][0] for line in unbuild_order_lines if line.get('product_uom')]
            uom_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'uom.uom', 'search_read',
                                                            [[['id', 'in', uom_ids]]],
                                                            {'fields': ['id', 'name']})
            uom_source_dict = {str(mo['id']): mo['name'] for mo in uom_source}

            uom_name = list(uom_source_dict.values())

            uom_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'uom.uom', 'search_read',
                [[['name', 'in', uom_name]]],
                {'fields': ['id', 'name']})

            uom_target_dict = {mo['name']: mo['id'] for mo in uom_target}

            print(uom_target_dict)

            product_ids = [line['product_id'][0] for line in unbuild_order_lines if line.get('product_id')]
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
                                                                            'product.product', 'search_read',
                                                                            [[['default_code', 'in', list(default_code_to_product_tmpl_id.keys())]]],
                                                                            {'fields': ['id', 'default_code']})

            # Step 5: Create a mapping from default_code to id in target_client
            default_code_to_target_id = {template['default_code']: template['id'] for template in product_template_target_source}

            def process_unbuild_order(record):
                try:
                    existing_unbuild = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                        self.target_client.uid, self.target_client.password,
                        'mrp.unbuild', 'search_read',
                        [[['vit_trxid', '=', record.get('name')]]],
                        {'fields': ['id'], 'limit': 1})
                    if existing_unbuild:
                        print(f"🚫 Unbuild Order {record.get('name')} already exists in target — skipped.")
                        return

                    
                    unbuild_order_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                            self.source_client.uid, self.source_client.password,
                                                                            'mrp.unbuild.line', 'search_read',
                                                                            [[['unbuild_id', 'in', [record['id']]]]],
                                                                            {'fields': ['product_id', 'location_id', 'product_uom_qty', 'product_uom', 'unbuild_id']})

                    location_id = location_source_dict.get(str(record['location_id'][0] if isinstance(record['location_id'], list) else record['location_id']))
                    location_dest_id = location_dest_source_dict.get(str(record['location_dest_id'][0] if isinstance(record['location_dest_id'], list) else record['location_dest_id']))
                    bom_id = bom_dict.get(str(record['bom_id'][0] if isinstance(record['bom_id'], list) else record['bom_id']))
                    src_mo_id = record['mo_id'][0] if isinstance(record['mo_id'], list) else record['mo_id']
                    mo_name = mo_source_dict.get(str(src_mo_id))
                    mo_id = mo_target_dict.get(mo_name)


                    src_variant_id = record['product_id'][0] if isinstance(record['product_id'], list) else record['product_id']
                    src_variant_code = product_id_to_default_code.get(src_variant_id)
                    product_variant_id = default_code_to_target_product_id.get(src_variant_code) if src_variant_code else False

                    if not all([location_id, location_dest_id, mo_id, product_variant_id, bom_id]):
                        print("❌ Missing mapping details:")
                        print(f"location_id={location_id}, location_dest_id={location_dest_id}, mo_id={mo_id}, product_id={product_variant_id}, bom_id={bom_id}")
                        return

                    missing_products = []
                    unbuild_order_line_ids = []
                    should_skip_create = False
                    for line in unbuild_order_lines:
                        source_product_code = product_source_dict.get(line.get('product_id')[0])

                        # Step 7: Get the target product ID using the default_code mapping
                        target_product_template_id = default_code_to_target_id.get(source_product_code)

                        if not target_product_template_id:
                            missing_products.append(source_product_code)
                            should_skip_create = True
                            continue

                        uom_id_source = line.get('product_uom')[0] if line.get('product_uom') else None
                        uom_name = uom_source_dict.get(str(uom_id_source))
                        uom_id_target = uom_target_dict.get(uom_name)

                        print(uom_id_target, uom_name)

                        unbuild_order_line_ids.append((0, 0, {
                            'product_id': int(target_product_template_id),
                            'product_uom_qty': line.get('product_uom_qty'),
                            'location_id': int(location_id),
                            'product_uom': int(uom_id_target)
                        }))
                    
                    if should_skip_create:
                        missing_products_str = ", ".join(missing_products)
                        message = f"Terdapat produk tidak ditemukan di target_client: {missing_products_str}. Unbuild Order tidak dibuat."
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'Unbuild Order', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'Unbuild Order', message, write_date)
                        return

                    unbuild_data = {
                        'product_qty': record.get('product_qty'),
                        'vit_trxid': record.get('name'),
                        'location_id': int(location_id),
                        'location_dest_id': int(location_dest_id),
                        'bom_id': bom_id,
                        'product_id': product_variant_id,
                        'mo_id': int(mo_id),
                        'unbuild_line_ids': unbuild_order_line_ids
                    }

                    new_unbuild_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                        self.target_client.uid, self.target_client.password,
                        'mrp.unbuild', 'create', [unbuild_data])

                    self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'mrp.unbuild', 'action_validate',
                                                [[new_unbuild_id]])
                    print(f"✅ Unbuild Order dengan ID {new_unbuild_id} telah divalidasi.")

                    self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'mrp.unbuild', 'write', [[record['id']], {'is_integrated': True}])

                    print(f"✅ Unbuild Order dengan ID {new_unbuild_id} berhasil dibuat dan ditandai integrated.")

                except Exception as e:
                    print(f"💥 Gagal proses MO ID {record.get('id')}: {e}")
                    
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_unbuild_order, record) for record in transaksi_unbuild_order]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"💣 ERROR di transfer_unbiild_order: {e}")


    def transfer_manufacture_order(self, model_name, fields, description, date_from, date_to):
        try:
            transaksi_manufacture_order = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                model_name, 'search_read',
                [[['state', '=', 'done'], ['is_integrated', '=', False]]],
                {'fields': fields})

            if not transaksi_manufacture_order:
                print("Semua transaksi telah diproses.")
                return

            # Extract IDs
            location_ids = [record['location_src_id'][0] if isinstance(record['location_src_id'], list) else record['location_src_id'] for record in transaksi_manufacture_order]
            location_dest_ids = [record['location_dest_id'][0] if isinstance(record['location_dest_id'], list) else record['location_dest_id'] for record in transaksi_manufacture_order]
            picking_type_ids = [record['picking_type_id'][0] if isinstance(record['picking_type_id'], list) else record['picking_type_id'] for record in transaksi_manufacture_order]
            bom_ids = [record['bom_id'][0] if isinstance(record['bom_id'], list) else record['bom_id'] for record in transaksi_manufacture_order]
            user_ids = [record['user_id'][0] if isinstance(record['user_id'], list) else record['user_id'] for record in transaksi_manufacture_order]

            # Product Mapping
            product_variant_ids = [record['product_id'][0] if isinstance(record['product_id'], list) else record['product_id'] for record in transaksi_manufacture_order if record.get('product_id')]

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

            # Location Mapping
            location_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'stock.location', 'search_read',
                [[['id', 'in', location_ids]]],
                {'fields': ['id', 'id_mc']})

            location_dest_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'stock.location', 'search_read',
                [[['id', 'in', location_dest_ids]]],
                {'fields': ['id', 'id_mc']})

            location_source_dict = {str(loc['id']): loc['id_mc'] for loc in location_source}
            location_dest_source_dict = {str(loc['id']): loc['id_mc'] for loc in location_dest_source}

            # Picking type mapping
            picking_type_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'stock.picking.type', 'search_read',
                [[['id', 'in', picking_type_ids]]],
                {'fields': ['id', 'id_mc']})

            picking_type_mapping = {str(pt['id']): pt['id_mc'] for pt in picking_type_source if pt.get('id_mc')}

            # BoM
            bom_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'mrp.bom', 'search_read',
                [[['id_mc', 'in', [str(bid) for bid in bom_ids]]]],
                {'fields': ['id', 'id_mc']})
            bom_dict = {str(rec['id_mc']): rec['id'] for rec in bom_target}

            # User Mapping
            user_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'res.users', 'search_read',
                [[['id', 'in', user_ids]]],
                {'fields': ['id', 'id_mc']})

            user_source_map = {str(u['id']): u['id_mc'] for u in user_source if u.get('id_mc')}
            user_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                self.target_client.uid, self.target_client.password,
                'res.users', 'search_read',
                [[['id', 'in', list(user_source_map.values())]]],
                {'fields': ['id']})
            user_map = {int(u['id']): u['id'] for u in user_target}
            user_id_mapping = {src_id: user_map.get(int(id_mc)) for src_id, id_mc in user_source_map.items()}

            # Manufacture Order Lines
            picking_ids = [record['id'] for record in transaksi_manufacture_order]
            manufacture_order_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                'stock.move', 'search_read',
                [[['raw_material_production_id', 'in', picking_ids]]],
                {'fields': ['product_id', 'location_id', 'product_uom_qty', 'quantity', 'picked', 'picking_id']})

            manufacture_order_lines_dict = {}
            for line in manufacture_order_lines:
                picking_id = line['picking_id'][0] if isinstance(line['picking_id'], list) else line['picking_id']
                manufacture_order_lines_dict.setdefault(picking_id, []).append(line)

            product_ids = [line['product_id'][0] for line in manufacture_order_lines if line.get('product_id')]
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
                                                                            'product.product', 'search_read',
                                                                            [[['default_code', 'in', list(default_code_to_product_tmpl_id.keys())]]],
                                                                            {'fields': ['id', 'default_code']})

            # Step 5: Create a mapping from default_code to id in target_client
            default_code_to_target_id = {template['default_code']: template['id'] for template in product_template_target_source}

            def process_mrp_order(record):
                try:
                    existing_mrp = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                        self.target_client.uid, self.target_client.password,
                        'stock.picking', 'search_read',
                        [[['vit_trxid', '=', record.get('name')]]],
                        {'fields': ['id'], 'limit': 1})
                    if existing_mrp:
                        return
                    
                    manufacture_order_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                            self.source_client.uid, self.source_client.password,
                                                                            'stock.move', 'search_read',
                                                                            [[['raw_material_production_id', 'in', [record['id']]]]],
                                                                            {'fields': ['product_id', 'location_id', 'product_uom_qty', 'quantity', 'picked', 'picking_id']})

                    location_id = location_source_dict.get(str(record['location_src_id'][0] if isinstance(record['location_src_id'], list) else record['location_src_id']))
                    location_dest_id = location_dest_source_dict.get(str(record['location_dest_id'][0] if isinstance(record['location_dest_id'], list) else record['location_dest_id']))
                    picking_type_id = picking_type_mapping.get(str(record['picking_type_id'][0] if isinstance(record['picking_type_id'], list) else record['picking_type_id']))
                    bom_id = bom_dict.get(str(record['bom_id'][0] if isinstance(record['bom_id'], list) else record['bom_id']))
                    user_id = user_id_mapping.get(str(record['user_id'][0] if isinstance(record['user_id'], list) else record['user_id']))

                    src_variant_id = record['product_id'][0] if isinstance(record['product_id'], list) else record['product_id']
                    src_variant_code = product_id_to_default_code.get(src_variant_id)
                    product_variant_id = default_code_to_target_product_id.get(src_variant_code) if src_variant_code else False

                    if not all([location_id, location_dest_id, picking_type_id, product_variant_id, bom_id, user_id]):
                        print(f"⏭️ Data tidak lengkap untuk MO {record.get('id')} → dilewati")
                        print(f"location_id: {location_id}, location_dest_id: {location_dest_id}, picking_type_id: {picking_type_id}, product_id: {product_variant_id}, bom_id: {bom_id}, user_id: {user_id}")
                        return

                    missing_products = []
                    manufacture_order_line_ids = []
                    should_skip_create = False
                    for line in manufacture_order_lines:
                        source_product_code = product_source_dict.get(line.get('product_id')[0])

                        # Step 7: Get the target product ID using the default_code mapping
                        target_product_template_id = default_code_to_target_id.get(source_product_code)

                        if not target_product_template_id:
                            missing_products.append(source_product_code)
                            should_skip_create = True
                            continue

                        manufacture_order_line_ids.append((0, 0, {
                            'product_id': int(target_product_template_id),
                            'product_uom_qty': line.get('product_uom_qty'),
                            'quantity': line.get('quantity'),
                            'location_id': int(location_id),
                        }))
                    
                    if should_skip_create:
                        missing_products_str = ", ".join(missing_products)
                        message = f"Terdapat produk tidak ditemukan di target_client: {missing_products_str}. Manufacture Order tidak dibuat."
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'Manufacture Order', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'Manufacture Order', message, write_date)
                        return

                    mrp_data = {
                        'date_start': record.get('date_start'),
                        'date_finished': record.get('date_finished'),
                        'product_qty': record.get('product_qty'),
                        'user_id': user_id,
                        'vit_trxid': record.get('name'),
                        'location_src_id': int(location_id),
                        'location_dest_id': int(location_dest_id),
                        'picking_type_id': int(picking_type_id),
                        'bom_id': bom_id,
                        'product_id': product_variant_id,
                        'move_raw_ids': manufacture_order_line_ids
                    }

                    new_mrp_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                        self.target_client.uid, self.target_client.password,
                        'mrp.production', 'create', [mrp_data])

                    for action in ['action_confirm', 'action_assign', 'button_mark_done']:
                        self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                            self.target_client.uid, self.target_client.password,
                            'mrp.production', action, [[new_mrp_id]])

                    self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'mrp.production', 'write', [[record['id']], {'is_integrated': True}])

                    print(f"✅ MRP ID {new_mrp_id} berhasil dibuat dan ditandai integrated.")

                except Exception as e:
                    print(f"💥 Gagal proses MO ID {record.get('id')}: {e}")

            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_mrp_order, record) for record in transaksi_manufacture_order]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"💣 ERROR di transfer_manufacture_order: {e}")
            
    def transfer_inventory_stock(self, model_name, fields, description, date_from, date_to):
        try:
            print(f"Memulai transfer {description}...")
            
            # Ambil data inventory.stock dari sumber
            inventory_stocks = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            model_name, 'search_read',
                                                            [[['is_integrated', '=', False], ['create_date', '>=', date_from], ['create_date', '<=', date_to], ['state', '=', 'counted']]],
                                                            {'fields': fields})

            if not inventory_stocks:
                print("Semua transaksi inventory.stock telah diproses.")
                return

            # Ambil data terkait warehouse, location, dan company untuk mapping
            warehouse_ids = [record.get('warehouse_id')[0] if isinstance(record.get('warehouse_id'), list) else record.get('warehouse_id') for record in inventory_stocks if record.get('warehouse_id')]
            location_ids = [record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id') for record in inventory_stocks if record.get('location_id')]
            company_ids = [record.get('company_id')[0] if isinstance(record.get('company_id'), list) else record.get('company_id') for record in inventory_stocks if record.get('company_id')]

            # Ambil mapping warehouse
            warehouses_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.warehouse', 'search_read',
                                                            [[['id', 'in', warehouse_ids]]],
                                                            {'fields': ['id', 'name']})
            warehouse_source_dict = {warehouse['id']: warehouse['name'] for warehouse in warehouses_source if 'name' in warehouse}

            warehouses_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.warehouse', 'search_read',
                                                            [[['name', 'in', list(warehouse_source_dict.values())]]],
                                                            {'fields': ['id', 'name']})
            warehouse_target_dict = {warehouse['id']: warehouse['id'] for warehouse in warehouses_target if 'id' in warehouse}    
            print(f"warehouse_target_dict: {warehouse_target_dict}")
            # Ambil mapping location
            locations_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.location', 'search_read',
                                                            [[['id', 'in', location_ids]]],
                                                            {'fields': ['id', 'id_mc']})
            location_source_dict = {location['id']: location['id_mc'] for location in locations_source if 'id_mc' in location}

            # Ambil mapping company
            companies_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                self.source_client.uid, self.source_client.password,
                                                'res.company', 'search_read',
                                                [[['id', 'in', company_ids]]],
                                                {'fields': ['id', 'name']})
            company_source_dict = {company['id']: company['name'] for company in companies_source if 'name' in company}

            companies_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'res.company', 'search_read',
                                                            [[['name', 'in', list(company_source_dict.values())]]],
                                                            {'fields': ['id', 'name']})
            company_target_dict = {company['id']: company['id'] for company in companies_target if 'id' in company}
            print(f"company_target_dict: {company_target_dict}")

            # Cek inventory stock yang sudah ada di target berdasarkan doc_num
            existing_inventory_stock_dict = {}
            for record in inventory_stocks:
                if record.get('doc_num'):
                    existing_stocks = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'inventory.stock', 'search_read',
                                                                [[['doc_num', '=', record.get('doc_num')]]],
                                                                {'fields': ['id'], 'limit': 1})
                    if existing_stocks:
                        existing_inventory_stock_dict[record['id']] = existing_stocks[0]['id']

            # Ambil inventory.counting untuk setiap inventory.stock
            inventory_stock_ids = [record['id'] for record in inventory_stocks]
            inventory_countings = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'inventory.counting', 'search_read',
                                                            [[['inventory_counting_id', 'in', inventory_stock_ids]]],
                                                            {'fields': ['product_id', 'location_id', 'qty_hand', 'counted_qty', 'difference_qty', 'uom_id', 'inventory_date', 'inventory_counting_id', 'state']})

            # Buat mapping inventory_counting berdasarkan inventory_stock
            inventory_counting_dict = {}
            for line in inventory_countings:
                if 'inventory_counting_id' in line and line['inventory_counting_id']:
                    counting_id = line['inventory_counting_id'][0] if isinstance(line['inventory_counting_id'], list) else line['inventory_counting_id']
                    if counting_id not in inventory_counting_dict:
                        inventory_counting_dict[counting_id] = []
                    inventory_counting_dict[counting_id].append(line)

            # Ambil product IDs dari inventory_counting
            product_ids = [line['product_id'][0] if isinstance(line['product_id'], list) else line['product_id'] for line in inventory_countings if line.get('product_id')]
            uom_ids = [line['uom_id'][0] if isinstance(line['uom_id'], list) else line['uom_id'] for line in inventory_countings if line.get('uom_id')]

            # Mapping product dari source ke target
            product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'product.product', 'search_read',
                                                        [[['id', 'in', product_ids]]],
                                                        {'fields': ['id', 'default_code']})
            product_source_dict = {product['id']: product['default_code'] for product in product_source if 'default_code' in product}

            # Get product IDs in target system
            default_codes = [product['default_code'] for product in product_source if 'default_code' in product]
            product_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'product.product', 'search_read',
                                                        [[['default_code', 'in', default_codes]]],
                                                        {'fields': ['id', 'default_code']})
            target_product_dict = {product['default_code']: product['id'] for product in product_target}

            # Mapping UOM dari source ke target
            uom_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    'uom.uom', 'search_read',
                                                    [[['id', 'in', uom_ids]]],
                                                    {'fields': ['id', 'id_mc']})
            uom_source_dict = {uom['id']: uom['id_mc'] for uom in uom_source if 'id_mc' in uom}

            def process_inventory_stock(record):
                if record['id'] in existing_inventory_stock_dict:
                    print(f"Inventory Stock dengan doc_num {record.get('doc_num')} sudah ada di target system.")
                    return
                
                # Ambil data warehouse, location, dan company dari mapping
                warehouse_id = warehouse_target_dict.get(record.get('warehouse_id')[0] if isinstance(record.get('warehouse_id'), list) else record.get('warehouse_id'))
                location_id = location_source_dict.get(record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id'))
                company_id = company_target_dict.get(record.get('company_id')[0] if isinstance(record.get('company_id'), list) else record.get('company_id'))
                
                print(warehouse_id, location_id, company_id)
                if not warehouse_id or not location_id or not company_id:
                    print(f"Data tidak lengkap untuk Inventory Stock dengan ID {record.get('id')}. Tidak membuat dokumen.")
                    write_date = self.get_write_date(model_name, record['id'])
                    message = f"Data warehouse, location, atau company tidak lengkap"
                    self.set_log_mc.create_log_note_failed(record, 'Inventory Stock', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Inventory Stock', message, write_date)
                    return
                
                # Persiapkan inventory counting lines jika ada
                inventory_counting_lines = []
                missing_products = []
                
                if record['id'] in inventory_counting_dict:
                    for line in inventory_counting_dict[record['id']]:
                        source_product_id = line.get('product_id')[0] if isinstance(line.get('product_id'), list) else line.get('product_id')
                        source_product_code = product_source_dict.get(source_product_id)
                        target_product_id = target_product_dict.get(source_product_code)
                        
                        source_uom_id = line.get('uom_id')[0] if isinstance(line.get('uom_id'), list) else line.get('uom_id')
                        target_uom_id = uom_source_dict.get(source_uom_id)
                        
                        if not target_product_id:
                            missing_products.append(source_product_code)
                            continue
                            
                        if not target_uom_id:
                            print(f"UOM dengan ID {source_uom_id} tidak ditemukan di target system.")
                            continue
                        
                        counting_line_data = {
                            'product_id': int(target_product_id),
                            'location_id': int(location_id),
                            'inventory_date': line.get('inventory_date'),
                            'qty_hand': line.get('qty_hand', 0.0),
                            'counted_qty': line.get('counted_qty', 0.0),
                            'difference_qty': line.get('difference_qty', 0.0),
                            'uom_id': int(target_uom_id),
                            'state': line.get('state', 'draft')
                        }
                        
                        inventory_counting_lines.append((0, 0, counting_line_data))
                
                if missing_products:
                    missing_products_str = ", ".join(missing_products)
                    message = f"Terdapat produk tidak aktif dalam Inventory Stock: {missing_products_str}"
                    print(message)
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Inventory Stock', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Inventory Stock', message, write_date)
                
                # Persiapkan data inventory stock untuk dibuat di target
                inventory_stock_data = {
                    'doc_num': record.get('doc_num'),
                    'warehouse_id': int(warehouse_id),
                    'location_id': int(location_id),
                    'company_id': int(company_id),
                    'create_date': record.get('create_date'),
                    'from_date': record.get('from_date'),
                    'to_date': record.get('to_date'),
                    'inventory_date': record.get('inventory_date'),
                    'state': record.get('state', 'draft'),
                    'inventory_counting_ids': inventory_counting_lines,
                }
                
                try:
                    start_time = time.time()
                    
                    # Buat inventory stock di target
                    new_inventory_stock_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'inventory.stock', 'create',
                                                                    [inventory_stock_data])
                    
                    # Update flag is_integrated di source
                    self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                            self.source_client.uid, self.source_client.password,
                                            'inventory.stock', 'write',
                                            [[record['id']], {'is_integrated': True}])
                    
                    end_time = time.time()
                    duration = end_time - start_time
                    
                    print(f"Inventory Stock baru telah dibuat dengan ID: {new_inventory_stock_id}")
                    
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Inventory Stock', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Inventory Stock', write_date)
                    
                except Exception as e:
                    error_message = str(e)
                    print(f"Gagal membuat Inventory Stock baru: {error_message}")
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Inventory Stock', error_message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Inventory Stock', error_message, write_date)
            
            # Proses semua inventory stock dengan multithreading
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_inventory_stock, record) for record in inventory_stocks]
                concurrent.futures.wait(futures)
            
            print(f"Transfer {description} selesai.")
            
        except Exception as e:
            print(f"Error pada transfer {description}: {e}")
    
    def transfer_pos_order_invoice_ss_to_mc(self, model_name, fields, description, date_from, date_to):
        try:
            # Fetching the data
            transaksi_posorder_invoice = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    model_name, 'search_read',
                                                                    [[['state', '=', 'invoiced'], ['is_integrated', '=', False], ['create_date', '>=', date_from], ['create_date', '<=', date_to]]],
                                                                    {'fields': fields})

            if not transaksi_posorder_invoice:
                print("Semua transaksi telah diproses.")
                return

            # Pre-fetch all necessary data
            partner_ids = [record.get('partner_id')[0] if isinstance(record.get('partner_id'), list) else record.get('partner_id') for record in transaksi_posorder_invoice]
            session_ids = [record.get('session_id')[0] if isinstance(record.get('session_id'), list) else record.get('session_id') for record in transaksi_posorder_invoice]
            employee_ids = [record.get('employee_id')[0] if isinstance(record.get('employee_id'), list) else record.get('employee_id') for record in transaksi_posorder_invoice]
            pricelist_id = [record.get('pricelist_id')[0] if isinstance(record.get('pricelist_id'), list) else record.get('pricelist_id') for record in transaksi_posorder_invoice]

            # Fetch partners
            partners_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'res.partner', 'search_read',
                                                        [[['id', 'in', partner_ids]]],
                                                        {'fields': ['id', 'id_mc']})
            partners_source_dict = {partner['id']: partner['id_mc'] for partner in partners_source}

            # Fetch sessions
            sessions_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.session', 'search_read',
                                                        [[['id', 'in', session_ids]]],
                                                        {'fields': ['id', 'id_mc']})
            sessions_source_dict = {session['id']: session['id_mc'] for session in sessions_source}

            # Fetch employees
            employees_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'hr.employee', 'search_read',
                                                            [[['id', 'in', employee_ids]]],
                                                            {'fields': ['id', 'id_mc']})
            employees_source_dict = {employee['id']: employee['id_mc'] for employee in employees_source}

                # Fetch pricelist
            pricelist_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'product.pricelist', 'search_read',
                                                            [[['id', 'in', pricelist_id]]],
                                                            {'fields': ['id', 'id_mc']})
            pricelist_source_dict = {pricelist['id']: pricelist['id_mc'] for pricelist in pricelist_source}

            # Pre-fetch all pos.order.line and pos.payment data
            order_ids = [record['id'] for record in transaksi_posorder_invoice]
            pos_order_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.order.line', 'search_read',
                                                        [[['order_id', 'in', order_ids]]],
                                                        {'fields': ['order_id', 'product_id', 'full_product_name', 'qty', 'price_unit', 'tax_ids_after_fiscal_position', 'tax_ids', 'discount', 'price_subtotal', 'price_subtotal_incl']})
            pos_payments = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.payment', 'search_read',
                                                        [[['pos_order_id', 'in', order_ids]]],
                                                        {'fields': ['pos_order_id', 'amount', 'payment_date', 'payment_method_id']})

            # Create dictionaries for quick lookup
            pos_order_lines_dict = {}
            for line in pos_order_lines:
                order_id = line['order_id'][0]
                if order_id not in pos_order_lines_dict:
                    pos_order_lines_dict[order_id] = []
                pos_order_lines_dict[order_id].append(line)

            pos_payments_dict = {}
            for payment in pos_payments:
                order_id = payment['pos_order_id'][0]
                if order_id not in pos_payments_dict:
                    pos_payments_dict[order_id] = []
                pos_payments_dict[order_id].append(payment)

            # Pre-fetch existing pos orders in target
            existing_pos_order_invoice_dict = {}
            for record in transaksi_posorder_invoice:
                existing_pos_order_invoice = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'pos.order', 'search_read',
                                                                        [[['vit_trxid', '=', record.get('name')], ['vit_id', '=', record.get('id')]]],
                                                                        {'fields': ['id'], 'limit': 1})
                if existing_pos_order_invoice:
                    existing_pos_order_invoice_dict[record['id']] = existing_pos_order_invoice[0]['id']

            product_ids = [line['product_id'][0] for line in pos_order_lines if line.get('product_id')]
            product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'product.product', 'search_read',
                                                        [[['id', 'in', product_ids]]],
                                                        {'fields': ['id', 'default_code']})
            product_source_dict = {product['id']: product['default_code'] for product in product_source}

            # Pemetaan ke target_client berdasarkan default_code
            product_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'product.product', 'search_read',
                                                        [[['default_code', 'in', list(product_source_dict.values())]]],
                                                        {'fields': ['id', 'default_code']})
            product_target_dict = {prod['default_code']: prod['id'] for prod in product_target}

            tax_ids = [tax_id for product in pos_order_lines for tax_id in product.get('tax_ids', [])]
            source_taxes = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'account.tax', 'search_read',
                                                        [[['id', 'in', tax_ids]]],
                                                        {'fields': ['id', 'id_mc']})
            source_taxes_dict = {tax['id']: tax['id_mc'] for tax in source_taxes}

            # Pre-fetch payment methods
            payment_method_ids = [payment['payment_method_id'][0] for payment in pos_payments if payment.get('payment_method_id')]
            payment_method_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'pos.payment.method', 'search_read',
                                                                [[['id', 'in', payment_method_ids]]],
                                                                {'fields': ['id', 'id_mc', 'name']})
            payment_method_source_dict = {payment['id']: payment['id_mc'] for payment in payment_method_source}
            pos_order_ids = []
            # Function to process each record
            def process_record(record):
                if record['id'] in existing_pos_order_invoice_dict:
                    print(f"Pos order {record['id']} already exists in target system. Skipping.")
                    return

                pos_order_invoice_lines = pos_order_lines_dict.get(record['id'], [])
                pos_order_invoice_line_ids = []
                pos_order_payment_ids = []
                missing_products = []

                # Check if all products exist in the target database
                for line in pos_order_invoice_lines:
                    source_product_code = product_source_dict.get(line.get('product_id')[0])
                    target_product_id = product_target_dict.get(source_product_code)

                    if not target_product_id:
                        missing_products.append(source_product_code)
                        continue

                    if missing_products:
                        missing_products_str = ", ".join(map(str, missing_products))
                        message = f"Terdapat produk tidak aktif dalam invoice: {missing_products_str}"
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'Invoice', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'Invoice', message, write_date)
                        return

                    tax_ids_mc = [source_taxes_dict.get(tax_id) for tax_id in line.get('tax_ids', []) if tax_id in source_taxes_dict]
                    pos_order_line_data = {
                        'product_id': int(target_product_id),
                        'name': line.get('full_product_name'),
                        'discount': line.get('discount'),
                        'full_product_name': line.get('full_product_name'),
                        'qty': line.get('qty'),
                        'price_unit': line.get('price_unit'),
                        'price_subtotal': line.get('price_subtotal'),
                        'price_subtotal_incl': line.get('price_subtotal_incl'),
                        'tax_ids': [(6, 0, tax_ids_mc)],
                    }
                    pos_order_invoice_line_ids.append((0, 0, pos_order_line_data))

                    # print(pos_order_invoice_line_ids)

                    if missing_products:
                        missing_products_str = ", ".join(map(str, missing_products))
                        message = f"Terdapat produk tidak aktif dalam invoice: {missing_products_str}"
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'Invoice', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'Invoice', message, write_date)
                        return

                # # # Fetch and process payments
                pos_order_payments = pos_payments_dict.get(record['id'], [])
                amount_paid = 0
                for payment in pos_order_payments:
                    amount_paid += payment.get('amount')
                    payment_method_id = payment_method_source_dict.get(payment.get('payment_method_id')[0] if isinstance(payment.get('payment_method_id'), list) else payment.get('payment_method_id'))
                    pos_order_payment_data = {
                        'amount': payment.get('amount'),
                        'payment_date': payment.get('payment_date'),
                        'payment_method_id': int(payment_method_id),
                    }
                    pos_order_payment_ids.append((0, 0, pos_order_payment_data))

                if not pos_order_payment_ids:
                    print(f"Tidak ada pembayaran untuk transaksi dengan ID {record.get('id')}. Tidak membuat dokumen.")
                    message_payment = f"Transaksi tidak memiliki metode pembayaran: {record.get('name')}."
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Invoice', message_payment, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Invoice', message_payment, write_date)
                    return

                partner_id = partners_source_dict.get(record.get('partner_id')[0] if isinstance(record.get('partner_id'), list) else record.get('partner_id'))
                session_id = sessions_source_dict.get(record.get('session_id')[0] if isinstance(record.get('session_id'), list) else record.get('session_id'))
                employee_id = employees_source_dict.get(record.get('employee_id')[0] if isinstance(record.get('employee_id'), list) else record.get('employee_id'))
                pricelist_id = employees_source_dict.get(record.get('pricelist_id')[0] if isinstance(record.get('pricelist_id'), list) else None)

                print(partner_id, session_id, employee_id, pricelist_id)

                if not partner_id or not session_id or not employee_id:
                    missing_fields = []
                    if not partner_id:
                        missing_fields.append(f"partner_id (source ID: {record.get('partner_id')}")
                    if not session_id:
                        missing_fields.append(f"session_id (source ID: {record.get('session_id')}")
                    if not employee_id:
                        missing_fields.append(f"employee_id (source ID: {record.get('employee_id')}")
                    
                    # For pricelist_id, only add if it's required in your data model
                    if not pricelist_id and pricelist_id is not None:
                        missing_fields.append(f"pricelist_id (source ID: {record.get('pricelist_id')}")
                    
                    missing_fields_str = ", ".join(missing_fields)
                    message = f"Tidak dapat membuat pos.order karena data tidak ditemukan: {missing_fields_str}"
                    print(message)
                    
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Invoice', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Invoice', message, write_date)
                    return
                

                pos_order_data = {
                    'name': record.get('name'),
                    'pos_reference': record.get('pos_reference'),
                    # 'pricelist_id': int(pricelist_id) if pricelist_id is not [] else [],  # Set to None if pricelist_id is None
                    'vit_trxid': record.get('name'),
                    'vit_id': record.get('id'),
                    'partner_id': int(partner_id),
                    'session_id': int(session_id),
                    'employee_id': int(employee_id),
                    'date_order': record.get('date_order', False),
                    'amount_tax': record.get('amount_tax'),
                    'amount_total': amount_paid,
                    'amount_paid': amount_paid,
                    'amount_return': record.get('amount_return'),
                    'tracking_number': record.get('tracking_number'),
                    'margin': record.get('margin'),
                    'state': 'paid',
                    'is_integrated': True,
                    'lines': pos_order_invoice_line_ids,
                    'payment_ids': pos_order_payment_ids,
                }

                print(pos_order_data)

                try:
                    start_time = time.time()
                    new_pos_order_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'pos.order', 'create',
                                                                    [pos_order_data])

                    print(f"Pos Order baru telah dibuat dengan ID: {new_pos_order_id}")

                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'pos.order', 'write',
                        [[record['id']], {'is_integrated': True, 'id_mc': new_pos_order_id}]
                    )

                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Invoice', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Invoice', write_date)
                except Exception as e:
                    message_exception = f"Terjadi kesalahan saat membuat invoice: {e}"
                    self.set_log_mc.create_log_note_failed(record, 'Invoice', message_exception, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Invoice', message_exception, write_date)

            # Use ThreadPoolExecutor to process records in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_record, record) for record in transaksi_posorder_invoice]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Error during processing: {e}")

    def transfer_pos_order_invoice_ss_to_mc_session_closed_before_inv(self, model_name, fields, description, date_from, date_to):
        try:
            # Fetching the data
            transaksi_posorder_invoice = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    model_name, 'search_read',
                                                                    [[['state', '=', 'invoiced'], ['is_integrated', '=', False], ['create_date', '>=', date_from], ['create_date', '<=', date_to]]],
                                                                    {'fields': fields})

            if not transaksi_posorder_invoice:
                print("Semua transaksi telah diproses.")
                return

            # Pre-fetch all necessary data
            partner_ids = [record.get('partner_id')[0] if isinstance(record.get('partner_id'), list) else record.get('partner_id') for record in transaksi_posorder_invoice]
            session_ids = [record.get('session_id')[0] if isinstance(record.get('session_id'), list) else record.get('session_id') for record in transaksi_posorder_invoice]
            employee_ids = [record.get('employee_id')[0] if isinstance(record.get('employee_id'), list) else record.get('employee_id') for record in transaksi_posorder_invoice]
            pricelist_id = [record.get('pricelist_id')[0] if isinstance(record.get('pricelist_id'), list) else record.get('pricelist_id') for record in transaksi_posorder_invoice]

            # Fetch partners
            partners_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'res.partner', 'search_read',
                                                        [[['id', 'in', partner_ids]]],
                                                        {'fields': ['id', 'id_mc']})
            partners_source_dict = {partner['id']: partner['id_mc'] for partner in partners_source}

            # Fetch sessions
            sessions_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.session', 'search_read',
                                                        [[['id', 'in', session_ids], ['state', '=', 'closed'], ['is_updated', '=', False]]],
                                                        {'fields': ['id', 'id_mc']})
            sessions_source_dict = {session['id']: session['id_mc'] for session in sessions_source}

            # Fetch employees
            employees_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'hr.employee', 'search_read',
                                                            [[['id', 'in', employee_ids]]],
                                                            {'fields': ['id', 'id_mc']})
            employees_source_dict = {employee['id']: employee['id_mc'] for employee in employees_source}

            # Fetch pricelist
            pricelist_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'product.pricelist', 'search_read',
                                                            [[['id', 'in', pricelist_id]]],
                                                            {'fields': ['id', 'id_mc']})
            pricelist_source_dict = {pricelist['id']: pricelist['id_mc'] for pricelist in pricelist_source}

            # Pre-fetch all pos.order.line and pos.payment data
            order_ids = [record['id'] for record in transaksi_posorder_invoice]
            pos_order_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.order.line', 'search_read',
                                                        [[['order_id', 'in', order_ids]]],
                                                        {'fields': ['order_id', 'product_id', 'full_product_name', 'qty', 'price_unit', 'tax_ids_after_fiscal_position', 'tax_ids', 'discount', 'price_subtotal', 'price_subtotal_incl']})
            pos_payments = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.payment', 'search_read',
                                                        [[['pos_order_id', 'in', order_ids]]],
                                                        {'fields': ['pos_order_id', 'amount', 'payment_date', 'payment_method_id']})

            # Create dictionaries for quick lookup
            pos_order_lines_dict = {}
            for line in pos_order_lines:
                order_id = line['order_id'][0]
                if order_id not in pos_order_lines_dict:
                    pos_order_lines_dict[order_id] = []
                pos_order_lines_dict[order_id].append(line)

            pos_payments_dict = {}
            for payment in pos_payments:
                order_id = payment['pos_order_id'][0]
                if order_id not in pos_payments_dict:
                    pos_payments_dict[order_id] = []
                pos_payments_dict[order_id].append(payment)

            # Pre-fetch existing pos orders in target
            existing_pos_order_invoice_dict = {}
            for record in transaksi_posorder_invoice:
                existing_pos_order_invoice = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'pos.order', 'search_read',
                                                                        [[['vit_trxid', '=', record.get('name')], ['vit_id', '=', record.get('id')]]],
                                                                        {'fields': ['id'], 'limit': 1})
                if existing_pos_order_invoice:
                    existing_pos_order_invoice_dict[record['id']] = existing_pos_order_invoice[0]['id']

            product_ids = [line['product_id'][0] for line in pos_order_lines if line.get('product_id')]
            product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'product.product', 'search_read',
                                                        [[['id', 'in', product_ids]]],
                                                        {'fields': ['id', 'default_code']})
            product_source_dict = {product['id']: product['default_code'] for product in product_source}

            # Pemetaan ke target_client berdasarkan default_code
            product_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'product.product', 'search_read',
                                                        [[['default_code', 'in', list(product_source_dict.values())]]],
                                                        {'fields': ['id', 'default_code']})
            product_target_dict = {prod['default_code']: prod['id'] for prod in product_target}

            tax_ids = [tax_id for product in pos_order_lines for tax_id in product.get('tax_ids', [])]
            source_taxes = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'account.tax', 'search_read',
                                                        [[['id', 'in', tax_ids]]],
                                                        {'fields': ['id', 'id_mc']})
            source_taxes_dict = {tax['id']: tax['id_mc'] for tax in source_taxes}

            # Pre-fetch payment methods
            payment_method_ids = [payment['payment_method_id'][0] for payment in pos_payments if payment.get('payment_method_id')]
            payment_method_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'pos.payment.method', 'search_read',
                                                                [[['id', 'in', payment_method_ids]]],
                                                                {'fields': ['id', 'id_mc', 'name']})
            payment_method_source_dict = {payment['id']: payment['id_mc'] for payment in payment_method_source}
            pos_order_ids = []
            # Function to process each record
            def process_record(record):
                if record['id'] in existing_pos_order_invoice_dict:
                    print(f"Pos order {record['id']} already exists in target system. Skipping.")
                    return

                pos_order_invoice_lines = pos_order_lines_dict.get(record['id'], [])
                pos_order_invoice_line_ids = []
                pos_order_payment_ids = []
                missing_products = []

                # Check if all products exist in the target database
                for line in pos_order_invoice_lines:
                    source_product_code = product_source_dict.get(line.get('product_id')[0])
                    target_product_id = product_target_dict.get(source_product_code)

                    if not target_product_id:
                        missing_products.append(source_product_code)
                        continue

                    if missing_products:
                        missing_products_str = ", ".join(map(str, missing_products))
                        message = f"Terdapat produk tidak aktif dalam invoice: {missing_products_str}"
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'Invoice', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'Invoice', message, write_date)
                        return

                    tax_ids_mc = [source_taxes_dict.get(tax_id) for tax_id in line.get('tax_ids', []) if tax_id in source_taxes_dict]
                    pos_order_line_data = {
                        'product_id': int(target_product_id),
                        'name': line.get('full_product_name'),
                        'discount': line.get('discount'),
                        'full_product_name': line.get('full_product_name'),
                        'qty': line.get('qty'),
                        'price_unit': line.get('price_unit'),
                        'price_subtotal': line.get('price_subtotal'),
                        'price_subtotal_incl': line.get('price_subtotal_incl'),
                        'tax_ids': [(6, 0, tax_ids_mc)],
                    }
                    pos_order_invoice_line_ids.append((0, 0, pos_order_line_data))

                    # print(pos_order_invoice_line_ids)

                    if missing_products:
                        missing_products_str = ", ".join(map(str, missing_products))
                        message = f"Terdapat produk tidak aktif dalam invoice: {missing_products_str}"
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'Invoice', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'Invoice', message, write_date)
                        return

                # # # Fetch and process payments
                pos_order_payments = pos_payments_dict.get(record['id'], [])
                amount_paid = 0
                for payment in pos_order_payments:
                    amount_paid += payment.get('amount')
                    payment_method_id = payment_method_source_dict.get(payment.get('payment_method_id')[0] if isinstance(payment.get('payment_method_id'), list) else payment.get('payment_method_id'))
                    pos_order_payment_data = {
                        'amount': payment.get('amount'),
                        'payment_date': payment.get('payment_date'),
                        'payment_method_id': int(payment_method_id),
                    }
                    pos_order_payment_ids.append((0, 0, pos_order_payment_data))

                if not pos_order_payment_ids:
                    print(f"Tidak ada pembayaran untuk transaksi dengan ID {record.get('id')}. Tidak membuat dokumen.")
                    message_payment = f"Transaksi tidak memiliki metode pembayaran: {record.get('name')}."
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Invoice', message_payment, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Invoice', message_payment, write_date)
                    return

                partner_id = partners_source_dict.get(record.get('partner_id')[0] if isinstance(record.get('partner_id'), list) else record.get('partner_id'))
                session_id = sessions_source_dict.get(record.get('session_id')[0] if isinstance(record.get('session_id'), list) else record.get('session_id'))
                employee_id = employees_source_dict.get(record.get('employee_id')[0] if isinstance(record.get('employee_id'), list) else record.get('employee_id'))
                pricelist_id = pricelist_source_dict.get(record.get('pricelist_id')[0] if isinstance(record.get('pricelist_id'), list) else record.get('pricelist_id'), [])

                print(partner_id, session_id, employee_id, pricelist_id)
                

                pos_order_data = {
                    'name': record.get('name'),
                    'pos_reference': record.get('pos_reference'),
                    # 'pricelist_id': int(pricelist_id) if pricelist_id is not [] else [],  # Set to None if pricelist_id is None
                    'vit_trxid': record.get('name'),
                    'vit_id': record.get('id'),
                    'partner_id': int(partner_id),
                    'session_id': int(session_id),
                    'employee_id': int(employee_id),
                    'date_order': record.get('date_order', False),
                    'amount_tax': record.get('amount_tax'),
                    'amount_total': amount_paid,
                    'amount_paid': amount_paid,
                    'amount_return': record.get('amount_return'),
                    'tracking_number': record.get('tracking_number'),
                    'margin': record.get('margin'),
                    'state': 'paid',
                    'is_integrated': True,
                    'lines': pos_order_invoice_line_ids,
                    'payment_ids': pos_order_payment_ids,
                }

                print(pos_order_data)

                try:
                    start_time = time.time()
                    new_pos_order_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'pos.order', 'create',
                                                                    [pos_order_data])

                    print(f"Pos Order baru telah dibuat dengan ID: {new_pos_order_id}")

                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'pos.order', 'write',
                        [[record['id']], {'is_integrated': True, 'id_mc': new_pos_order_id}]
                    )

                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Invoice', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Invoice', write_date)
                except Exception as e:
                    message_exception = f"Terjadi kesalahan saat membuat invoice: {e}"
                    self.set_log_mc.create_log_note_failed(record, 'Invoice', message_exception, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Invoice', message_exception, write_date)

            # Use ThreadPoolExecutor to process records in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_record, record) for record in transaksi_posorder_invoice]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Error during processing: {e}")

    def transfer_pos_order_invoice_session_closed(self, model_name, fields, description, date_from, date_to):
        try:
            # Fetching the data
            transaksi_posorder_invoice = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    model_name, 'search_read',
                                                                    [[['state', '=', 'invoiced'], ['is_integrated', '=', False], ['create_date', '>=', date_from], ['create_date', '<=', date_to]]],
                                                                    {'fields': fields})

            if not transaksi_posorder_invoice:
                print("Semua transaksi telah diproses.")
                return

            # Pre-fetch all necessary data
            partner_ids = [record.get('partner_id')[0] if isinstance(record.get('partner_id'), list) else record.get('partner_id') for record in transaksi_posorder_invoice]
            session_ids = [record.get('session_id')[0] if isinstance(record.get('session_id'), list) else record.get('session_id') for record in transaksi_posorder_invoice]
            employee_ids = [record.get('employee_id')[0] if isinstance(record.get('employee_id'), list) else record.get('employee_id') for record in transaksi_posorder_invoice]
            pricelist_id = [record.get('pricelist_id')[0] if isinstance(record.get('pricelist_id'), list) else record.get('pricelist_id') for record in transaksi_posorder_invoice]

            # Fetch partners
            partners_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'res.partner', 'search_read',
                                                        [[['id', 'in', partner_ids]]],
                                                        {'fields': ['id', 'id_mc']})
            partners_source_dict = {partner['id']: partner['id_mc'] for partner in partners_source}

            # Fetch sessions
            sessions_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.session', 'search_read',
                                                        [[['id', 'in', session_ids], ['state', '=', 'opened']]],
                                                        {'fields': ['id', 'id_mc']})
            sessions_source_dict = {session['id']: session['id_mc'] for session in sessions_source}

            # Fetch employees
            employees_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'hr.employee', 'search_read',
                                                            [[['id', 'in', employee_ids]]],
                                                            {'fields': ['id', 'id_mc']})
            employees_source_dict = {employee['id']: employee['id_mc'] for employee in employees_source}

            # Fetch pricelist
            pricelist_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'product.pricelist', 'search_read',
                                                            [[['id', 'in', pricelist_id]]],
                                                            {'fields': ['id', 'id_mc']})
            pricelist_source_dict = {pricelist['id']: pricelist['id_mc'] for pricelist in pricelist_source}

            # Pre-fetch all pos.order.line and pos.payment data
            order_ids = [record['id'] for record in transaksi_posorder_invoice]
            pos_order_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.order.line', 'search_read',
                                                        [[['order_id', 'in', order_ids]]],
                                                        {'fields': ['order_id', 'product_id', 'full_product_name', 'qty', 'price_unit', 'tax_ids_after_fiscal_position', 'tax_ids', 'discount', 'price_subtotal', 'price_subtotal_incl']})
            pos_payments = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.payment', 'search_read',
                                                        [[['pos_order_id', 'in', order_ids]]],
                                                        {'fields': ['pos_order_id', 'amount', 'payment_date', 'payment_method_id']})

            # Create dictionaries for quick lookup
            pos_order_lines_dict = {}
            for line in pos_order_lines:
                order_id = line['order_id'][0]
                if order_id not in pos_order_lines_dict:
                    pos_order_lines_dict[order_id] = []
                pos_order_lines_dict[order_id].append(line)

            pos_payments_dict = {}
            for payment in pos_payments:
                order_id = payment['pos_order_id'][0]
                if order_id not in pos_payments_dict:
                    pos_payments_dict[order_id] = []
                pos_payments_dict[order_id].append(payment)

            # Pre-fetch existing pos orders in target
            existing_pos_order_invoice_dict = {}
            for record in transaksi_posorder_invoice:
                existing_pos_order_invoice = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'pos.order', 'search_read',
                                                                        [[['vit_trxid', '=', record.get('name')], ['vit_id', '=', record.get('id')]]],
                                                                        {'fields': ['id'], 'limit': 1})
                if existing_pos_order_invoice:
                    existing_pos_order_invoice_dict[record['id']] = existing_pos_order_invoice[0]['id']

            product_ids = [line['product_id'][0] for line in pos_order_lines if line.get('product_id')]
            product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'product.product', 'search_read',
                                                        [[['id', 'in', product_ids]]],
                                                        {'fields': ['id', 'default_code']})
            product_source_dict = {product['id']: product['default_code'] for product in product_source}

            # Pemetaan ke target_client berdasarkan default_code
            product_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'product.product', 'search_read',
                                                        [[['default_code', 'in', list(product_source_dict.values())]]],
                                                        {'fields': ['id', 'default_code']})
            product_target_dict = {prod['default_code']: prod['id'] for prod in product_target}

            tax_ids = [tax_id for product in pos_order_lines for tax_id in product.get('tax_ids', [])]
            source_taxes = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'account.tax', 'search_read',
                                                        [[['id', 'in', tax_ids]]],
                                                        {'fields': ['id', 'id_mc']})
            source_taxes_dict = {tax['id']: tax['id_mc'] for tax in source_taxes}

            # Pre-fetch payment methods
            payment_method_ids = [payment['payment_method_id'][0] for payment in pos_payments if payment.get('payment_method_id')]
            payment_method_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'pos.payment.method', 'search_read',
                                                                [[['id', 'in', payment_method_ids]]],
                                                                {'fields': ['id', 'id_mc', 'name']})
            payment_method_source_dict = {payment['id']: payment['id_mc'] for payment in payment_method_source}
            pos_order_ids = []
            # Function to process each record
            def process_record(record):
                if record['id'] in existing_pos_order_invoice_dict:
                    print(f"Pos order {record['id']} already exists in target system. Skipping.")
                    return

                pos_order_invoice_lines = pos_order_lines_dict.get(record['id'], [])
                pos_order_invoice_line_ids = []
                pos_order_payment_ids = []
                missing_products = []

                # Check if all products exist in the target database
                for line in pos_order_invoice_lines:
                    source_product_code = product_source_dict.get(line.get('product_id')[0])
                    target_product_id = product_target_dict.get(source_product_code)

                    if not target_product_id:
                        missing_products.append(source_product_code)
                        continue

                    if missing_products:
                        missing_products_str = ", ".join(map(str, missing_products))
                        message = f"Terdapat produk tidak aktif dalam invoice: {missing_products_str}"
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'Invoice', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'Invoice', message, write_date)
                        return

                    tax_ids_mc = [source_taxes_dict.get(tax_id) for tax_id in line.get('tax_ids', []) if tax_id in source_taxes_dict]
                    pos_order_line_data = {
                        'product_id': int(target_product_id),
                        'name': line.get('full_product_name'),
                        'discount': line.get('discount'),
                        'full_product_name': line.get('full_product_name'),
                        'qty': line.get('qty'),
                        'price_unit': line.get('price_unit'),
                        'price_subtotal': line.get('price_subtotal'),
                        'price_subtotal_incl': line.get('price_subtotal_incl'),
                        'tax_ids': [(6, 0, tax_ids_mc)],
                    }
                    pos_order_invoice_line_ids.append((0, 0, pos_order_line_data))

                    # print(pos_order_invoice_line_ids)

                    if missing_products:
                        missing_products_str = ", ".join(map(str, missing_products))
                        message = f"Terdapat produk tidak aktif dalam invoice: {missing_products_str}"
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'Invoice', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'Invoice', message, write_date)
                        return

                # # # Fetch and process payments
                pos_order_payments = pos_payments_dict.get(record['id'], [])
                amount_paid = 0
                for payment in pos_order_payments:
                    amount_paid += payment.get('amount')
                    payment_method_id = payment_method_source_dict.get(payment.get('payment_method_id')[0] if isinstance(payment.get('payment_method_id'), list) else payment.get('payment_method_id'))
                    pos_order_payment_data = {
                        'amount': payment.get('amount'),
                        'payment_date': payment.get('payment_date'),
                        'payment_method_id': int(payment_method_id),
                    }
                    pos_order_payment_ids.append((0, 0, pos_order_payment_data))

                if not pos_order_payment_ids:
                    print(f"Tidak ada pembayaran untuk transaksi dengan ID {record.get('id')}. Tidak membuat dokumen.")
                    message_payment = f"Transaksi tidak memiliki metode pembayaran: {record.get('name')}."
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Invoice', message_payment, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Invoice', message_payment, write_date)
                    return

                partner_id = partners_source_dict.get(record.get('partner_id')[0] if isinstance(record.get('partner_id'), list) else record.get('partner_id'))
                session_id = sessions_source_dict.get(record.get('session_id')[0] if isinstance(record.get('session_id'), list) else record.get('session_id'))
                employee_id = employees_source_dict.get(record.get('employee_id')[0] if isinstance(record.get('employee_id'), list) else record.get('employee_id'))
                pricelist_id = pricelist_source_dict.get(record.get('pricelist_id')[0] if isinstance(record.get('pricelist_id'), list) else record.get('pricelist_id'), [])

                print(partner_id, session_id, employee_id, pricelist_id)
                

                pos_order_data = {
                    'name': record.get('name'),
                    'pos_reference': record.get('pos_reference'),
                    # 'pricelist_id': int(pricelist_id) if pricelist_id is not [] else [],  # Set to None if pricelist_id is None
                    'vit_trxid': record.get('name'),
                    'vit_id': record.get('id'),
                    'partner_id': int(partner_id),
                    'session_id': int(session_id),
                    'employee_id': int(employee_id),
                    'date_order': record.get('date_order', False),
                    'amount_tax': record.get('amount_tax'),
                    'amount_total': amount_paid,
                    'amount_paid': amount_paid,
                    'amount_return': record.get('amount_return'),
                    'tracking_number': record.get('tracking_number'),
                    'margin': record.get('margin'),
                    'state': 'paid',
                    'is_integrated': True,
                    'lines': pos_order_invoice_line_ids,
                    'payment_ids': pos_order_payment_ids,
                }

                print(pos_order_data)

                try:
                    start_time = time.time()
                    new_pos_order_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'pos.order', 'create',
                                                                    [pos_order_data])

                    print(f"Pos Order baru telah dibuat dengan ID: {new_pos_order_id}")

                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'pos.order', 'write',
                        [[record['id']], {'is_integrated': True, 'id_mc': new_pos_order_id}]
                    )

                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Invoice', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Invoice', write_date)
                except Exception as e:
                    message_exception = f"Terjadi kesalahan saat membuat invoice: {e}"
                    self.set_log_mc.create_log_note_failed(record, 'Invoice', message_exception, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Invoice', message_exception, write_date)

            # Use ThreadPoolExecutor to process records in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_record, record) for record in transaksi_posorder_invoice]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Error during processing: {e}")

    def transfer_pos_order_session(self, model_name, fields, description, date_from, date_to):
        try:
            # Ambil data dari sumber dengan offset dan limit
            transaksi_posorder_session = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    model_name, 'search_read',
                                                                    [[['state', '=', 'closed']]],
                                                                    {'fields': fields})

            if not transaksi_posorder_session:
                print("Semua transaksi telah diproses.")
                return

            # Pre-fetch all necessary data
            config_ids = [record.get('config_id')[0] if isinstance(record.get('config_id'), list) else record.get('config_id') for record in transaksi_posorder_session]
            user_ids = [record.get('user_id')[0] if isinstance(record.get('user_id'), list) else record.get('user_id') for record in transaksi_posorder_session]

            # Fetch pos.config
            config_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.config', 'search_read',
                                                        [[['id', 'in', config_ids]]],
                                                        {'fields': ['id', 'id_mc']})
            config_source_dict = {config['id']: config['id_mc'] for config in config_source}

            # Fetch res.users
            user_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'res.users', 'search_read',
                                                        [[['id', 'in', user_ids]]],
                                                        {'fields': ['id', 'id_mc']})
            user_source_dict = {user['id']: user['id_mc'] for user in user_source}

            # Pre-fetch existing pos sessions in target
            existing_pos_order_session_dict = {}
            for record in transaksi_posorder_session:
                existing_pos_order_session = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'pos.session', 'search_read',
                                                                        [[['name_session_pos', '=', record.get('name')]]],
                                                                        {'fields': ['id'], 'limit': 1})
                if existing_pos_order_session:
                    existing_pos_order_session_dict[record['id']] = existing_pos_order_session[0]['id']

            def process_record(record):
                if record['id'] in existing_pos_order_session_dict:
                    return

                cash_register_balance_start = record.get('cash_register_balance_start')
                cash_register_balance_end_real = record.get('cash_register_balance_end_real')

                # Debugging prints
                print(f"Cash Register Balance Start: {cash_register_balance_start}")
                print(f"Cash Register Balance End Real: {cash_register_balance_end_real}")

                # Ensure monetary values are properly handled
                cash_register_balance_start = float(cash_register_balance_start) if cash_register_balance_start else 0.0
                cash_register_balance_end_real = float(cash_register_balance_end_real) if cash_register_balance_end_real else 0.0

                config_id = config_source_dict.get(record.get('config_id')[0] if isinstance(record.get('config_id'), list) else record.get('config_id'))
                user_id = user_source_dict.get(record.get('user_id')[0] if isinstance(record.get('user_id'), list) else record.get('user_id'))

                if config_id is None or user_id is None:
                    print(f"Data tidak lengkap untuk transaksi dengan ID {record.get('id')}. Tidak membuat dokumen.")
                    return

                pos_session_data = {
                    'name_session_pos': record.get('name'),
                    'config_id': int(config_id),
                    'user_id': 2,
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
                    
                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'pos.session', 'write',
                        [[record['id']], {'id_mc': new_session_pos_order_id}]
                    )
                    
                    end_time = time.time()
                    duration = end_time - start_time

                    # write_date = self.get_write_date(model_name, record['id'])
                    # self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'POS Session', write_date)
                    # self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'POS Session', write_date)

                except Exception as e:
                    message_exception = f"Terjadi kesalahan saat membuat pos order baru: {e}"
                    # self.set_log_mc.create_log_note_failed(record, 'POS Session', message_exception, write_date)
                    # self.set_log_ss.create_log_note_failed(record, 'POS Session', message_exception, write_date)

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_record, record) for record in transaksi_posorder_session]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Terjadi kesalahan saat memproses batch: {e}")
            return

    
    def transfer_warehouse_master(self, model_name, fields, description, date_from, date_to):
        # Ambil data dari sumber
        transaksi_warehouse = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        model_name, 'search_read',
                                                        [[]],
                                                        {'fields': fields})
        
        # print(transaksi_warehouse)

        if not transaksi_warehouse:
            print("Tidak ada master yang ditemukan untuk ditransfer.")
            return
        
        location_transit_ids = [record.get('location_transit')[0] if isinstance(record.get('location_transit'), list) else record.get('location_transit') for record in transaksi_warehouse]
        # print(location_ids)

        location_transit = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'stock.location', 'search_read',
                                                        [[['id', 'in', location_transit_ids]]],
                                                        {'fields': ['id', 'complete_name']})
        
        location_transit_name_dict = {record['id']: record['complete_name'] for record in location_transit}
        location_transit_id_dict = {record['id']: record['id'] for record in location_transit}
        
        lot_stock_ids = [record.get('lot_stock_id')[0] if isinstance(record.get('lot_stock_id'), list) else record.get('lot_stock_id') for record in transaksi_warehouse]
        lot_stock_source = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'stock.location', 'search_read',
                                                        [[['id', 'in', lot_stock_ids]]],
                                                        {'fields': ['id','complete_name']})
        
        lot_stock_source_dict = {record['id']: record['complete_name'] for record in lot_stock_source}
        lot_stock_id_dict = {record['id']: record['id'] for record in lot_stock_source}

        # # Kirim data ke target
        for record in transaksi_warehouse:
            warehouse_name = record.get('name', False)
            warehouse_code = record.get('complete_name', False)
            
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

                location_id = location_transit_name_dict.get(record.get('location_transit')[0] if isinstance(record.get('location_transit'), list) else record.get('location_transit'), False)
                lot_transit = location_transit_id_dict.get(record.get('location_transit')[0] if isinstance(record.get('location_transit'), list) else record.get('location_transit'), False)
                lot_stock_id = lot_stock_source_dict.get(record.get('lot_stock_id')[0] if isinstance(record.get('lot_stock_id'), list) else record.get('lot_stock_id'), False)
                lot_id = lot_stock_id_dict.get(record.get('lot_stock_id')[0] if isinstance(record.get('lot_stock_id'), list) else record.get('lot_stock_id'), False)

                warehouse_data = {
                    'warehouse_name': warehouse_name,
                    'warehouse_code': str(lot_stock_id),
                    'warehouse_transit': str(location_id),
                    'id_mc_location': str(lot_id),
                    'id_mc_transit': str(lot_transit),
                    # 'warehouse_company': [(6, 0, [1])]  # Many2many relation expects a list of IDs
                }
                try:
                    start_time = time.time()
                    new_master_warehouse = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'master.warehouse', 'create',
                                                                        [warehouse_data])
                    print(f"Warehouse baru telah dibuat dengan ID: {new_master_warehouse}")

                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Warehouse UDT', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Warehouse UDT', write_date)
                except Exception as e:
                    message_exception = f"An error occurred while creating warehouse: {e}"
                    self.set_log_mc.create_log_note_failed(record, 'Warehouse UDT', message_exception, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Warehouse UDT', message_exception, write_date)

    def transfer_TSOUT_NEW(self, model_name, fields, description, date_from, date_to):
        try:
            # Ambil data dari sumber
            Ts_Out_data_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            model_name, 'search_read',
                                                            [[['picking_type_id.name', '=', 'TS Out'], ['is_integrated', '=', False], ['state', '=', 'done'], ['create_date', '>=', date_from], ['create_date', '<=', date_to]]],
                                                            {'fields': fields})

            if not Ts_Out_data_source:
                print("Semua transaksi telah diproses.")
                return

            target_location_ids = [record.get('target_location')[0] if isinstance(record.get('target_location'), list) else record.get('target_location') for record in Ts_Out_data_source]
            location_ids = [record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id') for record in Ts_Out_data_source]
            picking_type_ids = [record.get('picking_type_id')[0] if isinstance(record.get('picking_type_id'), list) else record.get('picking_type_id') for record in Ts_Out_data_source]

            target_location_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'master.warehouse', 'search_read',
                                                                [[['id', 'in', target_location_ids]]],
                                                                {'fields': ['id', 'id_mc_location', 'id_mc_transit', 'warehouse_name', 'warehouse_code', 'warehouse_transit'], 'limit': 1})
            target_location_source_dict = {target['id']: target['id_mc_location'] for target in target_location_source}
            transit_location_id_dict = {target['id']: target['id_mc_transit'] for target in target_location_source}
            target_location_name_dict = {target['id']: target['warehouse_name'] for target in target_location_source}
            
            location_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'stock.location', 'search_read',
                                                        [[['id', 'in', location_ids]]],
                                                        {'fields': ['id', 'id_mc'] , 'limit': 1})
            location_source_dict = {location['id']: location['id_mc'] for location in location_source}

            picking_type_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.picking.type', 'search_read',
                                                            [[['id', 'in', picking_type_ids]]],
                                                            {'fields': ['id', 'id_mc'] , 'limit': 1})
            picking_type_source_dict = {type['id']: type['id_mc'] for type in picking_type_source}

            picking_ids = [record['id'] for record in Ts_Out_data_source]
            tsout_transfer_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', 'in', picking_ids]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

            existing_ts_out_invoice_dict = {}
            for record in Ts_Out_data_source:
                location_id_value = location_source_dict.get(record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id'))
                existing_ts_out = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.picking', 'search_read',
                                                            [[['vit_trxid', '=', record.get('name')], ['location_id', '=', location_id_value]]],
                                                            {'fields': ['id'], 'limit': 1})
                if existing_ts_out:
                    existing_ts_out_invoice_dict[record['id']] = existing_ts_out[0]['id']

            tsout_transfer_inventory_lines_dict = {}
            for line in tsout_transfer_inventory_lines:
                if 'picking_id' in line:
                    picking_id = line['picking_id'][0]
                    if picking_id not in tsout_transfer_inventory_lines_dict:
                        tsout_transfer_inventory_lines_dict[picking_id] = []
                    tsout_transfer_inventory_lines_dict[picking_id].append(line)

            product_ids = [line['product_id'][0] for line in tsout_transfer_inventory_lines if line.get('product_id')]
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
                                                                            'product.product', 'search_read',
                                                                            [[['default_code', 'in', list(default_code_to_product_tmpl_id.keys())]]],
                                                                            {'fields': ['id', 'default_code']})

            # Step 5: Create a mapping from default_code to id in target_client
            default_code_to_target_id = {template['default_code']: template['id'] for template in product_template_target_source}

            def process_record(record):
                try:
                    if record['id'] in existing_ts_out_invoice_dict:
                        return
                
                    tsout_transfer_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                                self.source_client.uid, self.source_client.password,
                                                                                'stock.move', 'search_read',
                                                                                [[['picking_id', '=', record['id']]]],
                                                                                {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

                    location_id = location_source_dict.get(record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id'))
                    target_location = target_location_source_dict.get(record.get('target_location')[0] if isinstance(record.get('target_location'), list) else record.get('target_location'))
                    target_location_name = target_location_name_dict.get(record.get('target_location')[0] if isinstance(record.get('target_location'), list) else record.get('target_location'))
                    picking_type_id = picking_type_source_dict.get(record.get('picking_type_id')[0] if isinstance(record.get('picking_type_id'), list) else record.get('picking_type_id'))
                    transit_location_id = transit_location_id_dict.get(record.get('target_location')[0] if isinstance(record.get('target_location'), list) else record.get('target_location'))

                    if not location_id or not target_location or not picking_type_id or not transit_location_id:
                        print(f"Missing required data for record ID {record['id']}. Skipping.")
                        return

                    missing_products = []
                    tsout_transfer_inventory_line_ids = []
                    tsin_transfer_inventory_line_ids = []
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
                            'location_dest_id': int(transit_location_id),
                            'location_id': int(location_id)
                        }
                        tsout_transfer_inventory_line_ids.append((0, 0, tsout_transfer_inventory_line_data))

                        tsin_transfer_inventory_line_data = {
                            'product_id': int(target_product_template_id),
                            'product_uom_qty': line.get('product_uom_qty'),
                            'name': line.get('name'),
                            'quantity': line.get('quantity'),
                            'location_dest_id': int(target_location),
                            'location_id': int(transit_location_id),
                        }
                        tsin_transfer_inventory_line_ids.append((0, 0, tsin_transfer_inventory_line_data))

                    if should_skip_create:
                        missing_products_str = ", ".join(missing_products)
                        message = f"Terdapat produk tidak aktif dalam TS Out/TS In: {missing_products_str}"
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'TS Out/TS In', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'TS Out/TS In', message, write_date)
                        return

                    tsout_transfer_data = {
                        'scheduled_date': record.get('scheduled_date', False),
                        'date_done': record.get('date_done', False),
                        'location_id': int(location_id),
                        'location_dest_id': int(transit_location_id),
                        'target_location': target_location_name,
                        'picking_type_id': int(picking_type_id),
                        # 'is_integrated': True,
                        'vit_trxid': record.get('name', False),
                        'move_ids_without_package': tsout_transfer_inventory_line_ids,
                    }

                    new_tsout_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.picking', 'create',
                                                                [tsout_transfer_data])
                    print(f"TS Out baru telah dibuat di target dengan ID: {new_tsout_id}")

                    self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'stock.picking', 'button_validate',
                                                [[new_tsout_id]])
                    print(f"TS Out dengan ID {new_tsout_id} telah divalidasi.")

                    self.source_client.call_odoo(
                        'object', 'execute_kw', self.source_client.db,
                        self.source_client.uid, self.source_client.password,
                        'stock.picking', 'write',
                        [[record['id']], {'is_integrated': True, 'vit_trxid': record['name']}]
                    )

                    picking_type_name_ts_in = 'TS In'
                    picking_types_ts_in = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking.type', 'search_read',
                                                                    [[['name', '=', picking_type_name_ts_in]]],
                                                                    {'fields': ['id'], 'limit': 1})

                    if not picking_types_ts_in:
                        print(f"Tidak dapat menemukan tipe picking dengan nama '{picking_type_name_ts_in}' di database target.")
                        return

                    picking_type_id_ts_in = picking_types_ts_in[0]['id']

                    ts_in_transfer_data = {
                        'scheduled_date': record.get('scheduled_date', False),
                        'date_done': record.get('date_done', False),
                        'location_id': int(transit_location_id),
                        'location_dest_id': int(target_location),
                        'origin': record.get('name', False),
                        'picking_type_id': picking_type_id_ts_in,
                        'move_ids_without_package': tsin_transfer_inventory_line_ids,
                    }

                    # print(ts_in_transfer_data)
                    start_time = time.time()
                    new_ts_in_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.picking', 'create',
                                                                [ts_in_transfer_data])
                    print(f"TS In baru telah dibuat di target dengan ID: {new_ts_in_id}")

                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'TS Out/TS In', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'TS Out/TS In', write_date)
                except Exception as e:
                    message_exception = f"Gagal membuat atau memposting TS In di Source baru: {e}"
                    self.set_log_mc.create_log_note_failed(record, 'TS Out/TS In', message_exception, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'TS Out/TS In', message_exception, write_date)

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_record, record) for record in Ts_Out_data_source]
                concurrent.futures.wait(futures)
        except Exception as e:
            print(f"Gagal membuat atau memposting TS Out di Source baru: {e}")

    def validate_goods_receipts_mc(self, model_name, fields, description):
        # Retrieve TS Out records that match the specified criteria from the source database
        goods_receipts_validates = self.source_client.call_odoo(
            'object', 'execute_kw', 
            self.source_client.db, self.source_client.uid, self.source_client.password,
            'stock.picking', 'search_read',
            [[
                ['picking_type_id.name', '=', 'Goods Receipts'], 
                ['is_integrated', '=', True], 
                ['state', '=', 'assigned'],
            ]],
            {'fields': ['id', 'name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'vit_trxid', 'move_ids_without_package']}
        )

        # Check if any TS Out records are found
        if not goods_receipts_validates:
            print("Tidak ada Goods Receipts yang ditemukan di source.")
            return  # Exit if no records found

        # Process only the first 100 records
        for gr in goods_receipts_validates:
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
                self.set_log_mc.create_log_note_failed(gr, 'Goods Receipts', message_exception, write_date)
                self.set_log_ss.create_log_note_failed(gr, 'Goods Receipts', message_exception, write_date)

    def validate_goods_receipts_store(self, model_name, fields, description, date_from, date_to):
        # Retrieve TS Out records that match the specified criteria from the source database
        goods_receipts_validates = self.source_client.call_odoo(
            'object', 'execute_kw', 
            self.source_client.db, self.source_client.uid, self.source_client.password,
            'stock.picking', 'search_read',
            [[
                ['picking_type_id.name', '=', 'Goods Receipts'], 
                ['is_integrated', '=', True], 
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
                        self.set_log_mc.create_log_note_failed(gr, 'Goods Receipts', message_exception, write_date)
                        self.set_log_ss.create_log_note_failed(gr, 'Goods Receipts', message_exception, write_date)

    def validate_goods_issue_store(self, model_name, fields, description, date_from, date_to):
        # Retrieve TS Out records that match the specified criteria from the source database
        goods_issue_validates = self.source_client.call_odoo(
            'object', 'execute_kw', 
            self.source_client.db, self.source_client.uid, self.source_client.password,
            'stock.picking', 'search_read',
            [[
                ['picking_type_id.name', '=', 'Goods Issue'], 
                ['is_integrated', '=', True], 
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
                        self.set_log_mc.create_log_note_failed(gi, 'Goods Issue', message_exception, write_date)
                        self.set_log_ss.create_log_note_failed(gi, 'Goods Issue', message_exception, write_date)

    def validate_GRPO(self, model_name, fields, description, date_from, date_to):
        # Retrieve GRPO records that match the specified criteria from the source database
        GRPO_validates = self.source_client.call_odoo(
            'object', 'execute_kw', 
            self.source_client.db, self.source_client.uid, self.source_client.password,
            'stock.picking', 'search_read',
            [[
                ['picking_type_id.name', '=', 'GRPO'], 
                ['is_integrated', '=', True], 
                ['is_updated', '=', False],
                ['write_date', '>=', date_from], ['write_date', '<=', date_to],
                ['state', '=', 'done'],
            ]],
            {'fields': ['id', 'name', 'move_ids_without_package', 'vit_trxid']}
        )

        # Check if any GRPO records are found
        if not GRPO_validates:
            print("Tidak ada GRPO yang ditemukan di target.")
            return

        # Collect all move_ids and vit_trxids
        all_source_move_ids = []
        all_vit_trxids = []
        for res in GRPO_validates:
            all_source_move_ids.extend(res['move_ids_without_package'])
            all_vit_trxids.append(res.get('vit_trxid', False))

        # Fetch all source move lines in one call
        source_move_lines = self.source_client.call_odoo(
            'object', 'execute_kw',
            self.source_client.db, self.source_client.uid, self.source_client.password,
            'stock.move', 'read',
            [all_source_move_ids],
            {'fields': ['id', 'product_id', 'quantity']}
        )

        # Fetch all source product codes in one call
        source_product_ids = list(set([move['product_id'][0] for move in source_move_lines]))
        source_products = self.source_client.call_odoo(
            'object', 'execute_kw',
            self.source_client.db, self.source_client.uid, self.source_client.password,
            'product.product', 'search_read',
            [[('id', 'in', source_product_ids)]],
            {'fields': ['id', 'default_code']}
        )
        source_product_dict = {product['id']: product['default_code'] for product in source_products}

        # Fetch all target GRPO records that need validation
        GRPO_needs_validate = self.target_client.call_odoo(
            'object', 'execute_kw', 
            self.target_client.db, self.target_client.uid, self.target_client.password,
            'stock.picking', 'search_read',
            [[
                ['picking_type_id.name', '=', 'GRPO'], 
                ['vit_trxid', 'in', all_vit_trxids], 
                ['is_integrated', '=', False], 
                ['state', '=', 'assigned']
            ]],
            {'fields': ['id', 'move_ids_without_package', 'vit_trxid']}
        )

        # Create a dictionary to map vit_trxid to target GRPO records
        target_grpo_dict = {grpo['vit_trxid']: grpo for grpo in GRPO_needs_validate}

        for res in GRPO_validates:
            vit_trxid = res.get('vit_trxid', False)
            target_grpo = target_grpo_dict.get(vit_trxid)

            if not target_grpo:
                continue

            try:
                # Get source move lines for this GRPO
                grpo_source_move_lines = [move for move in source_move_lines if move['id'] in res['move_ids_without_package']]
                
                # Get source product codes and quantities
                source_product_info = {
                    source_product_dict.get(move['product_id'][0]): {
                        'product_id': move['product_id'][0],
                        'quantity': move['quantity']
                    }
                    for move in grpo_source_move_lines
                }
                source_codes_set = set(source_product_info.keys())

                # Get target move_ids
                target_move_ids = target_grpo['move_ids_without_package']

                # Fetch all target move lines in one call
                target_move_lines = self.target_client.call_odoo(
                    'object', 'execute_kw',
                    self.target_client.db, self.target_client.uid, self.target_client.password,
                    'stock.move', 'read',
                    [target_move_ids],
                    {'fields': ['id', 'product_id', 'quantity']}
                )

                # Fetch all target product codes in one call
                target_product_ids = [move['product_id'][0] for move in target_move_lines]
                target_products = self.target_client.call_odoo(
                    'object', 'execute_kw',
                    self.target_client.db, self.target_client.uid, self.target_client.password,
                    'product.product', 'search_read',
                    [[('id', 'in', target_product_ids)]],
                    {'fields': ['id', 'default_code']}
                )
                target_product_dict = {product['id']: product['default_code'] for product in target_products}

                moves_to_remove = []
                moves_to_update = []

                for target_move in target_move_lines:
                    target_product_id = target_move['product_id'][0]
                    target_product_code = target_product_dict.get(target_product_id)
                    
                    if target_product_code not in source_codes_set:
                        moves_to_remove.append(target_move['id'])
                    else:
                        source_quantity = source_product_info[target_product_code]['quantity']
                        target_quantity = target_move['quantity']
                        
                        if source_quantity != target_quantity:
                            moves_to_update.append((1, target_move['id'], {
                                'quantity': source_quantity
                            }))

                if moves_to_remove:
                    # Delete the target move lines for products that no longer exist in source
                    self.target_client.call_odoo(
                        'object', 'execute_kw',
                        self.target_client.db, self.target_client.uid, self.target_client.password,
                        'stock.move', 'unlink',
                        [moves_to_remove]
                    )
                    print(f"Deleted {len(moves_to_remove)} products from target_client for GRPO {target_grpo['id']}")

                if moves_to_update:
                    # Update the quantities of the target move lines
                    self.target_client.call_odoo(
                        'object', 'execute_kw',
                        self.target_client.db, self.target_client.uid, self.target_client.password,
                        'stock.picking', 'write',
                        [[target_grpo['id']], {'move_ids_without_package': moves_to_update}]
                    )
                    print(f"Updated quantities for {len(moves_to_update)} products in target_client for GRPO {target_grpo['id']}")

            
                self.target_client.call_odoo(
                    'object', 'execute_kw',
                    self.target_client.db, self.target_client.uid, self.target_client.password,
                    'stock.picking', 'write',
                    [[target_grpo['id']], {'is_integrated': True, 'is_closed': True}]
                )

                self.target_client.call_odoo(
                    'object', 'execute_kw',
                    self.target_client.db, self.target_client.uid, self.target_client.password,
                    'stock.picking', 'button_validate',
                    [[target_grpo['id']]]
                )
                print(f"Validated GRPO {target_grpo['id']} in target_client.")

                # Mark the GRPO as updated in the source system
                self.source_client.call_odoo(
                    'object', 'execute_kw',
                    self.source_client.db, self.source_client.uid, self.source_client.password,
                    'stock.picking', 'write',
                    [[res['id']], {'is_updated': True}]
                )

                message_success = f"Successfully validated and updated GRPO {target_grpo['id']}"
                self.set_log_mc.create_log_note_success(res, 'GRPO', message_success, write_date)
                self.set_log_ss.create_log_note_success(res, 'GRPO', message_success, write_date)
                write_date = self.get_write_date(model_name, res['id'])

            except Exception as e:
                message_exception = f"Failed to validate and update GRPO {target_grpo['id']}: {e}"
                # self.set_log_mc.create_log_note_failed(res, 'GRPO', message_exception, write_date)
                # self.set_log_ss.create_log_note_failed(res, 'GRPO', message_exception, write_date)

        print("GRPO validation and quantity update completed.")

    def transfer_internal_transfers_ss_to_mc(self, model_name, fields, description, date_from, date_to):
        try:
            # Ambil data dari sumber
            Ts_Out_data_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            model_name, 'search_read',
                                                            [[['picking_type_id.name', '=', 'Internal Transfers'], ['is_integrated', '=', False], ['state', '=', 'done'], ['create_date', '>=', date_from], ['create_date', '<=', date_to]]],
                                                            {'fields': fields})

            if not Ts_Out_data_source:
                print("Semua transaksi telah diproses.")
                return

            target_location_ids = [record.get('target_location')[0] if isinstance(record.get('target_location'), list) else record.get('target_location') for record in Ts_Out_data_source]
            location_ids = [record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id') for record in Ts_Out_data_source]
            picking_type_ids = [record.get('picking_type_id')[0] if isinstance(record.get('picking_type_id'), list) else record.get('picking_type_id') for record in Ts_Out_data_source]

            target_location_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'master.warehouse', 'search_read',
                                                                [[['id', 'in', target_location_ids]]],
                                                                {'fields': ['id', 'id_mc_location', 'id_mc_transit', 'warehouse_name', 'warehouse_code', 'warehouse_transit'], 'limit': 1})
            target_location_source_dict = {target['id']: target['id_mc_location'] for target in target_location_source}
            transit_location_id_dict = {target['id']: target['id_mc_transit'] for target in target_location_source}
            target_location_name_dict = {target['id']: target['warehouse_name'] for target in target_location_source}
            
            location_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'stock.location', 'search_read',
                                                        [[['id', 'in', location_ids]]],
                                                        {'fields': ['id', 'id_mc'] , 'limit': 1})
            location_source_dict = {location['id']: location['id_mc'] for location in location_source}

            picking_type_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.picking.type', 'search_read',
                                                            [[['id', 'in', picking_type_ids]]],
                                                            {'fields': ['id', 'id_mc'] , 'limit': 1})
            picking_type_source_dict = {type['id']: type['id_mc'] for type in picking_type_source}

            picking_ids = [record['id'] for record in Ts_Out_data_source]
            tsout_transfer_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', 'in', picking_ids]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

            existing_ts_out_invoice_dict = {}
            for record in Ts_Out_data_source:
                location_id_value = location_source_dict.get(record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id'))
                existing_ts_out = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.picking', 'search_read',
                                                            [[['vit_trxid', '=', record.get('name')], ['location_id', '=', location_id_value]]],
                                                            {'fields': ['id'], 'limit': 1})
                if existing_ts_out:
                    existing_ts_out_invoice_dict[record['id']] = existing_ts_out[0]['id']

            tsout_transfer_inventory_lines_dict = {}
            for line in tsout_transfer_inventory_lines:
                if 'picking_id' in line:
                    picking_id = line['picking_id'][0]
                    if picking_id not in tsout_transfer_inventory_lines_dict:
                        tsout_transfer_inventory_lines_dict[picking_id] = []
                    tsout_transfer_inventory_lines_dict[picking_id].append(line)

            product_ids = [line['product_id'][0] for line in tsout_transfer_inventory_lines if line.get('product_id')]
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

            def process_record(record):
                try:
                    if record['id'] in existing_ts_out_invoice_dict:
                        return
                
                    tsout_transfer_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                                self.source_client.uid, self.source_client.password,
                                                                                'stock.move', 'search_read',
                                                                                [[['picking_id', '=', record['id']]]],
                                                                                {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

                    location_id = location_source_dict.get(record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id'))
                    target_location = target_location_source_dict.get(record.get('target_location')[0] if isinstance(record.get('target_location'), list) else record.get('target_location'))
                    target_location_name = target_location_name_dict.get(record.get('target_location')[0] if isinstance(record.get('target_location'), list) else record.get('target_location'))
                    picking_type_id = picking_type_source_dict.get(record.get('picking_type_id')[0] if isinstance(record.get('picking_type_id'), list) else record.get('picking_type_id'))
                    transit_location_id = target_location_source_dict.get(record.get('target_location')[0] if isinstance(record.get('target_location'), list) else record.get('target_location'))

                    if not location_id or not target_location or not picking_type_id or not transit_location_id:
                        print(f"Missing required data for record ID {record['id']}. Skipping.")
                        return

                    missing_products = []
                    tsout_transfer_inventory_line_ids = []
                    tsin_transfer_inventory_line_ids = []
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
                            'location_dest_id': int(transit_location_id),
                            'location_id': int(location_id)
                        }
                        tsout_transfer_inventory_line_ids.append((0, 0, tsout_transfer_inventory_line_data))

                        tsin_transfer_inventory_line_data = {
                            'product_id': int(target_product_template_id),
                            'product_uom_qty': line.get('product_uom_qty'),
                            'name': line.get('name'),
                            'quantity': line.get('quantity'),
                            'location_dest_id': int(target_location),
                            'location_id': int(transit_location_id),
                        }
                        tsin_transfer_inventory_line_ids.append((0, 0, tsin_transfer_inventory_line_data))

                    if should_skip_create:
                        missing_products_str = ", ".join(missing_products)
                        message = f"Terdapat produk tidak aktif dalam Internal Transfers: {missing_products_str}"
                        print(message)
                        write_date = self.get_write_date(model_name, record['id'])
                        self.set_log_mc.create_log_note_failed(record, 'Internal Transfers', message, write_date)
                        self.set_log_ss.create_log_note_failed(record, 'Internal Transfers', message, write_date)
                        return

                    tsout_transfer_data = {
                        'scheduled_date': record.get('scheduled_date', False),
                        'date_done': record.get('date_done', False),
                        'location_id': int(location_id),
                        'location_dest_id': int(transit_location_id),
                        'target_location': target_location_name,
                        'picking_type_id': int(picking_type_id),
                        'is_integrated': True,
                        'vit_trxid': record.get('name', False),
                        'move_ids_without_package': tsout_transfer_inventory_line_ids,
                    }
                    try:
                        start_time = time.time()
                        new_tsout_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking', 'create',
                                                                    [tsout_transfer_data])
                        print(f"Internal Transfers baru telah dibuat di target dengan ID: {new_tsout_id}")

                        self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'stock.picking', 'write',
                            [[record['id']], {'is_integrated': True, 'vit_trxid': record['name']}]
                        )
                        end_time = time.time()
                        duration = end_time - start_time
                    except Exception as e:
                        write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Internal Transfers', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Internal Transfers', write_date)

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Internal Transfers', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Internal Transfers', write_date)
                except Exception as e:
                    message_exception = f"Gagal membuat atau memposting Internal Transfers di Source baru: {e}"
                    self.set_log_mc.create_log_note_failed(record, 'Internal Transfers', message_exception, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Internal Transfers', message_exception, write_date)

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_record, record) for record in Ts_Out_data_source]
                concurrent.futures.wait(futures)
        except Exception as e:
            print(f"Gagal membuat atau memposting Internal Transfers di Source baru: {e}")
            
    def transfer_goods_receipt(self, model_name, fields, description, date_from, date_to):
        try:
            # Ambil data dari sumber
            transaksi_goods_receipt = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                model_name, 'search_read',
                                                                [[['picking_type_id.name', '=', 'Goods Receipts'],
                                                                    ['is_integrated', '=', False], ['state', '=', 'done'],
                                                                    ['create_date', '>=', date_from], ['create_date', '<=', date_to]
                                                                    ]],
                                                                {'fields': fields})

            if not transaksi_goods_receipt:
                print("Semua transaksi telah diproses.")
                return

            # Persiapan dictionary untuk id source
            location_ids = [record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id') for record in transaksi_goods_receipt]
            location_dest_id = [record.get('location_dest_id')[0] if isinstance(record.get('location_dest_id'), list) else record.get('location_dest_id') for record in transaksi_goods_receipt]
            picking_type_ids = [record.get('picking_type_id')[0] if isinstance(record.get('picking_type_id'), list) else record.get('picking_type_id') for record in transaksi_goods_receipt]

            # Proses data lokasi
            location_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'stock.location', 'search_read',
                                                        [[['id', 'in', location_ids]]],
                                                        {'fields': ['id', 'id_mc'] , 'limit': 1})
            location_source_dict = {location['id']: location['id_mc'] for location in location_source}

            location_dest_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['id', 'in', location_dest_id]]],
                                                                {'fields': ['id', 'id_mc'] , 'limit': 1})
            location_dest_source_dict = {location_dest['id']: location_dest['id_mc'] for location_dest in location_dest_source}

            # Proses picking type
            picking_type_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.picking.type', 'search_read',
                                                            [[['id', 'in', picking_type_ids]]],
                                                            {'fields': ['id', 'id_mc'] , 'limit': 1})
            picking_type_source_dict = {type['id']: type['id_mc'] for type in picking_type_source}

            # Dapatkan data stock move (inventory lines)
            picking_ids = [record['id'] for record in transaksi_goods_receipt]
            goods_receipt_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', 'in', picking_ids]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

            existing_goods_receipts_dict = {}
            for record in transaksi_goods_receipt:
                location_id_value = location_source_dict.get(record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id'))
                existing_gr = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'stock.picking', 'search_read',
                                                        [[['vit_trxid', '=', record.get('name')], ['location_id', '=', location_id_value]]],
                                                        {'fields': ['id'], 'limit': 1})
                if existing_gr:
                    existing_goods_receipts_dict[record['id']] = existing_gr[0]['id']

            # Dictionary untuk goods_receipts_lines
            goods_receipts_lines_dict = {}
            for line in goods_receipt_inventory_lines:
                if 'picking_id' in line:
                    picking_id = line['picking_id'][0]
                    if picking_id not in goods_receipts_lines_dict:
                        goods_receipts_lines_dict[picking_id] = []
                    goods_receipts_lines_dict[picking_id].append(line)

            # Ambil data produk
            product_ids = [line['product_id'][0] for line in goods_receipt_inventory_lines if line.get('product_id')]
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
                                                                            'product.product', 'search_read',
                                                                            [[['default_code', 'in', list(default_code_to_product_tmpl_id.keys())]]],
                                                                            {'fields': ['id', 'default_code']})

            # Step 5: Create a mapping from default_code to id in target_client
            default_code_to_target_id = {template['default_code']: template['id'] for template in product_template_target_source}

            # Kumpulan ID untuk batch validate
            new_goods_receipts_ids = []

            def proces_goods_receipts_record(record):
                if record['id'] in existing_goods_receipts_dict:
                    return

                goods_receipt_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                            self.source_client.uid, self.source_client.password,
                                                                            'stock.move', 'search_read',
                                                                            [[['picking_id', '=', record['id']]]],
                                                                            {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

                # Check lokasi dan picking_type
                location_id = location_source_dict.get(record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id'))
                location_dest_id = location_dest_source_dict.get(record.get('location_dest_id')[0] if isinstance(record.get('location_dest_id'), list) else record.get('location_dest_id'))
                picking_type_id = picking_type_source_dict.get(record.get('picking_type_id')[0] if isinstance(record.get('picking_type_id'), list) else record.get('picking_type_id'))

                if location_id is None or location_dest_id is None or picking_type_id is None:
                    print(f"Data tidak lengkap untuk transaksi dengan ID {record.get('id')}. Tidak membuat dokumen.")
                    return

                missing_products = []
                goods_receipt_inventory_line_ids = []
                should_skip_create = False
                for line in goods_receipt_inventory_lines:
                    source_product_code = product_source_dict.get(line.get('product_id')[0])

                    # Step 7: Get the target product ID using the default_code mapping
                    target_product_template_id = default_code_to_target_id.get(source_product_code)

                    if not target_product_template_id:
                        missing_products.append(source_product_code)
                        should_skip_create = True
                        continue

                    goods_receipt_inventory_line_data = {
                        'product_id': int(target_product_template_id),
                        'product_uom_qty': line.get('product_uom_qty'),
                        'name': line.get('name'),
                        'quantity': line.get('quantity'),
                        'location_dest_id': int(location_dest_id),
                        'location_id': int(location_id),
                    }
                    goods_receipt_inventory_line_ids.append((0, 0, goods_receipt_inventory_line_data))

                if should_skip_create:
                    missing_products_str = ", ".join(missing_products)
                    message = f"Terdapat produk tidak ditemukan di target_client: {missing_products_str}. Stock.picking tidak dibuat."
                    print(message)
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Goods Receipts', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Goods Receipts', message, write_date)
                    return

                goods_receipts_transfer_data = {
                    'scheduled_date': record.get('scheduled_date', False),
                    'date_done': record.get('date_done', False),
                    'vit_trxid': record.get('name', False),
                    'origin': record.get('vit_trxid', False),
                    # 'is_integrated': True,
                    'location_id': int(location_id),
                    'location_dest_id': int(location_dest_id),
                    'picking_type_id': int(picking_type_id),
                    'move_ids_without_package': goods_receipt_inventory_line_ids,
                }

                try:
                    start_time = time.time()
                    # Buat Goods Receipt
                    new_goods_receipts_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'stock.picking', 'create',
                                                                        [goods_receipts_transfer_data])
                    print(f"Goods Receipt baru telah dibuat dengan ID: {new_goods_receipts_id}")

                    self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                self.source_client.uid, self.source_client.password,
                                                'stock.picking', 'write',
                                                [[record['id']], {'is_integrated': True, 'vit_trxid': record['name']}])
                

                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Goods Receipts', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Goods Receipts', write_date)
                except Exception as e:
                    message_exception = f"Gagal membuat atau memposting Goods Receipt baru: {e}"
                    self.set_log_mc.create_log_note_failed(record, 'Goods Receipts', message_exception, write_date)    
                    self.set_log_ss.create_log_note_failed(record, 'Goods Receipts', message_exception, write_date)

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(proces_goods_receipts_record, record) for record in transaksi_goods_receipt]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Gagal membuat atau memposting Goods Receipts di Source baru: {e}")

    def transfer_receipts_ss(self, model_name, fields, description, date_from, date_to):
        try:
            # Ambil data dari sumber
            transaksi_receipts = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                model_name, 'search_read',
                                                                [[['picking_type_id.name', '=', 'GRPO'],
                                                                    ['is_integrated', '=', True], ['state', '=', 'done'],
                                                                    ['create_date', '>=', date_from], ['create_date', '<=', date_to]
                                                                    ]],
                                                                {'fields': fields})

            if not transaksi_receipts:
                print("Semua transaksi telah diproses.")
                return

            # Persiapan dictionary untuk id source
            location_ids = [record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id') for record in transaksi_receipts]
            location_dest_id = [record.get('location_dest_id')[0] if isinstance(record.get('location_dest_id'), list) else record.get('location_dest_id') for record in transaksi_receipts]
            picking_type_ids = [record.get('picking_type_id')[0] if isinstance(record.get('picking_type_id'), list) else record.get('picking_type_id') for record in transaksi_receipts]

            # Proses data lokasi
            location_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'stock.location', 'search_read',
                                                        [[['id', 'in', location_ids]]],
                                                        {'fields': ['id', 'id_mc'] , 'limit': 1})
            location_source_dict = {location['id']: location['id_mc'] for location in location_source}

            location_dest_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['id', 'in', location_dest_id]]],
                                                                {'fields': ['id', 'id_mc'] , 'limit': 1})
            location_dest_source_dict = {location_dest['id']: location_dest['id_mc'] for location_dest in location_dest_source}

            # Proses picking type
            picking_type_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'stock.picking.type', 'search_read',
                                                            [[['id', 'in', picking_type_ids]]],
                                                            {'fields': ['id', 'id_mc'] , 'limit': 1})
            picking_type_source_dict = {type['id']: type['id_mc'] for type in picking_type_source}

            # Dapatkan data stock move (inventory lines)
            picking_ids = [record['id'] for record in transaksi_receipts]
            goods_receipt_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', 'in', picking_ids]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

            existing_goods_receipts_dict = {}
            for record in transaksi_receipts:
                location_id_value = location_source_dict.get(record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id'))
                existing_gr = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        'stock.picking', 'search_read',
                                                        [[['vit_trxid', '=', record.get('name')], ['location_id', '=', location_id_value]]],
                                                        {'fields': ['id'], 'limit': 1})
                if existing_gr:
                    existing_goods_receipts_dict[record['id']] = existing_gr[0]['id']

            # Dictionary untuk goods_receipts_lines
            goods_receipts_lines_dict = {}
            for line in goods_receipt_inventory_lines:
                if 'picking_id' in line:
                    picking_id = line['picking_id'][0]
                    if picking_id not in goods_receipts_lines_dict:
                        goods_receipts_lines_dict[picking_id] = []
                    goods_receipts_lines_dict[picking_id].append(line)

            # Ambil data produk
            product_ids = [line['product_id'][0] for line in goods_receipt_inventory_lines if line.get('product_id')]
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
                                                                            'product.product', 'search_read',
                                                                            [[['default_code', 'in', list(default_code_to_product_tmpl_id.keys())]]],
                                                                            {'fields': ['id', 'default_code']})

            # Step 5: Create a mapping from default_code to id in target_client
            default_code_to_target_id = {template['default_code']: template['id'] for template in product_template_target_source}

            # Kumpulan ID untuk batch validate
            new_goods_receipts_ids = []

            def proces_grpo_record(record):
                if record['id'] in existing_goods_receipts_dict:
                    return

                goods_receipt_inventory_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                            self.source_client.uid, self.source_client.password,
                                                                            'stock.move', 'search_read',
                                                                            [[['picking_id', '=', record['id']]]],
                                                                            {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

                # Check lokasi dan picking_type
                location_id = location_source_dict.get(record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id'))
                location_dest_id = location_dest_source_dict.get(record.get('location_dest_id')[0] if isinstance(record.get('location_dest_id'), list) else record.get('location_dest_id'))
                picking_type_id = picking_type_source_dict.get(record.get('picking_type_id')[0] if isinstance(record.get('picking_type_id'), list) else record.get('picking_type_id'))

                if location_id is None or location_dest_id is None or picking_type_id is None:
                    print(f"Data tidak lengkap untuk transaksi dengan ID {record.get('id')}. Tidak membuat dokumen.")
                    return

                missing_products = []
                goods_receipt_inventory_line_ids = []
                should_skip_create = False
                for line in goods_receipt_inventory_lines:
                    source_product_code = product_source_dict.get(line.get('product_id')[0])

                    # Step 7: Get the target product ID using the default_code mapping
                    target_product_template_id = default_code_to_target_id.get(source_product_code)

                    if not target_product_template_id:
                        missing_products.append(source_product_code)
                        should_skip_create = True
                        continue

                    goods_receipt_inventory_line_data = {
                        'product_id': int(target_product_template_id),
                        'product_uom_qty': line.get('product_uom_qty'),
                        'name': line.get('name'),
                        'quantity': line.get('quantity'),
                        'location_dest_id': int(location_dest_id),
                        'location_id': int(location_id),
                    }
                    goods_receipt_inventory_line_ids.append((0, 0, goods_receipt_inventory_line_data))

                if should_skip_create:
                    missing_products_str = ", ".join(missing_products)
                    message = f"Terdapat produk tidak ditemukan di target_client: {missing_products_str}"
                    print(message)
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'GRPO', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'GRPO', message, write_date)

                goods_receipts_transfer_data = {
                    'scheduled_date': record.get('scheduled_date', False),
                    'date_done': record.get('date_done', False),
                    'vit_trxid': record.get('name', False),
                    'origin': record.get('vit_trxid', False),
                    'is_integrated': True,
                    'location_id': int(location_id),
                    'location_dest_id': int(location_dest_id),
                    'picking_type_id': int(picking_type_id),
                    'move_ids_without_package': goods_receipt_inventory_line_ids,
                }

                try:
                    start_time = time.time()
                    # Buat Goods Receipt
                    new_goods_receipts_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'stock.picking', 'create',
                                                                        [goods_receipts_transfer_data])
                    print(f"GRPO baru telah dibuat dengan ID: {new_goods_receipts_id}")

                    self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                self.source_client.uid, self.source_client.password,
                                                'stock.picking', 'write',
                                                [[record['id']], {'is_integrated': True, 'vit_trxid': record['name']}])
                

                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'GRPO', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'GRPO', write_date)
                except Exception as e:
                    message_exception = f"Gagal membuat atau memposting GRPO baru: {e}"
                    self.set_log_mc.create_log_note_failed(record, 'GRPO', message_exception, write_date)    
                    self.set_log_ss.create_log_note_failed(record, 'GRPO', message_exception, write_date)

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(proces_grpo_record, record) for record in transaksi_receipts]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Gagal membuat atau memposting Goods Receipts di Source baru: {e}")

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

            location_ids = [record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id') for record in transaksi_goods_issue]
            location_dest_id = [record.get('location_dest_id')[0] if isinstance(record.get('location_dest_id'), list) else record.get('location_dest_id') for record in transaksi_goods_issue]
            picking_type_ids = [record.get('picking_type_id')[0] if isinstance(record.get('picking_type_id'), list) else record.get('picking_type_id') for record in transaksi_goods_issue]

            location_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['id', 'in', location_ids]]],
                                                                {'fields': ['id', 'id_mc'] , 'limit': 1})
            location_source_dict = {location['id']: location['id_mc'] for location in location_source}

            location_dest_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'stock.location', 'search_read',
                                                                [[['id', 'in', location_dest_id]]],
                                                                {'fields': ['id', 'id_mc'] , 'limit': 1})
            location_dest_source_dict = {location_dest['id']: location_dest['id_mc'] for location_dest in location_dest_source}

            picking_type_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    'stock.picking.type', 'search_read',
                                                                    [[['id', 'in', picking_type_ids]]],
                                                                    {'fields': ['id', 'id_mc'] , 'limit': 1})
            picking_type_source_dict = {type['id']: type['id_mc'] for type in picking_type_source}

            picking_ids = [record['id'] for record in transaksi_goods_issue]
            goods_issue_transfer_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                        self.source_client.uid, self.source_client.password,
                                                                        'stock.move', 'search_read',
                                                                        [[['picking_id', 'in', picking_ids]]],
                                                                        {'fields': ['product_id', 'product_uom_qty', 'quantity', 'name']})

            existing_goods_issue_dict = {}
            for record in transaksi_goods_issue:
                location_id_value = location_source_dict.get(record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id'))
                existing_gi = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'stock.picking', 'search_read',
                                                            [[['vit_trxid', '=', record.get('name')], ['location_id', '=', location_id_value]]],
                                                            {'fields': ['id'], 'limit': 1})
                if existing_gi:
                    existing_goods_issue_dict[record['id']] = existing_gi[0]['id']

            goods_issue_lines_dict = {}
            for line in goods_issue_transfer_lines:
                if 'picking_id' in line:
                    picking_id = line['picking_id'][0]
                    if picking_id not in goods_issue_lines_dict:
                        goods_issue_lines_dict[picking_id] = []
                    goods_issue_lines_dict[picking_id].append(line)

            product_ids = [line['product_id'][0] for line in goods_issue_transfer_lines if line.get('product_id')]
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
                                                                            'product.product', 'search_read',
                                                                            [[['default_code', 'in', list(default_code_to_product_tmpl_id.keys())]]],
                                                                            {'fields': ['id', 'default_code']})

            # Step 5: Create a mapping from default_code to id in target_client
            default_code_to_target_id = {template['default_code']: template['id'] for template in product_template_target_source}
            
            new_goods_issues_ids = []
            def proces_goods_issue_record(record):
                if record['id'] in existing_goods_issue_dict:
                    return
                
                goods_issue_transfer_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
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
                goods_issue_inventory_line_ids = []
                should_skip_create = False
                for line in goods_issue_transfer_lines:
                    source_product_code = product_source_dict.get(line.get('product_id')[0])

                    # Step 7: Get the target product ID using the default_code mapping
                    target_product_template_id = default_code_to_target_id.get(source_product_code)

                    if not target_product_template_id:
                        missing_products.append(source_product_code)
                        should_skip_create = True
                        continue

                    goods_issue_inventory_line_data = {
                        'product_id': int(target_product_template_id),
                        'product_uom_qty': line.get('product_uom_qty'),
                        'name': line.get('name'),
                        'quantity': line.get('quantity'),
                        'location_dest_id': int(location_dest_id),
                        'location_id': int(location_id),
                    }
                    goods_issue_inventory_line_ids.append((0, 0, goods_issue_inventory_line_data))

                if should_skip_create:
                    missing_products_str = ", ".join(missing_products)
                    message = f"Terdapat produk tidak aktif dalam Goods Issue: {missing_products_str}"
                    print(message)
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Goods Issue', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Goods Issue', message, write_date)

                internal_transfer_data = {
                    # 'name': record.get('name', False) + ' - ' + datetime.now(pytz.timezone('Asia/Jakarta')).strftime("%Y-%m-%d %H:%M:%S"),
                    # 'partner_id': customer_target_id,
                    'scheduled_date': record.get('scheduled_date', False),
                    'date_done': record.get('date_done', False),
                    'vit_trxid': record.get('name', False),
                    'origin': record.get('vit_trxid', False),
                    # 'is_integrated': True,
                    'location_id': int(location_id),
                    'location_dest_id': int(location_dest_id),
                    'picking_type_id': int(picking_type_id),
                    'move_ids_without_package': goods_issue_inventory_line_ids,
                }

                try:
                    new_goods_issue_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                    self.target_client.uid, self.target_client.password,
                                                                    'stock.picking', 'create',
                                                                    [internal_transfer_data])
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
                        [[record['id']], {'is_integrated': True, 'vit_trxid': record['name']}]
                    )
                    end_time = time.time()
                    duration = end_time - start_time

                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Goods Issue', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Goods Issue', write_date)
                except Exception as e:
                    print(f"Gagal membuat atau memposting Goods Issue baru: {e}")
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'Goods Issue', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'Goods Issue', message, write_date)

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(proces_goods_issue_record, record) for record in transaksi_goods_issue]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Gagal membuat atau memposting Goods Issue di Source baru: {e}")

    def transfer_stock_adjustment(self, model_name, fields, description, date_from, date_to):
        try:
            # Step 1: Fetch all needed data sequentially
            transaksi_stock_adjustment = self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db,
                self.source_client.uid, self.source_client.password,
                model_name, 'search_read',
                [[['reference', 'ilike', 'Quantity Updated'], ['is_integrated', '=', False], ['state', '=', 'done']]],
                {'fields': fields}
            )

            if not transaksi_stock_adjustment:
                print("Tidak ada transaksi yang ditemukan untuk ditransfer.")
                return

            # Step 2: Build dictionaries from required metadata
            product_ids = list({record.get('product_id')[0] if isinstance(record.get('product_id'), list) else record.get('product_id') for record in transaksi_stock_adjustment})
            location_ids = list({record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id') for record in transaksi_stock_adjustment})
            location_dest_ids = list({record.get('location_dest_id')[0] if isinstance(record.get('location_dest_id'), list) else record.get('location_dest_id') for record in transaksi_stock_adjustment})
            company_ids = list({record.get('company_id')[0] if isinstance(record.get('company_id'), list) else record.get('company_id') for record in transaksi_stock_adjustment if record.get('company_id')})

            location_source_dict = {loc['id']: loc['id_mc'] for loc in self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db, self.source_client.uid, self.source_client.password,
                'stock.location', 'search_read', [[['id', 'in', location_ids]]], {'fields': ['id', 'id_mc']}
            )}

            location_dest_source_dict = {loc['id']: loc['id_mc'] for loc in self.source_client.call_odoo(
                'object', 'execute_kw', self.source_client.db, self.source_client.uid, self.source_client.password,
                'stock.location', 'search_read', [[['id', 'in', location_dest_ids]]], {'fields': ['id', 'id_mc']}
            )}

            product_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                                        self.source_client.password, 'product.product', 'search_read',
                                                        [[['id', 'in', product_ids]]], {'fields': ['id', 'default_code']})
            product_source_dict = {product['id']: product['default_code'] for product in product_source}

            product_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                                        self.target_client.password, 'product.product', 'search_read',
                                                        [[['default_code', 'in', list(product_source_dict.values())]]],
                                                        {'fields': ['id', 'default_code']})
            product_target_dict = {product['default_code']: product['id'] for product in product_target}

            companies_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'res.company', 'search_read', [[['id', 'in', company_ids]]],
                                                            {'fields': ['id', 'name']})
            company_source_dict = {c['id']: c['name'] for c in companies_source if 'name' in c}

            companies_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            'res.company', 'search_read',
                                                            [[['name', 'in', list(company_source_dict.values())]]],
                                                            {'fields': ['id', 'name']})
            company_target_dict = {c['name']: c['id'] for c in companies_target}

            # Step 3: Parallel processing of each record
            def _process_single_adjustment_record(record):
                try:
                    # Unwrap values
                    company_name = company_source_dict.get(record.get('company_id')[0] if isinstance(record.get('company_id'), list) else record.get('company_id'))
                    company_id = company_target_dict.get(company_name)
                    if not company_id:
                        raise Exception(f"Company {company_name} tidak ditemukan di target.")

                    product_id = record.get('product_id')[0] if isinstance(record.get('product_id'), list) else record.get('product_id')
                    product_code = product_source_dict.get(product_id)
                    target_product_id = product_target_dict.get(product_code)

                    if not target_product_id:
                        raise Exception(f"Product {product_code} tidak ditemukan di target system")

                    source_loc_id = location_source_dict.get(record.get('location_id')[0] if isinstance(record.get('location_id'), list) else record.get('location_id'))
                    dest_loc_id = location_dest_source_dict.get(record.get('location_dest_id')[0] if isinstance(record.get('location_dest_id'), list) else record.get('location_dest_id'))

                    existing_quant = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.quant', 'search_read',
                                                                [[['product_id', '=', target_product_id], ['location_id', '=', int(source_loc_id)]]],
                                                                {'fields': ['quantity']})
                    existing_quantity = existing_quant[0]['quantity'] if existing_quant else 0
                    adjusted_quantity = record.get('quantity') - existing_quantity

                    if adjusted_quantity <= 0:
                        print(f"[SKIP] Product {product_code} sudah cukup stoknya.")
                        return

                    stock_move_id = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                self.target_client.uid, self.target_client.password,
                                                                'stock.move', 'create',
                                                                [{
                                                                    'product_id': target_product_id,
                                                                    'location_id': int(source_loc_id),
                                                                    'location_dest_id': int(dest_loc_id),
                                                                    'name': "Product Quantity Updated",
                                                                    'state': 'done',
                                                                    'company_id': int(company_id),
                                                                }])
                    self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'stock.move.line', 'create',
                                                [{
                                                    'product_id': target_product_id,
                                                    'location_id': int(source_loc_id),
                                                    'location_dest_id': int(dest_loc_id),
                                                    'quantity': adjusted_quantity,
                                                    'move_id': stock_move_id,
                                                    'state': 'done',
                                                    'company_id': int(company_id),
                                                }])
                    self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                self.source_client.uid, self.source_client.password,
                                                model_name, 'write',
                                                [[record['id']], {'is_integrated': True}])
                    print(f"[OK] {product_code} qty {adjusted_quantity} berhasil diproses.")

                except Exception as e:
                    print(f"[ERROR] Record ID {record['id']} gagal: {str(e)}")

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(_process_single_adjustment_record, rec) for rec in transaksi_stock_adjustment]
                concurrent.futures.wait(futures)

            return True

        except Exception as e:
            print(f"[FATAL ERROR] {str(e)}")
            return False

    def update_session_status(self, model_name, fields, description, date_from, date_to):
        pos_sessions = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    model_name, 'search_read',
                                                    [[['state', '=', 'closed'], ['is_updated', '=', False]]],
                                                    {'fields': fields})

        if not pos_sessions:
            print("Tidak ada sesi yang ditemukan untuk ditransfer.")
            return

        # Pre-fetch config_id data
        config_ids = [record.get('config_id')[0] if isinstance(record.get('config_id'), list) else record.get('config_id') for record in pos_sessions]
        config_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    'pos.config', 'search_read',
                                                    [[['id', 'in', config_ids]]],
                                                    {'fields': ['id', 'id_mc']})
        config_source_dict = {config['id']: config['id_mc'] for config in config_source}

        for sessions in pos_sessions:
            name = sessions.get('name')
            state = sessions.get('state')
            start_at = sessions.get('start_at')
            stop_at = sessions.get('stop_at')

            cash_register_balance_start = sessions.get('cash_register_balance_start')
            cash_register_balance_end_real = sessions.get('cash_register_balance_end_real')
            config_id = sessions.get('config_id')

            # Debugging prints
            print(f"Cash Register Balance Start: {cash_register_balance_start}")
            print(f"Cash Register Balance End Real: {cash_register_balance_end_real}")

            # Ensure monetary values are properly handled
            cash_register_balance_start = float(cash_register_balance_start) if cash_register_balance_start else 0.0
            cash_register_balance_end_real = float(cash_register_balance_end_real) if cash_register_balance_end_real else 0.0

            # Convert config_id to int if it's available
            config_id = int(config_id[0]) if isinstance(config_id, list) else int(config_id) if config_id else None
            # Use the mapping from config_source_dict for config_id
            config_id = config_source_dict.get(config_id) if config_id else None
            
            # print(config_id)

            if not state:
                print(f"Status sesi {name} tidak valid.")
                continue

            # Fetch the corresponding session on the target client based on the session name and config_id
            source_session = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        model_name, 'search_read',
                                                        [[['name_session_pos', '=', str(name)]]],  # Added config_id
                                                        {'fields': ['state'], 'limit': 1})

            if not source_session:
                print(f"Sesi dengan nama {name} dan config_id {config_id} tidak ditemukan di sumber.")
                continue

            # Update the state on the target client
            session_id = source_session[0]['id']

            update_result = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password,
                                                        model_name, 'write',
                                                        [[session_id], {'state': state, 'start_at': start_at, 'stop_at': stop_at, 
                                                                        'cash_register_balance_start': cash_register_balance_start, 
                                                                        'cash_register_balance_end_real': cash_register_balance_end_real, 
                                                                        'is_updated': True}])

            self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'pos.session', 'write',
                    [[sessions['id']], {'is_updated': True}]
            )


            # if update_result:
            #     message_succes = f"Berhasil mengupdate sesi {name} dengan status {state}."
            #     self.set_log_mc.create_log_note_success(sessions, 'Update Session', message_succes, sessions['write_date'])
            #     self.set_log_ss.create_log_note_success(sessions, 'Update Session', message_succes, sessions['write_date'])
            # else:
            #     message_exception = f"Gagal mengupdate sesi {name}."
            #     self.set_log_mc.create_log_note_failed(sessions, 'Update Session', message_exception, sessions['write_date'])
            #     self.set_log_ss.create_log_note_failed(sessions, 'Update Session', message_exception, sessions['write_date'])

    def create_loyalty_point_ss_to_mc(self, model_name, fields, description, date_from, date_to):
        id_program = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                self.source_client.uid, self.source_client.password,
                                                model_name, 'search_read',
                                                [[['program_type', '=', 'loyalty']]],
                                                {'fields': fields})

        for res in id_program:
            programs = res.get('id', False)

            loyalty_points = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'loyalty.card', 'search_read',
                                                        [[['program_id', '=', int(programs)]]],
                                                        {'fields': ['code', 'points_display', 'expiration_date', 'program_id', 'currency_id', 'partner_id', 'source_pos_order_id', 'points']})

            pos_order_ids = {record.get('source_pos_order_id')[0] for record in loyalty_points if record.get('source_pos_order_id')}
            pos_orders = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    'pos.order', 'search_read',
                                                    [[['id', 'in', list(pos_order_ids)]]],
                                                    {'fields': ['id', 'vit_trxid', 'name']})

            pos_order_map = {order['id']: order['name'] for order in pos_orders}

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

            order_references = {}
            program_id_sets = {}
            partner_id_sets = {}

            for record in loyalty_points:
                source_pos_order_id = record.get('source_pos_order_id')
                if source_pos_order_id and source_pos_order_id[0] not in order_references:
                    order_ref = pos_order_map.get(source_pos_order_id[0])
                    if order_ref:
                        order_reference = self.target_client.call_odoo(
                            'object', 'execute_kw', self.target_client.db,
                            self.target_client.uid, self.target_client.password,
                            'pos.order', 'search_read',
                            [[['name', '=', order_ref]]],
                            {'fields': ['id'], 'limit': 1}
                        )
                        order_references[source_pos_order_id[0]] = order_reference[0]['id'] if order_reference else False

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
                    [[['vit_trxid', '=', record['code']]]],
                    {'fields': ['id']}
                )
                if not existing_loyalty_points_mc:
                    code = record.get('code')
                    expiration_date = record.get('expiration_date')
                    points = record.get('points')
                    points_display = record.get('points_display')

                    order_id = order_references.get(record.get('source_pos_order_id')[0], False) if record.get('source_pos_order_id') else False
                    program_id = program_id_sets.get(record.get('program_id')[0], False) if record.get('program_id') else False
                    partner_id = partner_id_sets.get(record.get('partner_id')[0], False) if record.get('partner_id') else False

                    data_loyalty_mc = {
                        'expiration_date': expiration_date,
                        'points': points,
                        'points_display': points_display,
                        'source_pos_order_id': order_id,
                        'program_id': program_id,
                        'partner_id': partner_id,
                        'vit_trxid': record.get('code')
                    }

                    try:
                        self.target_client.call_odoo(
                            'object', 'execute_kw', self.target_client.db,
                            self.target_client.uid, self.target_client.password,
                            'loyalty.card', 'create',
                            [data_loyalty_mc]
                        )
                        print(f"Loyalty dengan ID {record['code']} telah dibuat di target_client.")
                    except Exception as e:
                        print(f"Terjadi kesalahan saat memperbarui loyalty: {e}")

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                for i in range(0, len(loyalty_points), 100):
                    batch = loyalty_points[i:i + 100]
                    executor.map(process_loyalty_point, batch)

    def update_loyalty_point_ss_to_mc(self, model_name, fields, description, date_from, date_to):
        id_program = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    model_name, 'search_read',
                                                    [[['program_type', 'in', ['loyalty', 'coupons']]]],
                                                    {'fields': fields})

        for res in id_program:
            programs = res.get('id', False)

            loyalty_points = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'loyalty.card', 'search_read',
                                                        [[['program_id', '=', int(programs)]]],
                                                        {'fields': ['code', 'points_display', 'expiration_date', 'program_id', 'currency_id', 'partner_id', 'source_pos_order_id', 'points']})

            pos_order_ids = {record.get('source_pos_order_id')[0] for record in loyalty_points if record.get('source_pos_order_id')}
            pos_orders = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                    self.source_client.uid, self.source_client.password,
                                                    'pos.order', 'search_read',
                                                    [[['id', 'in', list(pos_order_ids)]]],
                                                    {'fields': ['id', 'vit_trxid',  'name']})

            pos_order_map = {order['id']: order['name'] for order in pos_orders}

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

            order_references = {}
            program_id_sets = {}
            partner_id_sets = {}

            for record in loyalty_points:
                source_pos_order_id = record.get('source_pos_order_id')
                if source_pos_order_id and source_pos_order_id[0] not in order_references:
                    order_ref = pos_order_map.get(source_pos_order_id[0])
                    
                    if order_ref:
                        order_reference = self.target_client.call_odoo(
                            'object', 'execute_kw', self.target_client.db,
                            self.target_client.uid, self.target_client.password,
                            'pos.order', 'search_read',
                            [[['name', '=', order_ref]]],
                            {'fields': ['id'], 'limit': 1}
                        )
                        order_references[source_pos_order_id[0]] = order_reference[0]['id'] if order_reference else False

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
                    [[['vit_trxid', '=', record['code']]]],
                    {'fields': ['id']}
                )
                if not existing_loyalty_points_mc:
                    loyalty_id = existing_loyalty_points_mc[0]['id']
                    code = record.get('code')
                    expiration_date = record.get('expiration_date')
                    points = record.get('points')
                    points_display = record.get('points_display')

                    order_id = order_references.get(record.get('source_pos_order_id')[0], False) if record.get('source_pos_order_id') else False
                    program_id = program_id_sets.get(record.get('program_id')[0], False) if record.get('program_id') else False
                    partner_id = partner_id_sets.get(record.get('partner_id')[0], False) if record.get('partner_id') else False
                    
                    data_loyalty_mc = {
                        'expiration_date': expiration_date,
                        'points': points,
                        'points_display': points_display,
                        'source_pos_order_id': order_id,
                        'program_id': program_id,
                        'partner_id': partner_id,
                        'vit_trxid': record.get('code')
                    }

                    try:
                        self.target_client.call_odoo(
                            'object', 'execute_kw', self.target_client.db,
                            self.target_client.uid, self.target_client.password,
                            'loyalty.card', 'write',
                            [[loyalty_id], data_loyalty_mc]
                        )
                        print(f"Loyalty dengan ID {loyalty_id} telah diperbarui di target_client.")
                    except Exception as e:
                        print(f"Terjadi kesalahan saat memperbarui loyalty: {e}")

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                for i in range(0, len(loyalty_points), 100):
                    batch = loyalty_points[i:i + 100]
                    executor.map(process_loyalty_point, batch)
    
    def validate_tsin_tsout(self, model_name, fields, description, date_from, date_to):
        # Retrieve TS In records that match the specified criteria from the source database
        TS_in_validates = self.source_client.call_odoo(
            'object', 'execute_kw', 
            self.source_client.db, self.source_client.uid, self.source_client.password,
            'stock.picking', 'search_read',
            [[
                ['picking_type_id.name', '=', 'TS In'], 
                ['is_integrated', '=', True], 
                ['is_updated', '=', False],
                ['state', '=', 'done'],
                ['write_date', '>=', date_from],
                ['write_date', '<=', date_to],
            ]],
            {'fields': ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'vit_trxid', 'move_ids_without_package']}
        )

        # Check if any TS In records are found
        if not TS_in_validates:
            print("Tidak ada TS In yang ditemukan di target.")
        else:
            # Process in batches of 100
            for i in range(0, len(TS_in_validates), 100):
                batch = TS_in_validates[i:i + 100]
                for ts in batch:
                    vit_trxid = ts.get('vit_trxid', False)

                    # Retrieve TS In records that need validation from the target database
                    TS_in_needs_validate = self.target_client.call_odoo(
                        'object', 'execute_kw', 
                        self.target_client.db, self.target_client.uid, self.target_client.password,
                        'stock.picking', 'search_read',
                        [[
                            ['picking_type_id.name', '=', 'TS In'], 
                            ['name', '=', vit_trxid], 
                            ['state', '=', 'assigned']
                        ]],
                        {'fields': ['id', 'name']}
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
                            self.source_client.call_odoo(
                                'object', 'execute_kw', self.source_client.db,
                                self.source_client.uid, self.source_client.password,
                                'stock.picking', 'write',
                                [[ts['id']], {'is_updated': True}]
                            )

                            print(f"TS In with ID {ts_in_id} has been validated.")
                            end_time = time.time()
                            duration = end_time - start_time

                            write_date = self.get_write_date(model_name, ts_in['id'])
                            self.set_log_mc.create_log_note_success(ts_in, start_time, end_time, duration, 'TS Out/TS In', write_date)
                            self.set_log_ss.create_log_note_success(ts_in, start_time, end_time, duration, 'TS Out/TS In', write_date)
                        except Exception as e:
                            message_exception = f"Failed to validate TS In with ID {ts_in_id}: {e}"
                            self.set_log_mc.create_log_note_failed(ts_in, 'TS Out/TS In', message_exception, write_date)    
                            self.set_log_ss.create_log_note_failed(ts_in, 'TS Out/TS In', message_exception, write_date)

    def transfer_end_shift_from_store(self, model_name, fields, description):
        try:
            # Fetching the data
            end_shift_store = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                    self.source_client.uid, self.source_client.password,
                                                                    model_name, 'search_read',
                                                                    [[['state', '=', 'finished'], ['is_integrated', '=', False]]],
                                                                    {'fields': fields})

            if not end_shift_store:
                print("Semua shift telah diproses.")
                return
            
            cashier_ids = [record.get('cashier_id')[0] if isinstance(record.get('cashier_id'), list) else record.get('cashier_id') for record in end_shift_store]
            session_ids = [record.get('session_id')[0] if isinstance(record.get('session_id'), list) else record.get('session_id') for record in end_shift_store]

            cashier_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'hr.employee', 'search_read',
                                                        [[['id', 'in', cashier_ids]]],
                                                        {'fields': ['id', 'id_mc']})
            cashier_source_dict = {employee['id']: employee['id_mc'] for employee in cashier_source}

            # Fetch sessions
            sessions_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'pos.session', 'search_read',
                                                        [[['id', 'in', session_ids]]],
                                                        {'fields': ['id', 'id_mc']})
            sessions_source_dict = {session['id']: session['id_mc'] for session in sessions_source}

            end_shift_ids = [record['id'] for record in end_shift_store]
            end_shift_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password,
                                                        'end.shift.line', 'search_read',
                                                        [[['end_shift_id', 'in', end_shift_ids]]],
                                                        {'fields': ['end_shift_id', 'payment_date', 'payment_method_id', 'amount', 'expected_amount', 'amount_difference']})

            # Create dictionaries for quick lookup
            end_shift_lines_dict = {}
            for line in end_shift_lines:
                end_shift_id = line['end_shift_id'][0]
                if end_shift_id not in end_shift_lines_dict:
                    end_shift_lines_dict[end_shift_id] = []
                end_shift_lines_dict[end_shift_id].append(line)

            # Pre-fetch existing end shifts in target
            existing_end_shift_dict = {}
            for record in end_shift_store:
                existing_end_shift = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                                        self.target_client.uid, self.target_client.password,
                                                                        'end.shift', 'search_read',
                                                                        [[['doc_num', '=', record.get('doc_num')]]],
                                                                        {'fields': ['id'], 'limit': 1})
                if existing_end_shift:
                    existing_end_shift_dict[record['id']] = existing_end_shift[0]['id']

            payment_method_ids = [payment['payment_method_id'][0] for payment in end_shift_lines if payment.get('payment_method_id')]
            payment_method_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                'pos.payment.method', 'search_read',
                                                                [[['id', 'in', payment_method_ids]]],
                                                                {'fields': ['id', 'id_mc', 'name']})
            payment_method_source_dict = {payment['id']: payment['id_mc'] for payment in payment_method_source}

            def process_record_endshift(record):
                if record['id'] in existing_end_shift_dict:
                    print(f"End Shift {record['doc_num']} sudah diproses.")
                    return

                end_shift_lines = end_shift_lines_dict.get(record['id'], [])
                end_shift_line_ids = []
                missing_payment_methods = []

                for line in end_shift_lines:
                    payment_method_id_src = line.get('payment_method_id')
                    if not payment_method_id_src:
                        continue

                    payment_method_id = payment_method_source_dict.get(
                        payment_method_id_src[0] if isinstance(payment_method_id_src, list) else payment_method_id_src
                    )
                    if not payment_method_id:
                        missing_payment_methods.append(str(payment_method_id_src))
                        continue

                    # ⬇️ Gunakan amount dari source (bukan expected saja)
                    end_shift_line_data = {
                        'payment_date': line.get('payment_date'),
                        'payment_method_id': int(payment_method_id),
                        'expected_amount': line.get('expected_amount'),
                        'amount': line.get('amount'),  # 👉 Injected from source
                    }
                    end_shift_line_ids.append((0, 0, end_shift_line_data))

                if missing_payment_methods:
                    message = f"Terdapat metode pembayaran tidak ditemukan: {', '.join(missing_payment_methods)}"
                    print(message)
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'End Shift', message, write_date)
                    self.set_log_ss.create_log_note_failed(record, 'End Shift', message, write_date)
                    return

                cashier_id = cashier_source_dict.get(
                    record.get('cashier_id')[0] if isinstance(record.get('cashier_id'), list) else record.get('cashier_id'))
                session_id = sessions_source_dict.get(
                    record.get('session_id')[0] if isinstance(record.get('session_id'), list) else record.get('session_id'))

                if not cashier_id or not session_id:
                    print(f"❌ Data tidak lengkap untuk shift ID {record.get('id')} – dilewati.")
                    return

                end_shift_data = {
                    'doc_num': record.get('doc_num'),
                    'cashier_id': int(cashier_id),
                    'session_id': int(session_id),
                    'start_date': record.get('start_date'),
                    'end_date': record.get('end_date'),
                    'line_ids': end_shift_line_ids,
                }

                try:
                    start_time = time.time()
                    print(f"🔧 Creating end.shift: {end_shift_data}")
                    new_end_shift_id = self.target_client.call_odoo('object', 'execute_kw',
                        self.target_client.db, self.target_client.uid, self.target_client.password,
                        'end.shift', 'create', [end_shift_data])

                    if not new_end_shift_id:
                        raise Exception("End Shift gagal dibuat.")

                    print(f"✅ Created end.shift ID {new_end_shift_id}")

                    # 🔁 Call action_close
                    self.target_client.call_odoo('object', 'execute_kw',
                        self.target_client.db, self.target_client.uid, self.target_client.password,
                        'end.shift', 'action_close', [[new_end_shift_id]])

                    print(f"📦 action_close executed for ID {new_end_shift_id}")

                    # 🔁 Call action_finish
                    self.target_client.call_odoo('object', 'execute_kw',
                        self.target_client.db, self.target_client.uid, self.target_client.password,
                        'end.shift', 'action_finish', [[new_end_shift_id]])

                    print(f"🎯 action_finish executed for ID {new_end_shift_id}")

                    self.source_client.call_odoo('object', 'execute_kw',
                        self.source_client.db, self.source_client.uid, self.source_client.password,
                        'end.shift', 'write', [[record['id']], {'is_integrated': True}])

                    print(f"✅ Shift ID {record['id']} marked as integrated.")

                    end_time = time.time()
                    duration = end_time - start_time
                    write_date = self.get_write_date(model_name, record['id'])

                    self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'End Shift', write_date)
                    self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'End Shift', write_date)

                except Exception as e:
                    print(f"💥 Error saat proses shift: {str(e)}")
                    write_date = self.get_write_date(model_name, record['id'])
                    self.set_log_mc.create_log_note_failed(record, 'End Shift', str(e), write_date)
                    self.set_log_ss.create_log_note_failed(record, 'End Shift', str(e), write_date)


            # Use ThreadPoolExecutor to process records in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(process_record_endshift, record) for record in end_shift_store]
                concurrent.futures.wait(futures)

        except Exception as e:
            print(f"Error during processing: {e}")

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