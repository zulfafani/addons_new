def transfer_discount_loyalty(self, model_name, fields, description, date_from, date_to):
            try:
                # Ambil data dari sumber
                discount_loyalty = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                self.source_client.uid, self.source_client.password,
                                                                model_name, 'search_read',
                                                                [[['active', '=', True], ['is_integrated', '=', False]]],
                                                                {'fields': fields})
                if not discount_loyalty:
                    print("Tidak ada discount/loyalty yang ditemukan untuk ditransfer.")
                    return

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
                                                                {'fields': ['reward_type', 'discount', 'discount_applicability', 'discount_max_amount', 'required_points', 'description', 'discount_mode', 'discount_product_domain', 'discount_product_ids', 'discount_product_category_id', 'vit_trxid', 'program_id', 'reward_product_id', 'discount_product_tag_id']})

                rule_ids_lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            'loyalty.rule', 'search_read',
                                                            [[['program_id', 'in', order_ids]]],
                                                            {'fields': ['minimum_qty', 'minimum_amount', 'reward_point_amount', 'reward_point_mode', 'product_domain', 'product_ids', 'product_category_id', 'minimum_amount_tax_mode', 'vit_trxid', 'program_id', 'product_tag_id']})

                # === 1. Fetch ONLY IDs from source ===
                reward_product_id_source = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'product.product', 'search_read',
                    [[['id', 'in', reward_product_id]]],
                    {'fields': ['id', 'product_tmpl_id']}
                )

                products_source_reward = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'product.product', 'search_read',
                    [[['id', 'in', product_ids_reward]]],
                    {'fields': ['id', 'product_tmpl_id']}
                )

                products_source_rule = self.source_client.call_odoo(
                    'object', 'execute_kw', self.source_client.db,
                    self.source_client.uid, self.source_client.password,
                    'product.product', 'search_read',
                    [[['id', 'in', product_ids_rule]]],
                    {'fields': ['id', 'product_tmpl_id']}
                )

                categories_source_reward = category_ids_reward  # use raw IDs
                categories_source_rule = category_ids_rule
                product_tag_ids_reward_source = product_tag_ids_reward
                product_tag_ids_rule_source = product_tag_ids_rule
                currencies_source = currency_ids
                pricelists_source = pricelist_ids
                pos_configs_source = pos_config_ids

                # === 2. Map to target by id_mc ===
                def map_ids_by_id_mc(model, ids):
                    if not ids:
                        return []

                    return self.target_client.call_odoo(
                        'object', 'execute_kw', self.target_client.db,
                        self.target_client.uid, self.target_client.password,
                        model, 'search_read',
                        [[['id_mc', 'in', ids]]],
                        {'fields': ['id', 'id_mc']}
                    )

                # Map using product_tmpl_id for product.product -> product.template
                def extract_template_ids(product_list):
                    return [item['product_tmpl_id'][0] if isinstance(item['product_tmpl_id'], list) else item['product_tmpl_id'] for item in product_list]

                tmpl_ids_reward_product = extract_template_ids(reward_product_id_source)
                tmpl_ids_products_reward = extract_template_ids(products_source_reward)
                tmpl_ids_products_rule = extract_template_ids(products_source_rule)

                reward_product_id_target = map_ids_by_id_mc('product.template', tmpl_ids_reward_product)
                products_target_reward = map_ids_by_id_mc('product.template', tmpl_ids_products_reward)
                products_target_rule = map_ids_by_id_mc('product.template', tmpl_ids_products_rule)

                categories_target_reward = map_ids_by_id_mc('product.category', categories_source_reward)
                categories_target_rule = map_ids_by_id_mc('product.category', categories_source_rule)

                product_tag_target_reward = map_ids_by_id_mc('product.tag', product_tag_ids_reward_source)
                product_tag_target_rule = map_ids_by_id_mc('product.tag', product_tag_ids_rule_source)

                currencies_target = map_ids_by_id_mc('res.currency', currencies_source)
                pricelists_target = map_ids_by_id_mc('product.pricelist', pricelists_source)
                pos_configs_target = map_ids_by_id_mc('pos.config', pos_configs_source)

                # === 3. Build dictionary maps ===
                reward_product_id_dict = {item['id_mc']: item['id'] for item in reward_product_id_target}
                product_dict_reward = {item['id_mc']: item['id'] for item in products_target_reward}
                product_dict_rule = {item['id_mc']: item['id'] for item in products_target_rule}
                category_dict_reward = {item['id_mc']: item['id'] for item in categories_target_reward}
                category_dict_rule = {item['id_mc']: item['id'] for item in categories_target_rule}
                product_tag_dict_reward = {item['id_mc']: item['id'] for item in product_tag_target_reward}
                product_tag_dict_rule = {item['id_mc']: item['id'] for item in product_tag_target_rule}
                currency_dict = {item['id_mc']: item['id'] for item in currencies_target}
                pricelist_dict = {item['id_mc']: item['id'] for item in pricelists_target}
                pos_config_dict = {item['id_mc']: item['id'] for item in pos_configs_target}
                def process_create_discount(record):
                    if record['vit_trxid'] in existing_discount_dict:
                        return
                    current_reward_lines = [line for line in reward_ids_lines if line['program_id'][0] == record['id']]
                    current_rule_lines = [line for line in rule_ids_lines if line['program_id'][0] == record['id']]

                    discount_loyalty_line_ids = []
                    for line in current_reward_lines:
                        if isinstance(line, dict):
                            reward_product_ids = line.get('discount_product_ids', [])
                            reward_target_product_ids = [product_dict_reward.get(product['default_code']) for product in products_source_reward if product['id'] in reward_product_ids]

                            reward_product_id_field = line.get('reward_product_id')
                            reward_product_id_field = reward_product_id_field[0] if isinstance(reward_product_id_field, list) else reward_product_id_field

                            # Ambil nama default_code dari reward_product_id_source
                            if isinstance(reward_product_id_field, list) and len(reward_product_id_field) == 2:
                                reward_product_default_code = reward_product_id_field[1]
                            else:
                                reward_product_default_code = next(
                                    (product['default_code'] for product in reward_product_id_source if product['id'] == reward_product_id_field),
                                    None
                                )

                            # Ambil ID dari target database berdasarkan default_code
                            reward_product_id_id = reward_product_id_dict.get(reward_product_default_code)
                            # print(reward_product_id_id)

                            reward_source_category_id = line.get('discount_product_category_id')
                            if isinstance(reward_source_category_id, list) and len(reward_source_category_id) == 2:
                                reward_source_category_name = reward_source_category_id[1]
                            else:
                                reward_source_category_name = next((category['complete_name'] for category in categories_source_reward if category['id'] == reward_source_category_id), None)

                            reward_target_category_id = category_dict_reward.get(reward_source_category_name)

                            #Product Tag
                            reward_source_product_tag_id = line.get('discount_product_tag_id')
                            if isinstance(reward_source_product_tag_id, list) and len(reward_source_product_tag_id) == 2:
                                reward_source_product_tag_name = reward_source_product_tag_id[1]
                            else:
                                reward_source_product_tag_name = next((tag['name'] for tag in product_tag_source_reward if tag['id'] == reward_source_product_tag_id), None)

                            reward_target_product_tag_id = product_tag_dict_reward.get(reward_source_product_tag_name)

                        discount_line_data = {
                            'reward_type': line.get('reward_type'),
                            'discount': line.get('discount'),
                            'discount_applicability': line.get('discount_applicability'),
                            'discount_max_amount': line.get('discount_max_amount'),
                            'required_points': line.get('required_points'),
                            'description': line.get('description'),
                            'discount_mode': line.get('discount_mode'),
                            'discount_product_ids': [(6, 0, reward_target_product_ids)],
                            'reward_product_id': reward_product_id_id,
                            'discount_product_category_id': reward_target_category_id,
                            'discount_product_tag_id': reward_target_product_tag_id,
                            'vit_trxid': record.get('name')
                        }
                        discount_loyalty_line_ids.append((0, 0, discount_line_data))

                    rule_ids = []
                    for rule in current_rule_lines:
                        if isinstance(rule, dict):
                            rule_product_ids = rule.get('product_ids', [])
                            rule_target_product_ids = [product_dict_rule.get(product['default_code']) for product in products_source_rule if product['id'] in rule_product_ids]

                            rule_source_category_id = rule.get('product_category_id')
                            if isinstance(rule_source_category_id, list) and len(rule_source_category_id) == 2:
                                rule_source_category_name = rule_source_category_id[1]
                            else:
                                rule_source_category_name = next((category['complete_name'] for category in categories_source_rule if category['id'] == rule_source_category_id), None)

                            rule_target_category_id = category_dict_rule.get(rule_source_category_name)

                            #Product tag
                            rule_source_product_tag_id = rule.get('product_tag_id')
                            if isinstance(rule_source_product_tag_id, list) and len(rule_source_product_tag_id) == 2:
                                rule_source_product_tag_name = rule_source_product_tag_id[1]
                            else:
                                rule_source_product_tag_name = next((tag['name'] for tag in product_tag_source_rule if tag['id'] == rule_source_product_tag_id), None)

                            rule_target_product_tag_id = product_tag_dict_rule.get(rule_source_product_tag_name)
                            
                        # tag_name = next((tag['name'] for tag in tags_source if tag['id'] == rule.get('product_tag_id')), None)
                        # tag_id = tag_dict.get(tag_name)

                        rule_data = {
                            'minimum_qty': rule.get('minimum_qty'),
                            'minimum_amount': rule.get('minimum_amount'),
                            'reward_point_amount': rule.get('reward_point_amount'),
                            'reward_point_mode': rule.get('reward_point_mode'),
                            'product_domain': rule.get('product_domain'),
                            'product_ids': rule_target_product_ids,
                            'minimum_amount_tax_mode': rule.get('minimum_amount_tax_mode'),
                            'product_category_id': rule_target_category_id,
                            'product_tag_id': rule_target_product_tag_id,
                            'vit_trxid': record.get('name'),
                        }
                        rule_ids.append((0, 0, rule_data))

                    currency_id = record.get('currency_id')
                    currency_id = currency_id[0] if isinstance(currency_id, list) else currency_id
                    currency_name = next((currency['name'] for currency in currencies_source if currency['id'] == currency_id), None)
                    currency_target_id = currency_dict.get(currency_name)

                    source_pricelist_ids = record.get('pricelist_ids', [])
                    target_pricelist_ids = [pricelist_dict.get(pricelist['name']) for pricelist in pricelists_source if pricelist['id'] in source_pricelist_ids]

                    source_pos_config_ids = record.get('pos_config_ids', [])
                    target_pos_config_ids = [pos_config_dict.get(pos_config['name']) for pos_config in pos_configs_source if pos_config['id'] in source_pos_config_ids]

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
                        'id_mc': record.get('id'),
                        'pricelist_ids': target_pricelist_ids,
                        'limit_usage': record.get('limit_usage'),
                        'is_integrated': True,
                        'pos_ok': record.get('pos_ok'),
                        'sale_ok': record.get('sale_ok'),
                        'pos_config_ids': target_pos_config_ids,
                        'reward_ids': discount_loyalty_line_ids,
                        # 'schedule_ids': schedule_vals,
                        # 'member_ids': member_vals,
                        'rule_ids': rule_ids,
                    }
                    try:
                        start_time = time.time()
                        # Buat loyalty.program baru di target_client
                        new_discount_data = self.target_client.call_odoo(
                            'object', 'execute_kw', self.target_client.db,
                            self.target_client.uid, self.target_client.password,
                            'loyalty.program', 'create',
                            [discount_data]
                        )
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
                            'loyalty.program', 'write',
                            [[record['id']], {'vit_trxid': record['name'],
                                            'index_store': [(6, 0, setting_config_ids)],
                                            'reward_ids': [(1, line['id'], {'vit_trxid': record['name']}) for line in current_reward_lines],
                                            'rule_ids': [(1, rule['id'], {'vit_trxid': record['name']}) for rule in current_rule_lines],}]
                        )
                        print(f"Field is_integrated set to True for loyalty program ID {record['id']}.")

                        if len(index_store_ids) == len(setting_config_ids):
                            self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'loyalty.program', 'write',
                            [[record['id']], {'is_integrated': True, 'is_updated': False, 'index_store': [(5, 0, 0)]}])

                        else:
                            self.source_client.call_odoo(
                            'object', 'execute_kw', self.source_client.db,
                            self.source_client.uid, self.source_client.password,
                            'loyalty.program', 'write',
                            [[record['id']], {'is_integrated': False}])

                        end_time = time.time()
                        duration = end_time - start_time

                        write_date = self.get_write_date(model_name, record['id'])

                        message_succes = f"Discount baru telah dibuat dengan ID: {new_discount_data}"
                        self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, 'Discount/Loyalty', write_date)
                        self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, 'Discount/Loyalty', write_date)
                    except Exception as e:
                        message_exception = f"Terjadi kesalahan saat membuat discount baru: {e}"
                        self.set_log_ss.create_log_note_failed(record, 'Discount/Loyalty', message_exception, write_date)
                
                batch_size = 100
                for i in range(0, len(discount_loyalty), batch_size):
                    batch = discount_loyalty[i:i + batch_size]
                    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                        futures = [executor.submit(process_create_discount, record) for record in batch]
                    results = concurrent.futures.wait(futures)

            except Exception as e:
                print(f"Error during processing: {e}")