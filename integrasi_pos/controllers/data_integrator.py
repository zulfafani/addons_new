import time
<<<<<<< HEAD
# from set_logging import SetLogMC, SetLogSS
=======
from datetime import datetime, timedelta
>>>>>>> a3b005dfc957049c817262ac04f7b44bb96f2638


# kalau ada case store nya beda zona waktu gimana
class DataIntegrator:
    def __init__(self, source_client, target_client):
        self.source_client = source_client
        self.target_client = target_client
        self.set_log_mc = SetLogMC(self.source_client)
        self.set_log_ss = SetLogSS(self.target_client)

    # Master Console --> Store Server
    def transfer_data(self, model, fields, modul):
        data_list = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                                 self.source_client.password, model, 'search_read', [[]],
                                                 {'fields': fields})

        for record in data_list:
            param_existing = self.get_param_existing_data(modul)

            code = record.get(param_existing)
            if code is not False:
                existing_data = self.get_existing_data(model, param_existing)
                if not any(record.get(param_existing) == data.get(param_existing) for data in existing_data):
                    valid_record = self.validate_record_data(record, model)  # untuk case data type many2many
                    self.create_data(model, valid_record, modul)

                else:
                    target_record = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                                            self.target_client.password, model, 'search_read', [[[param_existing, '=', code]]],
                                                            {'fields': fields})
                    
                    for record_target in target_record:
                        updated_fields = {field: record[field] for field in fields if record.get(field) != record_target.get(field)}
                        if updated_fields:
                            record_id = record_target.get('id')
                            self.update_data(model, record_id, updated_fields, modul)
            #else:
                # print(f"Gagal sync karena {param_existing} masih kosong")

            # except Exception as e:
            #     print(f"Error occurred while transferring record {record.get('name')}: {e}")

    def get_param_existing_data(self, modul):
        try:
            if modul == 'Master Customer':
                param_existing = 'customer_code'
            elif modul == 'Master Item':
                param_existing = 'default_code'
            elif modul == 'Master Item Group':
                param_existing = 'display_name'
            elif modul == 'Master Users':
                param_existing = 'login'
            elif modul == 'Master Location':
                param_existing = 'complete_name'
            elif modul == 'Master Pricelist Header':
                param_existing = 'name'
            else:
                param_existing = None
            return param_existing
        except Exception as e:
            print(f"Error occurred when get param existing data: {e}")
            return None

    def get_field_uniq(self, model):
        try: 
            if model == 'res.partner':
                field_uniq = 'customer_code'
            elif model == 'product.template':
                field_uniq = 'customer_code'
            else:
                print("model not exist")
            return field_uniq
        except Exception as e:
            print(f"Error occurred when get field uniq: {e}")

    def get_existing_data(self, model, field_uniq):
        try:
            existing_data = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password, model,
                                                        'search_read', [[]], {'fields': [field_uniq]})
            return existing_data
        except Exception as e:
            print(f"Error occurred when get existing data: {e}")

    def validate_record_data(self, record, model):
        try:
            type_fields = self.get_type_data_source(model)
            for field_name, field_value in record.items():
                if field_name in type_fields:
                    field_metadata = type_fields[field_name]['type']
                    if field_metadata == 'many2one' and isinstance(field_value, list):
                        field_data = field_value[1] if field_value else False
                        
                        relation_fields = self.get_relation_data_source(model)
                        if field_name in relation_fields:
                            relation_model_info = relation_fields[field_name]
                            if isinstance(relation_model_info, dict) and 'relation' in relation_model_info:
                                relation_model = relation_model_info['relation']

                                if isinstance(relation_model, str):
                                    datas = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            relation_model, 'search_read',
                                                            [[['name', '=', field_data]]], {'fields': ['id']})
                                    
                                    if datas:
                                    # Assuming we want the first matching record's ID
                                        record[field_name] = datas[0]['id'] if datas[0] else False
                                    else:
                                    # If no matching record is found, set to False or handle accordingly
                                        record[field_name] = field_value[0] if field_value else False
            return record
        except Exception as e:
            print(f"An error occurred while validating record data: {e}")

    def get_type_data_source(self, model):
        try:
            type_info = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                     self.source_client.uid, self.source_client.password,
                                                     model, 'fields_get', [], {'attributes': ['type']})
            return type_info
        except Exception as e:
            print(f"Error occurred while get data type for fields: {e}")

    def update_data(self, model, record_id, updated_fields, modul):
        try:
            start_time = time.time()
            self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                        self.target_client.password, model, 'write', [[record_id], updated_fields])
            end_time = time.time()
            duration = end_time - start_time

            print(f"data {modul} yang update: record id {record_id} dan data {updated_fields}")
            # self.set_log_mc.create_log_note_success(record, modul)
            self.set_log_mc.create_log_runtime_odoo(start_time, end_time, duration, modul)  # masih ngelooping belum dikeluarin
            # self.set_log_ss.create_log_note_success(record, modul)
            self.set_log_ss.create_log_runtime_odoo(start_time, end_time, duration, modul)

        except Exception as e:
            sync_status = f"An error occurred while updating data: {e}"
            # self.set_log_mc.create_log_note_failed(record, modul, sync_status)
            # self.set_log_ss.create_log_note_failed(record, modul, sync_status)

    def create_data(self, model, record, modul):
        try:
            start_time = time.time()
            self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                         self.target_client.password, model, 'create', [record])
            end_time = time.time()
            duration = end_time - start_time

            self.set_log_mc.create_log_note_success(record, modul)
            self.set_log_mc.create_log_runtime_odoo(start_time, end_time, duration, modul)  # masih ngelooping belum dikeluarin
            self.set_log_ss.create_log_note_success(record, modul)
            self.set_log_ss.create_log_runtime_odoo(start_time, end_time, duration, modul)
        except Exception as e:
            sync_status = f"An error occurred while create data: {e}"
            self.set_log_mc.create_log_note_failed(record, modul, sync_status)
            self.set_log_ss.create_log_note_failed(record, modul, sync_status)
    
    # Store Server --> Master Console
    def transfer_transaksi(self, model, fields, modul):
        get_data_transaksi = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                      self.target_client.uid,
                                                      self.target_client.password, model, 'search_read', [[]],
                                                      {'fields': fields})

        for record in get_data_transaksi:
            if (record.get('state') == 'posted'):
                record['state'] = 'draft'
                
                valid_record = self.validate_record_data_target(record, model)
                if valid_record:
                    self.create_data_transaksi(model, valid_record, modul)
                

                # Mengeksekusi tombol "Konfirmasi" untuk objek account.move
                # move_id = valid_record.get('id')  # Mengambil ID dari data transaksi yang baru saja dibuat
                # if move_id:
                self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                self.source_client.uid, self.source_client.password,
                                                'account.move', 'action_post', [[]])

                # new_id = self.create_data_transaksi(model, record)
                # if new_id:
                # self.transfer_invoice_lines(new_id, record['id'])
                # self.post_transaction(model, new_id)

    def validate_record_data_target(self, record, model):
        try:
            type_fields = self.get_type_data_target(model)
            for field_name, field_value in record.items():
                if field_name in type_fields:
                    field_metadata = type_fields[field_name]
                    if field_metadata['type'] == 'many2one' and isinstance(field_value, list):
                        field_data = field_value[0] if field_value else False
                        
                        relation_fields = self.get_relation_data_target(model)
                        if field_name in relation_fields:
                            relation_model_info = relation_fields[field_name]
                            if isinstance(relation_model_info, dict) and 'relation' in relation_model_info:
                                relation_model = relation_model_info['relation']

                                field_uniq = self.get_field_uniq(relation_model)
                                if field_uniq:
                                    data_targets = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            relation_model, 'search_read',
                                                            [[['id', '=', field_data]]], {'fields': [field_uniq]})
                                
                                    if data_targets:
                                        for data_target in data_targets:
                                            if field_uniq in data_target:
                                                data_field_uniq = data_target[field_uniq]

                                                if isinstance(relation_model, str):
                                                    datas = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                                                self.source_client.uid, self.source_client.password,
                                                                                relation_model, 'search_read',
                                                                                [[[field_uniq, '=', data_field_uniq]]], {'fields': ['id']})
                                                    
                                                    if datas:
                                                        for data in datas:
                                                            if field_uniq in data:
                                                                record[field_name] = datas[0]['id'] if datas[0] else False
                                                            else:
                                                                print(f"Field '{field_uniq}' not found in data: {data}")
                                                    else:
                                                        print("No data targets found")
                                            else:
                                                print(f"Field '{field_uniq}' not found in data_target: {data_target}")
                                    else:
                                        print("No data targets found")
                                else:
                                    record[field_name] = datas[0]['id'] if datas[0] else False                  
                                
            return record
        except Exception as e:
            print(f"An error occurred while validating record data: {e}")

    def get_type_data_target(self, model):
        try:
            type_info = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                     self.target_client.uid, self.target_client.password,
                                                     model, 'fields_get', [], {'attributes': ['type']})
            return type_info
        except Exception as e:
            print(f"Error occurred while get data type for fields: {e}")

    def get_relation_data_target(self, model):
        try:
            relation_info = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                     self.target_client.uid, self.target_client.password,
                                                     model, 'fields_get', [], {'attributes': ['relation']})
            return relation_info
        except Exception as e:
            print(f"Error occurred while get data type for fields: {e}")

    def get_relation_data_source(self, model):
        try:
            relation_info = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                     self.target_client.uid, self.target_client.password,
                                                     model, 'fields_get', [], {'attributes': ['relation']})
            return relation_info
        except Exception as e:
            print(f"Error occurred while get data type for fields: {e}")

    
    def get_journal_id_by_name(self, journal_name):
        journals = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                self.source_client.uid, self.source_client.password,
                                                'account.journal', 'search_read',
                                                [[['name', '=', journal_name]]], {'fields': ['id']})
        if journals:
            return journals[0]['id']
        return None
    
    def get_partner_id_by_name(self, partner_name):
        partners = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                self.source_client.uid, self.source_client.password,
                                                'res.partner', 'search_read',
                                                [[['name', '=', partner_name]]], {'fields': ['id']})
        if partners:
            return partners[0]['id']
        return None

    def transfer_invoice_lines(self, old_move_id, model):
        try:
            lines = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                self.target_client.uid, self.target_client.password,
                                                'account.move.line', 'search_read',
                                                [[['move_id', '=', old_move_id]]],
                                                {'fields': ['product_id', 'name', 'quantity', 'product_uom_id', 'price_unit', 'tax_ids', 'price_subtotal']})

            formatted_invoice_lines = []
            for line in lines:
                valid_lines = self.validate_record_data_target(line, model)
                formatted_invoice_lines.append((0, 0, valid_lines))
            
            """
            for line in lines:
                # Ubah account_id dari nama menjadi ID
                
                account_name = line.get('account_id')
                if account_name:
                    account_id = self.get_account_id_by_code_or_name(account_name)
                    if account_id:
                        line['account_id'] = account_id
                    else:
                        print(f"Account with name or code {account_name} not found.")
                        continue
                

                line['move_id'] = new_move_id
                self.create_data_line('account.move.line', line)
            """
            return formatted_invoice_lines

        except Exception as e:
            sync_status = f"An error occurred while transfer invoice lines: {e}"
            print(sync_status)

    def get_account_id_by_code_or_name(self, account_code_or_name):
        accounts = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                self.source_client.uid, self.source_client.password,
                                                'account.account', 'search_read',
                                                [[['code', '=', account_code_or_name], ['name', '=', account_code_or_name]]],
                                                {'fields': ['id']})
        if accounts:
            return accounts[0]['id']
        return None

    def post_transaction(self, model, record_id):
        try:
            self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                         self.source_client.uid, self.source_client.password,
                                         model, 'action_post', [record_id])
        except Exception as e:
            print(f"Error occurred while posting transaction {record_id}: {e}")
    
    def create_data_line(self, model, record):
        try:
            self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                         self.source_client.password, model, 'create', [record])
        except Exception as e:
            sync_status = f"An error occurred while creating data line: {e}"
            print(sync_status)

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
