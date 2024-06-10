import time
# from set_logging import SetLogMC, SetLogSS


# kalau ada case store nya beda zona waktu gimana
class DataIntegrator:
    def __init__(self, source_client, target_client):
        self.source_client = source_client
        self.target_client = target_client
        # self.set_log_mc = SetLogMC(self.source_client)
        # self.set_log_ss = [SetLogSS(client) for client in self.target_client]



    def transfer_transaksi(self, model, fields, modul):
        data_transaksi = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                      self.target_client.uid,
                                                      self.target_client.password, model, 'search_read', [[]],
                                                      {'fields': fields})

        for record in data_transaksi:
            if (record.get('state') == 'posted'):
                start_time = time.time()

    def transfer_data(self, model, fields, modul):
        data_list = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                                 self.source_client.password, model, 'search_read', [[]],
                                                 {'fields': fields})

        # jika create_date dan write_date kemarin sampai hari ini

        for record in data_list:

            # terdapat pengecekan existing data, nama field nya mau apa
            sync_status = 'Success'
            param_existing = self.get_param_existing_data(modul)
            existing_data = self.get_existing_data(model, modul, param_existing)
            if not any(record.get(param_existing) == data.get(param_existing) for data in existing_data):
                start_time = time.time()
                valid_record = self.validate_record_data(record, model, modul)  # untuk case data type many2many
                self.create_data(model, valid_record, modul)
                end_time = time.time()
                duration = end_time - start_time

                # self.set_log_mc.create_log_note_odoo(record, modul, sync_status)
                # self.set_log_mc.create_log_runtime_odoo(start_time, end_time, duration, modul)  # masih ngelooping belum dikeluarin
            else:
                # kalau misalkan ada data, yang diupdate semua data apa difiter lagi pakai apa #apakah target client dikasih akses untuk create dan update master

                start_time = time.time()
                self.update_data(model, record, modul)
                end_time = time.time()
                duration = end_time - start_time

            # except Exception as e:
            #     print(f"Error occurred while transferring record {record.get('name')}: {e}")

    def validate_record_data(self, record, model, modul):
        try:
            # Retrieve model fields and their metadata
            type_fields = self.get_type_data_source(model)
            for field_name, field_value in record.items():
                if field_name in type_fields:
                    field_metadata = type_fields[field_name]
                    if field_metadata['type'] == 'many2one' and isinstance(field_value, list):
                        # For Many2one fields, extract ID from list
                        record[field_name] = field_value[0] if field_value else False
                    # Add more validation rules for other field types if needed
            return record
        except Exception as e:
            sync_status = f"An error occurred while validating record data: {e}"
            # self.set_log_mc.create_log_note_odoo(record, modul, sync_status)
            return None

    def update_data(self, model, record, modul):
        try:
            self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                         self.target_client.password, model, 'write', [record])
        except Exception as e:
            sync_status = f"An error occurred while update data: {e}"
            # self.set_log_mc.create_log_note_odoo(record, modul, sync_status)

    def create_data(self, model, record, modul):
        try:
            self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                         self.target_client.password, model, 'create', [record])
        except Exception as e:
            sync_status = f"An error occurred while create data: {e}"
            # self.set_log_mc.create_log_note_odoo(record, modul, sync_status)

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
            elif modul == 'Transaksi Invoice Header':
                param_existing = 'name'
            else:
                param_existing = None
            return param_existing
        except Exception as e:
            print(f"Error occurred when get param existing data: {e}")
            return None

    def get_existing_data(self, model, modul, field_uniq):
        try:
            existing_data = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password, model,
                                                        'search_read', [[]], {'fields': [field_uniq]})
            return existing_data
        except Exception as e:
            print(f"Error occurred when get existing data: {e}")
            return None

    def get_type_data_source(self, model):
        try:
            type_info = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                     self.source_client.uid, self.source_client.password,
                                                     model, 'fields_get', [], {'attributes': ['type']})
            return type_info
        except Exception as e:
            print(f"Error occurred while retrieving model fields: {e}")
            return {}
