import time
from datetime import datetime, timedelta
import re


class DataIntegrator:
    def __init__(self, source_client, target_client):
        self.source_client = source_client
        self.target_client = target_client
        self.set_log_mc = SetLogMC(self.source_client)
        self.set_log_ss = SetLogSS(self.target_client)

    def get_field_uniq_from_model(self, model):
        try:
            if model == 'res.partner':
                field_uniq = 'customer_code'
            elif model == 'product.template':
                field_uniq = 'default_code'
            elif model == 'product.category':
                field_uniq = 'complete_name'
            elif model == 'res.users':
                field_uniq = 'login'
            elif model == 'stock.location':
                field_uniq = 'complete_name'
            elif model == 'account.account':
                field_uniq = 'code'
            else:
                field_uniq = 'name'
            return field_uniq
        except Exception as e:
            print(f"Error occurred when get param existing data: {e}")

    # Master Console --> Store Server
    def get_existing_data(self, model, field_uniq):
        try:
            existing_data = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password, model,
                                                        'search_read', [[[field_uniq, '!=', False]]], {'fields': [field_uniq]})
            return existing_data
        except Exception as e:
            print(f"Error occurred when get existing data: {e}")

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

    def get_company_id(self, field_uniq):
        try:
            company_name_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                                        self.source_client.password, 'res.company', 'search_read', [[[field_uniq, '!=', False]]],
                                                        {'fields': ['name']})
            company_name_target = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                                self.target_client.password, 'res.company', 'search_read', [[[field_uniq, '!=', False]]],
                                                {'fields': ['name']})
            
            existing_company = {data['name'] for data in company_name_target}
            existing_company_str_one = next(iter(existing_company))

            company_id_source = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                                self.source_client.password, 'res.company', 'search_read', [[['name', '=', existing_company_str_one]]],
                                                {'fields': ['id']})
            company_id_source_dict = next(iter(company_id_source))
            company_id_source_str_one = company_id_source_dict['id']
            return company_id_source_str_one
        except Exception as e:
            print(f"Error occurred when get company id: {e}")
    
    def get_data_list(self, model, fields, field_uniq):
        try:
            if model == 'res.partner' or model == 'product.template':
                data_list = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                                    self.source_client.password, model, 'search_read', [[[field_uniq, '!=', False], ['is_integrated', '!=', True]]],
                                                    {'fields': fields})
            elif model == 'account.tax':
                company_id = self.get_company_id(field_uniq)
                data_list = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                                    self.source_client.password, model, 'search_read', [[[field_uniq, '!=', False], ['company_id', '=', company_id]]],
                                                    {'fields': fields})
            else:
                data_list = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                                    self.source_client.password, model, 'search_read', [[[field_uniq, '!=', False]]],
                                                    {'fields': fields})
            return data_list
        except Exception as e:
            print(f"Error occurred when get data list: {e}")

    def transfer_data(self, model, fields, modul):
        try:
            field_uniq = self.get_field_uniq_from_model(model)
            data_list = self.get_data_list(model, fields, field_uniq)
            existing_data = {data[field_uniq] for data in self.get_existing_data(model, field_uniq)}

            for record in data_list:
                code = record.get(field_uniq)

                if code not in existing_data:
                    valid_record = self.validate_record_data(record, model)
                    if valid_record is None:
                        continue  # Jika valid_record None, lewati iterasi ini
                    self.create_data(model, valid_record, modul)

                else:
                    target_record = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                                            self.target_client.password, model, 'search_read', [[[field_uniq, '=', code]]],
                                                            {'fields': fields})

                    for record_target in target_record:
                        updated_fields = {field: record[field] for field in fields if record.get(field) != record_target.get(field)}
                        if 'categ_id' in updated_fields:
                            if record.get('categ_id', [None, None])[1] == record_target.get('categ_id', [None, None])[1]:
                                del updated_fields['categ_id']
                        if 'parent_id' in updated_fields:
                            if record.get('parent_id', [None, None])[1] == record_target.get('parent_id', [None, None])[1]:
                                del updated_fields['parent_id']
                        if 'location_id' in updated_fields:
                            if record.get('location_id', [None, None])[1] == record_target.get('location_id', [None, None])[1]:
                                del updated_fields['location_id']
                        if 'partner_id' in updated_fields:
                            if record.get('partner_id', [None, None])[1] == record_target.get('partner_id', [None, None])[1]:
                                del updated_fields['partner_id']
                        if 'invoice_repartition_line_ids' and 'refund_repartition_line_ids' in updated_fields:
                            del updated_fields['invoice_repartition_line_ids']
                            del updated_fields['refund_repartition_line_ids']
                        if 'sequence_id' in updated_fields:
                            if record.get('sequence_id', [None, None])[1] == record_target.get('sequence_id', [None, None])[1]:
                                del updated_fields['sequence_id']
                        if 'warehouse_id' in updated_fields:
                            if record.get('warehouse_id', [None, None])[1] == record_target.get('warehouse_id', [None, None])[1]:
                                del updated_fields['warehouse_id']
                        if 'default_location_src_id' in updated_fields:
                            if record.get('default_location_src_id', [None, None])[1] == record_target.get('default_location_src_id', [None, None])[1]:
                                del updated_fields['default_location_src_id']
                        if 'return_picking_type_id' in updated_fields:
                            if record.get('return_picking_type_id', [None, None])[1] == record_target.get('return_picking_type_id', [None, None])[1]:
                                del updated_fields['return_picking_type_id']
                        if 'default_location_dest_id' in updated_fields:
                            if record.get('default_location_dest_id', [None, None])[1] == record_target.get('default_location_dest_id', [None, None])[1]:
                                del updated_fields['default_location_dest_id']
                        
                        valid_record = self.validate_record_data(updated_fields, model)
                        
                        if valid_record:
                            record_id = record_target.get('id')
                            self.update_data(model, record_id, valid_record, modul, record)
        except Exception as e:
            print(f"Error occurred while transferring record {record.get('name')}: {e}")

    def validate_record_data(self, record, model):
        try:
            type_fields = self.get_type_data_source(model)
            relation_fields = self.get_relation_data_source(model)

            for field_name, field_value in record.items():
                if field_name in type_fields:
                    field_metadata = type_fields[field_name]['type']
                    if (field_metadata == 'many2one' or field_metadata == 'many2many') and isinstance(field_value, list):
                        if (field_metadata == 'many2one'):
                            field_data = field_value[1] if field_value else False
                        if (field_name == 'tag_ids'):
                            field_data = field_value[0] if field_value else False
                            if field_data:
                                tag_tax = self.get_account_tag_source(field_data, 'account.account.tag')
                                field_data = tag_tax[0]['name']
                        if (field_name == 'taxes_id'):
                            field_data = field_value[0] if field_value else False
                            if field_data:
                                name_tax = self.get_account_tax_source(field_data, 'account.tax')
                                field_data = name_tax[0]['name']
                     
                        if field_name in relation_fields:
                            relation_model_info = relation_fields[field_name]
                            if isinstance(relation_model_info, dict) and 'relation' in relation_model_info:
                                relation_model = relation_model_info['relation']

                                if isinstance(relation_model, str):
                                    field_uniq = self.get_field_uniq_from_model(relation_model)

                                    if model == 'product.pricelist.item':
                                        pattern = r'\[(.*?)\]'
                                        match = re.search(pattern, field_data)
                                        field_data = match.group(1)

                                    if relation_model == 'account.account':
                                        # Menggunakan split untuk memisahkan string
                                        parts = field_data.split()
                                        # Mengambil bagian pertama yang merupakan angka
                                        field_data = parts[0]
  
                                    datas = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                            self.target_client.uid, self.target_client.password,
                                                            relation_model, 'search_read',
                                                            [[[field_uniq, '=', field_data]]], {'fields': ['id']})
                                    
                                    if datas:
                                        if field_name == 'tag_ids' or field_name == 'taxes_id':
                                            value = datas[0]['id'] if datas[0] else False
                                            # Jika value ada dan bukan list, bungkus dalam list
                                            record[field_name] = [value] if value and not isinstance(value, list) else value
                                        else:
                                            record[field_name] = datas[0]['id'] if datas[0] else False
                                    else:
                                        if model == 'account.tax.repartition.line':
                                            record[field_name] = field_value[0] if field_value else False
                                        else:
                                            return None  # Mengembalikan None jika kondisi else terpenuhi
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

    def get_relation_data_source(self, model):
        try:
            relation_info = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                     self.target_client.uid, self.target_client.password,
                                                     model, 'fields_get', [], {'attributes': ['relation']})
            return relation_info
        except Exception as e:
            print(f"Error occurred while get relation data for fields: {e}")

    def get_account_tag_source(self, field_data, model):
        try:
            account_tag = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                     self.source_client.uid, self.source_client.password,
                                                     model, 'search_read', [[['id', '=', field_data]]], {'fields': ['name']})
            return account_tag
        except Exception as e:
            print(f"Error occurred while get account tag: {e}")

    def get_account_tax_source(self, field_data, model):
        try:
            account_tax = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                     self.source_client.uid, self.source_client.password,
                                                     model, 'search_read', [[['id', '=', field_data]]], {'fields': ['name']})
            return account_tax
        except Exception as e:
            print(f"Error occurred while get account tax: {e}")

    def create_data(self, model, record, modul):
        try:
            if model == 'product.pricelist':
                record['item_ids'] = self.transfer_pricelist_lines(record['id'], 'product.pricelist.item')
            if model == 'account.tax':
                record['invoice_repartition_line_ids'] = self.transfer_tax_lines_invoice(record['id'], 'account.tax.repartition.line')
                record['refund_repartition_line_ids'] = self.transfer_tax_lines_refund(record['id'], 'account.tax.repartition.line')

            start_time = time.time()
            create = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                         self.target_client.password, model, 'create', [record])
            end_time = time.time()
            duration = end_time - start_time

            id = record.get('id')
            if create:
                self.update_isintegrated_source(model, id)

                write_date = self.get_write_date(model, id)

                self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, modul, write_date)
                self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, modul, write_date)
        except Exception as e:
            print(f"An error occurred while create data: {e}")
            id = record.get('id')
            write_date = self.get_write_date(model, id)
            self.set_log_mc.create_log_note_failed(record, modul, e, write_date)
            self.set_log_ss.create_log_note_failed(record, modul, e, write_date)

    def transfer_pricelist_lines(self, pricelist_id, model):
        try:
            lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                self.source_client.uid, self.source_client.password,
                                                model, 'search_read',
                                                [[['pricelist_id', '=', pricelist_id]]],
                                                {'fields': ['product_tmpl_id','min_quantity', 'fixed_price', 'date_start', 'date_end']})

            formatted_invoice_lines = []
            for line in lines:
                valid_lines = self.validate_record_data(line, model)
                formatted_invoice_lines.append((0, 0, valid_lines))

            return formatted_invoice_lines
        except Exception as e:
            sync_status = f"An error occurred while transfer pricelist lines: {e}"
            print(sync_status)

    def transfer_tax_lines_invoice(self, tax_id, model):
        try:
            lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                self.source_client.uid, self.source_client.password,
                                                model, 'search_read',
                                                [[['tax_id', '=', tax_id], ['document_type', '=', 'invoice']]],
                                                {'fields': ['tax_id','factor_percent','repartition_type', 'account_id', 'tag_ids', 'document_type', 'use_in_tax_closing']})

            formatted_invoice_lines = []
            for line in lines:
                valid_lines = self.validate_record_data(line, model)
                formatted_invoice_lines.append((0, 0, valid_lines))

            return formatted_invoice_lines
        except Exception as e:
            sync_status = f"An error occurred while transfer tax lines: {e}"
            print(sync_status)

    def transfer_tax_lines_refund(self, tax_id, model):
        try:
            lines = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                self.source_client.uid, self.source_client.password,
                                                model, 'search_read',
                                                [[['tax_id', '=', tax_id], ['document_type', '=', 'refund']]],
                                                {'fields': ['tax_id','factor_percent','repartition_type', 'account_id', 'tag_ids', 'document_type', 'use_in_tax_closing']})

            formatted_invoice_lines = []
            for line in lines:
                valid_lines = self.validate_record_data(line, model)
                formatted_invoice_lines.append((0, 0, valid_lines))

            return formatted_invoice_lines
        except Exception as e:
            sync_status = f"An error occurred while transfer tax lines: {e}"
            print(sync_status)
    
    def update_data(self, model, record_id, updated_fields, modul, record):
        try:
            start_time = time.time()
            update = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                        self.target_client.password, model, 'write', [[record_id], updated_fields])
            end_time = time.time()
            duration = end_time - start_time

            id = record.get('id')
            if update:
                self.update_isintegrated_source(model, id)

                write_date = self.get_write_date(model, id)

                self.set_log_mc.create_log_note_update_success(record,record_id, updated_fields, start_time, end_time, duration, modul, write_date)
                self.set_log_ss.create_log_note_update_success(record,record_id, updated_fields, start_time, end_time, duration, modul, write_date)

        except Exception as e:
            print(f"An error occurred while updating data: {e}")
            id = record.get('id')
            write_date = self.get_write_date(model, id)
            self.set_log_mc.create_log_note_failed(record, modul, e, write_date)
            self.set_log_ss.create_log_note_failed(record, modul, e, write_date)

    def update_isintegrated_source(self, model, id):
        try:
            if model == 'res.partner' or model == 'product.template':
                self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                            self.source_client.password, model, 'write', [id, {'is_integrated': True}])
  
        except Exception as e:
            print(f"Error occurred when update is_integrated : {e}")

    def update_isintegrated_from_ss(self, model, id):
        try:
            if model == 'res.partner' or model == 'product.template':
                self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                            self.source_client.password, model, 'write', [id, {'is_integrated': False}])
  
        except Exception as e:
            print(f"Error occurred when update is_integrated : {e}")


    
    

    # Store Server --> Master Console
    def get_existing_data_mc(self, model, field_uniq):
        try:
            existing_data = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                        self.source_client.uid, self.source_client.password, model,
                                                        'search_read', [[[field_uniq, '!=', False]]], {'fields': [field_uniq]})
            return existing_data
        except Exception as e:
            print(f"Error occurred when get existing data: {e}")

    def get_data_list_ss(self, model, fields, field_uniq):
        try:
            # hanya model res.partner.title, res.partner, hr.employee
            if model == 'res.partner':
                data_list = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                                    self.target_client.password, model, 'search_read', [[[field_uniq, '!=', False], ['is_integrated', '=', True]]],
                                                    {'fields': fields})
            elif model == 'res.partner.title' or model == 'hr.employee':
                data_list = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                                    self.target_client.password, model, 'search_read', [[[field_uniq, '!=', False]]],
                                                    {'fields': fields})
            return data_list
        except Exception as e:
            print(f"Error occurred when get data list: {e}")

    def get_write_date_ss(self, model, id):
        try:
            write_date = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                        self.target_client.uid, self.target_client.password, model,
                                                        'search_read', [[['id', '=', id]]], {'fields': ['write_date']})
            if write_date:
                write_date_value = write_date[0]['write_date']
                return write_date_value
        except Exception as e:
            print(f"Error occurred when get write date: {e}")

    
    def transfer_data_mc(self, model, fields, modul):
        try:
            field_uniq = self.get_field_uniq_from_model(model)
            data_list = self.get_data_list_ss(model, fields, field_uniq)
            existing_data = {data[field_uniq] for data in self.get_existing_data_mc(model, field_uniq)}

            for record in data_list:
                code = record.get(field_uniq)
                    
                if code not in existing_data:
                    valid_record = self.validate_record_data_mc(record, model)
                    self.create_data_mc(model, valid_record, modul)

                else:
                    target_record = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                                            self.source_client.password, model, 'search_read', [[[field_uniq, '=', code]]],
                                                            {'fields': fields})
                    
                    for record_target in target_record:
                        updated_fields = {field: record[field] for field in fields if record.get(field) != record_target.get(field)}
                        valid_record = self.validate_record_data_mc(updated_fields, model)
                        if valid_record:
                            record_id = record_target.get('id')
                            self.update_data_mc(model, record_id, valid_record, modul, record)
        except Exception as e:
            print(f"Error occurred while transferring record {record.get('name')}: {e}")

    def validate_record_data_mc(self, record, model):
        try:
            type_fields = self.get_type_data_source_mc(model)
            relation_fields = self.get_relation_data_source_mc(model)

            for field_name, field_value in record.items():
                if field_name in type_fields:
                    field_metadata = type_fields[field_name]['type']
                    if (field_metadata == 'many2one' or field_metadata == 'many2many') and isinstance(field_value, list):
                        field_data = field_value[1] if field_value else False
                     
                        if field_name in relation_fields:
                            relation_model_info = relation_fields[field_name]
                            if isinstance(relation_model_info, dict) and 'relation' in relation_model_info:
                                relation_model = relation_model_info['relation']

                                if isinstance(relation_model, str):
                                    field_uniq = self.get_field_uniq_from_model(relation_model)
                                    datas = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                            self.source_client.uid, self.source_client.password,
                                                            relation_model, 'search_read',
                                                            [[[field_uniq, '=', field_data]]], {'fields': ['id']})
                                    
                                    if datas:
                                        record[field_name] = datas[0]['id'] if datas[0] else False

            return record
        except Exception as e:
            print(f"An error occurred while validating record data: {e}")

    def get_type_data_source_mc(self, model):
        try:
            type_info = self.target_client.call_odoo('object', 'execute_kw', self.target_client.db,
                                                     self.target_client.uid, self.target_client.password,
                                                     model, 'fields_get', [], {'attributes': ['type']})
            return type_info
        except Exception as e:
            print(f"Error occurred while get data type for fields: {e}")

    def get_relation_data_source_mc(self, model):
        try:
            relation_info = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db,
                                                     self.source_client.uid, self.source_client.password,
                                                     model, 'fields_get', [], {'attributes': ['relation']})
            return relation_info
        except Exception as e:
            print(f"Error occurred while get data type for fields: {e}")

    def create_data_mc(self, model, record, modul):
        try:
            start_time = time.time()
            create = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                         self.source_client.password, model, 'create', [record])
            end_time = time.time()
            duration = end_time - start_time

            id = record.get('id')
            if create:
                write_date = self.get_write_date_ss(model, id)

                self.set_log_mc.create_log_note_success(record, start_time, end_time, duration, modul, write_date)
                self.set_log_ss.create_log_note_success(record, start_time, end_time, duration, modul, write_date)
        except Exception as e:
            print(f"An error occurred while create data: {e}")
            id = record.get('id')
            write_date = self.get_write_date_ss(model, id)
            self.set_log_mc.create_log_note_failed(record, modul, e, write_date)
            self.set_log_ss.create_log_note_failed(record, modul, e, write_date)
    
    def update_data_mc(self, model, record_id, updated_fields, modul, record):
        try:
            start_time = time.time()
            update = self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                        self.source_client.password, model, 'write', [[record_id], updated_fields])
            end_time = time.time()
            duration = end_time - start_time

            id = record.get('id')
            if update:
                self.update_isintegrated_from_ss(model, record_id)

                write_date = self.get_write_date_ss(model, id)

                self.set_log_mc.create_log_note_update_success(record, record_id, updated_fields, start_time, end_time, duration, modul, write_date)
                self.set_log_ss.create_log_note_update_success(record, record_id, updated_fields, start_time, end_time, duration, modul, write_date)

        except Exception as e:
            print(f"An error occurred while updating data: {e}")
            id = record.get('id')
            write_date = self.get_write_date_ss(model, id)
            self.set_log_mc.create_log_note_failed(record, modul, e, write_date)
            self.set_log_ss.create_log_note_failed(record, modul, e, write_date)

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
        
        # Convert the exception message to a string
        exception_message_str = str(sync_status)
        pattern = re.compile(r"'message': '(.+?)', 'arguments'")
        match = pattern.search(exception_message_str)
        if match:
            error_message = match.group(1)
        
        record_log_failed = {
            'vit_doc_type': modul,
            'vit_trx_key': record.get('name'),
            'vit_trx_date': write_date,
            'vit_sync_date': gmt_7_now.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_sync_status': 'Failed',
            'vit_sync_desc': f"Tidak dapat create data {error_message}"
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
            self.source_client.call_odoo('object', 'execute_kw', self.source_client.db, self.source_client.uid,
                                         self.source_client.password, 'log.note', 'create', [log_record])
            print(f"Data log note yang masuk: {log_record}")
        except Exception as e:
            print(f"An error occurred while creating log note: {e}")


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
        
        # Convert the exception message to a string
        exception_message_str = str(sync_status)
        pattern = re.compile(r"'message': '(.+?)', 'arguments'")
        match = pattern.search(exception_message_str)
        if match:
            error_message = match.group(1)
        
        record_log_failed = {
            'vit_doc_type': modul,
            'vit_trx_key': record.get('name'),
            'vit_trx_date': write_date,
            'vit_sync_date': gmt_7_now.strftime('%Y-%m-%d %H:%M:%S'),
            'vit_sync_status': 'Failed',
            'vit_sync_desc': f"Tidak dapat create data {error_message}"
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
            self.target_client.call_odoo('object', 'execute_kw', self.target_client.db, self.target_client.uid,
                                         self.target_client.password, 'log.note', 'create', [log_record])
            print(f"Data log note yang masuk: {log_record}")
        except Exception as e:
            print(f"An error occurred while creating log note: {e}")