import requests
from datetime import datetime, timedelta
import pytz
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class POSIntegration(models.Model):
    _inherit = 'pos.order'

    vit_trxid = fields.Char(string='Jubelio Transaction ID')

    def pos_jubelio_login(self, username, password):
        url = 'https://api2.jubelio.com/login'
        data = {
            'email': username,
            'password': password
        }
        response = requests.post(url, data=data)
        if response.status_code == 200:
            # Login successful
            token = response.json()['token']
            print('Login successful. Token:', token)
            return token
        else:
            # Login failed
            print('Login failed. Status code:', response.status_code)

    def taking_pos_from_jubelio_data(self, token, page_size, date_from, date_to):
        data_url = "https://api2.jubelio.com/sales/sales-returns/"
        headers = {
            "authorization": token
        }
        params = {
            "pageSize": page_size,
            "createdDateFrom": date_from,
            "createdDateTo": date_to,
            "page": 1  # Menggunakan halaman pertama sebagai awal
        }
        jubelio_data = []

        while True:
            response = requests.get(data_url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                current_page_data = data.get('data', [])
                jubelio_data.extend(current_page_data)
                total_count = data.get('totalCount', 0)

                if len(current_page_data) < page_size or len(jubelio_data) >= total_count:
                    break  # Keluar dari loop jika halaman saat ini sudah mencapai total data

                params['page'] += 1  # Pindah ke halaman berikutnya
            else:
                print('Failed to fetch data from Jubelio API. Status code:', response.status_code)
                print('Response:', response.text)
                raise Exception("Failed to fetch data from Jubelio API")

        return jubelio_data
    
    def create_pos_session(self):
        # Create a new POS session
        pos_session = self.env['pos.session'].create({
            'config_id': 1,  # Adjust config_id according to your POS configuration
            'cash_register_balance_start': 0,  # Set opening balance as needed
            'currency_id': self.env.company.currency_id.id,  # Use company currency
            # You may need to set other fields based on your requirements
        })
        return pos_session.id

    def put_into_pos_header(self, data, token, company_id):
        line_ids_commands = []
        pos_order_data = []
        orders_to_create = []
        session_id = self.create_pos_session() 

        for rec in data:
            ref_no = rec['ref_no']
            jubelio_transaction_date = rec['transaction_date']

            # Convert the datetime string to a datetime object
            jubelio_transaction_datetime = datetime.strptime(jubelio_transaction_date, "%Y-%m-%dT%H:%M:%S.%fZ")
            
            # Ensure that the datetime is naive (without timezone information)
            trans_date = jubelio_transaction_datetime.replace(tzinfo=None)
            doc_id = rec['doc_id']
            created_date = rec['created_date']
            doc_number = rec['doc_number']

            existing_data = self.env['pos.order'].search([('vit_trxid', '=', doc_number), ('company_id', '=', company_id)])
            if not existing_data:
                return_detail = self.pos_take_detail(doc_id, token)
                return_note = return_detail['note']
                return_items = []

                for return_item in return_detail['items']:
                    item_code = return_item['item_code']
                    qty_return = float(return_item['qty_in_base'])

                    users = self.env['res.partner'].search([('name', '=', 'Budi')], limit=1)
                    product = self.env['product.product'].search([('default_code', '=', 'ROTICOKLAT')], limit=1)

                    line_ids_commands.append((0, 0, {
                        'full_product_name': product.name,
                        'product_id': product.id,
                        'qty': qty_return,
                        'price_unit': 1000,
                        'price_subtotal': qty_return * 1000,
                        'price_subtotal_incl': qty_return * 1000,
                    }))

                # Calculate tax amount based on the order details (you may need to adjust this calculation based on your tax rules)
                tax_amount = sum(line[2]['price_unit'] * 0.1 for line in line_ids_commands)  # Assuming 10% tax rate

                total_amount = sum(line[2]['price_unit'] for line in line_ids_commands) + tax_amount

                amount_paid = total_amount
                amount_return = total_amount

                orders_to_create.append({
                    'partner_id': users.id,
                    'company_id': company_id,
                    'date_order': trans_date,
                    'vit_trxid': doc_number,
                    'session_id': session_id,
                    'lines': line_ids_commands,
                    'amount_tax': tax_amount,
                    'amount_total': total_amount,
                    'amount_paid': amount_paid,
                    'amount_return': amount_return,
                })

        # Create POS orders outside the loop
        for order_data in orders_to_create:
            pos_create = self.env['pos.order'].create(order_data)


    def pos_take_detail(self, doc_id, token):
        data_url = f"https://api2.jubelio.com/sales/sales-returns/{doc_id}"
        headers = {
            "authorization": token
        }
        response = requests.get(data_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print('Failed to fetch payment detail data from Jubelio API. Status code:', response.status_code)
            print('Response:', response.text)
            raise Exception("Failed to fetch payment detail data from Jubelio API")


    def pos_credentials(self, company_id):
        credentials = {
            1: {'email': 'integrasihekabekasi+test@gmail.com', 'password': '!@#MasQuerade235'},
        }

        return credentials.get(company_id, {'email': '', 'password': ''})
        
    @api.model
    def run_integration_pos(self):
        companies = self.env['res.company'].search([])
        for company in companies:
            credentials = self.pos_credentials(company.id)

            if not credentials['email'] or not credentials['password']:
                raise UserError("Company_id salah atau kredensial Jubelio hilang dari masing-masing company")

            # Get Jubelio token
            token = self.pos_jubelio_login(credentials['email'], credentials['password'])
            date_from = "2023-09-01"
            date_to = "2023-09-03"
            page_size = 200

            jubelio_data = self.taking_pos_from_jubelio_data(token, page_size, date_from, date_to)
            self.put_into_pos_header(jubelio_data, token, company.id)