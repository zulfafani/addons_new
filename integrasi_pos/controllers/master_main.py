from config import Config
from odoo_client import OdooClient
from data_integrator import DataIntegrator
from decouple import config as get_config


def main():
    file_path = r'C:\odoo\server\addons\integrasi_odoo_pos\controllers\config.json'
    key = get_config('key').encode()
    config = Config(file_path, key)

    instanceMC = config.get_instance('odoo_db_mc')
    instanceSS1 = config.get_instance('odoo_db1')
    # instanceSS2 = config.get_instance('odoo_db3')
    # instanceSS3 = config.get_instance('odoo_db4')

    decrypted_password1 = config.decrypt_password(instanceMC['password'])
    decrypted_password2 = config.decrypt_password(instanceSS1['password'])
    # decrypted_password3 = config.decrypt_password(instanceSS2['password'])
    # decrypted_password4 = config.decrypt_password(instanceSS3['password'])

    odoo_mc_client = OdooClient(instanceMC['url'], instanceMC['db'], instanceMC['username'], decrypted_password1)
    odoo_ss1_client = OdooClient(instanceSS1['url'], instanceSS1['db'], instanceSS1['username'], decrypted_password2)
    # odoo_ss2_client = OdooClient(instanceSS2['url'], instanceSS2['db'], instanceSS1['username'], decrypted_password3)
    # odoo_ss3_client = OdooClient(instanceSS3['url'], instanceSS3['db'], instanceSS1['username'], decrypted_password4)

    integrator = DataIntegrator(odoo_mc_client, [odoo_ss1_client])
    # integrator.delete_data()

    # Master MC to Store Server
    # integrator.transfer_data('res.partner', ['id','name', 'street', 'street2', 'phone', 'mobile', 'email', 'website','title','customer_rank', 'supplier_rank', 'write_uid', 'create_date', 'write_date'], 'Master Customer')
    # integrator.transfer_data('product.category', ['id', 'display_name', 'name', 'parent_id', 'property_valuation', 'write_uid', 'create_date', 'write_date'], 'Master Item Group')  # 'property_account_income_categ_id', 'product_account_expense_categ_id', 'property_cost_method', not exist
    integrator.transfer_data('product.template', ['id', 'name', 'sale_ok', 'purchase_ok', 'detailed_type', 'invoice_policy', 'uom_id', 'uom_po_id', 'list_price', 'standard_price', 'taxes_id', 'categ_id',  'default_code', 'write_uid', 'create_date', 'write_date'], 'Master Item')
    # integrator.transfer_data('res.users', ['id', 'login', 'write_uid', 'create_date', 'write_date'], 'Master Users')
    # integrator.transfer_data('stock.location', ['id', name, location_id, usage, company_id, scrap_location, return_location, replenish_location, 'write_uid', 'create_date', 'write_date'], 'Master Location')
    # integrator.transfer_data('product.pricelist', ['id', 'name', 'currency_id', 'write_uid', 'create_date', 'write_date'], Master Pricelist Header) #perlu menambah lines/ detail
    # integrator.transfer_data('product.pricelist.item', ['id', min_quantity, date_start, date_end, 'write_uid', 'create_date', 'write_date'], Master Pricelist Detail) #perlu menambah lines/ detail name, price not exist

    # Transaksi Store Server to Master Console
    integrator.transfer_transaksi('account.move', ['id', 'name', 'partner_id', 'invoice_date', 'payment_reference', 'invoice_date_due', 'journal_id', 'state', 'write_uid', 'create_date', 'write_date'], 'Transaksi Invoice Header')
    integrator.transfer_transaksi('account.move.line', ['id', 'product_id', 'name', 'account_id', 'quantity', 'product_uom_id'],  )

if __name__ == '__main__':
    main()