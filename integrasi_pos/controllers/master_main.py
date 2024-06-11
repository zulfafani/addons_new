import os
from config import Config
from odoo_client import OdooClient
from data_integrator import DataIntegrator
from decouple import config as get_config


def main():
    # base_dir = os.path.dirname(os.getcwd())
    # file_path = rf"{os.path.join(base_dir, 'integrasi_odoo_pos', 'controllers', 'config.json')}"
    file_path = r'c:\Program Files\Odoo 17.0.20231205\server\addons\integrasi_odoo_pos\controllers\config.json'
    key = get_config('key').encode()
    config = Config(file_path, key)

    instanceMC = config.get_instance('odoo_zulfa1_lt2')
    decrypted_password1 = config.decrypt_password(instanceMC['password'])
    odoo_mc_client = OdooClient(instanceMC['url'], instanceMC['db'], instanceMC['username'], decrypted_password1)

    instancesSS = [
    'odoo_zulfa2_lt2',
    ]

    for instance_name in instancesSS:
        instance = config.get_instance(instance_name)
        decrypted_password = config.decrypt_password(instance['password'])
        odoo_ss_client = OdooClient(instance['url'], instance['db'], instance['username'], decrypted_password)

        integrator = DataIntegrator(odoo_mc_client, odoo_ss_client)

        # Master MC to Store Server
        # integrator.transfer_data('res.partner', ['name', 'street', 'street2', 'phone', 'mobile', 'email', 'website','title','customer_rank', 'supplier_rank', 'customer_code'], 'Master Customer')
        # integrator.transfer_data('product.category', ['display_name', 'name', 'parent_id', 'property_valuation'], 'Master Item Group') #parent_id, parent_path
        integrator.transfer_data('product.template', ['name', 'sale_ok', 'purchase_ok', 'detailed_type', 'invoice_policy', 'uom_id', 'uom_po_id', 'list_price', 'standard_price', 'categ_id',  'default_code'], 'Master Item') # , 'taxes_id' belum ada master account.tax , 'available_in_pos'
        # integrator.transfer_data('stock.location', ['name', 'location_id', 'usage', 'scrap_location', 'return_location', 'replenish_location', 'last_inventory_date', 'next_inventory_date'], 'Master Location') # , 'company_id' belum ada master res.company
        # integrator.transfer_data('product.pricelist', ['name', 'currency_id', 'write_uid', 'create_date', 'write_date'], Master Pricelist Header) #perlu menambah lines/ detail
        # integrator.transfer_data('product.pricelist.item', [min_quantity, date_start, date_end, 'write_uid', 'create_date', 'write_date'], Master Pricelist Detail) #perlu menambah lines/ detail name, price not exist
        # integrator.transfer_data('res.users', ['login', 'write_uid', 'create_date', 'write_date'], 'Master Users')
        # Transaksi Store Server to Master Console
        # integrator.transfer_transaksi('account.move', ['name', 'partner_id', 'invoice_date', 'payment_reference', 'invoice_date_due', 'journal_id', 'state', 'move_type', 'invoice_line_ids'], 'Transaksi Invoice')
        #integrator.transfer_transaksi('account.move.line', ['product_id', 'name', 'quantity', 'product_uom_id'],  )

if __name__ == '__main__':
    main()