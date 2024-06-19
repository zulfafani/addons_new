import os
from config import Config
from odoo_client import OdooClient
from data_transaksi import DataTransaksi
from data_integrator import DataIntegrator
from decouple import config as get_config


def main():
    # base_dir = os.path.dirname(os.getcwd())
    # file_path = rf"{os.path.join(base_dir, 'integrasi_odoo', 'controllers', 'config.json')}"
    file_path = r"C:\Users\abhif\OneDrive\Documents\Development Testing\zulfa's_code\addons_new\integrasi_pos\controllers/config.json"
    key = get_config('key').encode()
    config = Config(file_path, key)

    instanceMC = config.get_instance('odoo_db1')
    # decrypted_password1 = config.decrypt_password(instanceMC['password'])
    odoo_mc_client = OdooClient(instanceMC['url'], instanceMC['db'], instanceMC['username'], instanceMC['password'])

    instancesSS = [
    'odoo_db1a',
    ]

    for instance_name in instancesSS:
        instance = config.get_instance(instance_name)
        # decrypted_password = config.decrypt_password(instance['password'])
        odoo_ss_client = OdooClient(instance['url'], instance['db'], instance['username'], instance['password'])

        integrator_master = DataIntegrator(odoo_mc_client, odoo_ss_client)
        integrator_transaksi = DataTransaksi(odoo_ss_client, odoo_mc_client)
        

        # Master MC to Store Server
        # integrator_master.transfer_data('res.partner.title', ['name', 'shortcut'], 'Master Customer Title')
        # integrator_master.transfer_data('res.partner', ['name', 'street', 'street2', 'phone', 'mobile', 'email', 'website','title','customer_rank', 'supplier_rank', 'customer_code'], 'Master Customer')
        # integrator_master.transfer_data('product.category', ['complete_name', 'name', 'parent_id', 'property_valuation'], 'Master Item Group')
        # integrator_master.transfer_data('product.template', ['name', 'sale_ok', 'purchase_ok', 'detailed_type', 'invoice_policy', 'uom_id', 'uom_po_id', 'list_price', 'standard_price', 'categ_id',  'default_code', 'available_in_pos'], 'Master Item') # , 'taxes_id' belum ada master account.tax , 
        # integrator_master.transfer_data('stock.location', ['complete_name', 'name', 'location_id', 'usage', 'scrap_location', 'return_location', 'replenish_location', 'last_inventory_date', 'next_inventory_date'], 'Master Location') # , 'company_id' belum ada master res.company
        # integrator_master.transfer_data('product.pricelist', ['name', 'currency_id', 'item_ids'], 'Master Pricelist')
        # integrator_master.transfer_data_mc('hr.employee', ['name', 'mobile_phone', 'work_phone', 'work_email'], 'Sales Employee')
        # Master Store Server to MC
        # integrator_master.transfer_data_mc('res.partner.title', ['name', 'shortcut'], 'Master Customer Title')
        # integrator_master.transfer_data_mc('res.partner', ['name', 'street', 'street2', 'phone', 'mobile', 'email', 'website','title','customer_rank', 'supplier_rank', 'customer_code'], 'Master Customer')
        # Transaksi Store Server to Master Console
        # integrator_transaksi.transfer_transaksi('account.move', ['name', 'partner_id', 'invoice_date', 'payment_reference', 'invoice_date_due', 'journal_id', 'state', 'ref', 'move_type', 'invoice_line_ids'], 'Transaksi Invoice')
        # integrator_transaksi.transfer_pos_order_inventory('stock.picking', ['name', 'partner_id', 'location_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.update_session_status('pos.session', ['name', 'state', 'start_at', 'stop_at', 'cash_register_balance_start', 'cash_register_balance_end_real'], 'Update Session PoS Order')
        # integrator_transaksi.transfer_pos_order_session('pos.session', ['name', 'config_id', 'user_id', 'start_at', 'stop_at', 'state'], 'Master Session PoS Order Invoice')
        # integrator_transaksi.transfer_pos_order_invoice('pos.order', ['name', 'date_order', 'session_id', 'user_id', 'partner_id', 'pos_reference', 'order_ref', 'tracking_number', 'margin', 'amount_tax', 'amount_total', 'amount_paid', 'amount_return', 'state', 'lines', 'payment_ids'], 'Transaksi PoS Order Invoice')
        # integrator_transaksi.debug_taxes('account.tax', ['name', 'id'], 'Taxes Invoice')
        # integrator_transaksi.update_integrated('pos.session', ['is_integrated'], "Session Updated")
        integrator_transaksi.update_status_order_pos('pos.order', ['name', 'state'], "Session Updated")

if __name__ == '__main__':
    main()