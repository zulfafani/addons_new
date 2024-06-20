import os
from config import Config
from odoo_client import OdooClient
from data_transaksi import DataTransaksi
from data_integrator import DataIntegrator
from data_transaksiMCtoSS import DataTransaksiMCtoSS
from decouple import config as get_config


def main():
    # base_dir = os.path.dirname(os.getcwd())
    # file_path = rf"{os.path.join(base_dir, 'integrasi_odoo', 'controllers', 'config.json')}"
    file_path = r"C:\Users\abhif\OneDrive\Documents\Development Testing\zulfa's_code\addons_new\integrasi_pos\controllers\config.json"
    key = get_config('key').encode()
    config = Config(file_path, key)

    instanceMC = config.get_instance('odoo_db1_mc')
    # decrypted_password1 = config.decrypt_password(instanceMC['password'])
    odoo_mc_client = OdooClient(instanceMC['url'], instanceMC['db'], instanceMC['username'], instanceMC['password'])

    instancesSS = [
    'odoo_db1a',
    'odoo_db1b',
    ]

    for instance_name in instancesSS:
        instance = config.get_instance(instance_name)
        # decrypted_password = config.decrypt_password(instance['password'])
        odoo_ss_client = OdooClient(instance['url'], instance['db'], instance['username'], instance['password'])

        integrator_master = DataIntegrator(odoo_mc_client, odoo_ss_client)
        integrator_transaksi = DataTransaksi(odoo_ss_client, odoo_mc_client)
        integrator_transaksiMCtoSS = DataTransaksiMCtoSS(odoo_mc_client, odoo_ss_client)

        # Master MC to Store Server
        # integrator_master.transfer_data('res.partner.title', ['name', 'shortcut'], 'Master Customer Title')
        # integrator_master.transfer_data('res.partner', ['name', 'street', 'street2', 'phone', 'mobile', 'email', 'website','title','customer_rank', 'supplier_rank', 'customer_code'], 'Master Customer')
        # integrator_master.transfer_data('account.tax', ['name', 'description', 'amount_type', 'active', 'type_tax_use', 'tax_scope', 'amount', 'invoice_label', 'tax_group_id', 'price_include', 'include_base_amount', 'include_base_amount', 'invoice_repartition_line_ids'], 'Master Tax')
        # integrator_master.transfer_data('product.category', ['complete_name', 'name', 'parent_id', 'property_valuation'], 'Master Item Group')
        # integrator_master.transfer_data('uom.category', ['name', 'is_pos_groupable'], 'Master UoM Group')
        # integrator_master.transfer_data('uom.uom', ['category_id', 'uom_type', 'name', 'factor', 'rounding', 'active'], 'Master UoM')
        # integrator_master.transfer_data('pos.category', ['name', 'parent_id', 'sequence'], 'Master POS Category')
        # integrator_master.transfer_data('product.template', ['name', 'sale_ok', 'purchase_ok', 'detailed_type', 'invoice_policy', 'uom_id', 'uom_po_id', 'list_price', 'standard_price', 'categ_id', 'default_code', 'available_in_pos', 'taxes_id'], 'Master Item') # , 'taxes_id' belum ada master account.tax , 
        # integrator_master.transfer_data('stock.location', ['complete_name', 'name', 'location_id', 'usage', 'scrap_location', 'return_location', 'replenish_location', 'last_inventory_date', 'next_inventory_date'], 'Master Location') # , 'company_id' belum ada master res.company
        # integrator_master.transfer_data('product.pricelist', ['name', 'currency_id', 'item_ids'], 'Master Pricelist')
        # tidak pakai integrator_master.transfer_data('res.users', ['name', 'login', 'partner_id'], 'Users')
        # tidak pakai integrator_master.transfer_data('hr.employee', ['name', 'mobile_phone', 'work_phone', 'work_email', 'user_id'], 'Sales Employee')
        
        # Master Store Server to MC
        integrator_master.transfer_data_mc('res.partner.title', ['name', 'shortcut'], 'Master Customer Title')
        integrator_master.transfer_data_mc('res.partner', ['name', 'street', 'street2', 'phone', 'mobile', 'email', 'website','title','customer_rank', 'supplier_rank', 'customer_code'], 'Master Customer')
        # integrator_master.transfer_data_mc('hr.employee', ['name', 'mobile_phone', 'work_phone', 'work_email'], 'Sales Employee')
        
        # Transaksi Store Server to Master Console
        # integrator_transaksi.transfer_transaksi('account.move', ['name', 'partner_id', 'invoice_date', 'payment_reference', 'invoice_date_due', 'journal_id', 'state', 'ref', 'move_type', 'state', 'payment_state', 'invoice_line_ids'], 'Transaksi Invoice')
        # integrator_transaksi.transfer_pos_order_inventory('stock.picking', ['name', 'partner_id', 'location_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.transfer_internal_transfers('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.transfer_goods_issue('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.transfer_goods_receipt('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.transfer_TSOutTsIn('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'target_location', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.transfer_TSOUT_NEW('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'target_location', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.debug_operatin_type('stock.location', ['name', 'complete_name', 'id'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.validate_tsin_tsout('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'vit_trxid', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        integrator_transaksi.update_session_status('pos.session', ['name', 'state', 'start_at', 'stop_at', 'cash_register_balance_start', 'cash_register_balance_end_real'], 'Update Session PoS Order')
        # integrator_transaksi.transfer_pos_order_session('pos.session', ['name', 'config_id', 'user_id', 'start_at', 'stop_at', 'state'], 'Master Session PoS Order Invoice')
        # integrator_transaksi.transfer_warehouse_master('stock.warehouse', ['name', 'lot_stock_id', 'company_id'], 'Insert Warehouse')
        # integrator_transaksi.transfer_pos_order_invoice('pos.order', ['name', 'date_order', 'session_id', 'user_id', 'partner_id', 'pos_reference', 'order_ref', 'tracking_number', 'margin', 'amount_tax', 'amount_total', 'amount_paid', 'amount_return', 'state', 'lines', 'payment_ids'], 'Transaksi PoS Order Invoice')
        # integrator_transaksi.transfer_stock_adjustment('stock.move.line', ['reference', 'quantity', 'product_id', 'location_id', 'location_dest_id', 'company_id', 'state'], 'Transaksi Adjustment Stock')
        # integrator_transaksi.debug_taxes('account.tax', ['name', 'id'], 'Taxes Invoice')
        # integrator_transaksi.update_integrated('pos.session', ['is_integrated'], "Session Updated")
        # integrator_transaksi.update_session_status('pos.session', ['name', 'id','state'], "Session Updated")

        # Transaksi Master Console to Store Server
        # integrator_transaksiMCtoSS.transfer_transaksi_MCtoSS('account.move', ['name', 'partner_id', 'invoice_date', 'payment_reference', 'invoice_date_due', 'journal_id', 'state', 'ref', 'move_type', 'state', 'payment_state', 'invoice_line_ids'], 'Transaksi Invoice')
        # integrator_transaksiMCtoSS.transfer_discount_loyalty('loyalty.program', ['name', 'program_type', 'currency_id', 'portal_point_name', 'portal_visible', 'trigger', 'applies_on', 'date_from', 'date_to', 'limit_usage', 'pos_ok', 'sale_ok', 'reward_ids'], 'Transfer Discount/Loyalty')
        # integrator_transaksiMCtoSS.transfer_pos_order_inventory_MCtoSS('stock.picking', ['name', 'partner_id', 'location_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksiMCtoSS.transfer_internal_transfers('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksiMCtoSS.transfer_goods_issue('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksiMCtoSS.transfer_goods_receipt('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksiMCtoSS.update_session_status_MCtoSS('pos.session', ['name', 'state', 'start_at', 'stop_at', 'cash_register_balance_start', 'cash_register_balance_end_real'], 'Update Session PoS Order')
        # integrator_transaksiMCtoSS.transfer_warehouse_master_MCtoSS('stock.warehouse', ['name', 'lot_stock_id'], 'Insert Warehouse')
        # integrator_transaksiMCtoSS.transfer_pos_order_session_MCtoSS('pos.session', ['name', 'config_id', 'user_id', 'start_at', 'stop_at', 'state'], 'Master Session PoS Order Invoice')
        # integrator_transaksiMCtoSS.transfer_pos_order_invoice_MCtoSS('pos.order', ['name', 'date_order', 'session_id', 'user_id', 'partner_id', 'pos_reference', 'order_ref', 'tracking_number', 'margin', 'amount_tax', 'amount_total', 'amount_paid', 'amount_return', 'state', 'lines', 'payment_ids'], 'Transaksi PoS Order Invoice')
        # integrator_transaksiMCtoSS.ts_in_from_mc('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'target_location', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksiMCtoSS.transfer_stock_adjustment_MCtoSS('stock.move.line', ['reference', 'quantity', 'product_id', 'location_id'], 'Transaksi Adjustment Stock')
        # integrator_transaksiMCtoSS.debug_taxes_MCtoSS('account.tax', ['name', 'id'], 'Taxes Invoice')
        # integrator_transaksiMCtoSS.update_integrated_MCtoSS('pos.session', ['is_integrated'], "Session Updated")
        # integrator_transaksiMCtoSS.update_session_status_MCtoSS('pos.session', ['name', 'id','state'], "Session Updated")
        
if __name__ == '__main__':
    main()