import os
from config import Config
from odoo_client import OdooClient
from data_transaksi import DataTransaksi
from data_integrator import DataIntegrator
from data_transaksiMCtoSS import DataTransaksiMCtoSS
from decouple import config as get_config
from datetime import datetime, timedelta


def main():
    # base_dir = os.path.dirname(os.getcwd())
    # file_path = rf"{os.path.join(base_dir, 'integrasi_odoo', 'controllers', 'config.json')}"
    file_path = r"C:\Users\abhif\OneDrive\Documents\Development Testing\zulfa's_code\addons_new\integrasi_pos\controllers\config.json"
    key = get_config('key').encode()
    config = Config(file_path, key)

    instanceMC = config.get_instance('odoo_db1_mc')
    # decrypted_password1 = config.decrypt_password(instanceMC['password'])
    odoo_mc_client = OdooClient(instanceMC['url'], instanceMC['server_name'], instanceMC['db'], instanceMC['username'], instanceMC['password'])

    instancesSS = [
    'odoo_db1_a',
    # 'odoo_db2_a',
    ]

    for instance_name in instancesSS:
        instance = config.get_instance(instance_name)
        # decrypted_password = config.decrypt_password(instance['password'])
        odoo_ss_client = OdooClient(instance['url'], instance['server_name'], instance['db'], instance['username'], instance['password'])

        integrator_master = DataIntegrator(odoo_mc_client, odoo_ss_client)
        integrator_transaksi = DataTransaksi(odoo_ss_client, odoo_mc_client)
        integrator_transaksiMCtoSS = DataTransaksiMCtoSS(odoo_mc_client, odoo_ss_client)

        date_to = datetime.today()
        date_from = date_to - timedelta(days=3)

        if isinstance(date_from, datetime):
            date_from = date_from.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(date_to, datetime):
            date_to = date_to.strftime('%Y-%m-%d %H:%M:%S')

        # integrator_transaksi.transfer_pos_order_invoice_session_closed('pos.order', ['id', 'name', 'date_order', 'session_id', 'user_id', 'partner_id', 'pos_reference', 'vit_trxid', 'tracking_number', 'employee_id', 'margin', 'amount_tax', 'amount_total', 'amount_paid', 'amount_return', 'state', 'lines', 'payment_ids'], 'Transaksi PoS Order Invoice')
        # integrator_transaksi.update_session_status('pos.session', ['name', 'id','state', 'start_at', 'stop_at', 'cash_register_balance_start', 'cash_register_balance_end_real'], "Session Updated")
        # integrator_transaksi.transfer_pos_order_session('pos.session', ['name', 'config_id', 'user_id', 'start_at', 'stop_at', 'state'], 'Master Session PoS Order Invoice')
        # integrator_transaksi.transfer_pos_order_invoice('pos.order', ['id', 'name', 'date_order', 'session_id', 'user_id', 'partner_id', 'pos_reference', 'vit_trxid', 'tracking_number', 'employee_id', 'margin', 'amount_tax', 'amount_total', 'amount_paid', 'amount_return', 'state', 'lines', 'payment_ids'], 'Transaksi PoS Order Invoice')

        # Master MC to Store Server
        # integrator_master.transfer_data('ir.sequence', ['name', 'implementation', 'code', 'active', 'prefix', 'suffix', 'use_date_range', 'padding', 'number_increment', 'create_date', 'write_date'], 'Master Sequence')
        # integrator_master.transfer_data('stock.picking.type', ['name', 'code', 'sequence_id', 'sequence_code', 'warehouse_id', 'reservation_method', 'return_picking_type_id', 'default_location_return_id', 'create_backorder', 'use_create_lots', 'use_existing_lots', 'default_location_src_id', 'default_location_dest_id', 'create_date', 'write_date'], 'Master Operation')
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
        # integrator_master.transfer_data_mc('res.partner.title', ['name', 'shortcut'], 'Master Customer Title')
        # integrator_master.transfer_data_mc('res.partner', ['name', 'street', 'street2', 'phone', 'mobile', 'email', 'website','title','customer_rank', 'supplier_rank', 'customer_code'], 'Master Customer', date_from, date_to)
        # integrator_master.transfer_data_mc('hr.employee', ['name', 'mobile_phone', 'work_phone', 'work_email'], 'Sales Employee')
        
        # integrator_transaksi.transfer_TSOUT_NEW('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'target_location', 'move_ids_without_package'], 'Transaksi TS Out')
        # integrator_transaksiMCtoSS.ts_in_from_mc('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'vit_trxid', 'target_location', 'move_ids_without_package'], 'Transaksi TS In')
        # integrator_transaksi.validate_tsin_tsout('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'vit_trxid', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')

        # Transaksi Store Server to Master Console
        # integrator_transaksi.transfer_end_shift_from_store('end.shift', ['doc_num', 'cashier_id', 'session_id', 'start_date', 'end_date', 'is_integrated', 'line_ids'], 'Transfer End Shift')
        # integrator_transaksi.update_loyalty_point_ss_to_mc('loyalty.program', ['id', 'name', 'program_type', 'currency_id', 'pricelist_ids', 'portal_point_name', 'portal_visible', 'trigger', 'applies_on', 'date_from', 'date_to', 'limit_usage', 'pos_ok', 'pos_config_ids', 'sale_ok', 'vit_trxid', 'reward_ids', 'rule_ids'], 'Transfer Discount/Loyalty', date_from, date_to)
        # integrator_transaksi.create_loyalty_point_ss_to_mc('loyalty.program', ['id', 'name', 'program_type', 'currency_id', 'pricelist_ids', 'portal_point_name', 'portal_visible', 'trigger', 'applies_on', 'date_from', 'date_to', 'limit_usage', 'pos_ok', 'pos_config_ids', 'sale_ok', 'vit_trxid', 'reward_ids', 'rule_ids'], 'Transfer Discount/Loyalty', date_from, date_to)
        # integrator_transaksi.transfer_transaksi('account.move', ['name', 'partner_id', 'invoice_date', 'payment_reference', 'invoice_date_due', 'journal_id', 'state', 'ref', 'move_type', 'state', 'payment_state', 'invoice_line_ids'], 'Transaksi Invoice')
        # integrator_transaksi.transfer_pos_order_inventory('stock.picking', ['name', 'partner_id', 'location_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.transfer_internal_transfers('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.validate_GRPO('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package', 'vit_trxid'], 'Transaksi PoS Order Inventory', date_from, date_to)
        # integrator_transaksi.transfer_goods_issue('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory', date_from, date_to)
        # integrator_transaksi.transfer_goods_receipt('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'],'Transaksi Goods Receipt', date_from, date_to)
        # integrator_transaksi.transfer_receipts('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.transfer_TSOutTsIn('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'target_location', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.transfer_TSOUT_NEW('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'target_location', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.debug_operatin_type('stock.location', ['name', 'complete_name', 'id'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.validate_tsin_tsout('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'vit_trxid', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksi.update_session_status('pos.session', ['name', 'state', 'start_at', 'config_id', 'stop_at', 'cash_register_balance_start', 'cash_register_balance_end_real'], 'Update Session PoS Order', date_from, date_to)
        # integrator_transaksi.transfer_pos_order_session('pos.session', ['name', 'config_id', 'user_id', 'start_at', 'stop_at', 'state'], 'Master Session PoS Order Invoice', date_from, date_to)
        # integrator_transaksi.update_session_status('pos.session', ['name', 'id','state', 'start_at', 'stop_at', 'cash_register_balance_start', 'cash_register_balance_end_real',], "Session Updated", date_from, date_to)
        # integrator_transaksi.update_id_mc_payment_method('pos.payment.method', ['id', 'name'], 'Master Session PoS Order Payment Method')
        # integrator_transaksi.update_id_mc_pos_session('pos.session', ['id', 'name'], 'Master Session PoS Order Payment Method')
        # integrator_transaksi.update_invoice_mc('pos.order', ['id', 'name'], 'Master Session PoS Order Payment Method')
        # integrator_transaksi.transfer_pos_order_invoice_ss_to_mc('pos.order', ['id', 'name', 'date_order', 'session_id', 'user_id', 'partner_id', 'pos_reference', 'vit_trxid', 'tracking_number', 'pricelist_id', 'employee_id', 'margin', 'amount_tax', 'amount_total', 'amount_paid', 'amount_return', 'state', 'lines', 'payment_ids'], 'Transaksi PoS Order Invoice', date_from, date_to)
        # integrator_transaksi.transfer_pos_order_invoice('pos.order', ['id', 'name', 'date_order', 'session_id', 'user_id', 'partner_id', 'pos_reference', 'vit_trxid', 'tracking_number', 'pricelist_id', 'employee_id', 'margin', 'amount_tax', 'amount_total', 'amount_paid', 'amount_return', 'state', 'lines', 'payment_ids'], 'Transaksi PoS Order Invoice')
        # integrator_transaksi.transfer_warehouse_master('stock.warehouse', ['name','lot_stock_id', 'location_transit', 'company_id'], 'Insert Warehouse')
        # integrator_transaksi.transfer_inventory_stock('inventory.stock', ['id', 'doc_num', 'warehouse_id', 'location_id', 'company_id', 'create_date', 'from_date', 'to_date', 'inventory_date', 'state'], 'Transaksi Inventory Counting', date_from, date_to)
        # integrator_transaksi.debug_taxes('account.tax', ['name', 'id'], 'Taxes Invoice')
        # integrator_transaksi.update_integrated('pos.session', ['is_integrated'], "Session Updated")
        # integrator_transaksi.transfer_loyalty_point('loyalty.card', ['code', 'points_display', 'expiration_date', 'program_id', 'partner_id', 'source_pos_order_id', 'points'], 'Transfer Discount/Loyalty')
        # integrator_transaksi.transfer_loyalty_point('loyalty.card', ['code', 'points_display', 'expiration_date', 'program_id', 'partner_id', 'source_pos_order_id', 'points'], 'Transfer Discount/Loyalty')
        # integrator_transaksi.transfer_manufacture_order('mrp.production', ['id', 'name', 'state', 'is_integrated', 'create_date', 'location_src_id', 'location_dest_id', 'picking_type_id', 'product_id', 'product_qty', 'bom_id', 'user_id', 'date_start', 'date_finished', 'move_raw_ids'], 'Transaksi Manufacture Order Inventory', date_from, date_to)
        # integrator_transaksi.transfer_unbuild_order('mrp.unbuild', ['id', 'name', 'state', 'is_integrated', 'create_date', 'location_id', 'location_dest_id', 'product_id', 'product_qty', 'bom_id', 'mo_id', 'unbuild_line_ids'], 'Transaksi Unbuild Order Inventory', date_from, date_to)
        # integrator_transaksi.transfer_bom_master('mrp.bom', ['id', 'product_tmpl_id', 'product_id', 'code', 'type', 'product_qty', 'consumption', 'produce_delay', 'days_to_prepare_mo', 'create_date', 'is_integrated', 'bom_line_ids'], 'Transaksi Manufacture Order Inventory', date_from, date_to)

        # Transaksi Master Console to Store Server
        # integrator_transaksiMCtoSS.validate_invoice('pos.order', ['id', 'name'], 'Validate PoS Order Invoice', date_from, date_to)
        # integrator_transaksiMCtoSS.transfer_master_operation_type('stock.picking.type', ['name', 'code', 'sequence_id', 'sequence_code', 'warehouse_id', 'reservation_method', 'return_picking_type_id', 'default_location_return_id', 'create_backorder', 'use_create_lots', 'use_existing_lots', 'default_location_src_id', 'default_location_dest_id', 'create_date', 'write_date'], 'Master Operation')
        # integrator_transaksiMCtoSS.create_stock_invoice('pos.order', ['id'], 'Transaksi PoS Order Invoice')
        # integrator_transaksiMCtoSS.validate_and_generate_invoice('pos.order', ['id'], 'Transaksi PoS Order Invoice')
        # integrator_transaksiMCtoSS.update_location_id_mc('stock.location', ['id', 'complete_name'], 'Update ID MC')
        # integrator_transaksiMCtoSS.update_company_id_mc('res.company', ['id', 'name'], 'Update ID MC')
        # integrator_transaksiMCtoSS.transfer_discount_loyalty('loyalty.program', ['id', 'name', 'program_type', 'currency_id', 'pricelist_ids', 'portal_point_name', 'portal_visible', 'trigger', 'applies_on', 'date_from', 'date_to', 'limit_usage', 'pos_ok', 'pos_config_ids', 'sale_ok', 'vit_trxid', 'index_store', 'reward_ids', 'rule_ids', 'schedule_ids', 'member_ids'], 'Transfer Discount/Loyalty', date_from, date_to)
        # integrator_transaksiMCtoSS.product_tag_master('product.tag', ['id', 'name', 'product_template_ids', 'create_date', 'is_integrated'], 'Transfer Product Tag', date_from, date_to)
        integrator_transaksiMCtoSS.update_discount_loyalty('loyalty.program', ['id', 'name', 'program_type', 'currency_id', 'pricelist_ids', 'portal_point_name', 'portal_visible', 'trigger', 'currency_id', 'applies_on', 'date_from', 'date_to', 'limit_usage', 'pos_ok', 'pos_config_ids', 'sale_ok', 'vit_trxid', 'index_store', 'reward_ids', 'rule_ids', 'schedule_ids', 'member_ids'], 'Transfer Discount/Loyalty', date_from, date_to)
        # integrator_transaksiMCtoSS.transfer_internal_transfers_mc_to_ss('stock.picking', ['id','name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'target_location', 'move_ids_without_package'], 'Transaksi PoS Order Inventory', date_from, date_to)
        # integrator_transaksiMCtoSS.transfer_loyalty_point_mc_to_ss('loyalty.program', ['id', 'name', 'program_type', 'currency_id', 'pricelist_ids', 'portal_point_name', 'portal_visible', 'trigger', 'currency_id', 'applies_on', 'date_from', 'date_to', 'limit_usage', 'pos_ok', 'pos_config_ids', 'sale_ok', 'vit_trxid', 'index_store', 'reward_ids', 'rule_ids'], 'Transfer Discount/Loyalty', date_from, date_to)
        # integrator_transaksiMCtoSS.update_loyalty_point_mc_to_ss('loyalty.program', ['id', 'name', 'program_type', 'currency_id', 'pricelist_ids', 'portal_point_name', 'portal_visible', 'trigger', 'currency_id', 'applies_on', 'date_from', 'date_to', 'limit_usage', 'pos_ok', 'pos_config_ids', 'sale_ok', 'vit_trxid', 'index_store', 'reward_ids', 'rule_ids'], 'Transfer Discount/Loyalty', date_from, date_to)
        # integrator_transaksiMCtoSS.transfer_transaksi_MCtoSS('account.move', ['name', 'partner_id', 'invoice_date', 'payment_reference', 'invoice_date_due', 'journal_id', 'state', 'ref', 'move_type', 'state', 'payment_state', 'invoice_line_ids'], 'Transaksi Invoice')
        # integrator_transaksiMCtoSS.update_integrated_discount('loyalty.program', ['id', 'name', 'program_type', 'currency_id', 'pricelist_ids', 'portal_point_name', 'portal_visible', 'trigger', 'applies_on', 'date_from', 'date_to', 'limit_usage', 'pos_ok', 'pos_config_ids', 'sale_ok', 'vit_trxid', 'reward_ids', 'rule_ids'], 'Transfer Discount/Loyalty')
        # integrator_transaksiMCtoSS.transfer_pos_order_inventory_MCtoSS('stock.picking', ['name', 'partner_id', 'location_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksiMCtoSS.transfer_internal_transfers('stock.picking', ['id', 'name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory', date_from, date_to)
        # integrator_transaksiMCtoSS.transfer_goods_issue('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory', date_from, date_to)
        # integrator_transaksiMCtoSS.transfer_goods_receipt('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory', date_from, date_to)
        # integrator_transaksiMCtoSS.transfer_receipts('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'move_ids_without_package'], 'Transaksi PoS Order Inventory')
        # integrator_transaksiMCtoSS.update_session_status_MCtoSS('pos.session', ['name', 'state', 'start_at', 'stop_at', 'cash_register_balance_start', 'cash_register_balance_end_real'], 'Update Session PoS Order')
        # integrator_transaksiMCtoSS.transfer_warehouse_master('stock.warehouse', ['name', 'lot_stock_id', 'location_transit'], 'Insert Warehouse')
        # integrator_transaksiMCtoSS.purchase_order_from_mc('purchase.order', ['name', 'partner_id', 'partner_ref', 'currency_id', 'date_approve', 'date_planned', 'picking_type_id', 'vit_trxid'], 'Transaksi PoS Order Inventory', date_from, date_to)
        # integrator_transaksiMCtoSS.account_account_from_mc('account.account', ['id', 'name', 'code', 'account_type', 'reconcile'], 'Transaksi COA', date_from, date_to)
        # integrator_transaksiMCtoSS.journal_account_from_mc('account.journal', ['id', 'name', 'type', 'is_store', 'refund_sequence', 'code', 'account_control_ids', 'invoice_reference_type', 'invoice_reference_model', 'vit_trxid', 'default_account_id', 'suspense_account_id', 'profit_account_id', 'loss_account_id'], 'Transaksi Journal', date_from, date_to)
        # integrator_transaksiMCtoSS.pos_config_from_mc('pos.config', ['id', 'name', 'module_pos_hr', 'is_posbox', 'is_store', 'other_devices', 'vit_trxid'], 'Transaksi PoS Config', date_from, date_to)
        # integrator_transaksiMCtoSS.payment_method_from_mc('pos.payment.method', ['id', 'name', 'is_store', 'split_transactions', 'journal_id', 'config_ids', 'vit_trxid'], 'Transaksi PoS Payment Method', date_from, date_to)
        # integrator_transaksiMCtoSS.transfer_pos_order_session_MCtoSS('pos.session', ['name', 'config_id', 'user_id', 'start_at', 'stop_at', 'state'], 'Master Session PoS Order Invoice')
        # integrator_transaksiMCtoSS.transfer_pos_order_invoice_MCtoSS('pos.order', ['name', 'date_order', 'session_id', 'user_id', 'partner_id', 'pos_reference', 'order_ref', 'tracking_number', 'margin', 'amount_tax', 'amount_total', 'amount_paid', 'amount_return', 'state', 'lines', 'payment_ids'], 'Transaksi PoS Order Invoice')
        # integrator_transaksiMCtoSS.ts_in_from_mc('stock.picking', ['name', 'partner_id', 'location_id', 'picking_type_id', 'location_dest_id', 'scheduled_date', 'date_done', 'origin', 'target_location', 'move_ids_without_package'], 'Transaksi PoS Order Inventory',    date_from, date_to)
        # integrator_transaksiMCtoSS.validate_goods_receipts_mc('stock.picking', ['id'], 'Validate Goods Receipts MC')
        # integrator_transaksiMCtoSS.update_item('product.template', ['name', 'default_code',], 'Transaksi PoS Order Inventory')
        # integrator_transaksiMCtoSS.transfer_stock_adjustment_MCtoSS('stock.move.line', ['reference', 'quantity', 'product_id', 'location_id'], 'Transaksi Adjustment Stock')
        # integrator_transaksiMCtoSS.debug_taxes_MCtoSS('account.tax', ['name', 'id'], 'Taxes Invoice')
        # integrator_transaksiMCtoSS.update_integrated_MCtoSS('pos.session', ['is_integrated'], "Session Updated")
        # integrator_transaksiMCtoSS.update_session_status_MCtoSS('pos.session', ['name', 'id','state'], "Session Updated")
        # integrator_transaksiMCtoSS.purchase_order_from_mc('purchase.order', ['name', 'partner_id', 'partner_ref', 'currency_id', 'date_approve', 'date_planned', 'picking_type_id', 'vit_trxid'], 'Transaksi Purchase Order')

if __name__ == '__main__':
    main()