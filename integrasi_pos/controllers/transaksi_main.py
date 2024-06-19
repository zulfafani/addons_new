import os
from config import Config
from odoo_client import OdooClient
from data_transaksi import DataIntegrator
from decouple import config as get_config


def main():
    # base_dir = os.path.dirname(os.getcwd())
    # file_path = rf"{os.path.join(base_dir, 'integrasi_odoo', 'controllers', 'config.json')}"
    file_path = r'/vit/odoo/odoo17/addons_new/integrasi_pos/controllers/config.json'
    key = get_config('key').encode()
    config = Config(file_path, key)

    instanceMC = config.get_instance('odoo_db1')
    decrypted_password1 = config.decrypt_password(instanceMC['password'])
    odoo_mc_client = OdooClient(instanceMC['url'], instanceMC['db'], instanceMC['username'], decrypted_password1)

    instancesSS = [
    'odoo_db1a',
    ]

    for instance_name in instancesSS:
        instance = config.get_instance(instance_name)
        decrypted_password = config.decrypt_password(instance['password'])
        odoo_ss_client = OdooClient(instance['url'], instance['db'], instance['username'], decrypted_password)

        integrator = DataIntegrator(odoo_ss_client, odoo_mc_client)

    
        # Transaksi Store Server to Master Console
        integrator.transfer_transaksi('account.move', ['name', 'partner_id', 'invoice_date', 'payment_reference', 'invoice_date_due', 'journal_id', 'state', 'move_type', 'invoice_line_ids'], 'Transaksi Invoice')
        # integrator.transfer_transaksi('account.move.line', ['product_id', 'name', 'quantity', 'product_uom_id'],  )

if __name__ == '__main__':
    main()