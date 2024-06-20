from odoo import models, fields, _, api
from odoo.exceptions import UserError
import os
from config import Config
from odoo_client import OdooClient
from data_transaksi import DataTransaksi
from data_integrator import DataIntegrator
from data_transaksiMCtoSS import DataTransaksiMCtoSS
from decouple import config as get_config

class InheritManualSync(models.Model):
    _inherit = 'manual.sync'

    def action_start(self):
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

                integrator_transaksi = DataTransaksi(odoo_ss_client, odoo_mc_client)

                manual_sync_records = self.env['manual.sync'].search([])  # Dapat berbeda tergantung pada framework Odoo yang digunakan

                for record in manual_sync_records:
                    if record.sync_model == 'pos invoice':
                        date_from = record.date_from
                        date_to = record.date_to

                        # Memanggil fungsi transfer_pos_order_invoice
                        integrator_transaksi.transfer_pos_order_invoice('pos.order', fields, 'Invoice', date_from, date_to)
                    

        if __name__ == '__main__':
            main()