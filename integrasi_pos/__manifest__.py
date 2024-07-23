# -*- coding: utf-8 -*-
{
    'name': "Log Note Integrasi",
    'version': '1.0',
    'category': 'Uncategorized',  # Sales/CRM Kategori modul. Modul ini termasuk dalam kategori "Sales/CRM"
    # 'sequence': 15,                            #Urutan tampilan modul dalam daftar aplikasi.

    'summary': """                              
        Short (1 phrase/line) summary of the module's purpose, used as
        subtitle on modules listing or apps.openerp.com""",
    # Ringkasan singkat tentang fungsionalitas modul/ modul ini untuk apa

    'description': """
        Long description of module's purpose
    """,  # Deskripsi lebih rinci tentang fungsionalitas modul.

    'author': "My Company",
    # 'website': "https://www.yourcompany.com",  # URL situs web yang terkait dengan modul

    # Categories can be used to filter modules in modules listing
    # Check https://github.com/odoo/odoo/blob/16.0/odoo/addons/base/data/ir_module_category_data.xml
    # for the full list

    # Daftar modul yang dibutuhkan untuk diinstal sebelum modul ini dapat diinstal. Modul ini bergantung pada modul-modul didalam depends
    'depends': ['web', 'base', 'sale', 'stock', 'point_of_sale', 'account', 'loyalty', 'purchase'],  # ['base', 'sale', 'mrp'],

    # always loaded
    # Daftar file XML yang menyediakan data dan konfigurasi tambahan untuk modul ini. Ini termasuk file keamanan, data, panduan, tampilan, dan lain-lain.
    'data': [
        'security/ir.model.access.csv',
        'views/res_partner_view.xml',
        'views/log_note_view.xml',
        'views/log_menu.xml',
        'views/stock_picking_view.xml',
        'views/pos_session_view.xml',
        'views/pos_order_view.xml',
        'views/master_warehouse_view.xml',
        'views/stock_move_line_view.xml',
        'views/pos_config_view.xml',
        'data/sequence.xml',
        'views/loyalty_program_view.xml',
        'views/favicon.xml',
        'views/views_loyalty_reward.xml',
        'views/views_loyalty_rule.xml',
        'views/purchase_order_view.xml',
        'views/views_account_move.xml'

    ],
    'assets': {
        'web.assets_backend': [
            'integrasi_pos/static/src/css/theme_custom.css',
        ],
        'point_of_sale._assets_pos': [
            'integrasi_pos/static/src/js/custom_invoice.js',
            'integrasi_pos/static/src/js/get_customer.js',
            'integrasi_pos/static/src/js/disable_download.js',
        ],
    },
    'license': 'LGPL-3',
    'installable': True,
    'auto_install': False,
    'application': False,
}
