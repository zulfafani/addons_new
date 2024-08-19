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
    'depends': ['web', 'base', 'sale', 'stock', 'point_of_sale', 'account', 'loyalty', 'purchase', 'mail'],  # ['base', 'sale', 'mrp'],

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
        'data/sequence_end_shift.xml',
        'views/loyalty_program_view.xml',
        'views/favicon.xml',
        'views/views_loyalty_reward.xml',
        'views/views_loyalty_rule.xml',
        'views/purchase_order_view.xml',
        'views/views_account_move.xml',
        'views/product_template.xml',
        'views/res_users_view.xml',
        'views/hr_employee_view.xml',
        'views/account_tax_views.xml',
        'views/pos_payment_method_views.xml',
        'views/ir_sequence_view.xml',
        'views/pos_category_view.xml',
        'views/product_category_view.xml',
        'views/product_pricelist_view.xml',
        'views/stock_location_view.xml',
        'views/stock_picking_type_view.xml',
        'views/uom_category_view.xml',
        'views/uom_uom_view.xml',
        'views/res_currency_views.xml',
        'views/account_journal_view.xml',
        'views/loyalty_card_view.xml',
        'views/pos_cashier_log_view.xml',
        'views/end_shift_view.xml',
    ],
    'assets': {
        'point_of_sale._assets_pos': [
            'integrasi_pos/static/src/xml/button_end_screen_shift.xml',
            'integrasi_pos/static/src/js/custom_invoice.js',
            'integrasi_pos/static/src/js/get_customer.js',
            'integrasi_pos/static/src/js/disable_download.js',
            'integrasi_pos/static/src/js/custom_shift.js',
            'integrasi_pos/static/src/js/automated_print.js',
            'integrasi_pos/static/src/js/cashier_login_custom.js',
            'integrasi_pos/static/src/js/end_shift_session.js',
            'integrasi_pos/static/src/js/pop_up_end_shift.js',
            'integrasi_pos/static/src/js/select_product_cashier.js',
            'integrasi_pos/static/src/css/pop_up.css',
        ],
    },
    'license': 'LGPL-3',
    'installable': True,
    'auto_install': False,
    'application': False,
}
