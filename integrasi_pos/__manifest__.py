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
    'depends': ['base', 'sale', 'stock', 'point_of_sale'],  # ['base', 'sale', 'mrp'],

    # always loaded
    # Daftar file XML yang menyediakan data dan konfigurasi tambahan untuk modul ini. Ini termasuk file keamanan, data, panduan, tampilan, dan lain-lain.
    'data': [
        'security/ir.model.access.csv',
        'views/res_partner_view.xml',
        'views/log_code_runtime_view.xml',
        'views/log_note_view.xml',
        'views/log_menu.xml',
    ],

    'installable': True,
    'application': True,
    'auto_install': False
}
