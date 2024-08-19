# -*- coding: utf-8 -*-
{
    'name': "POS-VIT-LOYALTY",
    'version': '17.0.1.0.0',
    'category': 'Point Of sale', 
    'summary': """                              
        Modul POS-VIT 2.0 LOYALTY""",
    'description': """
        POS-VIT 2.0 LOYALTY Store
    """,
    'author': "Visi Intech",
    'depends': ['web', 'base', 'sale', 'stock', 'point_of_sale', 'pos_loyalty', 'account', 'loyalty', 'purchase', 'mail'], 
    'data': [
    ],
    'assets': {
        'point_of_sale._assets_pos': [
            'integrasi_pos_loyalty/static/src/overrides/models/reward_pos_override.js',
        ],
    },
    'installable': True,
    'auto_install': False,
    'application': True,
}
