from odoo import models


class PosSession(models.Model):
    """The class PosSession is used to inherit pos.session"""
    _inherit = 'pos.session'

    def load_pos_data(self):
        """Load POS data and add `hr_employee` (sales only) to the response dictionary.
        return: A dictionary containing the POS data.
        """
        res = super().load_pos_data()
        # Filter hanya employee dengan is_sales = True
        res['hr_employee'] = self.env['hr.employee'].search_read(
            domain=[('is_sales', '=', True)],
            fields=['name']
        )
        return res
