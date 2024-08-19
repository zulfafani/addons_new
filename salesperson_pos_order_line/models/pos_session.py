from odoo import models


class PosSession(models.Model):
    """The class PosSession is used to inherit pos.session"""
    _inherit = 'pos.session'

    def load_pos_data(self):
        """Load POS data and add `res_users` to the response dictionary.
        return: A dictionary containing the POS data.
        """
        res = super().load_pos_data()
        res['hr_employee'] = self.env['hr.employee'].search_read(fields=['name'])
        return res
