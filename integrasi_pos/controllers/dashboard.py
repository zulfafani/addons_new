from odoo import http
from odoo.http import request

class HomeRedirect(http.Controller):

    @http.route('/', type='http', auth="user")
    def home_redirect(self):
        return http.redirect_with_hash('/pos/web')
