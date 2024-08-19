import requests
from datetime import datetime, timedelta
import pytz
from odoo.http import request
import base64
from odoo import models, fields, api, SUPERUSER_ID
from odoo.exceptions import UserError

class MasterWarehouse(models.Model):
    _name = 'master.warehouse'
    _rec_name = 'warehouse_name'

    warehouse_name = fields.Char(string="Warehouse Name", tracking=True, readonly=True)
    warehouse_code = fields.Char(string="Warehouse Code", tracking=True, readonly=True)
    warehouse_transit = fields.Char(string="Warehouse Transit", tracking=True, readonly=True)
    id_mc_location = fields.Char(string="ID MC Location", tracking=True, readonly=True)
    id_mc_transit = fields.Char(string="ID MC Transit", tracking=True, readonly=True)