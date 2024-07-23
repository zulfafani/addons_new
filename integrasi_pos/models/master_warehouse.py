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

    warehouse_name = fields.Char(string="Warehouse Name")
    warehouse_code = fields.Char(string="Warehouse Code")
    warehouse_transit = fields.Char(string="Warehouse Transit")
    warehouse_short = fields.Char(string="Warehouse Short")