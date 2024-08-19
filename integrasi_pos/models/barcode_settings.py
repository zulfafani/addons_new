from odoo import models, fields, api
from pytz import timezone
from datetime import datetime, timedelta
from odoo.exceptions import ValidationError

class BarcodeConfig(models.Model):
    _name = "barcode.config"
    _description = "Barcode Config"

    digit_awal = fields.Integer(string="Jumlah Digit Satuan")
    digit_akhir = fields.Integer(string="Jumlah Digit Decimal")
    prefix_timbangan = fields.Char(string="Prefix Timbangan")
    panjang_barcode = fields.Integer(string="Panjang Barcode")
    multiple_barcode_activate = fields.Boolean(string="Multiple Barcode Activation")

    @api.model
    def create(self, vals):
        """Override create method to ensure only one record can be created"""
        if self.search_count([]) >= 1:
            raise ValidationError("Anda hanya boleh membuat satu konfigurasi barcode!")
        return super(BarcodeConfig, self).create(vals)