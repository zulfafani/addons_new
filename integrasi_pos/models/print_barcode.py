# from odoo import models, fields, api, _
# from pytz import timezone
# from datetime import datetime, timedelta
# from odoo.exceptions import ValidationError, UserError
# import base64
# from reportlab.graphics import renderPM
# from reportlab.graphics.barcode import createBarcodeDrawing
# from reportlab.lib.pagesizes import letter, A4, landscape, portrait
# from reportlab.lib.units import mm, inch
# from reportlab.pdfgen import canvas
# import subprocess

# import tempfile
# import os
# import platform
# from reportlab.pdfbase import pdfmetrics
# from reportlab.pdfbase.ttfonts import TTFont
# from reportlab.pdfbase.pdfmetrics import registerFont
# from matplotlib import font_manager
# import barcode
# from barcode import EAN13, Code128
# from barcode.writer import ImageWriter
# import io
# from PIL import Image


# class PrintBarcode(models.Model):
#     _name = "print.barcode"
#     _description = "Print Barcode"

#     pilihan = fields.Selection([
#         ('print', 'Print'),
#         ('margin', 'Margin'),
#         ('setting', 'Setting'),
#         ('font', 'Font'),
#     ])

#     # Config Printer
#     nama_printer = fields.Many2one('printer.list', string="Printer")
#     printer_list = fields.Selection(selection='_get_printer_selection', string="Printer")
#     ukuran_kertas = fields.Many2one('paper.size', string="Ukuran Kertas")
#     size_kertas = fields.Selection(selection='_get_paper_sizes', string="Ukuran Kertas", default='a4')
#     lebar = fields.Float(string="Lebar", default=210.0)  # Default A4 width in mm
#     tinggi = fields.Float(string="Tinggi", default=297.0)  # Default A4 height in mm
#     orientasi = fields.Selection([
#         ('landscape', 'Landscape'),
#         ('portrait', 'Portrait'),
#     ], default='portrait')

#     # Margin Config
#     margin_atas = fields.Float(string="Margin Atas", default=10.0)
#     margin_bawah = fields.Float(string="Margin Bawah", default=10.0)
#     margin_kiri = fields.Float(string="Margin Kiri", default=10.0)
#     margin_kanan = fields.Float(string="Margin Kanan", default=10.0)
#     label = fields.Selection([
#         ('1', '1 Label'),
#         ('2', '2 Label'),
#     ], default='1')
#     jumlah_baris = fields.Float(string="Jumlah Baris", default=10.0)

#     # Setting Barcode
#     jumlah_kolom = fields.Float(string="Jumlah Kolom", default=3.0)
#     jarak_antar_kolom = fields.Float(string="Jarak Antar Kolom", default=5.0)
#     tinggi_baris = fields.Float(string="Tinggi Baris", default=20.0)
#     lebar_kolom = fields.Float(string="Lebar Kolom", default=60.0)
#     jumlah_karakter = fields.Float(string="Jumlah Karakter", default=13.0)

#     # Font Barcode
#     available_fonts = fields.Selection(selection='_get_available_fonts', string="Jenis Fonts Text")
#     available_fonts_barcode = fields.Selection([
#         ('ean13', 'EAN-13'),
#         ('ean8', 'EAN-8'),
#         ('ean', 'EAN'),
#         ('code39', 'Code 39'),
#         ('code128', 'Code 128'),
#         ('pzn', 'PZN'),
#         ('upc', 'UPC'),
#         ('isbn13', 'ISBN-13'),
#         ('isbn10', 'ISBN-10'),
#         ('issn', 'ISSN'),
#         ('jan', 'JAN'),
#         ('upca', 'UPC-A'),
#     ], string="Jenis Barcode")
#     ukuran_font_barcode = fields.Float(string="Ukuran Font Barcode", default=40.0)
#     ukuran_font_kode = fields.Float(string="Ukuran Font Kode", default=8.0)
#     ukuran_font_nama = fields.Float(string="Ukuran Font Nama", default=8.0)
#     ukuran_font_harga = fields.Float(string="Ukuran Font Harga", default=10.0)
#     posisi_barcode = fields.Float(string="Posisi Barcode", default=15.0)
#     posisi_kode = fields.Float(string="Posisi Kode", default=5.0)
#     tinggi_kode = fields.Float(string="Tinggi Kode", default=10.0)
#     posisi_harga = fields.Float(string="Posisi Harga", default=30.0)
#     posisi_nama_barang = fields.Float(string="Posisi Nama Barang", default=10.0)
#     tinggi_nama_barnag = fields.Float(string="Tinggi Nama Barang", default=15.0)

#     #Filter date
#     start_date = fields.Datetime(string="Date From")
#     end_date = fields.Datetime(string="Date To", default=lambda self: fields.Date.today())
    
#     #Document Inventory
#     doc_type = fields.Selection([('receipt', 'GRPO'), ('good_receipts', 'Good Receipts'), ('ts_out', 'TS Out'), ('ts_in', 'TS In')], string="Tipe Dokumen")

#     # For storing the generated PDF
#     barcode_pdf = fields.Binary(string="Generated Barcode PDF", attachment=True)
#     barcode_filename = fields.Char(string="Barcode Filename")

#     product_line_ids = fields.One2many('print.barcode.product.line', 'barcode_id', string='Products')

#     @api.model
#     def _get_paper_sizes(self):
#         """Get standard paper sizes using papersize library"""
#         try:
#             import papersize
            
#             # Get standard paper sizes from papersize library
#             paper_sizes = []
            
#             # ISO A series
#             for size in ['a0', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8']:
#                 width_mm = papersize.parse_length(papersize.SIZES[size][0]) * 25.4  # convert inches to mm
#                 height_mm = papersize.parse_length(papersize.SIZES[size][1]) * 25.4  # convert inches to mm
#                 paper_sizes.append((size, f"{size.upper()} ({width_mm:.1f} × {height_mm:.1f} mm)"))
            
#             # Common North American sizes
#             for size in ['letter', 'legal', 'tabloid']:
#                 width_mm = papersize.parse_length(papersize.SIZES[size][0]) * 25.4  # convert inches to mm
#                 height_mm = papersize.parse_length(papersize.SIZES[size][1]) * 25.4  # convert inches to mm
#                 paper_sizes.append((size, f"{size.title()} ({width_mm:.1f} × {height_mm:.1f} mm)"))
            
#             # Add custom option
#             paper_sizes.append(('custom', 'Custom'))
            
#             return paper_sizes
#         except ImportError:
#             # Fallback if papersize library is not available
#             return [
#                 ('a0', 'A0 (841.0 × 1189.0 mm)'),
#                 ('a1', 'A1 (594.0 × 841.0 mm)'),
#                 ('a2', 'A2 (420.0 × 594.0 mm)'),
#                 ('a3', 'A3 (297.0 × 420.0 mm)'),
#                 ('a4', 'A4 (210.0 × 297.0 mm)'),
#                 ('a5', 'A5 (148.0 × 210.0 mm)'),
#                 ('a6', 'A6 (105.0 × 148.0 mm)'),
#                 ('a7', 'A7 (74.0 × 105.0 mm)'),
#                 ('a8', 'A8 (52.0 × 74.0 mm)'),
#                 ('letter', 'Letter (215.9 × 279.4 mm)'),
#                 ('legal', 'Legal (215.9 × 355.6 mm)'),
#                 ('tabloid', 'Tabloid (279.4 × 431.8 mm)'),
#                 ('custom', 'Custom'),
#             ]
    
#     def _get_paper_size_dimensions(self, size_key):
#         """Get dimensions (width, height) in mm for the given paper size key"""
#         try:
#             import papersize
#             if size_key in papersize.SIZES:
#                 width_in = papersize.parse_length(papersize.SIZES[size_key][0])
#                 height_in = papersize.parse_length(papersize.SIZES[size_key][1])
#                 # Convert from inches to mm
#                 return (width_in * 25.4, height_in * 25.4)
#         except ImportError:
#             # Fallback dimensions if papersize library is not available
#             fallback_sizes = {
#                 'a0': (841.0, 1189.0),
#                 'a1': (594.0, 841.0),
#                 'a2': (420.0, 594.0),
#                 'a3': (297.0, 420.0),
#                 'a4': (210.0, 297.0),
#                 'a5': (148.0, 210.0),
#                 'a6': (105.0, 148.0),
#                 'a7': (74.0, 105.0),
#                 'a8': (52.0, 74.0),
#                 'letter': (215.9, 279.4),
#                 'legal': (215.9, 355.6),
#                 'tabloid': (279.4, 431.8),
#             }
#             return fallback_sizes.get(size_key.lower(), (210.0, 297.0))  # Default to A4 if not found
            
#         # Default to A4 if size not in papersize library
#         return (210.0, 297.0)
    
#     @api.onchange('size_kertas')
#     def _onchange_size_kertas(self):
#         """Update lebar and tinggi fields when paper size changes"""
#         if self.size_kertas and self.size_kertas != 'custom':
#             self.lebar, self.tinggi = self._get_paper_size_dimensions(self.size_kertas)

#     @api.onchange('doc_type')
#     def _onchange_doc_type(self):
#         """
#         Automatically populate product_line_ids when doc_type is selected
#         """
#         # Clear existing product lines
#         self.product_line_ids = [(5, 0, 0)]
        
#         # If doc_type is not selected, do nothing
#         if not self.doc_type:
#             return
            
#         # Validate dates are provided
#         if not self.start_date or not self.end_date:
#             return {'warning': {
#                 'title': 'Information',
#                 'message': 'Mohon tentukan Tanggal Mulai dan Tanggal Akhir terlebih dahulu.'
#             }}
            
#         # Validate date range
#         if self.end_date < self.start_date:
#             return {'warning': {
#                 'title': 'Warning',
#                 'message': 'Tanggal Akhir tidak boleh lebih awal dari Tanggal Mulai.'
#             }}
            
#         # Mapping dari pilihan doc_type ke nama picking_type_id.name
#         doc_type_mapping = {
#             'receipt': 'GRPO',
#             'good_receipts': 'Goods Receipts',
#             'ts_out': 'TS Out',
#             'ts_in': 'TS In',
#         }

#         picking_type_name = doc_type_mapping.get(self.doc_type)
        
#         if not picking_type_name:
#             return {'warning': {
#                 'title': 'Warning',
#                 'message': 'Tipe Dokumen tidak valid.'
#             }}

#         # Convert start_date dan end_date ke datetime
#         start_datetime = datetime.combine(self.start_date, datetime.min.time())
#         end_datetime = datetime.combine(self.end_date, datetime.max.time())

#         domain = [
#             ('picking_type_id.name', '=', picking_type_name),
#             ('scheduled_date', '>=', start_datetime),
#             ('scheduled_date', '<=', end_datetime),
#             ('state', '=', 'done')
#         ]

#         pickings = self.env['stock.picking'].search(domain)

#         if not pickings:
#             return {'warning': {
#                 'title': 'Information',
#                 'message': f"Tidak ditemukan dokumen {picking_type_name} yang selesai dalam rentang {self.start_date} hingga {self.end_date}."
#             }}

#         products_data = {}

#         for picking in pickings:
#             for move in picking.move_ids_without_package:
#                 product = move.product_id
#                 if not product:
#                     continue

#                 if product.id in products_data:
#                     products_data[product.id]['qty'] += move.quantity
#                 else:
#                     products_data[product.id] = {
#                         'product_id': product.id,
#                         'qty': move.quantity,
#                         'receipt_date': fields.Date.to_date(picking.scheduled_date)
#                     }

#         if not products_data:
#             return {'warning': {
#                 'title': 'Information',
#                 'message': "Tidak ada produk dengan barcode yang ditemukan pada dokumen yang dipilih."
#             }}

#         # Prepare product lines
#         product_lines = []
#         for product_data in products_data.values():
#             product_lines.append((0, 0, {
#                 'product_id': product_data['product_id'],
#                 'jumlah_copy': 1.0,
#                 'tanggal_masuk': product_data['receipt_date']
#             }))
            
#         # Update product_line_ids with new records
#         if product_lines:
#             self.product_line_ids = product_lines
    
#     @api.model
#     def _get_printer_selection(self):
#         """Get available printers as selection options"""
#         printers = self._get_system_printers()
#         return [(printer, printer) for printer in printers]
    
#     @api.model
#     def _get_system_printers(self):
#         """
#         Get list of system printers
#         Supports Windows, Linux (including Ubuntu) and macOS
#         """
#         printers = []
#         system = platform.system()
        
#         try:
#             if system == "Windows":
#                 try:
#                     import win32print
#                     # Using Win32 API for Windows
#                     for printer in win32print.EnumPrinters(win32print.PRINTER_ENUM_LOCAL, None, 1):
#                         printer_name = printer[2]
#                         printers.append(printer_name)
#                 except ImportError:
#                     # Fallback if win32print is not available
#                     output = subprocess.check_output(['wmic', 'printer', 'get', 'name'], 
#                                                     universal_newlines=True, 
#                                                     shell=True)
#                     for line in output.split('\n'):
#                         if line.strip() and line.strip().lower() != 'name':
#                             printers.append(line.strip())
            
#             elif system == "Linux" or system == "Darwin":  # Linux or macOS
#                 # Try lpstat (CUPS)
#                 try:
#                     output = subprocess.check_output(['lpstat', '-a'], 
#                                                    universal_newlines=True)
#                     for line in output.split('\n'):
#                         if line.strip():
#                             # Format: "PrinterName accepting requests since..."
#                             printer_name = line.split()[0]
#                             printers.append(printer_name)
#                 except:
#                     # Fallback to lpadmin or lpc
#                     try:
#                         output = subprocess.check_output(['lpc', 'status'], 
#                                                        universal_newlines=True)
#                         current_printer = None
#                         for line in output.split('\n'):
#                             if line.strip() and not line.startswith('\t'):
#                                 current_printer = line.split(':')[0]
#                                 printers.append(current_printer)
#                     except:
#                         # Last fallback: check /etc/cups/printers.conf
#                         if os.path.exists('/etc/cups/printers.conf'):
#                             try:
#                                 with open('/etc/cups/printers.conf', 'r') as f:
#                                     for line in f:
#                                         if line.startswith('<Printer '):
#                                             # Format: <Printer PrinterName>
#                                             printer_name = line[9:-1]  # Extract printer name
#                                             printers.append(printer_name)
#                             except:
#                                 pass
        
#         except Exception as e:
#             # Just return empty list on error instead of raising exception
#             # since this is called during field selection initialization
#             return []
        
#         # Add a 'None' option if needed
#         if not printers:
#             printers = [('none', 'No printers found')]
            
#         return printers

#     @api.model
#     def _get_available_fonts(self):
#         """Method to populate available fonts for selection field"""
#         return self._get_system_fonts()

#     @api.model
#     def _get_system_fonts(self):
#         """Load system fonts available."""
#         font_files = font_manager.findSystemFonts(fontpaths=None, fontext='ttf')
#         font_names = []
#         for font_path in font_files:
#             try:
#                 font = font_manager.FontProperties(fname=font_path)
#                 font_name = font.get_name()
#                 font_names.append((font_name, font_name))
#             except Exception:
#                 continue
#         # Remove duplicates
#         font_names = list(set(font_names))
#         return sorted(font_names)

#     @api.model
#     def fields_view_get(self, view_id=None, view_type='form', toolbar=False, submenu=False):
#         """Override to inject fonts dynamically into selection field."""
#         res = super(PrintBarcode, self).fields_view_get(view_id, view_type, toolbar=toolbar, submenu=submenu)
#         fonts = self._get_system_fonts()
#         if 'fields' in res and 'available_fonts' in res['fields']:
#             res['fields']['available_fonts']['selection'] = fonts
#         return res
    
#     def _get_page_size(self):
#         """Get page size based on configuration"""
#         if self.size_kertas and self.size_kertas != 'custom':
#             width_mm, height_mm = self._get_paper_size_dimensions(self.size_kertas)
#         else:
#             width_mm, height_mm = self.lebar, self.tinggi

#         # Convert mm to points (ReportLab uses points)
#         width_pts = width_mm * mm
#         height_pts = height_mm * mm

#         # Return dimensions based on orientation
#         if self.orientasi == 'landscape':
#             return (height_pts, width_pts)  # Swap dimensions for landscape
#         return (width_pts, height_pts)  # Portrait orientation
    
#     def _create_barcode_drawing(self, barcode_value, width=50*mm, height=20*mm):
#         """Create a barcode using ReportLab's built-in functionality."""
#         try:
#             # Default to Code128 if no barcode type selected (more versatile)
#             barcode_type = self.available_fonts_barcode or 'code128'
            
#             # Map python-barcode types to ReportLab barcode types
#             barcode_type_mapping = {
#                 'ean13': 'EAN13',
#                 'ean8': 'EAN8',
#                 'ean': 'EAN13',  # Default to EAN13 for generic EAN
#                 'code39': 'Standard39',
#                 'code128': 'Code128',
#                 'upc': 'UPCA',
#                 'upca': 'UPCA',
#                 'isbn13': 'EAN13',  # ISBN-13 is EAN-13 format
#                 'isbn10': 'ISBN',
#                 'issn': 'ISSN',
#                 'jan': 'JAN',
#                 'pzn': 'PZN',
#             }
            
#             reportlab_barcode_type = barcode_type_mapping.get(barcode_type, 'Code128')
            
#             # Handle specific barcode type requirements
#             if reportlab_barcode_type == 'EAN13' and len(barcode_value) < 12:
#                 # EAN13 needs at least 12 digits (13th is checksum)
#                 barcode_value = barcode_value.zfill(12)
#             elif reportlab_barcode_type == 'EAN8' and len(barcode_value) < 7:
#                 # EAN8 needs at least 7 digits (8th is checksum)
#                 barcode_value = barcode_value.zfill(7)
            
#             # Create the barcode drawing using ReportLab's built-in functionality
#             barcode_drawing = createBarcodeDrawing(
#                 reportlab_barcode_type,
#                 value=barcode_value,
#                 width=width,
#                 height=height,
#                 humanReadable=False  # Don't add text, we'll do it separately
#             )
            
#             return barcode_drawing
#         except Exception as e:
#             # If ReportLab's built-in function fails, provide fallback that always works
#             try:
#                 # Code128 is the most versatile and accepts almost any string
#                 barcode_drawing = createBarcodeDrawing(
#                     'Code128',
#                     value=barcode_value,
#                     width=width,
#                     height=height,
#                     humanReadable=False
#                 )
#                 return barcode_drawing
#             except Exception as inner_e:
#                 # Last resort: create a dummy Drawing with a message
#                 from reportlab.graphics.shapes import Drawing, String
#                 drawing = Drawing(width, height)
#                 drawing.add(String(width/2, height/2, f"Invalid: {barcode_value}", textAnchor='middle'))
#                 return drawing
    
#     def action_print_barcode(self):
#         """Action to print barcodes for selected products"""
#         if not self.product_line_ids:
#             raise ValidationError("No products selected for printing barcodes.")
        
#         # Collect product IDs and copies only for products with barcodes
#         product_data = {}
#         for line in self.product_line_ids:
#             if line.product_id.barcode:  # Only include products with barcodes
#                 product_data[line.product_id.id] = int(line.jumlah_copy)
        
#         if not product_data:
#             raise ValidationError("None of the selected products have barcodes assigned. Please assign barcodes to products first.")
        
#         # Optionally, inform the user about skipped products
#         skipped_products = self.product_line_ids.filtered(lambda l: not l.product_id.barcode).mapped('product_id.name')
#         if skipped_products:
#             message = f"Note: {len(skipped_products)} products without barcodes were skipped."
#             # You could log this or add it to a notification but we'll proceed with the ones that have barcodes
        
#         # Pass the product data to generate_barcode_pdf
#         return self.generate_barcode_pdf(product_data)
        
#     # Update your generate_barcode_pdf method to handle copies per product
#     def generate_barcode_pdf(self, product_data):
#         """Generate PDF with barcodes for selected products, supporting custom fonts and copies."""
#         temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
#         file_path = temp_file.name
#         temp_file.close()

#         # Get page size based on the selected paper size or custom dimensions
#         page_size = self._get_page_size()
#         c = canvas.Canvas(file_path, pagesize=page_size)
        
#         # Get the dimensions of the page in the correct orientation
#         page_width, page_height = page_size

#         # Calculate usable area considering margins
#         usable_width = page_width - (self.margin_kiri + self.margin_kanan) * mm
#         usable_height = page_height - (self.margin_atas + self.margin_bawah) * mm

#         # Calculate label dimensions based on the number of columns and rows
#         label_width = usable_width / int(self.jumlah_kolom)  # Convert float to int
#         label_height = usable_height / int(self.jumlah_baris)  # Convert float to int

#         # Get spacing factor based on label selection
#         spacing_factor = int(self.label) if self.label else 1

#         products = self.env['product.product'].browse(product_data.keys())

#         x_start = self.margin_kiri * mm
#         y_start = page_height - self.margin_atas * mm

#         current_row = 0
#         current_col = 0

#         # Get font from field
#         font_name_product = self.available_fonts or "Helvetica"
#         font_name_barcode = self.available_fonts or "Helvetica"
#         font_name_code = self.available_fonts or "Helvetica"
#         font_name_price = self.available_fonts or "Helvetica"

#         # Check if font is not a standard ReportLab font, then register the TTF font
#         standard_fonts = [
#             'Courier', 'Courier-Bold', 'Courier-Oblique', 'Courier-BoldOblique',
#             'Helvetica', 'Helvetica-Bold', 'Helvetica-Oblique', 'Helvetica-BoldOblique',
#             'Times-Roman', 'Times-Bold', 'Times-Italic', 'Times-BoldItalic',
#             'Symbol', 'ZapfDingbats'
#         ]

#         if font_name_product not in standard_fonts:
#             # Find TTF path
#             font_paths = font_manager.findSystemFonts(fontpaths=None, fontext='ttf')
#             for path in font_paths:
#                 try:
#                     font_prop = font_manager.FontProperties(fname=path)
#                     if font_prop.get_name() == font_name_product:
#                         font_id = font_name_product.replace(" ", "_")
#                         pdfmetrics.registerFont(TTFont(font_id, path))
#                         font_name_product = font_id
#                         font_name_barcode = font_id
#                         font_name_code = font_id
#                         font_name_price = font_id
#                         break
#                 except Exception:
#                     continue

#         for product in products:
#             if not product.barcode:
#                 continue

#             barcode_value = product.barcode.strip()
#             copies = product_data.get(product.id, 1)

#             for copy in range(copies):
#                 # Apply spacing based on the label field
#                 x = x_start + current_col * label_width * spacing_factor
#                 y = y_start - current_row * label_height

#                 # Draw Product Name
#                 c.setFont(font_name_product, self.ukuran_font_nama)
#                 product_name = product.name[:25] + ("..." if len(product.name) > 25 else "")
#                 c.drawCentredString(x + label_width / 2, y - self.posisi_nama_barang * mm, product_name)

#                 # Draw Barcode
#                 barcode = self._create_barcode_drawing(
#                     barcode_value,
#                     width=self.lebar_kolom * mm,
#                     height=self.ukuran_font_barcode * mm
#                 )
#                 barcode_x = x + (label_width - self.lebar_kolom * mm) / 2
#                 barcode_y = y - (self.posisi_barcode * mm) - (self.ukuran_font_barcode * mm)
#                 barcode.drawOn(c, barcode_x, barcode_y)

#                 # Draw Barcode Number
#                 c.setFont(font_name_code, self.ukuran_font_kode)
#                 c.drawCentredString(x + label_width / 2, barcode_y - self.posisi_kode * mm, product.barcode)

#                 # Draw Price
#                 product_line = self.env['print.barcode.product.line'].search([
#                     ('barcode_id', '=', self.id),
#                     ('product_id', '=', product.id)
#                 ], limit=1)

#                 if product_line and product_line.harga_jual:
#                     c.setFont(font_name_price, self.ukuran_font_harga)
#                     price_text = f"Rp {product_line.harga_jual:,.0f}"
#                     c.drawCentredString(x + label_width / 2, y - self.posisi_harga * mm, price_text)

#                 # Move to next label position with appropriate spacing
#                 current_col += 1
#                 # Check if we've reached the maximum columns for this row
#                 # We need to adjust the maximum columns based on the spacing factor
#                 max_cols = int(self.jumlah_kolom) // spacing_factor
#                 if max_cols < 1:
#                     max_cols = 1  # Ensure at least one column
                    
#                 if current_col >= max_cols:
#                     current_col = 0
#                     current_row += 1

#                 # Check if we need a new page
#                 if current_row >= int(self.jumlah_baris):
#                     c.showPage()
#                     current_row = 0
#                     current_col = 0
#                     c.setFont(font_name_code, self.ukuran_font_kode)

#         c.save()

#         with open(file_path, 'rb') as pdf_file:
#             pdf_data = pdf_file.read()

#         os.unlink(file_path)

#         filename = f"barcodes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
#         self.write({
#             'barcode_pdf': base64.b64encode(pdf_data),
#             'barcode_filename': filename
#         })

#         return {
#             'type': 'ir.actions.act_url',
#             'url': f'/web/content/?model=print.barcode&id={self.id}&field=barcode_pdf&filename={filename}',
#             'target': 'new',
#         }

#     def action_open_pdf(self):
#         """Open the generated PDF in a new browser tab"""
#         if not self.barcode_pdf:
#             raise ValidationError("No barcode PDF has been generated yet.")
            
#         filename = self.barcode_filename or f"barcodes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        
#         return {
#             'type': 'ir.actions.act_url',
#             'url': f'/web/content/?model=print.barcode&id={self.id}&field=barcode_pdf&filename={filename}',
#             'target': 'new',
#         }

    
#     def action_print_to_printer(self):
#         """Send the generated PDF directly to the selected printer"""
#         if not self.nama_printer:
#             raise ValidationError("Please select a printer.")
        
#         if not self.barcode_pdf:
#             raise ValidationError("No barcode PDF generated. Please generate barcode first.")
        
#         # This is where you would integrate with a printing system
#         # The implementation depends on your server setup and printing system
#         try:
#             # Decode the PDF
#             pdf_data = base64.b64decode(self.barcode_pdf)
            
#             # Create temporary file
#             temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
#             file_path = temp_file.name
#             temp_file.write(pdf_data)
#             temp_file.close()
            
#             # Print using the printer's system name
#             printer_name = self.nama_printer.system_name
#             os.system(f'lpr -P {printer_name} {file_path}')
            
#             # Clean up
#             os.unlink(file_path)
            
#             return {'type': 'ir.actions.client', 'tag': 'reload'}
        
#         except Exception as e:
#             raise ValidationError(f"Error printing: {str(e)}")

# class PrintBarcodeProductLine(models.Model):
#     _name = "print.barcode.product.line"
#     _description = "Print Barcode Product Line"
    
#     barcode_id = fields.Many2one('print.barcode', string='Barcode Print', ondelete='cascade')
#     product_id = fields.Many2one('product.product', string='Product')
#     product_name = fields.Char(string='Product Name', related='product_id.name', readonly=True)
#     jumlah_copy = fields.Float(string="Jumlah Copy", default=1.0)
#     harga_jual = fields.Float(string="Harga Jual", related='product_id.list_price', readonly=True)
#     tanggal_masuk = fields.Date(string="Tanggal Masuk")
    
#     @api.onchange('product_id')
#     def _onchange_product_id(self):
#         if self.product_id:
#             self.harga_jual = self.product_id.list_price
#             # Find the latest receipt date for this product
#             self.tanggal_masuk = self._get_product_receipt_date()
    
#     def _get_product_receipt_date(self):
#         """Get the receipt date from stock.picking for GRPO or Goods Receipts"""
#         if not self.product_id:
#             return False
            
#         # Find stock moves for this product in GRPO or Goods Receipts pickings
#         stock_moves = self.env['stock.move'].search([
#             ('product_id', '=', self.product_id.id),
#             ('state', '=', 'done'),
#             ('picking_id.picking_type_id.name', 'in', ['GRPO', 'Goods Receipts']),
#         ], order='date desc', limit=1)
        
#         if stock_moves:
#             # Convert datetime to date
#             return fields.Date.to_date(stock_moves.date)
        
#         # Alternative approach: search directly in stock.picking
#         if not stock_moves:
#             picking = self.env['stock.picking'].search([
#                 ('picking_type_id.name', 'in', ['GRPO', 'Goods Receipts']),
#                 ('state', '=', 'done'),
#                 ('move_ids_without_package.product_id', '=', self.product_id.id)
#             ], order='date_done desc', limit=1)
            
#             if picking:
#                 return fields.Date.to_date(picking.scheduled_date)
                
#         return False

# class PrinterList(models.Model):
#     _name = "printer.list"
#     _description = "Printer List"
    
#     name = fields.Char(string="Printer Name")
#     system_name = fields.Char(string="System Printer Name")
#     is_active = fields.Boolean(string="Active", default=True)
    
#     @api.model
#     def get_system_printers(self):
#         """
#         Mendapatkan daftar printer dari sistem
#         Mendukung Windows, Linux (termasuk Ubuntu) dan macOS
#         """
#         printers = []
#         system = platform.system()
        
#         try:
#             if system == "Windows":
#                 try:
#                     import win32print
#                     # Menggunakan Win32 API untuk Windows
#                     for printer in win32print.EnumPrinters(win32print.PRINTER_ENUM_LOCAL, None, 1):
#                         printer_name = printer[2]
#                         printers.append(printer_name)
#                 except ImportError:
#                     # Fallback jika win32print tidak tersedia
#                     output = subprocess.check_output(['wmic', 'printer', 'get', 'name'], 
#                                                     universal_newlines=True, 
#                                                     shell=True)
#                     for line in output.split('\n'):
#                         if line.strip() and line.strip().lower() != 'name':
#                             printers.append(line.strip())
            
#             elif system == "Linux" or system == "Darwin":  # Linux atau macOS
#                 # Coba lpstat (CUPS)
#                 try:
#                     output = subprocess.check_output(['lpstat', '-a'], 
#                                                    universal_newlines=True)
#                     for line in output.split('\n'):
#                         if line.strip():
#                             # Format: "PrinterName accepting requests since..."
#                             printer_name = line.split()[0]
#                             printers.append(printer_name)
#                 except:
#                     # Fallback ke lpadmin atau lpc
#                     try:
#                         output = subprocess.check_output(['lpc', 'status'], 
#                                                        universal_newlines=True)
#                         current_printer = None
#                         for line in output.split('\n'):
#                             if line.strip() and not line.startswith('\t'):
#                                 current_printer = line.split(':')[0]
#                                 printers.append(current_printer)
#                     except:
#                         # Fallback terakhir: cek di /etc/cups/printers.conf
#                         if os.path.exists('/etc/cups/printers.conf'):
#                             try:
#                                 with open('/etc/cups/printers.conf', 'r') as f:
#                                     for line in f:
#                                         if line.startswith('<Printer '):
#                                             # Format: <Printer PrinterName>
#                                             printer_name = line[9:-1]  # Ambil nama printer
#                                             printers.append(printer_name)
#                             except:
#                                 pass
        
#         except Exception as e:
#             raise UserError(_("Failed to get system printers: %s") % str(e))
        
#         return printers
    
#     def action_load_system_printers(self):
#         """
#         Action untuk memuat printer sistem ke dalam database Odoo
#         """
#         printers = self.env['printer.list'].get_system_printers()
#         existing_printers = self.env['printer.list'].search([]).mapped('system_name')
        
#         new_printers_count = 0
#         for printer in printers:
#             if printer not in existing_printers:
#                 self.env['printer.list'].create({
#                     'name': printer,
#                     'system_name': printer,
#                     'is_active': True
#                 })
#                 new_printers_count += 1
        
#         return {
#             'type': 'ir.actions.client',
#             'tag': 'display_notification',
#             'params': {
#                 'title': _('Printers Loaded'),
#                 'message': _('%s printers detected and %s new printers added to the system') % 
#                           (len(printers), new_printers_count),
#                 'sticky': False,
#                 'next': {
#                     'type': 'ir.actions.act_window_close'
#                 }
#             }
#         }
    
    
# class PaperSize(models.Model):
#     _name = "paper.size"
#     _description = "Paper Size"
    
#     name = fields.Char(string="Paper Name", required=True)
#     width = fields.Float(string="Width (mm)", required=True)
#     height = fields.Float(string="Height (mm)", required=True)