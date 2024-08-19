# main.py
import cv2
import requests
from pyzbar import pyzbar
from kivy.app import App
from kivy.clock import Clock
from kivy.graphics.texture import Texture
from kivy.uix.image import Image
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label

ODOO_URL = "http://pos-store.visi-intech.com/barcode-callback"  # 🔁 GANTI dengan IP Odoo Anda

class BarcodeScannerApp(App):
    def build(self):
        self.img = Image()
        self.label = Label(text="Scanning...", size_hint=(1, 0.1))
        layout = BoxLayout(orientation='vertical')
        layout.add_widget(self.img)
        layout.add_widget(self.label)

        self.capture = cv2.VideoCapture(0)
        Clock.schedule_interval(self.update, 1.0 / 30.0)

        self.last_code = None
        return layout

    def update(self, dt):
        ret, frame = self.capture.read()
        if not ret:
            return

        barcodes = pyzbar.decode(frame)
        for barcode in barcodes:
            barcode_data = barcode.data.decode('utf-8')
            if self.last_code != barcode_data:
                self.last_code = barcode_data
                self.label.text = f"Scanned: {barcode_data}"
                self.send_to_odoo(barcode_data)

        buf = cv2.flip(frame, 0).tobytes()
        image_texture = Texture.create(size=(frame.shape[1], frame.shape[0]), colorfmt='bgr')
        image_texture.blit_buffer(buf, colorfmt='bgr', bufferfmt='ubyte')
        self.img.texture = image_texture

    def send_to_odoo(self, barcode):
        try:
            res = requests.post(ODOO_URL, data={'code': barcode}, timeout=5)
            if res.status_code == 200:
                self.label.text = f"✅ Sent: {barcode}"
            else:
                self.label.text = f"❌ Failed ({res.status_code})"
        except Exception as e:
            self.label.text = f"❌ Error: {str(e)}"

    def on_stop(self):
        self.capture.release()

if __name__ == '__main__':
    BarcodeScannerApp().run()
