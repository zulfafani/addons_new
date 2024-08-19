import json
import xmlrpc.client
import random
import requests

# Odoo Server information
url = "http://pos-store.visi-intech.com/jsonrpc"
db = "odoo-store-1"
username = "admin"
password = "f44603d2a14298c330fe15d603ca3a3db707f4d6"

# Connect to the server
response = requests.post(f'{url}/jsonrpc', json={
    "jsonrpc": "2.0",
    "method": "call",
    "params": {
        "db": db,
        "login": username,
        "password": password
    },
    "id": 1
})

uid = response.json()['result']['uid']
session_id = response.cookies.get('session_id')

# Get the active POS session
response = requests.post(f'{url}/jsonrpc', json={
    "jsonrpc": "2.0",
    "method": "call",
    "params": {
        "model": "pos.session",
        "method": "search",
        "args": [[("state", "=", "opened")]],
        "kwargs": {
            "limit": 1
        },
        "context": {"uid": uid, "session_id": session_id}
    },
    "id": 2
})
pos_session = response.json()['result']
if not pos_session:
    raise Exception('No active POS session found.')

# Get all products to be used in orders
product_codes = ['LBR00001', 'LBR00002', 'LBR00003', 'LBR00088', 'LBR00099', 'LBR00008', 'LBR00007', 'LBR00006', 'LBR00009', 'LBR00004']
response = requests.post(f'{url}/jsonrpc', json={
    "jsonrpc": "2.0",
    "method": "call",
    "params": {
        "model": "product.product",
        "method": "search_read",
        "args": [[("default_code", "in", product_codes)], ["id", "name", "list_price", "taxes_id"]],
        "context": {"uid": uid, "session_id": session_id}
    },
    "id": 3
})
products = response.json()['result']
if not products:
    raise Exception('No products found.')

# Create 50 POS orders
pos_orders = []
for i in range(50):
    order_lines = []
    total_amount = 0
    total_tax = 0

    for product in products:
        qty = random.randint(1, 500)
        
        # Compute taxes
        response = requests.post(f'{url}/jsonrpc', json={
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "model": "account.tax",
                "method": "compute_all",
                "args": [product['list_price'], qty, product['id']],
                "context": {"uid": uid, "session_id": session_id}
            },
            "id": 4
        })
        taxes = response.json()['result']
        line_tax = sum(t['amount'] for t in taxes['taxes'])
        line_total = taxes['total_included']
        line_subtotal = taxes['total_excluded']
        line_subtotal_incl = line_subtotal + line_tax

        order_line = (0, 0, {
            'product_id': product['id'],
            'name': product['name'],
            'full_product_name': product['name'],
            'qty': qty,
            'price_unit': product['list_price'],
            'price_subtotal': line_subtotal,
            'price_subtotal_incl': line_subtotal_incl,
            'tax_ids': [(6, 0, product['taxes_id'])],
        })
        order_lines.append(order_line)

        total_amount += line_total
        total_tax += line_tax

    # Get payment method
    response = requests.post(f'{url}/jsonrpc', json={
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "pos.payment.method",
            "method": "search_read",
            "args": [[], ["id"]],
            "context": {"uid": uid, "session_id": session_id},
            "limit": 1
        },
        "id": 5
    })
    payment_method = response.json()['result']
    if not payment_method:
        raise Exception('No payment method found.')
    
    payment_line = (0, 0, {
        'payment_method_id': payment_method[0]['id'],
        'amount': total_amount,
    })

    # Create POS order
    pos_order_data = {
        'session_id': pos_session[0],
        'name': f"POS-{i+1:05d}",
        'partner_id': False,
        'lines': order_lines,
        'partner_id': 7,
        'employee_id': 1,
        'payment_ids': [payment_line],
        'amount_total': total_amount,
        'amount_tax': total_tax,
        'amount_paid': total_amount,
        'amount_return': 0,
    }

    response = requests.post(f'{url}/jsonrpc', json={
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "pos.order",
            "method": "create",
            "args": [pos_order_data],
            "context": {"uid": uid, "session_id": session_id}
        },
        "id": 6
    })
    pos_order = response.json()['result']
    pos_orders.append(pos_order)

    # Action POS order invoice
    response = requests.post(f'{url}/jsonrpc', json={
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "pos.order",
            "method": "action_pos_order_invoice",
            "args": [pos_order],
            "context": {"uid": uid, "session_id": session_id}
        },
        "id": 7
    })

print("POS Orders created:", pos_orders)