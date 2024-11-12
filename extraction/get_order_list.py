import os
import sys
import requests
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.shopee_parameters import *


def get_order_list(partner_id, partner_key, shop_id, access_token, date_str, prefix):
    hora_inicio = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("Coletando pedidos de", date_str, "para shop_id:", shop_id)
    timestamp = int(time.time())
    path = '/api/v2/order/get_order_list'
    time_from = date_to_timestamp(date_str)
    time_to = time_from + 3 * 86400  # Cobre 2 dias
    #time_to = time_from + 86400  # Cobre 2 dias
    all_orders = []
    cursor = ''

    while True:
        params = {
            'timestamp': timestamp,
            'shop_id': shop_id,
            'order_status': 'READY_TO_SHIP',
            'partner_id': partner_id,
            'access_token': access_token,
            'page_size': 100,
            'response_optional_fields': 'order_status',
            'time_range_field': 'create_time',
            'time_from': time_from,
            'time_to': time_to,
            'cursor': cursor
        }

        signature = generate_signature(partner_id, path, timestamp, access_token, shop_id, partner_key)
        params['sign'] = signature

        url = f'https://partner.shopeemobile.com{path}'
        response = requests.get(url, params=params)

        if response.status_code == 200:
            response_json = response.json()
            orders = response_json.get('response', {}).get('order_list', [])
            all_orders.extend(orders)
            
            if response_json.get('response', {}).get('more') == True:
                cursor = response_json.get('response', {}).get('next_cursor', '')
            else:
                break
        else:
            print("Error:", response.status_code, response.text)
            break
    
    hora_fim = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tempo_total = datetime.strptime(hora_fim, '%Y-%m-%d %H:%M:%S') - datetime.strptime(hora_inicio, '%Y-%m-%d %H:%M:%S')
    print(f"Processo de extração de pedidos finalizado em {tempo_total}")
    return [f"{prefix}{order['order_sn']}" for order in all_orders]
