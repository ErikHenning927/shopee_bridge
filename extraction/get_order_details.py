import os
import sys
import requests
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.shopee_parameters import *

def get_order_details(partner_id, partner_key, shop_id, access_token, order_sn_list, prefix):
    hora_inicio = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("Coletando detalhes de", len(order_sn_list), "pedidos para shop_id:", shop_id)
    path = '/api/v2/order/get_order_detail'
    all_details = []

    for i in range(0, len(order_sn_list), 50):
        subset = ','.join(order_sn_list[i:i + 50])  # Pega até 50 order_sn por vez
        timestamp = int(time.time())  # Atualiza o timestamp a cada chamada
        params = {
            'timestamp': timestamp,
            'shop_id': shop_id,
            'partner_id': partner_id,
            'access_token': access_token,
            'order_sn_list': subset,
            'response_optional_fields': 'order_sn,order_status,create_time,pay_time'
        }
        signature = generate_signature(partner_id, path, timestamp, access_token, shop_id, partner_key)
        params['sign'] = signature
        url = f'https://partner.shopeemobile.com{path}'
        response = requests.get(url, params=params)
        if response.status_code == 200:
            response_json = response.json()
            for detail in response_json.get('response', {}).get('order_list', []):
                detail['order_sn'] = f"{prefix}{detail['order_sn']}"
            all_details.extend(response_json.get('response', {}).get('order_list', []))
        else:
            print("Error:", response.status_code, response.text)
            break
    
    hora_fim = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tempo_total = datetime.strptime(hora_fim, '%Y-%m-%d %H:%M:%S') - datetime.strptime(hora_inicio, '%Y-%m-%d %H:%M:%S')
    print(f"Processo detalhe dos pedidos finalizado em {tempo_total}")

    return all_details
