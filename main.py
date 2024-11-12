import os
import sys
from datetime import datetime, timedelta
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from extraction.get_order_list import get_order_list
from extraction.get_order_details import get_order_details
from SQL.insert_orders import insert_order_details_to_db
from connections.all_connections import load_token_data_shopee
def main():
    data = load_token_data_shopee()

    brit_token = data.get('811879342', {}).get('access_token')
    phil_token = data.get('811034337', {}).get('access_token')
    hora_inicio = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    partner_id = 2007922
    partner_key = '51684e6444564b5a5a504c546977755742594a6d544476556952786e54776362'
    data_ini = datetime.now() - timedelta(days=3)
    date_str = data_ini.strftime('%Y-%m-%d')
    
    shop_data = {
        811034337: phil_token,  
        811879342: brit_token
    }

    for shop_id, access_token in shop_data.items():
        prefix = 'SHP-' if shop_id == 811034337 else 'SHB-'  # Define o prefixo com base no shop_id
        
        order_sn_list = get_order_list(partner_id, partner_key, shop_id, access_token, date_str, prefix)
        cleaned_order_sn_list = [sn.replace(prefix, '') for sn in order_sn_list]
        
        order_details = get_order_details(partner_id, partner_key, shop_id, access_token, cleaned_order_sn_list, prefix)
        insert_order_details_to_db(order_details)
    
    hora_fim = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tempo_total = datetime.strptime(hora_fim, '%Y-%m-%d %H:%M:%S') - datetime.strptime(hora_inicio, '%Y-%m-%d %H:%M:%S')
    print(f"Processo finalizado em {tempo_total}")

main()
