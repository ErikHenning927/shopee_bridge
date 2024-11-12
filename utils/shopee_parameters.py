import time
import hmac
import hashlib
from datetime import datetime

def date_to_timestamp(date_str):
    return int(time.mktime(datetime.strptime(date_str, "%Y-%m-%d").timetuple()))

def generate_signature(partner_id, path, timestamp, access_token, shop_id, partner_key):
    base_string = f"{partner_id}{path}{timestamp}{access_token}{shop_id}"
    return hmac.new(partner_key.encode('utf-8'), base_string.encode('utf-8'), hashlib.sha256).hexdigest()
