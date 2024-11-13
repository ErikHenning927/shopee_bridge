import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from connections.all_connections import *


def create():
    conn = dw()
    if conn is None:
        print("Conexão não realizada.")
        return
    cursor = conn.cursor()
    query = '''
    
    CREATE TABLE DW.biecom.orders (
        order_sn VARCHAR(20) PRIMARY KEY,
        order_status VARCHAR(20),
        create_time DATE,
        pay_time TIMESTAMP
    );
    '''
    cursor.execute(query)
    conn.commit()
    conn.close()


  
create()


