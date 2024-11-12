import os
import sys
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from connections.all_connections import *

def insert_order_details_to_db(order_details):
    hora_inicio = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = dw()
    if conn is None:
        print("Conexão não realizada.")
        return
    cursor = conn.cursor()
    print("Iniciando inserção de dados...")
    batch_size = 2000
    batch = []
    unique_order_sn = set()

    try:
        for detail in order_details:
            order_sn = detail.get('order_sn')
            if order_sn in unique_order_sn:
                continue  # Skip duplicates in order_details
            unique_order_sn.add(order_sn)

            create_time = detail.get('create_time')
            formatted_create_time = datetime.fromtimestamp(create_time).strftime('%Y-%m-%d') if create_time else None
            pay_time = detail.get('pay_time')
            formatted_pay_time = datetime.fromtimestamp(pay_time).strftime('%Y-%m-%d %H:%M:%S') if pay_time else None
            batch.append((order_sn, detail.get('order_status'), formatted_create_time, formatted_pay_time))
            
            if len(batch) >= batch_size:
                cursor.executemany(
                    """
                    MERGE biecom.orders_shopee_monitor AS target
                    USING (VALUES (?, ?, ?, ?)) AS source (order_sn, order_status, create_time, pay_time)
                    ON target.order_sn = source.order_sn
                    WHEN NOT MATCHED THEN 
                        INSERT (order_sn, order_status, create_time, pay_time)
                        VALUES (source.order_sn, source.order_status, source.create_time, source.pay_time);
                    """,
                    batch
                )
                conn.commit()
                print(f"Batch de {batch_size} registros inserido com sucesso.")
                batch = []

        if batch:
            cursor.executemany(
                """
                MERGE biecom.orders_shopee_monitor AS target
                USING (VALUES (?, ?, ?, ?)) AS source (order_sn, order_status, create_time, pay_time)
                ON target.order_sn = source.order_sn
                WHEN NOT MATCHED THEN 
                    INSERT (order_sn, order_status, create_time, pay_time)
                    VALUES (source.order_sn, source.order_status, source.create_time, source.pay_time);
                """,
                batch
            )
            conn.commit()
            print(f"Batch final de {len(batch)} registros inserido com sucesso.")

    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
    finally:
        cursor.close()
        conn.close()
        print("Conexão fechada.")
    hora_fim = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tempo_total = datetime.strptime(hora_fim, '%Y-%m-%d %H:%M:%S') - datetime.strptime(hora_inicio, '%Y-%m-%d %H:%M:%S')
    print(f"Processo de inserção de pedidos finalizado em {tempo_total}")

# def insert_order_details_to_db(order_details):
#     conn = dw()
#     cursor = conn.cursor()

#     batch_size = 100
#     batch = []
#     for detail in order_details:
#         create_time = detail.get('create_time')
#         formatted_create_time = datetime.fromtimestamp(create_time).strftime('%Y-%m-%d') if create_time else None
#         pay_time = detail.get('pay_time')
#         formatted_pay_time = datetime.fromtimestamp(pay_time).strftime('%Y-%m-%d %H:%M:%S') if pay_time else None
#         batch.append((detail.get('order_sn'), detail.get('order_status'), formatted_create_time, formatted_pay_time))

#         if len(batch) >= batch_size:
#             try:
#                 cursor.executemany(
#                     """
#                     MERGE biecom.orders_shopee_monitor AS target
#                     USING (VALUES (?, ?, ?, ?)) AS source (order_sn, order_status, create_time, pay_time)
#                     ON target.order_sn = source.order_sn
#                     WHEN MATCHED THEN 
#                         UPDATE SET order_status = source.order_status, 
#                                    create_time = source.create_time,
#                                    pay_time = source.pay_time
#                     WHEN NOT MATCHED THEN 
#                         INSERT (order_sn, order_status, create_time, pay_time)
#                         VALUES (source.order_sn, source.order_status, source.create_time, source.pay_time);
#                     """,
#                     batch
#                 )
#                 conn.commit()
#                 print(f"Batch of {len(batch)} records inserted/updated successfully.")
#             except Exception as e:
#                 print(f"Error inserting/updating batch: {e}")
#             batch = []

#     if batch:
#         try:
#             cursor.executemany(
#                 """
#                 MERGE biecom.orders_shopee_monitor AS target
#                 USING (VALUES (?, ?, ?, ?)) AS source (order_sn, order_status, create_time, pay_time)
#                 ON target.order_sn = source.order_sn
#                 WHEN MATCHED THEN 
#                     UPDATE SET order_status = source.order_status, 
#                                create_time = source.create_time,
#                                pay_time = source.pay_time
#                 WHEN NOT MATCHED THEN 
#                     INSERT (order_sn, order_status, create_time, pay_time)
#                     VALUES (source.order_sn, source.order_status, source.create_time, source.pay_time);
#                 """,
#                 batch
#             )
#             conn.commit()
#             print(f"Final batch of {len(batch)} records inserted/updated successfully.")
#         except Exception as e:
#             print(f"Error inserting/updating final batch: {e}")

#     cursor.close()
#     conn.close()