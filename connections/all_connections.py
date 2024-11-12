import os
import io
import pyodbc
import json
from dotenv import load_dotenv
from smb.SMBConnection import SMBConnection

load_dotenv()

server_dw = os.getenv("server_dw")
username_dw = os.getenv("username_dw")
password_dw = os.getenv("password_dw")
port_dw = os.getenv("port_dw")


def conn_smb():
    password_smb = os.getenv('password_smb')
    username = os.getenv('username')
    my_name = os.getenv('my_name')
    remote_name = os.getenv('remote_name')
    domain = os.getenv('domain')
    server_ip = os.getenv('server_ip')
    
    password_smb = password_smb
    username = username
    my_name = my_name
    remote_name = remote_name
    domain = domain
    server_ip = server_ip
    
    conn = SMBConnection(
        username,
        password_smb,
        my_name,
        remote_name,
        domain, 
        use_ntlm_v2=True
    )
    connected = conn.connect(server_ip, 139)
    if not connected:
        raise ConnectionError("Unable to connect to the SMB server")
    
    return conn

def dw():
    server = server_dw
    database = 'dw'  
    username = username_dw
    password = password_dw
    port = port_dw

    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'
    try:
        conn = pyodbc.connect(connection_string)
        print("Conexão estabelecida com sucesso!")

        return conn
    except Exception as e:
        print(f"Erro ao conectar: {e}")


def load_token_data_shopee(service_name="DEPARTAMENTAL", remote_path="COMERCIO ELETRONICO/GERAL/20. INTEGRAÇÕES/auth", file_name="token_shopee.txt"):
    conn = conn_smb()
    try:
        with io.BytesIO() as file_obj:
            conn.retrieveFile(service_name, f"{remote_path}/{file_name}", file_obj)
            file_obj.seek(0)
            data = json.load(file_obj)
            return data
    except Exception as e:
        print(f"Erro ao ler o arquivo '{file_name}' no caminho SMB: {e}")
        return None
    

