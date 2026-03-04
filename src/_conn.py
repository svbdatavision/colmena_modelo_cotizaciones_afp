import snowflake.connector as sc
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

private_key_pass = 'Colmena2024'.encode('utf-8')

with open('rsa_key.pem', 'rb') as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=private_key_pass,
        backend=default_backend
    )

private_key_der = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn_params = {
    'account': 'isapre_colmena.us-east-1',
    'user': 'AFP_CERTIF_OUT',
    'private_key': private_key_der,
    'role': 'AFP_CERTIF_OUT',
    'warehouse': 'P_OPX',
    'database': 'OPX',
    'schema': 'P_DDV_OPX'
}

def connection():
    return sc.connect(**conn_params)
