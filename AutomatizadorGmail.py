import pandas as pd
import sqlite3
import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Credenciales del correo
name_account = "AutoProgram"
email_account = "..."
password_account = "..."

# Configuración del servidor SMTP
server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
server.ehlo()
server.login(email_account, password_account)

# Conectar a la base de datos SQLite (se creará si no existe)
conn = sqlite3.connect('bases_de_datos.db')
cursor = conn.cursor()

# Crear tabla si no existe
cursor.execute('''
CREATE TABLE IF NOT EXISTS bases_de_datos (
    db_name TEXT,
    owner_email TEXT,
    manager_email TEXT,
    classification TEXT
)
''')

# Leer el archivo JSON
json_path = r'...'
with open(json_path, 'r') as file:
    db_classifications = json.load(file)

# Leer el archivo CSV
csv_path = r'...'
usuarios_df = pd.read_csv(csv_path)

# Procesar los datos y guardar en la base de datos
for db in db_classifications:
    db_name = db.get('db_name', 'Unknown')
    owner_email = db.get('owner_email', 'Unknown')
    classification = db.get('classification', 'Unknown')

    # Encontrar el manager para el owner
    manager_row = usuarios_df[usuarios_df['user_id'] == owner_email]
    if not manager_row.empty:
        manager_email = manager_row.iloc[0]['user_manager']
    else:
        manager_email = 'Unknown'

    # Guardar en la base de datos
    cursor.execute('''
    INSERT INTO bases_de_datos (db_name, owner_email, manager_email, classification)
    VALUES (?, ?, ?, ?)
    ''', (db_name, owner_email, manager_email, classification))

    # Enviar correo si la clasificación es alta
    if classification.lower() == 'high' and manager_email != 'Unknown':
        subject = f"Revisión de Clasificación Alta: {db_name}"
        message = f"Hola,\n\nLa base de datos {db_name} tiene una clasificación alta. Por favor, revisa y aprueba esta clasificación.\n\nSaludos,\n{name_account}"

        msg = MIMEMultipart()
        msg['From'] = f"{name_account} <{email_account}>"
        msg['To'] = manager_email
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'plain'))

        try:
            server.sendmail(email_account, manager_email, msg.as_string())
            print(f"Correo enviado a {manager_email} para la base de datos {db_name}.")
        except Exception as e:
            print(f"No se pudo enviar el correo a {manager_email}. Error: {str(e)}")

# Confirmar cambios en la base de datos y cerrar la conexión
conn.commit()
conn.close()

# Cerrar la conexión con el servidor SMTP
server.close()
