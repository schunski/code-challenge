import psycopg2, csv, os, datetime, subprocess

db_initial_params = {
    "host": "localhost",
    "database": "northwind",
    "user": "northwind_user",
    "password": "thewindisblowing",
    "port": "5432"
}

output_base_directory = os.path.join(os.path.dirname(__file__), "data", "postgres")

try:
    conn = psycopg2.connect(**db_initial_params)
    cursor = conn.cursor()
    
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tables = [table[0] for table in cursor.fetchall()]
    

    for table in tables:

        table_directory = os.path.join(output_base_directory, table)
        os.makedirs(table_directory, exist_ok=True)

        now = datetime.datetime.now()
        date_folder = now.strftime("%Y-%m-%d")
        date_directory = os.path.join(table_directory, date_folder)
        os.makedirs(date_directory, exist_ok=True)

        csv_filename = table + ".csv"
        csv_file_path = os.path.join(date_directory, csv_filename)
    
        cursor.execute(f"SELECT * FROM {table};")
        data = cursor.fetchall()

        with open(csv_file_path, "w", newline="") as file:
            csv_writer = csv.writer(file)
            csv_writer.writerows(data)

            print(f"Dados da tabela {table} exportados para {csv_file_path}")

except (Exception, psycopg2.Error) as error:
    print("Erro ao conectar ao banco de dados", error)
except Exception as general_error:
    print("Erro durante a exportação/importação dos dados:", type(general_error).__name__, ":", general_error)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("Conexão com o banco de dados fechada para o banco inicial.")