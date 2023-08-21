import psycopg2, csv, os, datetime, subprocess, glob, pandas as pd

db_final_params = {
    "host": "localhost",
    "database": "northwind_final",
    "user": "northwind_user",
    "password": "thewindisblowing",
    "port": "5433"
}

csv_directory = os.path.join(os.path.dirname(__file__), "data", "postgres")
directory_result = os.path.join(os.path.dirname(__file__), "result_csv", datetime.datetime.now().strftime("%Y-%m-%d"))

try:
    os.makedirs(directory_result, exist_ok=True)

    db_final_conn = psycopg2.connect(**db_final_params)
    db_final_cursor = db_final_conn.cursor()

    most_recent_csv_folder = max(glob.glob(os.path.join(csv_directory, "*", "*")), key=os.path.getmtime)
    csv_files_in_most_recent_folder = glob.glob(os.path.join(most_recent_csv_folder, "*.csv"))

    for csv_file_path in csv_files_in_most_recent_folder:
        table_name = os.path.splitext(os.path.basename(csv_file_path))[0]
        
        db_final_cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s);", (table_name,))
        table_exists = db_final_cursor.fetchone()[0]

        if not table_exists:
            db_final_cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ();")
            db_final_conn.commit()
            print(f"Tabela {table_name} criada no banco de destino.")

            db_final_cursor.execute(f"SELECT EXISTS (SELECT 1 FROM {table_name});")
            table_empty = not db_final_cursor.fetchone()[0]

        if table_empty:
            with open(csv_file_path, "r") as csv_file:
                db_final_cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", csv_file)

                db_final_conn.commit()
                print(f"Dados da tabela {table_name} importados para o banco de destino.")
        else:
                print(f"Tabela {table_name} já contém dados, importação não realizada.")
                db_final_conn.commit()
                print(f"Dados da tabela {table_name} importados para o banco de destino.")

        order_details_csv_file = os.path.join(os.path.dirname(__file__), "data", "order_details.csv")
        table_name_order_details = "order_details"

        db_final_cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name_order_details,))
        table_exists_order_details = db_final_cursor.fetchone()[0]

        if not table_exists_order_details:
            db_final_cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name_order_details} (Order_id integer, Product_id integer, Unit_price numeric, Quantity integer, Discount numeric);")
            db_final_conn.commit()
            print(f"Tabela {table_name_order_details} criada no banco de destino.")

            db_final_cursor.execute(f"SELECT EXISTS (SELECT 1 FROM {table_name_order_details});")
            table_empty_order_details = not db_final_cursor.fetchone()[0]

        if table_empty_order_details:
            with open(order_details_csv_file, "r") as csv_file_order_details:
                db_final_cursor.copy_expert(f"COPY {table_name_order_details} FROM STDIN WITH CSV HEADER", csv_file_order_details)

                db_final_conn.commit()
                print(f"Dados da tabela {table_name_order_details} importados para o banco de destino.")
        else:
                print(f"Dados da tabela {table_name_order_details} já contém dados, importação não realizada.")
        
        db_final_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        table_names = [row[0] for row in db_final_cursor.fetchall()]

        result_csv_path = os.path.join(directory_result, "result.csv")

        with open(result_csv_path, "w") as result_csv_file:
            for table_name in table_names:
                db_final_cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH CSV HEADER", result_csv_file)
                result_csv_file.write("\n")
                print(f"Todos os dados das tabelas foram exportados: {result_csv_path}")


except (Exception, psycopg2.Error) as error:
    print("Erro ao conectar ao banco de dados", error)
except Exception as general_error:
    print("Erro durante a exportação/importação dos dados:", general_error)
finally:
    if db_final_conn:
        db_final_cursor.close()
        db_final_conn.close()
        print("Conexão com o banco de dados fechada para o banco final.")