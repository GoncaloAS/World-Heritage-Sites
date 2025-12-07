import pandas as pd
import sqlite3
from new_dataset import create_dataset

xlsx_path = 'data\whc-sites-2024.xlsx'
csv_path= 'data\whc-sites-2024-new.csv'
db_path = 'world-heritage-sites.db'




create_tables = [
    """
    CREATE TABLE IF NOT EXISTS Sitios (
        id_no INTEGER PRIMARY KEY,
        nome TEXT,
        descricao TEXT,
        data_inscricao NUMERIC,
        area_hectares NUMERIC,
        rev_bis TEXT
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS Paises (
        iso_code TEXT PRIMARY KEY,
        nome TEXT,
        regiao TEXT
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS Categorias (
        categoria_short TEXT PRIMARY KEY,
        categoria TEXT
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS Criterios (
        id_criterio TEXT PRIMARY KEY,
        descricao TEXT,
        categoria TEXT REFERENCES Categorias (categoria_short)
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS Datas_Secundarias (
        sitio INTEGER REFERENCES Sitios (id_no),
        data NUMERIC,
        PRIMARY KEY(sitio)
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS Sitios_Numeros (
        sitio INTEGER REFERENCES Sitios (id_no),
        unique_number NUMERIC,
        PRIMARY KEY(sitio)
    );
    """,    

    """
    CREATE TABLE IF NOT EXISTS Localizacoes (
        sitio INTEGER REFERENCES Sitios (id_no),
        longitude NUMERIC,
        latitude NUMERIC,
        PRIMARY KEY(sitio)
    );
    """,
    
    """
    CREATE TABLE IF NOT EXISTS Periodos_Perigo (
        sitio INTEGER REFERENCES Sitios (id_no),
        data_inicio NUMERIC,
        data_fim NUMERIC,
        PRIMARY KEY(sitio)
    );
    """,
    
    """
    CREATE TABLE IF NOT EXISTS Paises_Codigos (
        pais TEXT REFERENCES Paises (iso_code),
        udnp_code TEXT,
        PRIMARY KEY(pais)
    );
    """,
    
    """
    CREATE TABLE IF NOT EXISTS Justificacoes (
        sitio INTEGER REFERENCES Sitios (id_no),
        criterio TEXT REFERENCES Criterios (id_criterio),
        justificacao TEXT,
        PRIMARY KEY(sitio, criterio)
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS Sitio_Pais (
        sitio INTEGER REFERENCES Sitios (id_no),
        pais TEXT REFERENCES Paises (iso_code),
        PRIMARY KEY(sitio, pais)
    );
    """,
]



insert_queries = [
    """
    INSERT INTO Sitios (
        id_no, nome, descricao, data_inscricao, area_hectares, rev_bis)
    SELECT DISTINCT
        t.id_no, t.name_en, t.short_description_en, t.date_inscribed, t.area_hectares, t.rev_bis
    FROM temp_table t;
    """,

    """
    INSERT OR IGNORE INTO Paises (
        iso_code, nome, regiao)
    SELECT DISTINCT
        t.iso_code, t.states_name_en, t.region_en
    FROM temp_table t;
    """,

    """
    INSERT INTO Categorias (
        categoria_short, categoria)
    SELECT DISTINCT
        t.category_short, t.category
    FROM temp_table t;
    """,

    """
    INSERT INTO Criterios (
        id_criterio, descricao, categoria)
    SELECT DISTINCT
        t.criterio, t.criterio_desc, c.categoria_short
    FROM temp_table t
    JOIN Categorias c ON t.category_short = c.categoria_short; 
    """,

    """
    INSERT OR IGNORE INTO Datas_Secundarias (
        sitio, data)
    SELECT DISTINCT
        s.id_no, t.secondary_dates
    FROM temp_table t
    JOIN Sitios s ON t.id_no = s.id_no;
    """,
    
    """
    INSERT INTO Sitios_Numeros (
        sitio, unique_number)
    SELECT DISTINCT
        s.id_no, t.unique_number
    FROM temp_table t
    JOIN Sitios s ON t.id_no = s.id_no;
    """,

    """
    INSERT INTO Localizacoes (
        sitio, longitude, latitude)
    SELECT DISTINCT
        s.id_no, t.longitude, t.latitude
    FROM temp_table t
    JOIN Sitios s ON t.id_no = s.id_no;
    """,
    
    """
    INSERT OR IGNORE INTO Periodos_Perigo (
        sitio, data_inicio, data_fim)
    SELECT DISTINCT
        s.id_no, t.perigo_inicio, t.perigo_fim
    FROM temp_table t
    JOIN Sitios s ON t.id_no = s.id_no;
    """,

    """
    INSERT INTO Paises_Codigos (
        pais, udnp_code)
    SELECT DISTINCT
        p.iso_code, t.udnp_code
    FROM temp_table t
    JOIN Paises p ON t.iso_code = p.iso_code;
    """,

    
    """
    INSERT INTO Justificacoes (
        sitio, criterio, justificacao)
    SELECT DISTINCT
        s.id_no, c.id_criterio, t.justification_en
    FROM temp_table t
    JOIN Sitios s ON t.id_no = s.id_no
    JOIN Criterios c ON t.criterio = c.id_criterio;
    """,

    """
    INSERT INTO Sitio_Pais (
        sitio, pais)
    SELECT DISTINCT
        s.id_no, p.iso_code
    FROM temp_table t
    JOIN Sitios s ON t.id_no = s.id_no
    JOIN Paises p ON t.iso_code = p.iso_code;
    """,
]




def import_csv_to_db():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for table_sql in create_tables:
        cursor.execute(table_sql)

    create_dataset(xlsx_path,csv_path)
    data = pd.read_csv(csv_path)
    



    data.to_sql('temp_table', conn, if_exists='replace', index=False)


    for query in insert_queries:
        cursor.execute(query)


    cursor.execute("DROP TABLE IF EXISTS temp_table;")
    print("Temporary table dropped.")


    conn.commit()
    conn.close()

    print("CSV data imported into the permanent tables and tables set up successfully!")


def count_rows_in_all_tables():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        print("Contagem de registros em todas as tabelas:")

        # para cada tabela conta as linhas
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0] #retorna o numero obtido pela query
            print(f"Tabela {table_name}: {count} registros.")

    except sqlite3.Error as e:
        print(f"Erro ao contar registros: {e}")

    conn.close()




def execute_query_and_print(query):

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(query)

    column_names = [description[0] for description in cursor.description] #extrai os nomes das colunas e retorna informações sobre as colunas do resultado da query
    print(column_names)


    results = cursor.fetchall()


    for row in results:
        print(row)

    conn.close()
    for row in results:
        print(row)

    conn.close()
import_csv_to_db()


