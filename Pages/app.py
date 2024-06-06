import streamlit as st
import pandas as pd
import snowflake.connector as sc

# Fonction pour se connecter à Snowflake
def connect_to_snowflake(account, user, password):
    try:
        con = sc.connect(
            account=account,
            user=user,
            password=password
        )
        return con
    except Exception as e:
        st.error(f"Erreur lors de la connexion à Snowflake : {e}")
        return None

# Fonction pour lister les datawarehouses existants
def list_datawarehouses(con):
    cursor = con.cursor()
    cursor.execute("SHOW WAREHOUSES")
    datawarehouses = cursor.fetchall()
    cursor.close()
    return [row[1] for row in datawarehouses]

# Fonction pour créer un nouveau datawarehouse
def create_datawarehouse(con, name):
    cursor = con.cursor()
    cursor.execute(f"CREATE WAREHOUSE IF NOT EXISTS {name}")
    cursor.close()

# Fonction pour lister les bases de données existantes
def list_databases(con):
    cursor = con.cursor()
    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall()
    cursor.close()
    return [row[1] for row in databases]

# Fonction pour créer une nouvelle base de données
def create_database(con, name):
    cursor = con.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {name}")
    cursor.close()

# Fonction pour lister les schémas existants dans une base de données sélectionnée
def list_schemas(con, database):
    cursor = con.cursor()
    cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
    schemas = cursor.fetchall()
    cursor.close()
    return [row[1] for row in schemas]

# Fonction pour ajouter un nouveau schéma
def create_schema(con, database, name):
    cursor = con.cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{name}")
    cursor.close()

# Fonction pour lister les tables existantes dans un schéma sélectionné
def list_tables(con, schema):
    cursor = con.cursor()
    cursor.execute(f"SHOW TABLES IN SCHEMA {schema}")
    tables = cursor.fetchall()
    cursor.close()
    return [row[1] for row in tables]

# Fonction pour créer une nouvelle table
def create_table(con, schema, name, columns):
    cursor = con.cursor()
    columns_str = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{name} ({columns_str})")
    cursor.close()

# Fonction pour récupérer les données d'une table
def fetch_table_data(con, schema, table):
    cursor = con.cursor()
    cursor.execute(f"SELECT * FROM {schema}.{table}")
    data = cursor.fetchall()
    cursor.close()
    return data

# Fonction pour insérer des données dans une table
def insert_into_table(con, schema, table, data):
    cursor = con.cursor()
    for row in data:
        placeholders = ", ".join(["%s"] * len(row))
        cursor.execute(f"INSERT INTO {schema}.{table} VALUES ({placeholders})", row)
    cursor.close()

# Fonction pour supprimer des données dans une table
def delete_from_table(con, schema, table, condition):
    cursor = con.cursor()
    cursor.execute(f"DELETE FROM {schema}.{table} WHERE {condition}")
    cursor.close()

# Fonction pour mettre à jour des données dans une table
def update_table(con, schema, table, updates, condition):
    cursor = con.cursor()
    set_clause = ", ".join([f"{col} = %s" for col in updates.keys()])
    cursor.execute(f"UPDATE {schema}.{table} SET {set_clause} WHERE {condition}", list(updates.values()))
    cursor.close()

# Fonction pour afficher un graphique interactif montrant le nombre de datawarehouses, de bases de données et de schémas
def visualize_data(con):
    datawarehouses = list_datawarehouses(con)
    databases = list_databases(con)
    schemas = list_schemas(con, databases[0]) if databases else []
    
    st.subheader("Visualisation des Données")
    st.write(f"Nombre de Datawarehouses : {len(datawarehouses)}")
    st.write(f"Nombre de Bases de Données : {len(databases)}")
    st.write(f"Nombre de Schémas : {len(schemas)}")

def display_dashboard(con):
    # Récupérer des données de Snowflake (exemple)
    cursor = con.cursor()
    cursor.execute("SELECT * FROM datadasboor.dash.contacts")
    data = cursor.fetchall()
    cursor.close()

    # Convertir les données en DataFrame
    df = pd.DataFrame(data)

    # Afficher les données dans un tableau
    st.subheader("Dashboard Simple")
    st.dataframe(df)


def main():
    st.title("Dashboard Snowflake")

    # Interface utilisateur pour la connexion à Snowflake
    st.subheader("Connexion à Snowflake")
    account = st.text_input("Compte")
    user = st.text_input("Utilisateur")
    password = st.text_input("Mot de passe", type="password")
    if st.button("Se connecter"):
        con = connect_to_snowflake(account, user, password)
        if con:
            st.success("Connexion à Snowflake réussie")
            visualize_data(con)
            display_dashboard(con)
            con.close()

if __name__ == "__main__":
    main()
