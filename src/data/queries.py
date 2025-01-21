import psycopg2
from psycopg2 import sql
from config import config


def is_table_allowed(table: str):
    """Tarkistaa, että taulu on sallittu"""
    allowed_tables = {"person", "certificates"}
    if table not in allowed_tables:
        raise ValueError(f"Table {table} is not allowed!")


def query_all_rows(table: str):
    """Hakee kaikki rivit taulusta"""
    is_table_allowed(table)
    
    # Suoritetaan SQL-kysely turvallisesti psycopg2.sql avulla
    query_sql = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table))  # Käytetään sql.Identifier
    rows = query(query_sql)
    print_rows(rows)

def query_person_by_name(name: str):
    """Hakee henkilöitä, joiden nimi sisältää annetun tekstin."""
    search_term = f"%{name}%"  # Lisää jokerimerkit
    sql = 'SELECT * FROM person WHERE name LIKE %s;'
    rows = query(sql, (search_term,))
    print_rows(rows)

def is_person_in_the_database(name: str):
    """Hakee henkilöitä, joiden nimi sisältää annetun tekstin."""
    search_term = f"%{name}%"  # Lisää jokerimerkit
    sql = 'SELECT * FROM person WHERE name LIKE %s;'
    return query(sql, (search_term,))
    

def print_rows(rows):
    for row in rows:
        print(row)

def query_persons_mean_age():
    sql = "SELECT AVG(age) FROM person;"
    rows = query(sql)
    if rows:
        mean_age = rows[0][0]  # Haetaan keskiarvo ensimmäiseltä riviltä ja ensimmäisestä sarakkeesta
        print(f"Person's mean age: {mean_age}")
    else:
        print("No data found!")
   # print_rows(rows)

def query_column_names_person():    
    # Suoritetaan SQL-kysely turvallisesti psycopg2.sql avulla
    query_sql = "SELECT column_name FROM information_schema.columns WHERE table_name = 'person';"
    rows = query(query_sql)
    print_rows(rows)

def query_column_names_certificate():    
    # Suoritetaan SQL-kysely turvallisesti psycopg2.sql avulla
    query_sql = "SELECT column_name FROM information_schema.columns WHERE table_name = 'certificates';"
    
    rows = query(query_sql)
    print_rows(rows)

def query_persons_with_certificate():    
    # tee tämä
    pass

def insert_certificate(name, person_id):
    SQL = "INSERT INTO certificates (name, person_id) VALUES (%s, %s);" # Note: no quotes
    data = (name, person_id, )
    # tee tämä
    insert_to_table(SQL, data)

def insert_person(name, age, student):
    if is_person_in_the_database(name):
        print(f"{name} already exists in the person table")
        return
    
    sql = "INSERT INTO person (name, age, student) VALUES (%s, %s, %s);"
    data = (name, age, student)
    insert_to_table(sql, data)

def delete_person(person_id):
    """Poistaa henkilön person-taulusta henkilön id:n perusteella."""
    sql = "DELETE FROM person WHERE id = %s;"
    data = (person_id,)
    insert_to_table(sql, data)

def update_person(person_id, name, age, student):
    """Päivittää henkilön tiedot person-taulussa henkilön id:n perusteella."""
    sql = "UPDATE person SET age = %s, name = %s, student = %s WHERE id = %s;"
    data = (age, name, student, person_id)
    insert_to_table(sql, data)

def update_certificate(certificate_id, name):
    """Päivittää henkilön tiedot person-taulussa henkilön id:n perusteella."""
    sql = "UPDATE certificates SET name = %s WHERE id = %s;"
    data = (name, certificate_id)
    insert_to_table(sql, data)

def delete_certificate(certificate_id):
    """Poistaa henkilön person-taulusta henkilön id:n perusteella."""
    sql = "DELETE FROM certificates WHERE id = %s;"
    data = (certificate_id,)
    insert_to_table(sql, data)

def query(sql_query: sql.Composed, params=None):
    """Suorittaa SQL-kyselyn ja palauttaa tulokset"""
    cursor = None
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        cursor.execute(sql_query, params,)  # Suoritetaan parametrisoitu kysely
        rows = cursor.fetchall()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")
    finally:
        if cursor is not None:
            cursor.close()  # Suljetaan kursori
        if con is not None:
            con.close()  # Suljetaan yhteys

def insert_to_table(sql_query: sql.Composed, params=None):
    """Suorittaa SQL-kyselyn ja palauttaa tulokset"""
    con = None
    cursor = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        cursor.execute(sql_query, params,)  # Suoritetaan parametrisoitu kysely
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Database error: {error}")
    finally:
        if cursor is not None:
            cursor.close()  # Suljetaan kursori
        if con is not None:
            con.close()  # Suljetaan yhteys

# Testaa funktioita
#query_all_rows("person")
#query_column_names_person()
#query_column_names_certificate()
#query_person_by_name("Pekka")
#query_persons_mean_age()
#insert_certificate("leader certificate", 5)
#insert_person("Donald Trump", 77, True)

#delete_person(5)
#delete_certificate(11)
#query_person_by_name("Pekka")
#