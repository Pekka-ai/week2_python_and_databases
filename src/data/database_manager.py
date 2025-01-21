import psycopg2
from psycopg2 import sql
from config import config


class DatabaseManager:
    def __init__(self):
        pass

    def is_table_allowed(self, table: str):
        """Tarkistaa, että taulu on sallittu"""
        allowed_tables = {"person", "certificates"}
        if table not in allowed_tables:
            raise ValueError(f"Table {table} is not allowed!")

    def query_all_rows(self, table: str):
        """Hakee kaikki rivit taulusta"""
        self.is_table_allowed(table)
        
        # Suoritetaan SQL-kysely turvallisesti psycopg2.sql avulla
        query_sql = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table))  # Käytetään sql.Identifier
        rows = self.query(query_sql)
        self.print_rows(rows)

    def query_person_by_name(self, name: str):
        """Hakee henkilöitä, joiden nimi sisältää annetun tekstin."""
        search_term = f"%{name}%"  # Lisää jokerimerkit
        sql_query = 'SELECT * FROM person WHERE name LIKE %s;'
        rows = self.query(sql_query, (search_term,))
    #    print(rows)
        return rows

    def query_persons_certificates(self, person_id: str):
        sql_query = 'SELECT * FROM certificates WHERE person_id = %s;'
        rows = self.query(sql_query, (person_id,))
    #    print(rows)
        return rows

    def is_person_in_the_database(self, name: str):
        """Hakee henkilöitä, joiden nimi sisältää annetun tekstin."""
        search_term = f"%{name}%"  # Lisää jokerimerkit
        sql_query = 'SELECT * FROM person WHERE name LIKE %s;'
        return self.query(sql_query, (search_term,))

    def print_rows(self, rows):
        for row in rows:
            print(row)

    def query_persons_mean_age(self):
        sql = "SELECT AVG(age) FROM person;"
        rows = self.query(sql)
        if rows:
            mean_age = rows[0][0]  # Haetaan keskiarvo ensimmäiseltä riviltä ja ensimmäisestä sarakkeesta
            print(f"Person's mean age: {mean_age}")
        else:
            print("No data found!")

    def query_column_names_person(self):    
        query_sql = "SELECT column_name FROM information_schema.columns WHERE table_name = 'person';"
        rows = self.query(query_sql)
        self.print_rows(rows)

    def query_column_names_certificate(self):    
        query_sql = "SELECT column_name FROM information_schema.columns WHERE table_name = 'certificates';"
        rows = self.query(query_sql)
        self.print_rows(rows)

    def insert_certificate(self, name, person_id):
        SQL = "INSERT INTO certificates (name, person_id) VALUES (%s, %s);"
        data = (name, person_id,)
        self.insert_to_table(SQL, data)

    def insert_person(self, name, age, student):
        if self.is_person_in_the_database(name):
            print(f"{name} already exists in the person table")
            return
        
        sql = "INSERT INTO person (name, age, student) VALUES (%s, %s, %s);"
        data = (name, age, student)
        self.insert_to_table(sql, data)

    def delete_person(self, person_id):
        """Poistaa henkilön person-taulusta henkilön id:n perusteella."""
        sql = "DELETE FROM person WHERE id = %s;"
        data = (person_id,)
        self.insert_to_table(sql, data)

    def update_person(self, person_id, name, age, student):
        """Päivittää henkilön tiedot person-taulussa henkilön id:n perusteella."""
        sql = "UPDATE person SET age = %s, name = %s, student = %s WHERE id = %s;"
        data = (age, name, student, person_id)
        self.insert_to_table(sql, data)

    def update_certificate(self, certificate_id, name):
        """Päivittää henkilön tiedot person-taulussa henkilön id:n perusteella."""
        sql = "UPDATE certificates SET name = %s WHERE id = %s;"
        data = (name, certificate_id)
        self.insert_to_table(sql, data)

    def delete_certificate(self, certificate_id):
        """Poistaa henkilön person-taulusta henkilön id:n perusteella."""
        sql = "DELETE FROM certificates WHERE id = %s;"
        data = (certificate_id,)
        self.insert_to_table(sql, data)

    def query(self, sql_query: sql.Composed, params=None):
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

    def insert_to_table(self, sql_query: sql.Composed, params=None):
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


# # Example usage:

# # Create an instance of DatabaseManager
# db_manager = DatabaseManager()
# print(db_manager.query_person_by_name("Pekka"))
# print(db_manager.query_persons_certificates(2))

# # Insert a new person
# db_manager.insert_person("John Doe", 30, True)

# # Update a person
#db_manager.update_person(2, "Jane Doe", 25, False)

# # Insert a new certificate
# db_manager.insert_certificate("Leadership Certificate", 2)

# # Update a certificate
# db_manager.update_certificate(10, "Advanced Leadership Certificate")

# # Query all persons
# db_manager.query_all_rows("person")

# # Query all columns from the "person" table
# db_manager.query_column_names_person()

# # Print the mean age of persons
# db_manager.query_persons_mean_age()
