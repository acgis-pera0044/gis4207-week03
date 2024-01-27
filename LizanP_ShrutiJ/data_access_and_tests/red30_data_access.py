from sqlalchemy import create_engine, text

# DB URI for the production database
DB_URI = 'sqlite:///red30.db'

def create_salesperson(first_name, last_name, email_address, city, state):
    engine = create_engine(DB_URI)
    with engine.connect() as conn:
        sql = text("""
            INSERT INTO "SalesPerson" (first_name, last_name, email_address, city, state)
            VALUES (:first_name, :last_name, :email_address, :city, :state)""")
        values = {
            'first_name': first_name,
            'last_name': last_name,
            'email_address': email_address,
            'city': city,
            'state': state
        }
        result = conn.execute(sql, values)
        conn.commit()
        return result.lastrowid
        

def read_salesperson(first_name, last_name):
    engine = create_engine(DB_URI)
    with engine.connect() as conn:
        sql = text("""SELECT * FROM SalesPerson
                   where first_name = :first_name and
                   last_name = :last_name""")
        values = {"first_name": first_name,
                  "last_name": last_name}
        result = conn.execute(sql, values)
        return result.fetchone() 


def read_salespersons():
    engine = create_engine(DB_URI)
    with engine.connect() as conn:
        sql = text("SELECT * FROM SalesPerson")
        result = conn.execute(sql)
        return result.fetchall()


def update_salesperson(email_address, new_city, new_state):
    engine = create_engine(DB_URI)
    with engine.connect() as conn:
        sql = text("""
            UPDATE "SalesPerson"
            SET city = :new_city, state = :new_state
            WHERE email_address = :email_address
        """)
        values = {
            'email_address': email_address,
            'new_city': new_city,
            'new_state': new_state
        }
        result = conn.execute(sql, values)
        conn.commit()
        return result.rowcount


def delete_salesperson(email_address):
    engine = create_engine(DB_URI)
    with engine.connect() as conn:
        sql = text("DELETE FROM SalesPerson WHERE email_address = :email_address")
        values = {"email_address":email_address}
        result = conn.execute(sql, values)
        conn.commit()
        return result.rowcount
