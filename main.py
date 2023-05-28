import psycopg2

def delete_table(conn):
    with conn.cursor() as cur:
        # удаление таблиц
        cur.execute("""
        DROP TABLE list_of_telephones;
        DROP TABLE list_of_clients;
        """)

        conn.commit()


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS list_of_clients(
                client_id SERIAL PRIMARY KEY,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                email VARCHAR(40) UNIQUE NOT NULL
            );
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS list_of_telephones(
                telephone_id SERIAL PRIMARY KEY,
                client_id INTEGER REFERENCES list_of_clients(client_id) ON DELETE CASCADE,
                telephone BIGINT UNIQUE
            );
            """
        )

        conn.commit()

    return "Таблицы добавлены"


def add_client(conn, first_name, last_name, email, phone=None):
    with conn.cursor() as cur:
        if find_client(conn, email=email):
            return "Клиент с таким email уже есть"

        cur.execute(
            """
            INSERT INTO list_of_clients(first_name, last_name, email) 
            VALUES(%s, %s, %s)
            RETURNING client_id;
            """, (first_name, last_name, email)
        )

        if phone:
            client_id = cur.fetchone()
            phone_data = add_phone(conn, client_id, phone)
            if phone_data == 'Такой клиент есть':
                conn.rollback()
                return 'Добавить невозможно'

        conn.commit()

        return "Клиент добавлен"


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:

        if find_client(conn, phone=phone):
            return "Такой клиент есть"

        cur.execute(
            """
            SELECT client_id FROM list_of_clients
            WHERE client_id = %s
            """, (client_id,)
        )

        if not cur.fetchone():
            return "Такого клиента нет"

        cur.execute(
            """
            INSERT INTO list_of_telephones(client_id, telephone) VALUES(%s, %s);
            """, (client_id, phone)
        )

        conn.commit()

    return "Телефон успешно добавлен"




def change_client(conn, client_id, first_name=None, last_name=None, email=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT first_name, last_name, email, client_id
            FROM list_of_clients
            WHERE client_id=%s;
            """, (client_id,)
        )

        sel = cur.fetchone()

        if not sel:
            return "Такого клиента нет"

        if first_name == None:
            first_name = sel[0]
        if last_name == None:
            last_name = sel[1]
        if email == None:
            email = sel[2]

        cur.execute(
            """
            UPDATE list_of_clients SET first_name=%s, last_name=%s, email=%s WHERE client_id=%s;
            """, (first_name, last_name, email, client_id)
        )

        conn.commit()

    return "Пользователь успешно изменен"


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM list_of_telephones
            WHERE telephone=%s AND client_id=%s
            RETURNING *;
            """, (phone, client_id)
        )

        if not cur.fetchone():
            return "Такого номера нет"

        conn.commit()

    return "Телефон успешно удален"


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM list_of_clients WHERE client_id=%s
            """, (client_id,)
        )

        conn.commit()

    return "Клиент успешно удален"


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:

        if first_name is None:
            first_name = '%'
        if last_name is None:
            last_name = '%'
        if email is None:
            email = '%'

        data_list = [first_name, last_name, email]
        new_str = ''

        if phone is not None:
            new_str = ' AND ARRAY_AGG(telephone) && ARRAY[%s::BIGINT]'
            data_list.append(phone)

        search = f"""
            SELECT
                first_name, last_name, email,
                CASE
                    WHEN ARRAY_AGG(telephone) = '{{Null}}' THEN '{{}}'
                    ELSE ARRAY_AGG(telephone)
                END telephone
            FROM list_of_clients AS loc
            LEFT JOIN list_of_telephones AS lot ON loc.client_id = lot.client_id
            GROUP BY first_name, last_name, email
            HAVING first_name ILIKE %s AND last_name ILIKE %s AND email ILIKE %s{new_str}
        """

        cur.execute(
            search,
            data_list
        )



        return cur.fetchall()




with psycopg2.connect(database="clients_db", user="postgres", password="") as conn:
    delete_table(conn)
    create_db(conn)
    add_client(conn, "Serega", "Petrov", "SGP@mail.ru", "1022")
    add_client(conn, "Serega", "Petrov", "SGP1@mail.ru", "1023")
    add_client(conn, "Stas", "Ivanov", "SGP11@mail.ru", "1024")
    add_client(conn, "Egor", "Semenov", "SGP111@mail.ru", "1025")
    add_client(conn, "Igor", "Petrov", "SGP1111@mail.ru", "1026")
    add_phone(conn, 1, "321")
    print(change_client(conn, 1, first_name="Gleb", last_name=None, email=None))
    delete_phone(conn, 1, "123")
    delete_client(conn, 1)
    print(find_client(conn, last_name="Petrov"))
    add_phone(conn, 5, "123321")
