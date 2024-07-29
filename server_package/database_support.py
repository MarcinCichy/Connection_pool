from functools import wraps
import server_package.server_response as server_response
from server_package.connect import connect
from psycopg2 import sql


def handle_database_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Error: {e}")
            return server_response.E_DATABASE_ERROR
    return wrapper


class DatabaseSupport:

    @handle_database_errors
    def data_update(self, table, column, user_name, new_value=None):
        with connect() as conn:
            with conn.cursor() as cur:
                query = sql.SQL("UPDATE {table} SET {column} = %s WHERE user_name = %s").format(table=sql.Identifier(table), column=sql.Identifier(column))
                cur.execute(query, (new_value, user_name))
                conn.commit()

    @handle_database_errors
    def get_info_about_user(self, user_name):
        with connect() as conn:
            with conn.cursor() as cur:
                # Zmodyfikowane zapytanie do pobrania danych użytkownika wraz z zahaszowanym hasłem i solą
                cur.execute("""
                    SELECT u.*, p.hashed_password, p.salt
                    FROM users u
                    JOIN passwords p ON u.user_id = p.user_id
                    WHERE u.user_name = %s
                """, (user_name,))
                result = cur.fetchone()

                # Dodanie nazw kolumn z tabeli passwords do listy nazw kolumn
                column_names = [desc[0] for desc in cur.description]
                result_dict = dict(zip(column_names, result)) if result else None

                print(f'RESULT_DICT: {result_dict}')
                return result_dict
    @handle_database_errors
    def get_all_users_list(self):
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT user_name, permissions, status FROM users ORDER BY user_id")
                result = cur.fetchall()
                return result

    @handle_database_errors
    def check_if_user_exist(self, user_name):
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM users WHERE user_name = %s", (user_name,))
                if cur.fetchone():
                    return True  # User exist
                else:
                    return False  # User not exist

    @handle_database_errors
    def inbox_msg_counting(self, recipient_id):
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM messages WHERE recipient_id = %s", (recipient_id,))
                count = cur.fetchone()[0]
                return count

    @handle_database_errors
    def check_if_user_is_logged_in(self, user_name):
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT login_time FROM users WHERE user_name = %s", (user_name,))
                result = cur.fetchone()
                if result and result[0] is not None:
                    return True  # User is looged in
                else:
                    return False  # User is not logged in

    @handle_database_errors
    def add_account_to_db(self, new_data, password_data):
        with connect() as conn:
            with conn.cursor() as cur:
                query_users = sql.SQL("INSERT INTO users (user_name, permissions, status, activation_date) VALUES (%s, %s, %s, %s) RETURNING user_id;")
                cur.execute(query_users, new_data)
                user_id = cur.fetchone()[0]

                querry_passwords = sql.SQL("INSERT INTO passwords (user_id, hashed_password, salt) VALUES (%s, %s, %s);")
                password_data_with_id = (user_id,) + password_data
                cur.execute(querry_passwords, password_data_with_id)
                conn.commit()

    @handle_database_errors
    def delete_record_from_db(self, table, data):
        with connect() as conn:
            with conn.cursor() as cur:
                query = sql.SQL("DELETE FROM {table} WHERE user_name = %s").format(table=sql.Identifier(table))
                cur.execute(query, (data,))
                conn.commit()

    @handle_database_errors
    def show_all_messages_inbox(self, username):
        with connect() as conn:
            with conn.cursor() as cur:
                query = sql.SQL("SELECT message_id, sender_id, date FROM messages WHERE recipient_id = %s ORDER BY message_id")
                cur.execute(query, (username,))
                result = cur.fetchall()
                return result

    @handle_database_errors
    def show_selected_message(self, msg_id):
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM messages WHERE message_id = %s", (msg_id,))
                result = cur.fetchone()

                column_names = [desc[0] for desc in cur.description]

                result_dict = dict(zip(column_names, result)) if result else None

                print(f'RESULT = {result_dict}')
                return result_dict

    @handle_database_errors
    def delete_selected_message(self, msg_id):
        with connect() as conn:
            with conn.cursor() as cur:
                query = sql.SQL(
                    "DELETE FROM messages WHERE message_id = %s")
                cur.execute(query, (msg_id,))
                conn.commit()

    @handle_database_errors
    def delete_all_user_messages(self, user_to_del):
        with connect() as conn:
            with conn.cursor() as cur:
                query = sql.SQL(
                    "DELETE FROM messages WHERE recipient_id = %s")
                cur.execute(query, (user_to_del,))
                conn.commit()

    @handle_database_errors
    def add_new_message_to_db(self, new_data):
        with connect() as conn:
            with conn.cursor() as cur:
                query = sql.SQL("INSERT INTO messages (sender_id, date, recipient_id, content) VALUES (%s, %s, %s, %s)")
                cur.execute(query, new_data)
                conn.commit()

    @handle_database_errors
    def password_update(self, table, column1, column2, user_id, new_value1=None, new_value2=None ):
        with connect() as conn:
            with conn.cursor() as cur:
                query = sql.SQL("UPDATE {table} SET {column} = %s WHERE user_id = %s").format(
                    table=sql.Identifier(table), column=sql.Identifier(column1))
                cur.execute(query, (new_value1, user_id))
                query = sql.SQL("UPDATE {table} SET {column} = %s WHERE user_id = %s").format(
                    table=sql.Identifier(table), column=sql.Identifier(column2))
                cur.execute(query, (new_value2, user_id))
                conn.commit()





