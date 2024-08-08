import bcrypt
import psycopg2

# Nawiązanie połączenia z bazą danych
conn = psycopg2.connect("dbname=db_CS user=pozamiataj password=pozamiataj.pl host=127.0.0.1")
# conn = psycopg2.connect(dbname='db_CS', user=params['user'], password=params['password'], host=params['host'])
cursor = conn.cursor()

# Pobranie haseł użytkowników
cursor.execute("SELECT user_id, password FROM users")
users_passwords = cursor.fetchall()

for user_id, password in users_passwords:
    # Generowanie soli
    salt = bcrypt.gensalt()
    # Hashowanie hasła z solą
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), salt)

    # Wstawianie zahashowanego hasła i soli do tabeli passwords
    cursor.execute(
        "INSERT INTO passwords (user_id, hashed_password, salt) VALUES (%s, %s, %s)",
        (user_id, hashed_password, salt)
    )

# Zatwierdzenie transakcji i zamknięcie połączenia
conn.commit()
cursor.close()
conn.close()