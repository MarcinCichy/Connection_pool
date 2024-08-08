import bcrypt


class CryptoSupport:

    def password_hashing(self, password):
        salt = bcrypt.gensalt()
        print(f'SALT: {salt}')
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), salt)
        print(f'hashed passw: {hashed_password}')
        return hashed_password, salt

    def verifying_password(self, stored_hashed_password, provided_password):
        if isinstance(provided_password, str):
            provided_password = provided_password.encode('utf-8')
        if isinstance(stored_hashed_password, memoryview):
            stored_hashed_password = stored_hashed_password.tobytes()
        return bcrypt.checkpw(provided_password, stored_hashed_password)


