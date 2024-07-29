import server_package.server_response as server_response
from server_package.database_support import handle_database_errors
from server_package.crypt_supprt import CryptoSupport


class UserAuthentication:
    def __init__(self, database_support):
        self.crypto = CryptoSupport()
        self.database_support = database_support

    @handle_database_errors
    def login(self, login_data):
        print(f'LOGIN_DATA = {login_data}')
        if not login_data or not isinstance(login_data, list) or len(login_data) != 2:
            return server_response.E_INVALID_DATA

        login_username = login_data[0]['username']
        login_password = login_data[1]['password']

        user_data = self.database_support.get_info_about_user(login_username)
        print(f'USER_DATA_LOGIN = {user_data}')
        password_is_OK = self.crypto.verifying_password(user_data['hashed_password'], login_password)
        if user_data is not None and user_data['status'] == "active" and password_is_OK:
            print(f'Access granted to {login_username}')
            self.database_support.data_update('users', 'login_time', login_username, 'NOW()')
            return {"Login": "OK", "login_username": login_username, "user_permissions": user_data['permissions']}

        elif user_data is not None and user_data['status'] == "banned":
            print(f'Access denied to {login_username}, user banned')
            return server_response.E_USER_IS_BANNED
        else:
            print(f'Access denied to {login_username}, invalid credentials')
            return server_response.E_INVALID_CREDENTIALS

    @handle_database_errors
    def logout(self, logged_in_user):
        if not logged_in_user:
            return server_response.E_INVALID_DATA

        is_user_login = self.database_support.check_if_user_is_logged_in(logged_in_user)

        if is_user_login:
            self.database_support.data_update('users', 'login_time', logged_in_user, )
            print(f'{logged_in_user} is logged out')
            return {"Logout": "Successful"}
        else:
            pass