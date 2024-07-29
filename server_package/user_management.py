import connection_pool.server_package.server_response as server_response
from connection_pool.server_package.crypt_supprt import CryptoSupport


class UserManagement:
    def __init__(self, database_support):
        self.crypto = CryptoSupport()
        self.database_support = database_support

    @staticmethod
    def user_add():
        return {"User-add": "OK"}

    def create_account(self, data):
        print(f'create_user={data}')
        if not data:
            return server_response.E_INVALID_DATA

        new_user_data = list(d[next(iter(d))] for d in data)
        print(f'NEW_USER_DATA = {new_user_data}')
        if new_user_data:
            username = new_user_data[0]
            password = new_user_data.pop(1)
            permissions = new_user_data[1]

            password_data = self.crypto.password_hashing(password)
            print(password_data)
            print(f'NEWUSERDATAAFTERHASH = {new_user_data}')

        if len(username) > 0:
            if self.database_support.check_if_user_exist(username):
                return server_response.E_ACCOUNT_EXIST
            print(f'permissions = {permissions}')
            if permissions not in ['user', 'admin']:
                return server_response.E_WRONG_PERMISSIONS
            else:
                self.database_support.add_account_to_db(new_user_data, password_data)
                return server_response.NEW_ACCOUNT_CREATED
        else:
            return server_response.E_USER_NAME_NOT_PROVIDED

    def user_del(self, user_to_del):
        print(f'User to delete = {user_to_del}')
        if not self.database_support.check_if_user_exist(user_to_del):
            return server_response.E_USER_DOES_NOT_EXIST
        elif self.database_support.check_if_user_is_logged_in(user_to_del):
            return server_response.E_USER_LOGGED_CANNOT_BE_DELETED
        else:
            #  self.database_support.delete_all_user_messages(user_to_del)
            self.database_support.delete_record_from_db('users', user_to_del)
            return {user_to_del: server_response.USER_DELETED}

    def user_list(self):
        all_user_data = self.database_support.get_all_users_list()
        users_dict = {}
        for row in all_user_data:
            user_name, permissions, status = row
            users_dict[user_name] = {'permissions': permissions, 'status': status}
        return {server_response.EXISTING_ACCOUNTS: users_dict}

    def user_info(self, username):
        database_response = self.database_support.get_info_about_user(username)
        if not self.database_support.check_if_user_exist(username):
            return server_response.E_USER_DOES_NOT_EXIST
        elif "Error" in database_response:
            return server_response.E_DATABASE_ERROR
        else:
            selected_user_data = self.database_support.get_info_about_user(username)
            new_selected_user_data = {'user': selected_user_data.pop('user_name')}
            new_selected_user_data.update(selected_user_data)
            new_selected_user_data['activation_date'] = self.convert_datetime_datetime_to_string_date(selected_user_data['activation_date'])
            del new_selected_user_data['hashed_password']
            del new_selected_user_data['salt']
            print(f'NSUD = {new_selected_user_data}')
            inbox_msg_count = self.database_support.inbox_msg_counting(username)
            new_selected_user_data["inbox messages"] = inbox_msg_count
            new_selected_user_data['login_time'] = self.convert_datetime_datetime_to_string_date(selected_user_data['login_time'])

            return {server_response.ACCOUNT_INFO: new_selected_user_data}

    def user_perm(self, data):
        if not data:
            return server_response.E_INVALID_DATA
        else:
            user_to_change_permission, new_permissions = next(iter(data.items()))
            print(f'User name to change permissions: {user_to_change_permission}, to new permissions: {new_permissions}')

        if not self.database_support.check_if_user_exist(user_to_change_permission):
            return server_response.E_USER_DOES_NOT_EXIST

        elif self.database_support.check_if_user_is_logged_in(user_to_change_permission):
            return server_response.E_USER_LOGGED_CANNOT_CHANGE_PERMISSIONS
        elif new_permissions not in ['user', 'admin']:
            return server_response.E_WRONG_PERMISSIONS
        else:
            self.database_support.data_update('users', 'permissions', user_to_change_permission, new_permissions)
            return {user_to_change_permission: server_response.USER_PERMISSIONS_CHANGED}

    def user_stat(self, data):
        if not data:
            return server_response.E_INVALID_DATA
        else:
            user_to_change_status, new_status = next(iter(data.items()))
            print(f'User name to change status: {user_to_change_status}, to new status: {new_status}')

        if not self.database_support.check_if_user_exist(user_to_change_status):
            return server_response.E_USER_DOES_NOT_EXIST
        elif self.database_support.check_if_user_is_logged_in(user_to_change_status):
            return server_response.E_USER_LOGGED_CANNOT_CHANGE_STATUS
        elif new_status not in ['banned', 'active']:
            return server_response.E_WRONG_STATUS
        else:
            self.database_support.data_update('users', 'status', user_to_change_status, new_status)
            return {user_to_change_status: server_response.USER_STATUS_CHANGED}

    @staticmethod
    def user_pass():
        return {"User-pass": "OK"}

    def change_password(self, data):
        if not data:
            return server_response.E_INVALID_DATA

        print(f'CHANGE PASSWORD DATA = {data}')
        data_to_change_password = list(d[next(iter(d))] for d in data)
        print(f'User data to change password: {data_to_change_password}')
        if data_to_change_password:
            user_to_change_password = data_to_change_password[0]
            new_password = data_to_change_password.pop(1)
            confirmed_new_password = data_to_change_password[1]

        if len(user_to_change_password) > 0:
            if self.database_support.check_if_user_exist(user_to_change_password):

                if new_password == confirmed_new_password:
                    hashed_password, salt = self.crypto.password_hashing(new_password)
                    user_to_change_password_data = self.database_support.get_info_about_user(user_to_change_password)
                    user_id = user_to_change_password_data['user_id']
                    self.database_support.password_update('passwords', 'hashed_password', 'salt', user_id, hashed_password, salt)
                    print("HASŁO ZMIENIONE")
                    return {user_to_change_password: server_response.USER_PASSWORD_CHANGED}
                else:
                    print("HASŁO NIE ZMIENIONE")
                    return server_response.E_NEW_PASSWORD_ERROR
            else:
                return server_response.E_USER_DOES_NOT_EXIST
        else:
            return server_response.E_USER_NAME_NOT_PROVIDED

    def convert_datetime_datetime_to_string_date(self, datetime_from_db):
        if not datetime_from_db:
            return None
        else:
            converted_datetime = datetime_from_db.strftime('%Y-%m-%d')
            return converted_datetime
