from server_package.functions import SystemUtilities
from server_package.message_management import MessageManagement
from server_package.user_management import UserManagement
from server_package.user_authentication import UserAuthentication
from server_package.database_support import DatabaseSupport
import server_package.server_response as server_response


class CommandHandler:
    def __init__(self):
        self.database_support = DatabaseSupport()
        self.username = ""
        self.new_command = ""
        self.permissions = ""
        self.user_auth = UserAuthentication(self.database_support)
        self.user_management = UserManagement(self.database_support)
        self.message_management = MessageManagement(self.database_support)
        self.sys_utils = SystemUtilities()

        self.all_users_commands = {
            "login": self.user_auth.login,
            "logout": self.user_auth.logout,
            "help": self.sys_utils.help,
            "info": self.sys_utils.info,
            "uptime": self.sys_utils.uptime,
            "clear": self.sys_utils.clear,
            "msg_count": self.message_management.msg_count,
            "msg-list": self.message_management.msg_list,
            "msg-snd": self.message_management.msg_snd,
            "msg-del": self.message_management.msg_del,
            "new_message": self.message_management.new_message,
            "msg-show": self.message_management.msg_show
        }
        self.admin_commands = {
            "stop": self.sys_utils.stop,
            "user-add": self.user_management.user_add,
            "user-list": self.user_management.user_list,
            "user-del": self.user_management.user_del,
            "user-perm": self.user_management.user_perm,
            "user-stat": self.user_management.user_stat,
            "user-info": self.user_management.user_info,
            "create_account": self.user_management.create_account,
            "user-pass": self.user_management.user_pass,
            "change_password": self.user_management.change_password
        }

    def prepare_command_and_user_data(self, entrance_command):
        if isinstance(entrance_command, dict):
            # Extract the first key, which is the username submitted
            username = next(iter(entrance_command))
            print(f'prep_com_username = {username}')
            # Based on this username, create a new dictionary with the command
            new_command = entrance_command.pop(username)
            print(f'new_command = {new_command}')
            return new_command, username

    def use_command(self, entrance_command, permissions):
        print(f'entrance_command = {entrance_command}')

        self.new_command, self.username = self.prepare_command_and_user_data(entrance_command)
        self.permissions = permissions

        if isinstance(self.new_command, dict):
            command = list(self.new_command.keys())[0]
            data = self.new_command[command]
        else:
            command = self.new_command
            data = None

        if command in self.all_users_commands:
            match command:
                case "login":
                    self.username = data[0]['username']
                case "logout":
                    data = self.username
                case "help":
                    data = self.permissions
                case "msg-list":
                    data = self.username
                case "msg-del":
                    data = {self.username: data}
                case "msg-show":
                    data = {self.username: data}
                case "msg_count":
                    data = self.username
                case _:
                    pass

            if data is not None:
                result = self.all_users_commands[command](data)
            else:
                result = self.all_users_commands[command]()

        elif command in self.admin_commands:
            if self.permissions == "admin":
                if data is not None:
                    result = self.admin_commands[command](data)
                else:
                    result = self.admin_commands[command]()
            else:
                result = server_response.E_COMMAND_UNAVAILABLE
        else:
            result = server_response.UNRECOGNISED_COMMAND

        # print(f'Server response: {result}')
        print(f'EXIT USERNAME = {self.username}')
        print(f'EXIT PERMISSIONS: {self.permissions}')
        print(f'EXIT DATA = {data}')

        return result





