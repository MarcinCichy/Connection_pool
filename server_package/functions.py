from datetime import datetime
import os
import server_package.server_data as server_data
import server_package.server_response as server_response
from itertools import islice


class SystemUtilities:
    @staticmethod
    def clear_screen():
        """Clear the screen in depends on operating system
        (Windows, Linux or iOS)."""

        if os.name == "nt":
            os.system("cls")
        else:
            os.system("clear")

    @staticmethod
    def uptime():
        now = datetime.now()
        live_time = now - server_data.START_TIME
        return {"uptime": str(live_time).split(".")[0]}

    @staticmethod
    def info():
        return {"version": server_data.VERSION, "start_at": str(server_data.DATE)}

    @staticmethod
    def help(permissions):
        if "user" in permissions:
            user_help_dict = dict(islice(server_response.HELP_DICT.items(), 9))
            help_content = user_help_dict
        elif "admin" in permissions:
            help_dict = server_response.HELP_DICT
            help_content = help_dict
        else:
            help_content = server_response.E_WRONG_PERMISSIONS
        return help_content

    @staticmethod
    def stop():
        return server_response.CONNECTION_CLOSE

    @staticmethod
    def clear():
        return {"Clear": ""}
