# -------------- ERRORS RESPONSES -------------------
E_RECIPIENT_DOES_NOT_EXIST = {"Error": "Recipient does not exist"}
E_RECIPIENT_INBOX_IS_FULL = {"Error": "Recipient inbox is full"}
E_MESSAGE_NOT_FOUND = {"Error": "Message not found."}
E_ACCOUNT_EXIST = {"Error": "Account exists"}
E_WRONG_PERMISSIONS = {"Error": "Wrong permissions"}
E_USER_NAME_NOT_PROVIDED = {"Error": "Username not provided"}
E_USER_DOES_NOT_EXIST = {"Error": "User does not exist"}
E_USER_LOGGED_CANNOT_BE_DELETED = {"Error": "User logged cannot be deleted"}
E_USER_LOGGED_CANNOT_CHANGE_PERMISSIONS = {"Error": "User logged, cannot change permissions"}
E_USER_LOGGED_CANNOT_CHANGE_STATUS = {"Error": "User logged, cannot change status"}
E_WRONG_STATUS = {"Error": "Wrong status"}
E_USER_IS_BANNED = {"Error": "User is banned"}
E_INVALID_CREDENTIALS = {"Error": "Invalid credentials. Try again"}
E_UNABLE_TO_OPEN_DB_FILE = "Error: Unable to open db file."
E_UNABLE_TO_SAVE_DB_FILE = "Error: Unable to save db file."
E_FILE_IS_UNAVAILABLE = {"Error": "DB file is unavailable"}
E_COMMAND_UNAVAILABLE = {"Error": "Command unavailable. No permissions"}
E_INVALID_DATA = {"Error": "Invalid data."}
E_DATABASE_ERROR = {"Error": "A database error occurred"}
E_NEW_PASSWORD_ERROR = {"Error": "The confirmed password is incorrect."}

# -------------- OTHERS RESPONSES -------------------
MESSAGE_WAS_SENT = {"Message": "was sent"}
MESSAGE_WAS_DELETED = {"Message": "was deleted"}
NEW_ACCOUNT_CREATED = {"New account": "was created"}
EXISTING_ACCOUNTS = "Existing_accounts"
ACCOUNT_INFO = "Account_info"
USER_DELETED = " - user was deleted"
USER_PERMISSIONS_CHANGED = " - user permissions was changed"
USER_STATUS_CHANGED = " - user status was changed"
USER_PASSWORD_CHANGED = " - user password was changed"
YOUR_INBOX_IS_FULL = " / Your Inbox is full"
CONNECTION_CLOSE = {"Connection": "close"}
UNRECOGNISED_COMMAND = {"Unrecognised command": "Please correct or type <help>."}


# -------------- HELP RESPONSES -------------------

HELP_DICT = {
            "uptime": "returns the server's live time",
            "info": "returns the version number of the server and the date it was created",
            "help": "returns the list of available commands with short description",
            "logout": "to log out the User",
            "clear": " to clear the screen",
            "msg-list": "to show content of inbox",
            "msg-snd": " to create and send message",
            "msg-del [number of message]": "to delete selected message",
            "msg-show [number of message]": "to show details of message (from, date, content)",
            "stop": "stops both the server and the client",
            "user-add": "create an account",
            "user-list": "shows the list of existing accounts",
            "user-del [username]": "deletes the selected account",
            "user-perm [username] [permission]": "change permissions [user] or [admin]",
            "user-stat [username] [status]": "change user status [active] or [banned]",
            "user-info [username]": "to show information about account of selected user",
            "user-pass": "to change password"
}
