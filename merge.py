import os
from os.path import isfile, abspath, exists, isdir, samefile
from sys import argv
import sqlite3


def create_database(data: str = "", name: str = "merged") -> [bool, str]:
    _status = False
    try:
        with open(f"{name}.db", "w+") as db:
            if data != "":
                db.write(data)
            _status = True
        return _status, abspath(f"{name}.db")
    except IOError as _e:
        print(_e)
        return _status, ""


try:
    old_database_file = argv[1].replace("\\", "/")
    new_database_file = argv[2].replace("\\", "/")
    print(old_database_file)
    print(new_database_file)
    # check if the passed arguments are files
    if not isfile(old_database_file) and not isfile(new_database_file):
        print("the passed paths are not files")
        exit(1)
    # check if the passed files are the same file or not
    if samefile(old_database_file, new_database_file):
        print("the passed files are the same")
        exit(1)
    # check if the passed files are a database file (.db)
    if not old_database_file.endswith(".db") and not new_database_file.endswith(".db"):
        print("the passed files are not a database file (ends with .db)\nRecheck the files and retry")
        exit(0)
    # check if the user has passed a custom output directory
    outputDir = "./output"
    try:
        if argv[3] is not None:
            if isdir(argv[3]) and exists(argv[3]):
                outputDir = argv[3]
    except IndexError:
        pass
    # check if the directory exists if not create one
    if not exists(outputDir):
        os.system(f"mkdir {outputDir}")
    # create a connection to the databases
    old_database_connection = sqlite3.connect(old_database_file)
    new_database_connection = sqlite3.connect(new_database_file)
    # get the cursors
    old_database_cursor = old_database_connection.cursor()
    new_database_cursor = new_database_connection.cursor()
    # get all the tables
    # old_db_table_list = [a for a in old_database_cursor.execute
    # ("SELECT name FROM sqlite_master WHERE type = 'table'")]
    # new_db_table_list = [a for a in new_database_cursor.execute
    # ("SELECT name FROM sqlite_master WHERE type = 'table'")]
    # messages
    old_db_msgs_list = [a for a in old_database_cursor.execute("SELECT * FROM message")]
    print("a")
    msgs_last_id = old_database_connection.execute("SELECT * FROM message ORDER BY _id DESC LIMIT 1").fetchone()[0]
    # old_database_cursor.execute("SELECT * FROM message ORDER BY _id DESC LIMIT 1")
    old_msgs_len = int(msgs_last_id)
    print(f"old messages length = {old_msgs_len}")
    new_db_msgs_list = [a for a in new_database_cursor.execute("SELECT * FROM message")]
    # create the new merged database
    status, path = create_database()
    if not status:
        print("couldn't create the new merged database")
        exit(0)
    merged_database_connection = sqlite3.connect(path)
    merged_database_cursor = merged_database_connection.cursor()

    # make the database
    # message table
    merged_database_cursor.execute(
        "CREATE TABLE message (_id INTEGER PRIMARY KEY AUTOINCREMENT, chat_row_id INTEGER NOT NULL, from_me INTEGER "
        "NOT NULL, key_id TEXT NOT NULL, sender_jid_row_id INTEGER, status INTEGER, broadcast INTEGER, "
        "recipient_count INTEGER, participant_hash TEXT, origination_flags INTEGER, origin INTEGER, timestamp "
        "INTEGER, received_timestamp INTEGER, receipt_server_timestamp INTEGER, message_type INTEGER, text_data TEXT, "
        "starred INTEGER, lookup_tables INTEGER, message_add_on_flags INTEGER, sort_id INTEGER NOT NULL DEFAULT 0 )")
    # call_logs
    print("transferring the old msg list")
    old_messages_key_ids = []
    for m in old_db_msgs_list:  # m : messages
        v = []
        for i in range(20):
            if m[i] is None:
                v.append("''")
            else:
                v.append(f"'{m[i]}'")
        params = (
            m[0], m[1], m[2], m[3], m[4],
            m[5], m[6], m[7], m[8], m[9],
            m[10], m[11], m[12], m[13],
            m[14], m[15], m[16], m[17], m[18], m[0]
        )
        print(params)
        sql = "INSERT INTO message VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        # sql = re.subn("(\'None\')", "NULL", sql)
        merged_database_cursor.execute(sql, params)
        old_messages_key_ids.append(m[3])
        print("inserted")
        print("\n\n")
    print("merging the second messages table with the old one")
    for m in new_db_msgs_list:
        if m[3] in old_messages_key_ids:
            print("duplicate message skipping...")
            continue
        new_id = int(m[0]) + old_msgs_len
        v = []
        for i in range(20):
            if m[i] is None:
                v.append("''")
            else:
                v.append(f"'{m[i]}'")
        params = (
            new_id, m[1], m[2], m[3], m[4],
            m[5], m[6], m[7], m[8], m[9], m[10],
            m[11], m[12], m[13], m[14], m[15],
            m[16], m[17], m[18], new_id
        )
        print(params)
        sql = "INSERT INTO message VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        merged_database_cursor.execute(sql, params)
        print("inserted")
        print("\n\n")

    print("merged the messages")

    # merging the call logs

    merged_database_cursor.execute("CREATE TABLE call_log(_id INTEGER PRIMARY KEY AUTOINCREMENT,jid_row_id INTEGER,"
                                   "from_me INTEGER,call_id TEXT,transaction_id INTEGER,timestamp INTEGER,video_call "
                                   "INTEGER,duration INTEGER,call_result INTEGER,is_dnd_mode_on INTEGER,"
                                   "bytes_transferred INTEGER,group_jid_row_id INTEGER NOT NULL DEFAULT 0,"
                                   "is_joinable_group_call INTEGER,call_creator_device_jid_row_id INTEGER NOT NULL "
                                   "DEFAULT 0,call_random_id TEXT,call_link_row_id INTEGER NOT NULL DEFAULT 0,"
                                   "call_type INTEGER,offer_silence_reason INTEGER,scheduled_id TEXT)")

    old_db_call_logs_list = [a for a in old_database_cursor.execute("SELECT * FROM call_log")]
    new_db_call_logs_list = [a for a in new_database_cursor.execute("SELECT * FROM call_log")]

    calls_id = []
    last_call_id = int(old_database_connection.execute
                       ("SELECT * FROM call_log ORDER BY _id DESC LIMIT 1").fetchone()[0])
    calls_log_sql = "INSERT INTO call_logs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
    for cl in old_db_call_logs_list:
        params = (
            cl[0], cl[1], cl[2], cl[3], cl[4], cl[5], cl[6], cl[7], cl[8], cl[9],
            cl[10], cl[11], cl[12], cl[13], cl[14], cl[15], cl[16], cl[17], cl[18]
        )
        merged_database_cursor.execute(calls_log_sql, params)
        calls_id.append(cl[3])
        print("inserted")
    for cl in new_db_call_logs_list:
        if cl[3] in calls_id:
            print("skipping duplicates...")
            continue
        new_id = last_call_id + int(cl[0])
        params = (
            new_id, cl[1], cl[2], cl[3], cl[4], cl[5], cl[6], cl[7], cl[8], cl[9],
            cl[10], cl[11], cl[12], cl[13], cl[14], cl[15], cl[16], cl[17], cl[18]
        )
        merged_database_cursor.execute(calls_log_sql, params)
        print("inserted")
    print("call_logs merge complete")

    # merge chat_view

    old_db_chat_view = old_database_cursor.execute("SELECT * chat_view ORDER BY sort_timestamp DESC")
    new_db_chat_view = new_database_cursor.execute("SELECT * chat_view ORDER BY sort_timestamp DESC")

    chat_view = []
    chat_sql = ("INSERT INTO chat_view VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?")
    last_chat_view_id = int(old_database_connection.execute
                            ("SELECT * FROM chat_view ORDER BY _id DESC LIMIT 1").fetchone()[0])
    for c in new_db_chat_view:
        chat = [c[1], c[11]]
        params = (
            c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8], c[9],
            c[10], c[11], c[12], c[13], c[14], c[15], c[16], c[17], c[18], c[19],
            c[20], c[21], c[22], c[23], c[24], c[25], c[26], c[27], c[28], c[29],
            c[30], c[31], c[32], c[33], c[34], c[35], c[36], c[37], c[38], c[39],
            c[40], c[41], c[42]
        )
        merged_database_cursor.execute(chat_sql, params)
        chat_view.append(chat)
        print("inserted")
    for c in old_db_chat_view:
        f = False
        for chat in chat_view:
            if c[1] in chat:
                if c[11] < chat[1]:
                    print("the chat is old skipping...")
                    continue
        if f:
            continue
        new_id = last_chat_view_id + int(c[0])
        params = (
            new_id, c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8], c[9],
            c[10], c[11], c[12], c[13], c[14], c[15], c[16], c[17], c[18], c[19],
            c[20], c[21], c[22], c[23], c[24], c[25], c[26], c[27], c[28], c[29],
            c[30], c[31], c[32], c[33], c[34], c[35], c[36], c[37], c[38], c[39],
            c[40], c[41], c[42]
        )
        merged_database_cursor.execute(chat_sql, params)
        print("inserted")
    print("chat_view merge complete")

    merged_database_connection.commit()
    merged_database_connection.close()
    old_database_connection.close()
    new_database_connection.close()


except Exception as e:
    print("We hit a wall...")
    print(e)
