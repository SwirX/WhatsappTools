import os
from os.path import isfile, abspath, exists, isdir, samefile
from sys import argv
import sqlite3

def create_database(data:str="", name:str="merged")->[bool, str]:
    status = False
    try:
        with open(f"{name}.db", "w+") as db:
            if data != "":
                db.write(data)
            status = True
        return status, abspath(f"{name}.db")
    except:
        return status, ""


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
    #check if the passed files are a database file (.db)
    if not old_database_file.endswith(".db") and not new_database_file.endswith(".db"):
        print("the passed files are not a database file (ends with .db)\nRecheck the files and retry")
        exit(0)
    # check if the user has passed a custom output directory
    outputDir = "./output"
    try:
        if argv[3] is not None:
            if isdir(argv[3]) and exists(argv[3]):
                outputDir = argv[3]
    except Exception:
        pass
    # check if the directory exists if not create one
    if not exists(outputDir):
        os.system(f"mkdir {outputDir}")
    # create a connection to the databases
    old_database_connection = sqlite3.connect(old_database_file)
    new_database_connection = sqlite3.connect(new_database_file)
    #get the cursors
    old_database_cursor = old_database_connection.cursor()
    new_database_cursor = new_database_connection.cursor()
    # get all the tables
    # old_db_table_list = [a for a in old_database_cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
    # new_db_table_list = [a for a in new_database_cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
    # messages
    old_db_msgs_list = [a for a in old_database_cursor.execute("SELECT * FROM message")]
    print("a")
    msglencur = old_database_connection.execute("SELECT * FROM message ORDER BY _id DESC LIMIT 1")
    # old_database_cursor.execute("SELECT * FROM message ORDER BY _id DESC LIMIT 1")
    old_msgs_len = int(msglencur.fetchone()[0])
    print(f"old messages lenght = {old_msgs_len}")
    new_db_msgs_list = [a for a in new_database_cursor.execute("SELECT * FROM message")]
    #create the new merged database
    status, path = create_database()
    if not status:
        print("couldn't create the new merged database")
        exit(0)
    merged_database_connection = sqlite3.connect(path)
    merged_database_cursor = merged_database_connection.cursor()
    # make the database
    merged_database_cursor.execute("CREATE TABLE message (_id INTEGER PRIMARY KEY AUTOINCREMENT, chat_row_id INTEGER NOT NULL, from_me INTEGER NOT NULL, key_id TEXT NOT NULL, sender_jid_row_id INTEGER, status INTEGER, broadcast INTEGER, recipient_count INTEGER, participant_hash TEXT, origination_flags INTEGER, origin INTEGER, timestamp INTEGER, received_timestamp INTEGER, receipt_server_timestamp INTEGER, message_type INTEGER, text_data TEXT, starred INTEGER, lookup_tables INTEGER, message_add_on_flags INTEGER, sort_id INTEGER NOT NULL DEFAULT 0 )")
    print("transfering the old msg list")
    old_messages_key_ids = []
    for m in old_db_msgs_list: # m : messages
        v = []
        for i in range(20):
            if m[i] == None:
                v.append("''")
            else:
                v.append(f"'{m[i]}'")
        params = (
          int(m[0]), m[1], m[2], m[3], m[4],
          m[5], m[6], m[7], m[8], m[9],
          m[10], m[11], m[12], m[13],
          m[14], m[15], m[16], m[17], m[18], int(m[0])
          )
        print(params)
        # oldsql = f"INSERT INTO \"main\".\"message\" (\"_id\", \"chat_row_id\", \"from_me\", \"key_id\", \"sender_jid_row_id\", \"status\", \"broadcast\", \"recipient_count\", \"participant_hash\", \"origination_flags\", \"origin\", \"timestamp\", \"received_timestamp\", \"receipt_server_timestamp\", \"message_type\", \"text_data\", \"starred\", \"lookup_tables\", \"message_add_on_flags\", \"sort_id\") VALUES ({v[0]}, {v[1]}, {v[2]}, {v[3]}, {v[4]}, {v[5]}, {v[6]}, {v[7]}, {v[8]}, {v[9]}, {v[10]}, {v[11]}, {v[12]}, {v[13]}, {v[14]}, {m15}, {v[16]}, {v[17]}, {v[18]}, {v[19]});"
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
        newid = int(m[0]) + old_msgs_len
        v = []
        for i in range(20):
            if m[i] == None:
                v.append("''")
            else:
                v.append(f"'{m[i]}'")
        params = (
          int(newid), m[1], m[2], m[3], m[4],
          m[5], m[6], m[7], m[8], m[9],
          m[10], m[11], m[12], m[13],
          m[14], m[15], m[16], m[17], m[18], int(newid)
          )
        print(params)
        # oldsql = f"INSERT INTO \"main\".\"message\" (\"_id\", \"chat_row_id\", \"from_me\", \"key_id\", \"sender_jid_row_id\", \"status\", \"broadcast\", \"recipient_count\", \"participant_hash\", \"origination_flags\", \"origin\", \"timestamp\", \"received_timestamp\", \"receipt_server_timestamp\", \"message_type\", \"text_data\", \"starred\", \"lookup_tables\", \"message_add_on_flags\", \"sort_id\") VALUES ({v[0]}, {v[1]}, {v[2]}, {v[3]}, {v[4]}, {v[5]}, {v[6]}, {v[7]}, {v[8]}, {v[9]}, {v[10]}, {v[11]}, {v[12]}, {v[13]}, {v[14]}, {m15}, {v[16]}, {v[17]}, {v[18]}, {v[19]});"
        sql = "INSERT INTO message VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        # sql = re.subn("(\'None\')", "NULL", sql)
        merged_database_cursor.execute(sql, params)
        print("inserted")
        print("\n\n")

    print("merged the messages")

    # merging the call logs

    
    merged_database_connection.commit()
    merged_database_connection.close()
    old_database_connection.close()
    new_database_connection.close()


except Exception as e:
    print("We hit a wall...")
    print(e)