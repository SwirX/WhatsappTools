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
    # call_log
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

    print("making the call_log table")

    merged_database_cursor.execute("CREATE TABLE call_log(_id INTEGER PRIMARY KEY AUTOINCREMENT,jid_row_id INTEGER,"
                                   "from_me INTEGER,call_id TEXT,transaction_id INTEGER,timestamp INTEGER,video_call "
                                   "INTEGER,duration INTEGER,call_result INTEGER,is_dnd_mode_on INTEGER,"
                                   "bytes_transferred INTEGER,group_jid_row_id INTEGER NOT NULL DEFAULT 0,"
                                   "is_joinable_group_call INTEGER,call_creator_device_jid_row_id INTEGER NOT NULL "
                                   "DEFAULT 0,call_random_id TEXT,call_link_row_id INTEGER NOT NULL DEFAULT 0,"
                                   "call_type INTEGER,offer_silence_reason INTEGER,scheduled_id TEXT)")

    print("getting the call logs lists")

    old_db_call_log_list = [a for a in old_database_cursor.execute("SELECT * FROM call_log")]
    new_db_call_log_list = [a for a in new_database_cursor.execute("SELECT * FROM call_log")]

    calls_id = []
    last_call_id = int(old_database_connection.execute
                       ("SELECT * FROM call_log ORDER BY _id DESC LIMIT 1").fetchone()[0])
    calls_log_sql = "INSERT INTO call_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    print("looping through old call logs")
    for cl in old_db_call_log_list:
        params = (
            cl[0], cl[1], cl[2], cl[3], cl[4], cl[5], cl[6], cl[7], cl[8], cl[9],
            cl[10], cl[11], cl[12], cl[13], cl[14], cl[15], cl[16], cl[17], cl[18]
        )
        print(params)
        merged_database_cursor.execute(calls_log_sql, params)
        calls_id.append(cl[3])
        print("inserted")
    for cl in new_db_call_log_list:
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
    print("call_log merge complete")

    # merge the chat table
    print("creating the chat table")
    merged_database_cursor.execute("CREATE TABLE chat(_id INTEGER PRIMARY KEY AUTOINCREMENT,jid_row_id INTEGER "
                                   "UNIQUE,hidden INTEGER,subject TEXT,created_timestamp INTEGER,"
                                   "display_message_row_id INTEGER,last_message_row_id INTEGER,"
                                   "last_read_message_row_id INTEGER,last_read_receipt_sent_message_row_id INTEGER,"
                                   "last_important_message_row_id INTEGER,archived INTEGER,sort_timestamp INTEGER,"
                                   "mod_tag INTEGER,gen REAL,spam_detection INTEGER,"
                                   "unseen_earliest_message_received_time INTEGER,unseen_message_count INTEGER,"
                                   "unseen_missed_calls_count INTEGER,unseen_row_count INTEGER,plaintext_disabled "
                                   "INTEGER,vcard_ui_dismissed INTEGER,change_number_notified_message_row_id INTEGER,"
                                   "show_group_description INTEGER,ephemeral_expiration INTEGER,"
                                   "last_read_ephemeral_message_row_id INTEGER,ephemeral_setting_timestamp INTEGER,"
                                   "ephemeral_displayed_exemptions INTEGER,ephemeral_disappearing_messages_initiator "
                                   "INTEGER,unseen_important_message_count INTEGER NOT NULL DEFAULT 0,group_type "
                                   "INTEGER NOT NULL DEFAULT 0,last_message_reaction_row_id INTEGER,"
                                   "last_seen_message_reaction_row_id INTEGER,unseen_message_reaction_count INTEGER,"
                                   "growth_lock_level INTEGER,growth_lock_expiration_ts INTEGER,"
                                   "last_read_message_sort_id INTEGER,display_message_sort_id INTEGER,"
                                   "last_message_sort_id INTEGER,last_read_receipt_sent_message_sort_id INTEGER,"
                                   "has_new_community_admin_dialog_been_acknowledged INTEGER NOT NULL DEFAULT 0,"
                                   "history_sync_progress INTEGER,chat_lock INTEGER)")

    print("get the chat lists")

    old_db_chat_list = [a for a in old_database_cursor.execute("SELECT * FROM chat")]
    new_db_chat_list = [a for a in new_database_cursor.execute("SELECT * FROM chat")]

    chat_jids = []
    chat_sql = ("INSERT INTO chat VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
    last_chat_id = int(old_database_connection.execute
                       ("SELECT * FROM chat ORDER BY _id DESC LIMIT 1").fetchone()[0])
    for c in old_db_chat_list:
        params = (
            c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8], c[9], c[10], c[11], c[12], c[13], c[14], c[15],
            c[16], c[17], c[18], c[19], c[20], c[21], c[22], c[23], c[24], c[25], c[26], c[27], c[28], c[29],
            c[30], c[31], c[32], c[33], c[34], c[35], c[36], c[37], c[38], c[39], c[40], c[41]
        )
        merged_database_cursor.execute(chat_sql, params)
        chat_jids.append(c[1])
        print("inserted")
    for c in new_db_chat_list:
        if c[1] in chat_jids:
            print("duplicate chat, skipping...")
            continue
        new_id = last_chat_id + int(c[0])
        params = (
            new_id, c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8], c[9], c[10], c[11], c[12], c[13], c[14], c[15],
            c[16], c[17], c[18], c[19], c[20], c[21], c[22], c[23], c[24], c[25], c[26], c[27], c[28], c[29],
            c[30], c[31], c[32], c[33], c[34], c[35], c[36], c[37], c[38], c[39], c[40], c[41]
        )
        merged_database_cursor.execute(chat_sql, params)
        print("inserted")
    print("merged the chat table")

    # merge jid

    print("creating the jid table")
    merged_database_cursor.execute("CREATE TABLE jid(_id INTEGER PRIMARY KEY AUTOINCREMENT,user TEXT NOT NULL,"
                                   "server TEXT NOT NULL,agent INTEGER,device INTEGER,type INTEGER,raw_string TEXT)")

    print("getting the jid from the tables")
    old_db_jid_list = [a for a in old_database_cursor.execute("SELECT * FROM jid")]
    new_db_jid_list = [a for a in new_database_cursor.execute("SELECT * FROM jid")]

    jid_sql = "INSERT INTO jid VALUES (?, ?, ?, ?, ?, ?, ?);"

    last_jid_id = int(old_database_connection.execute
                      ("SELECT * FROM jid ORDER BY _id DESC LIMIT 1").fetchone()[0])

    jid_users = []
    for u in old_db_jid_list:
        params = (u[0], u[1], u[2], u[3], u[4], u[5], u[6])
        merged_database_cursor.execute(jid_sql, params)
        jid_users.append(u[1])
        print("inserted")
    for u in new_db_jid_list:
        if u[1] in jid_users:
            print("duplicate entry, skipping...")
            continue
        new_id = last_jid_id + int(u[0])
        params = (new_id, u[1], u[3], u[4], u[5], u[6], u[7])
        merged_database_cursor.execute(jid_sql, params)
        print("inserted")
    print("merged jid table")

    # merge chat_view

    print("making the chat_view view")

    merged_database_cursor.execute("CREATE VIEW chat_view AS SELECT chat._id AS _id, jid.raw_string AS "
                                   "raw_string_jid, hidden AS hidden, subject AS subject, created_timestamp AS "
                                   "created_timestamp, display_message_row_id AS display_message_row_id, "
                                   "last_message_row_id AS last_message_row_id, last_read_message_row_id AS "
                                   "last_read_message_row_id, last_read_receipt_sent_message_row_id AS "
                                   "last_read_receipt_sent_message_row_id, last_important_message_row_id AS "
                                   "last_important_message_row_id, archived AS archived, sort_timestamp AS "
                                   "sort_timestamp, mod_tag AS mod_tag, gen AS gen, spam_detection AS spam_detection, "
                                   "unseen_earliest_message_received_time AS unseen_earliest_message_received_time, "
                                   "unseen_message_count AS unseen_message_count, unseen_missed_calls_count AS "
                                   "unseen_missed_calls_count, unseen_row_count AS unseen_row_count, "
                                   "unseen_message_reaction_count AS unseen_message_reaction_count, "
                                   "last_message_reaction_row_id AS last_message_reaction_row_id, "
                                   "last_seen_message_reaction_row_id AS last_seen_message_reaction_row_id, "
                                   "plaintext_disabled AS plaintext_disabled, vcard_ui_dismissed AS "
                                   "vcard_ui_dismissed, change_number_notified_message_row_id AS "
                                   "change_number_notified_message_row_id, show_group_description AS "
                                   "show_group_description, ephemeral_expiration AS ephemeral_expiration, "
                                   "last_read_ephemeral_message_row_id AS last_read_ephemeral_message_row_id, "
                                   "ephemeral_setting_timestamp AS ephemeral_setting_timestamp, "
                                   "ephemeral_displayed_exemptions AS ephemeral_displayed_exemptions, "
                                   "ephemeral_disappearing_messages_initiator AS "
                                   "ephemeral_disappearing_messages_initiator, unseen_important_message_count AS "
                                   "unseen_important_message_count, group_type AS group_type, growth_lock_level AS "
                                   "growth_lock_level, growth_lock_expiration_ts AS growth_lock_expiration_ts, "
                                   "last_read_message_sort_id AS last_read_message_sort_id, display_message_sort_id "
                                   "AS display_message_sort_id, last_message_sort_id AS last_message_sort_id, "
                                   "last_read_receipt_sent_message_sort_id AS last_read_receipt_sent_message_sort_id, "
                                   "has_new_community_admin_dialog_been_acknowledged AS "
                                   "has_new_community_admin_dialog_been_acknowledged, history_sync_progress AS "
                                   "history_sync_progress, chat_lock AS chat_lock FROM chat chat LEFT JOIN jid jid ON "
                                   "chat.jid_row_id = jid._id")

    print("chat_view creation complete")

    # deleted_chat_job

    merged_database_cursor.execute("CREATE TABLE deleted_chat_job(_id INTEGER PRIMARY KEY AUTOINCREMENT, chat_row_id "
                                   "INTEGER NOT NULL, block_size INTEGER, singular_message_delete_rows_id TEXT, "
                                   "deleted_message_row_id  INTEGER, deleted_starred_message_row_id  INTEGER, "
                                   "deleted_messages_remove_files BOOLEAN, deleted_categories_message_row_id INTEGER, "
                                   "deleted_categories_starred_message_row_id INTEGER, "
                                   "deleted_categories_remove_files BOOLEAN, deleted_message_categories TEXT, "
                                   "delete_files_singular_delete BOOLEAN)")

    old_db_dcj = [a for a in old_database_cursor.execute("SELECT * FROM deleted_chat_job")]
    new_db_dcj = [a for a in new_database_cursor.execute("SELECT * FROM deleted_chat_job")]

    dcj_sql = "INSERT INTO deleted_chat_job VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"

    dcj_ids = []

    for d in old_db_dcj:
        if not d:
            break
        params = (d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11])
        merged_database_cursor.execute(dcj_sql, params)
        dcj_ids.append(d[0])
        print("inserted")
    for d in new_db_dcj:
        if not d:
            break
        new_id = len(dcj_ids) + d[0]
        params = (new_id, d[1], d[2], d[3], d[4], d[5], d[6], d[7], d[8], d[9], d[10], d[11])
        merged_database_cursor.execute(dcj_sql, params)
        print("inserted")
    print("merged deleted_chat_job")

    # message_ephemeral

    merged_database_cursor.execute("CREATE TABLE message_ephemeral(message_row_id INTEGER PRIMARY KEY,duration "
                                   "INTEGER NOT NULL,expire_timestamp INTEGER NOT NULL,keep_in_chat INTEGER NOT NULL "
                                   "DEFAULT 0)")

    old_db_me = [a for a in old_database_cursor.execute("SELECT * FROM message_ephemeral")]
    new_db_me = [a for a in new_database_cursor.execute("SELECT * FROM message_ephemeral")]

    me_sql = "INSERT INTO message_ephemeral VALUES (?, ?, ?, ?);"

    me_ids = []
    for e in old_db_me:
        if not e:
            break
        params = (e[0], e[1], e[2], e[3])
        merged_database_cursor.execute(me_sql, params)
        me_ids.append(e[0])
        print("inserted")
    for e in new_db_me:
        if not e:
            break
        if e[0] in me_ids:
            print("duplicate")
            continue
        params = (e[0], e[1], e[2], e[3])
        merged_database_cursor.execute(me_sql, params)
        print("inserted")
    print("merged message_ephemeral")

    # available_message_view

    print("making the available_message_view view")

    merged_database_cursor.execute("CREATE VIEW available_message_view AS  SELECT message._id AS _id, message.sort_id "
                                   "AS sort_id, message.chat_row_id AS chat_row_id, from_me, key_id, "
                                   "sender_jid_row_id, NULL AS sender_jid_raw_string, status, broadcast, "
                                   "recipient_count, participant_hash, origination_flags, origin, timestamp, "
                                   "received_timestamp, receipt_server_timestamp, message_type, text_data, starred, "
                                   "lookup_tables, message_add_on_flags, NULL AS data, NULL AS media_url, "
                                   "NULL AS media_mime_type, NULL AS media_size, NULL AS media_name, "
                                   "NULL AS media_caption, NULL AS media_hash, NULL AS media_duration, "
                                   "NULL AS latitude, NULL AS longitude, NULL AS thumb_image, NULL AS raw_data, "
                                   "NULL AS quoted_row_id, NULL AS mentioned_jids, NULL AS multicast_id, "
                                   "NULL AS edit_version, NULL AS media_enc_hash, NULL AS payment_transaction_id, "
                                   "NULL AS preview_type, NULL AS receipt_device_timestamp, "
                                   "NULL AS read_device_timestamp, NULL AS played_device_timestamp, "
                                   "NULL AS future_message_type, 2 AS table_version, expire_timestamp, keep_in_chat "
                                   "FROM message LEFT JOIN deleted_chat_job AS job ON job.chat_row_id = "
                                   "message.chat_row_id LEFT JOIN message_ephemeral AS message_ephemeral ON "
                                   "message._id = message_ephemeral.message_row_id WHERE  IFNULL(NOT((IFNULL("
                                   "message.starred, 0) = 0 AND message.sort_id <= IFNULL(job.deleted_message_row_id, "
                                   "-9223372036854775808)) OR (IFNULL(message.starred, 0) = 1 AND message.sort_id <= "
                                   "IFNULL(job.deleted_starred_message_row_id, -9223372036854775808)) OR ( ("
                                   "job.deleted_message_categories IS NOT NULL) AND (job.deleted_message_categories "
                                   "LIKE '%"' || message.message_type || '"%') AND ((IFNULL(message.starred, "
                                   "0) = 0 AND message.sort_id <= IFNULL(job.deleted_categories_message_row_id, "
                                   "-9223372036854775808)) OR (IFNULL(message.starred, 0) = 1 AND message.sort_id <= "
                                   "IFNULL(job.deleted_categories_starred_message_row_id, -9223372036854775808)))) OR "
                                   "((job.singular_message_delete_rows_id IS NOT NULL) AND ("
                                   "job.singular_message_delete_rows_id LIKE '%"' || message._id || '"%'))), 0)")

    merged_database_connection.commit()
    merged_database_connection.close()
    old_database_connection.close()
    new_database_connection.close()

    print("completed the merging process")

except Exception as e:
    print("We hit a wall...")
    print(e)
