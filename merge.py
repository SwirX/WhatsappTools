import os
from os.path import isfile, abspath, exists, isdir, samefile
from sys import argv
import sqlite3


def create_database(olddb, name: str = "merged") -> [bool, str]:
    _status = False
    try:
        with open(f'{abspath(olddb)}', 'rb') as old_db:
            db_bytes = old_db.read()
            with open("merged.db", "wb+") as merged_db:
                merged_db.write(db_bytes)
        _status = True
    except Exception as exception:
        print("an error has occurred")
        print(exception)

    return _status, abspath("merged.db")



def create_insert_command(table_name, entries_count):
    print(f"INSERT INTO {table_name} VALUES (" + ("?, " * (entries_count - 1)) + "?);")
    return f"INSERT INTO {table_name} VALUES (" + ("?, " * (entries_count - 1)) + "?);"


def validate_arguments(first_file, second_file):
    if not isfile(first_file) and not isfile(second_file):
        print("the passed paths are not files")
        exit(1)
    # check if the passed files are the same file or not
    if samefile(first_file, second_file):
        print("the passed files are the same")
        exit(1)
    # check if the passed files are a database file (.db)
    if not first_file.endswith(".db") and not second_file.endswith(".db"):
        if first_file.endswith(".crypt14") and second_file.endswith(".crypt14"):
            print("Still not implemented")
            exit(1)
        print("the passed files are not a database file (ends with .db)\nRecheck the files and retry")
        exit(1)


try:
    old_database_file = argv[1].replace("\\", "/")
    new_database_file = argv[2].replace("\\", "/")
    validate_arguments(old_database_file, new_database_file)

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

    old_database_connection = sqlite3.connect(old_database_file)
    new_database_connection = sqlite3.connect(new_database_file)
    old_database_cursor = old_database_connection.cursor()
    new_database_cursor = new_database_connection.cursor()

    l = [a for a in old_database_cursor.execute
    ("SELECT name FROM sqlite_master WHERE type = 'trigger'")]

    old_tables = [a[0] for a in old_database_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    old_views = [a[0] for a in old_database_cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")]
    old_triggers = [a[0] for a in old_database_cursor.execute("SELECT name FROM sqlite_master WHERE type='trigger'")]

    new_tables = [a[0] for a in new_database_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    new_views = [a[0] for a in new_database_cursor.execute("SELECT name FROM sqlite_master WHERE type='view'")]
    new_triggers = [a[0] for a in new_database_cursor.execute("SELECT name FROM sqlite_master WHERE type='trigger'")]

    added_tables = []
    added_views = []
    added_triggers = []

    status, path = create_database(old_database_file)
    if not status:
        print("an error has occurred while creating the merged file")
        exit(1)
    merged_database_connection = sqlite3.connect(path)
    merged_database_cursor = merged_database_connection.cursor()

    for table in tables:
        if table in added_tables:
            continue
        skip_merging = table == "android_metadata"
        print("\n" + table + "\n")
        old_db_table_entries = [a for a in old_database_cursor.execute(f"SELECT * FROM {table}")]
        new_db_table_entries = [a for a in new_database_cursor.execute(f"SELECT * FROM {table}")]
        sql_create_table_function = old_database_cursor.execute(f"SELECT sql FROM sqlite_master WHERE name = '{table}' "
                                                                "AND type = 'table'").fetchone()[0].replace("FTS3(", "FTS4(")
        print("create function: ", sql_create_table_function)
        merged_database_cursor.execute(sql_create_table_function)
        last_id = int(
            old_database_connection.execute("SELECT * FROM message ORDER BY _id DESC LIMIT 1").fetchone()[0]) or 0
        if last_id == 0:
            continue

        merged_entries = []

        for e in old_db_table_entries:
            merged_entries.append(e)

        if not skip_merging:
            for e in new_db_table_entries:
                if not e or e is None:
                    break
                if e in merged_entries:
                    print("merged. skipping...")
                    continue
                params = ()
                try:
                    new_id = last_id + e[0]
                    params += (new_id,)
                    if type(e[1]) == int or type(e[1]) == tuple:
                        try:
                            params += (new_id,)
                            for i in range(2, len(e)):
                                params += (e[i],)
                        except Exception as ex:
                            print(ex)
                    elif type(e[1]) == str:
                        continue
                    else:
                        for i in range(1, len(e)):
                            params += (e[i],)
                except Exception as exp:
                    print(exp)
                print(params)
                merged_database_cursor.execute(create_insert_command(table, len(e)), params)
            added_tables.append(table)

        merged_database_connection.commit()

    for view in views:
        if view in added_views:
            continue
        print("\n" + view + "\n")
        sql_create_view_function = new_database_cursor.execute(
            f"SELECT sql FROM sqlite_master WHERE name = '{view}' AND type = 'view'").fetchone()[0]
        merged_database_cursor.execute(sql_create_view_function)
        added_views.append(view)
        merged_database_connection.commit()

    for trigger in triggers:
        if trigger in added_triggers:
            continue
        print("\n" + trigger + "\n")
        sql_create_trigger_function = new_database_cursor.execute(
            f"SELECT sql FROM sqlite_master WHERE name = '{trigger}' AND type = 'trigger'").fetchone()[0]
        merged_database_cursor.execute(sql_create_trigger_function)
        added_triggers.append(trigger)
        merged_database_connection.commit()

    print("merged the data")
    print("saving the file")
    merged_database_connection.close()
    old_database_connection.close()
    new_database_connection.close()


except IndexError as e:
    print(e)
    exit(1)
