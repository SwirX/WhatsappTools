import os
import shutil
from os.path import isfile, abspath, exists, isdir, samefile
from sys import argv
import sqlite3
from sqlite3 import Connection, Cursor

DB_CONNECTIONS = []
OUTPUT_FOLDER_PATH = "output/"
NEW_DATABASE_NAME = "msgstore.db"
TABLES = []
VIEWS = []


def create_db(db: str, name: str = f"{OUTPUT_FOLDER_PATH}/{NEW_DATABASE_NAME}") -> bool:
    try:
        shutil.copyfile(db, name)
        return True
    except Exception as e:
        print("An error has occurred")
        print(e)
        return False


def get_db_info(cursor: Cursor, _type: str) -> list:
    return [a[0] for a in cursor.execute(f"SELECT name FROM sqlite_master WHERE type='{_type}'")]


def get_create_cmd(cursor: Cursor, name: str, _type: str = None) -> list:
    if not _type:
        return [a[0] for a in cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='{_type}' AND name ='{name}'")]
    else:
        return [a[0] for a in cursor.execute(f"SELECT sql FROM sqlite_master WHERE name='{name}'")]


def get_all(cursor: Cursor, _type: str = "table") -> list:
    return [a for a in cursor.execute(f"SELECT name FROM sqlite_master WHERE type = '{_type}'")]


def clean_db(cursor: Cursor) -> None:
    cursor.execute('PRAGMA foreign_keys=off')
    t = get_all(cursor)
    v = get_all(cursor, "views")
    print("Cleaning the database")
    print("Cleaning the views")
    print("views: ", v)
    if len(v) > 0:
        for view in v:
            cursor.execute(f"DROP VIEW {view} IF EXISTS")
    print(get_db_info(cursor, "view"))
    print('tables: ', t)
    print("Cleaning the tables")
    for table in t:
        # Check if the table is a virtual table (type='table') before deletion
        table_type = cursor.execute(f"SELECT type FROM sqlite_master WHERE name = ?", (table,)).fetchone()
        if table_type and table_type[0] == 'table':
            print(table, table_type[0])
            cursor.execute(f'DELETE FROM "main".{table}')
        else:
            cursor.execute(f'DROP {table}')
    print(get_db_info(cursor, "table"))
    cursor.connection.commit()


def make_insert_cmd(table: str, entries_count: int) -> str:
    return f"INSERT INTO {table} VALUES (" + ("?, " * (entries_count - 1)) + "?);"


def make_update_cmd(table: str, entries_count: int) -> str:
    return f"UPDATE INTO {table} VALUES (" + ("?, " * (entries_count - 1)) + "?);"


def get_table_entries(cursor: Cursor, table) -> list:
    return [a for a in cursor.execute(f"SELECT * FROM {table}")]


def get_last_id(cursor: Cursor, table: str) -> int:
    try:
        ret = int(cursor.execute(f"SELECT _id from {table} ORDER BY _id DESC LIMIT 1").fetchone()[0])
        print(f"last id is {ret}")
    except TypeError:
        print("TypeError returning 0 and continuing")
        ret = 0
    return ret


def get_last_entry(cursor: Cursor, table: str, column: str) -> int:
    try:
        ret = int(cursor.execute(f"SELECT {column} from {table} ORDER BY {column} DESC LIMIT 1").fetchone()[0])
        print(f"last id is {ret}")
    except TypeError:
        print("TypeError returning 0 and continuing")
        ret = 0
    return ret


def get_unique_column(cursor, table_name):
    # Execute a query to retrieve column information for the specified table
    cursor.execute(f"PRAGMA table_info({table_name})")

    # Fetch all the rows from the result set
    columns = cursor.fetchall()

    # Check if there is a unique column in the result set
    for column in columns:
        if column[5] == 1:  # Check the 'unique' constraint (index 5 in PRAGMA table_info)
            return column[1]  # Return the name of the unique column

    return ""  # Return None if no unique column is found


def check_id_exists(cursor: Cursor, table: str):
    # Execute a query to retrieve column information for the specified table
    cursor.execute(f"PRAGMA table_info({table})")

    # Fetch all the rows from the result set
    columns = cursor.fetchall()

    # Check if the column with the specified name exists in the result set
    return any(column[1] == "_id" for column in columns) or any("id" in column[1] for column in columns)


def validate_args() -> tuple[str, tuple[str, str, str]]:
    first, second, third, fourth = None, None, None, None
    try:
        first, second, third = argv[1], argv[2], argv[3]
    except IndexError:
        print(
            "missing arguments\n -d [keyfile] [encrypted_database] [decrypted_file_path]\n -m [old_database] [new_database]")
        exit(1)
    paths = second and third
    arefiles = isfile(second) and isfile(third)
    same = samefile(second, third)
    if first == "-d":
        try:
            fourth = argv[4]
        finally:
            fourth = "msgstore.db"
        print("decrypt mode:")
        arefiles = arefiles and isfile(fourth)
        if not arefiles:
            print("one or more of the passed paths is not a file")
            exit(1)
        same = same or samefile(second, fourth) or samefile(third, fourth)
        if same:
            print("one of the files is a duplicate")
            exit(1)
        ending = not (third.endswith(".crypt14") and fourth.endswith(".db"))
        if ending:
            print("the passed input or output file is malformed\neg:\ninput file -> *.crypt14\noutput file -> *.db")
            exit(1)
        return "decrypt", (second, third, fourth)
    elif first == "-m":
        print("merge mode:")
        if not arefiles:
            print("one of the passed paths is not a file")
            exit(1)
        if same:
            print("one of the files is a duplicate")
            exit(1)
        ending = not (second.endswith(".db") and third.endswith(".db"))
        if ending:
            print("one of the passed files is malformed\neg\nold_database -> *.db\nnew_database -> *.db")
            exit(1)
        return "merge", (second, third, "")


def check_output_dir() -> None:
    if not exists(OUTPUT_FOLDER_PATH):
        os.makedirs(OUTPUT_FOLDER_PATH)


def connect(*dbs: str) -> list[Connection]:
    for db in dbs:
        con = sqlite3.connect(db)
        DB_CONNECTIONS.append(con)
    return DB_CONNECTIONS


def decrypt(key: str, inp: str, out: str) -> None:
    os.system(f"python decrypt.py {key} {inp} {out}")


def merge(old: str, new: str):
    check_output_dir()
    old_con, new_con = connect(old, new)
    old_cur, new_cur = old_con.cursor(), new_con.cursor()
    global TABLES, VIEWS
    TABLES, VIEWS = get_db_info(old_cur, "table"), get_db_info(old_cur, "view")
    create_db(old)
    out_con = connect(OUTPUT_FOLDER_PATH + NEW_DATABASE_NAME)[0]
    out_cur = out_con.cursor()
    for table in TABLES:
        print("table name: ", table)
        old_entries = get_table_entries(old_cur, table)
        id_exists = check_id_exists(old_cur, table)
        unique_col = get_unique_column(old_cur, table)
        if table not in [t[0] for t in get_all(new_cur)]:
            continue
        if id_exists is None:
            raise
        elif not id_exists or "hash" in unique_col:
            print("the id column does not exist appending normally")
            try:
                new_entries = get_table_entries(new_cur, table)
            except:
                print(f"{table} doesnt exist in the new table")
                continue
            merged_entries = [e for e in old_entries]
            for entry in new_entries:
                if not entry or entry is None:
                    break
                if entry in merged_entries:
                    continue
                cmd = make_insert_cmd(table, len(entry))
                print(cmd, entry)
                out_cur.execute(cmd, entry)
        else:
            new_entries = get_table_entries(new_cur, table)
            merged_entries = [e for e in old_entries]
            last_id = get_last_entry(old_cur, table, unique_col)
            for entry in new_entries:
                if (entry[0] in m_entry for m_entry in merged_entries) or (entry in merged_entries):
                    continue
                cmd = make_insert_cmd(table, len(entry))
                print(cmd, entry)
                out_cur.execute(cmd, entry)
        try:
            new_entries = get_table_entries(new_cur, table)
            last_id = get_last_id(old_cur, table)
            merged_entries = [e for e in old_entries]
            if table == "props":
                for entry in new_entries:
                    cmd = make_update_cmd(table, len(entry))
                    print(cmd, entry)
                    out_cur.execute(cmd, entry)
            elif table == "jid":
                for entry in new_entries:
                    cmd = make_update_cmd(table, len(entry))
                    print(cmd, entry)
                    out_cur.execute(cmd, entry)

            added_ids = []
            entry: tuple
            for entry in new_entries:
                if not entry or entry is None:
                    break
                if entry in merged_entries:
                    continue
                params = ()
                try:
                    _id = entry[0]
                    if _id in added_ids:
                        last_id *= 2
                        last_id -= 1
                    if entry[0] <= last_id != 0:
                        print(f"{table} entry w/ id {entry[0]} is alr present")
                        print(f"appending the id w/ {last_id}")
                        _id += last_id
                        added_ids.append(_id)
                        print(f"the new id is now {_id}")
                    # else:
                    #    print(f"The id {entry[0]} is not a duplicate")
                    #    _id = entry[0]
                    params += (_id,)
                    if type(entry[1]) == int or type(entry[1]) == tuple:
                        params += (_id,)
                        for i in range(2, len(entry)):
                            params += (entry[i],)
                    else:
                        for i in range(1, len(entry)):
                            params += (entry[i],)
                except Exception as e:
                    print(e)
                finally:
                    cmd = make_insert_cmd(table, len(entry))
                    print(cmd, params)
                    out_cur.execute(cmd, params)
            print(f"merged {table}")
            print("next")
        except sqlite3.OperationalError as e:
            print("Table not found on the new database skipping...")
            continue

    for view in VIEWS:
        print("view name: ", view)
        try:
            out_cur.execute(get_create_cmd(new_cur, view, "view")[0])
        except:
            continue

    print("merging ended")
    print("committing changes")
    out_con.commit()
    print("changes commit")
    print("closing the files")
    out_con.close()
    old_con.close()
    new_con.close()


def main():
    print('Welcome to SwirX\'s Whatsapp Tools')
    mode, args = validate_args()
    if mode == "decrypt":
        decrypt(args[0], args[1], args[2])
    else:
        merge(args[0], args[1])


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("an error has occurred")
        print("cleaning the residuals and closing the connections")
        for con in DB_CONNECTIONS:
            con.close()
        os.system(f"rm ./{OUTPUT_FOLDER_PATH}{NEW_DATABASE_NAME}")
        print(f"executed rm ./{OUTPUT_FOLDER_PATH}{NEW_DATABASE_NAME}")
        raise
