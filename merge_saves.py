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


def clean_db(cursor: Cursor) -> None:
    cursor.execute('PRAGMA foreign_keys=off')
    print("Cleaning the database")
    print("Cleaning the views")
    print("views: ", VIEWS)
    if len(VIEWS) > 0:
        for view in VIEWS:
            cursor.execute(f"DROP VIEW {view} IF EXISTS")
    print(get_db_info(cursor, "view"))
    print('tables: ', TABLES)
    print("Cleaning the tables")
    for table in TABLES:
        # Check if the table is a virtual table (type='table') before deletion
        table_type = cursor.execute(f"SELECT type FROM sqlite_master WHERE name = ?", (table,)).fetchone()
        if table_type and table_type[0] == 'table':
            if table == "message_ftsv2":
                print(" the problem maker is here")
            print(table, table_type[0])
            cursor.execute(f'DELETE FROM "main".{table}')
        else:
            cursor.execute(f'DROP {table}')
    print(get_db_info(cursor, "table"))
    cursor.connection.commit()


def make_insert_cmd(table: str, entries_count: int) -> str:
    return f"INSERT INTO {table} VALUES (" + ("?, " * (entries_count - 1)) + "?);"


def get_table_entries(cursor: Cursor, table) -> list:
    return [a for a in cursor.execute(f"SELECT * FROM {table}")]


def get_last_id(cursor: Cursor, table: str) -> int:
    return int(cursor.execute(f"SELECT * from {table} ORDER BY _id DESC LIMIT 1").fetchone()[0]) or 0


def validate_args() -> tuple[str, tuple[str, str, str]]:
    first, second, third, fourth = None, None, None, None
    try:
        first, second, third = argv[1], argv[2], argv[3]
    except IndexError:
        print("missing arguments\n -d [keyfile] [encrypted_database] [decrypted_file_path]\n -m [old_database] [new_database]")
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
    out_con = connect(OUTPUT_FOLDER_PATH+NEW_DATABASE_NAME)[0]
    out_cur = out_con.cursor()
    # clean_db(out_cur)
    for table in TABLES:
        print("table name: ", table)
        old_entries = get_table_entries(old_cur, table)
        new_entries = get_table_entries(new_cur, table)
        last_id = get_last_id(old_cur, table)
        merged_entries = [e for e in old_entries]

        for e in new_entries:
            if not e or e is None:
                break
            if e in merged_entries:
                continue
            params = ()
            try:
                _id = 0
                for oe in merged_entries:
                    if e[0] in oe:
                        _id = last_id + e[0]
                    else:
                        _id = e[0]
                params += (_id,)
                if type(e[1]) == int or type(e[1]) == tuple:
                    params += (_id,)
                    for i in range(2, len(e)):
                        params += (e[i],)
                else:
                    for i in range(1, len(e)):
                        params += (e[i],)
            except Exception as e:
                print(e)
            finally:
                cmd = make_insert_cmd(table, len(e))
                print(cmd, params)
                out_cur.execute(cmd, params)

    for view in VIEWS:
        print("view name: ", view)
        out_cur.execute(get_create_cmd(new_cur, view, "view")[0])

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
        os.system("rm ./output/msgstore.db")
        raise
