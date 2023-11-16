# just a bare-bones script to generate some parts of the code cuz it's tedious, and I'm lazy

m = ""  # mode empty creates the sql command to insert the values if not writes the params

t = "message_ephemeral"  # the table name

n = "e"  # the looping variable name
c = 4  # values count

o = ""  # output file

if m != "":
    for i in range(0, c):
        if i == c-1:
            o = o + n + f"[{i}]"
        else:
            o = o + n + f"[{i}], "
    o = "params = (" + o + ")"
else:
    o = f"INSERT INTO {t} VALUES (" + ("?, " * (c-1)) + "?);"

print(o)
