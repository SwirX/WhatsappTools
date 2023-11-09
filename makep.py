# just a bare-bones script to generate some parts of the code cuz it's tedious, and I'm lazy

m = ""

t = "message_ephemeral"

n = "e"
c = 4

o = ""

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
