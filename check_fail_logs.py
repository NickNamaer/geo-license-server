import sqlite3

conn = sqlite3.connect("data/licenses.db")
conn.row_factory = sqlite3.Row

rows = conn.execute("""
select
    id,
    license_key,
    machine_id,
    product,
    version,
    success,
    error_code,
    message,
    remote_addr,
    created_at
from license_verifications
order by id desc
limit 20
""").fetchall()

print("=== license_verifications ===")
for r in rows:
    print(dict(r))