import sqlite3

conn = sqlite3.connect("data/licenses.db")
conn.row_factory = sqlite3.Row

row = conn.execute("""
select
    license_key,
    license_type,
    status,
    machine_id,
    current_machine_count,
    last_verified_at,
    next_verify_due_at
from licenses
where license_key = 'TRIAL'
""").fetchone()

print(dict(row) if row else None)