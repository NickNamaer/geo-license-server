import sqlite3

conn = sqlite3.connect("data/licenses.db")
conn.row_factory = sqlite3.Row

print("=== licenses ===")
rows = conn.execute("""
select
    license_key,
    license_type,
    max_machines,
    current_machine_count,
    plan_code,
    support_enabled,
    is_perpetual
from licenses
where license_key = 'FULL-TEST-001'
""").fetchall()

for r in rows:
    print(dict(r))

print("\n=== machines ===")
rows = conn.execute("""
select
    license_id,
    machine_id,
    is_active,
    app_version
from machines
where license_id in (
    select id from licenses where license_key = 'FULL-TEST-001'
)
order by id
""").fetchall()

for r in rows:
    print(dict(r))