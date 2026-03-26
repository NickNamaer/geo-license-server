import sqlite3

conn = sqlite3.connect("data/licenses.db")
conn.row_factory = sqlite3.Row

rows = conn.execute("""
select
    id,
    admin_username,
    action,
    target_type,
    target_key,
    detail_json,
    created_at
from admin_audit_logs
order by id desc
limit 50
""").fetchall()

print("=== admin_audit_logs ===")
for r in rows:
    print(dict(r))