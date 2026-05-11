import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BUILD = os.path.join(os.path.dirname(HERE), "build")
sys.path.insert(0, BUILD)

import seekdb_pyclient as sc

if not os.environ.get("SEEKDB_BIN"):
    sys.exit("Set SEEKDB_BIN=/path/to/seekdb-server-binary")
db_dir = os.environ.get("SEEKDB_DB", "/tmp/seekdb_python_test_data")
os.makedirs(db_dir, exist_ok=True)

# Same shape as ob_embed_impl: module-level open + connect, then cursor/execute/fetch.
print(f"sc.open(db_dir={db_dir})")
sc.open(db_dir=db_dir)

print("sc.connect(database='test', autocommit=True)")
conn = sc.connect(database="test", autocommit=True)

cur = conn.cursor()
print('cur.execute("SELECT 1")')
n = cur.execute("SELECT 1")
print(f"  row count returned: {n}")

print("cur.fetchall():")
for row in cur.fetchall():
    print(f"  {row}")

print("OK")
