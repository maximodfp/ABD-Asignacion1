# falta adaptar el codigo a la pregunta 1
# repositorio consultado >> https://gist.github.com/jarrettmeyer/26b3e1fcd423071a7a6d

import csv
import happybase
import time

batch_size = 17416
host = "0.0.0.0"
file_path = "vgsales.csv"
namespace = "sample_data"
row_count = 0
start_time = time.time()
table_name = "rfic"

def connect_to_hbase():
    conn = happybase.Connection(host = host,
        table_prefix = namespace,
        table_prefix_separator = ":")
conn.open()
table = conn.table(table_name)
batch = table.batch(batch_size = batch_size)
return conn, batch

def insert_row(batch, row):
    batch.put(row[0], {
        "data:kw": row[1],
        "data:sub": row[2],
        "data:type": row[3],
        "data:town": row[4],
        "data:city": row[5],
        "data:zip": row[6],
        "data:cdist": row[7],
        "data:open": row[8],
        "data:close": row[9],
        "data:status": row[10],
        "data:origin": row[11],
        "data:loc": row[12]
    })

def read_csv():
    csvfile = open(file_path, "r")
csvreader = csv.reader(csvfile)
return csvreader, csvfile

conn, batch = connect_to_hbase()
print "Connect to HBase. table name: %s, batch size: %i" % (table_name, batch_size)
csvreader, csvfile = read_csv()
print "Connected to file. name: %s" % (file_path)

try:
for row in csvreader:
    row_count += 1
if row_count == 1:
    pass
else :
    insert_row(batch, row)
batch.send()
finally:
csvfile.close()
conn.close()

duration = time.time() - start_time
print "Done. row count: %i, duration: %.3f s" % (row_count, duration)