#!/usr/bin/env python3
"""
export_csv.py — Export all tables from the sdeyaml MySQL database to
Excel-compatible CSV files (UTF-8 BOM, comma-separated, all fields quoted).

Usage:
    python export_csv.py [output_dir]

    output_dir: defaults to ./csv_export
"""

import csv
import os
import sys
import configparser
from sqlalchemy import create_engine, inspect, text

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
INI_FILE   = os.path.join(SCRIPT_DIR, 'sdeloader.cfg')

output_dir = sys.argv[1] if len(sys.argv) >= 2 else os.path.join(SCRIPT_DIR, 'csv_export')
os.makedirs(output_dir, exist_ok=True)

config = configparser.ConfigParser()
config.read(INI_FILE)
connection_string = config.get('Database', 'mysql')

print("Connecting to MySQL (sdeyaml)...")
engine = create_engine(connection_string)
insp   = inspect(engine)
tables = insp.get_table_names()
print("Found {} tables".format(len(tables)))

with engine.connect() as conn:
    for table in sorted(tables):
        outfile = os.path.join(output_dir, '{}.csv'.format(table))
        try:
            result = conn.execute(text('SELECT * FROM `{}`'.format(table)))
            cols   = list(result.keys())
            rows   = result.fetchall()
        except Exception as e:
            print("  SKIP {} — {}".format(table, e))
            continue

        with open(outfile, 'w', newline='', encoding='utf-8-sig') as fh:
            writer = csv.writer(fh, dialect='excel', quoting=csv.QUOTE_ALL)
            writer.writerow(cols)
            writer.writerows(rows)

        print("  {:40s} {:>8} rows".format(table, len(rows)))

print("\nDone. Files written to: {}".format(output_dir))
