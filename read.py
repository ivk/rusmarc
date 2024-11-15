#!/bin/env python3
import argparse
import os
import sys
import sqlite3
from array import array
from email.policy import default
from fileinput import filename
from math import trunc
from random import choices

import rusmarc.rusmarc
import rusmarc.rusmarc as RM
import rusmarc.rusmarc_iter as RMI


def create_table(connection, tblname, fields):
    drop_sql = f'drop table if exists {tblname}'
    sql = f'''
            CREATE TABLE IF NOT EXISTS {tblname} (
            id INTEGER PRIMARY KEY
        '''
    for field in fields:
        sql += f', field{field} TEXT'
    sql += ')'

    connection.cursor().execute(drop_sql)
    connection.cursor().execute(sql)
    connection.commit()

    return sql


def write_table(connection, tblname, record):
    sql = f"insert into {tblname} "
    fields = []
    values = []
    for k in record:
        # if k in (856, 953, 10, 330, 333, 205, 215, 675):
        if k in (953, 10, 330, 333, 205, 215, 675):
            try:
                val = record[k][0]['sf'][0][1]
            except KeyError:
                val = None
                print(k, record[k])
        else:
            if len(record[k]) == 1:
                val = record[k][0]
            else:
                val = record[k][0]['sf']
        fields.append(f'field{k}')
        values.append(f"{val}")
    sql += f"({','.join(fields)}) values ({','.join('?'*len(values))})"
    connection.cursor().execute(sql, values)
    return sql


def read(filename, encoding='UTF-8'):
    count = 0
    err_count = 0
    records = []
    fields = []
    with RMI.MarcFileIterator(filename, encoding) as it:
        for mrc in it:
            try:
                r = RM.Rusmarc(mrc, encoding).fields
                records.append(r)
                fields.extend(list(r.keys()))

                count += 1

                if args.save_file:
                    if count == 1:
                        print("new iso file created")
                    write_good_to_file(mrc)

                # if count == 1 and args.save_bad_records:
                #     # write_corrupted_to_file("good sample:")/
                #     write_corrupted_to_file(mrc)

            except rusmarc.rusmarc.MalformedRecord as mr:
                err_count += 1

                if args.save_bad_records:
                    write_corrupted_to_file(mrc)

    print("Total: %s recs, %s errors" % (count, err_count) )

    return records, set(fields)


def write_corrupted_to_file(data):
    with open(log_file, "ab") as f:
        f.write(data)
        # f.write('\n')


def write_good_to_file(data):
    with open(modified_iso_file, "ab") as f:
        f.write(data)


def write_to_db(fields, file_name, records):
    if len(records):
        connection = sqlite3.connect('db/rusmarc.db')
        tblname = file_name[0:-4]
        create_table(connection, tblname, fields)

        for r in records:
            write_table(connection, tblname, r)
        connection.commit()


def main():
    if args.save_bad_records:
        if os.path.exists(log_file):
            os.remove(log_file)

    if args.save_file:
        if os.path.exists(modified_iso_file):
            os.remove(modified_iso_file)

    records, fields = read(args.filename)

    if args.save_db:
        file_name = os.path.basename(args.filename)
        write_to_db(fields, file_name, records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Marcfiles parser')
    parser.add_argument('filename')
    parser.add_argument('-d', '--save_db',
                        action='store_true',
                        help='Save results into db?')
    parser.add_argument('-f', "--save_file",
                        required=False,
                        action='store_true',
                        help='Save good records into a file?')
    parser.add_argument('-b', "--save_bad_records",
                        required=False,
                        action='store_true',
                        help='Save corrupted records into a file?')

    args = parser.parse_args()
    modified_iso_file = 'marcfiles/newmarc.iso'
    log_file = "corrupted.log"

    main()