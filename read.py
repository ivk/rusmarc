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
from pathlib import Path

import rusmarc.rusmarc
import rusmarc.rusmarc as RM
import rusmarc.rusmarc_iter as RMI


standard_fields = {
    1: "Название",
    10: "ISBN",
    11: "ISSN",
    100: "Данные общей обработки",
    101: "Язык ресурса",
    102: "Страна публикации или производства",
    105: "Поле кодированных данных: текстовые ресурсы, монографические",
    106: "Поле кодированных данных: текстовые ресурсы – форма",
    200: "Заглавие",
    205: "Сведения об издании",
    210: "Публикация, производство, распространение и т.д",
    215: "Физические характеристики",
    300: "Общие примечания",
    330: "Резюме или реферат",
    333: "Примечания об особенностях распространения и использования",
    461: "УРОВЕНЬ НАБОРА ",
    610: "Ключевые слова",
    675: "УДК",
    686: "Индексы других классификаций",
    700: "Автор",
    701: "Соавтор",
    801: "Источник записи",
    856: "Местонахождение электронных ресурсов и доступ к ним",
    # 953: "хз"
}


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
        if len(record[k]) == 1:
            tmp = record[k][0]
            if type(tmp) == dict:
                try:
                    val = ', '. join(item[1] for item in tmp['sf'])
                except Exception as e:
                    val = tmp['sf']

            else:
                val = record[k][0]
        else:
            try:
                tmp = [item[1] for d in record[k] for item in d['sf']]
                val =  ', '.join(tmp)
            except Exception as e:
                val = tmp
                # val = tmp['sf']

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
    sample = {}
    with RMI.MarcFileIterator(filename, encoding) as it:
        for mrc in it:
            if mrc is None:
                print(f"Error happened: {it.last_error}")
                err_count += 1
                continue
            try:
                r = RM.Rusmarc(mrc, encoding).fields
                records.append(r)
                fields.extend(list(r.keys()))

                if not count:
                    for f in fields:
                        sample[f] = r[f]

                count += 1

                if args.save_file:
                    if count == 1:
                        print("new iso file created")
                    write_good_to_file(mrc)

                # if count == 1 and args.save_bad_records:
                #     # write_corrupted_to_file("good sample:")/
                #     write_corrupted_to_file(mrc)

            except rusmarc.rusmarc.MalformedRecord as mr:
                print(mr)
                write_log(str(mr))

                err_count += 1

                if args.save_bad_records:
                    write_corrupted_to_file(mrc)

    return records, set(fields), sample, err_count


def write_log(data):
    with open(log_file, "a") as f:
        f.write(data)
        f.write('\n')


def write_corrupted_to_file(data):
    with open(corrupted_iso_file, "ab") as f:
        f.write(data)


def write_good_to_file(data):
    with open(modified_iso_file, "ab") as f:
        f.write(data)


def write_to_db(fields, file_name, records):
    if len(records):
        connection = sqlite3.connect('db/rusmarc.db')
        if len(file_name) > 4 and '.' in file_name:
            tblname = file_name[0:-4]
        else:
            tblname = file_name

        create_table(connection, tblname, fields)

        for r in records:
            write_table(connection, tblname, r)
        connection.commit()


def main():
    if args.save_bad_records:
        if os.path.exists(corrupted_iso_file):
            os.remove(corrupted_iso_file)

    if args.save_file:
        if os.path.exists(modified_iso_file):
            os.remove(modified_iso_file)

    if os.path.exists(log_file):
        os.remove(log_file)

    err_count = 0
    if not args.is_dir:
        records, fields, sample, err_count = read(args.filename, args.encoding)
        recs_count = len(records)
    else:
        records = []
        fields = set()
        files = [os.path.join(args.filename, f) for f in os.listdir(args.filename) if
                 os.path.isfile(os.path.join(args.filename, f))]
        print(len(files))

        for f in files:
            rec, rec_fields, sample, errs = read(f, args.encoding)

            records.extend(rec)
            fields.update(rec_fields)
            err_count += errs
            recs_count = len(records)

    print("Total: %s recs, %s errors" % (recs_count, err_count) )

    print(f"Fields are: {sorted(fields)}")
    for f in sorted(fields):
        if f in standard_fields.keys():
            field_name = standard_fields[f]
        else:
            field_name = "unknown field"
        if f in sample:
            sample_data = f"{str(sample[f])[:60]}..."
        else:
            sample_data = "no sample"
        print(f"{f} -> {field_name} -> {sample_data}...")

    # print(sample)


    if args.save_db:
        file_name = os.path.basename(os.path.normpath(args.filename))
        write_to_db(fields, file_name, records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Marcfiles parser')
    parser.add_argument('filename')

    parser.add_argument('-enc', '--encoding', default='UTF-8', choices=['UTF-8', 'cp1251', 'cp866'])
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

    if os.path.isdir(args.filename):
        print('It is a directory')
        args.save_file = False
        args.save_bad_records = False
        args.is_dir = True
    else:
        args.is_dir = False
        modified_iso_file = 'marcfiles/newmarc.iso'
        corrupted_iso_file = 'marcfiles/corrupted.iso'

    log_file = "corrupted.log"

    main()