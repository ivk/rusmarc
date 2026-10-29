import argparse
import rusmarc.rusmarc
import rusmarc.rusmarc as RM
import rusmarc.rusmarc_iter as RMI


def read(filename, encoding='UTF-8'):
    records = []
    with RMI.MarcFileIterator(filename, encoding) as it:
        for mrc in it:
            if mrc is None:
                print(f"Error happened: {it.last_error}")
                continue
            try:
                r = RM.Rusmarc(mrc, encoding).fields
                records.append(r)

            except rusmarc.rusmarc.MalformedRecord as mr:
                print(mr)

    return records


def main():
    records = read(args.filename)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convertor^ from marc21 to rusmarc')
    parser.add_argument('filename')
    args = parser.parse_args()
    main()