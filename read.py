#!/bin/env python3

import sys

import rusmarc.rusmarc as RM
import rusmarc.rusmarc_iter as RMI

def read(filename):
    count = 0
    with RMI.MarcFileIterator(filename) as it:
        for mrc in it:
            r = RM.Rusmarc(mrc, encoding='UTF-8')
            count += 1
    print("Total: %s recs" % count)

    with open(filename, "rb") as fp:
        data = fp.read()
        rec = RM.Rusmarc(data, encoding='UTF-8')
        rec_t = rec.serialize(encoding='UTF-8')
        print("Read done")

def usage():
    print("Usage: read <filename>")
    return 1


def main():
    if len(sys.argv) != 2:
        sys.exit(usage())

    return read(sys.argv[1])

if __name__ == "__main__":
    main()