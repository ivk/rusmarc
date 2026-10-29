import io
import os
from itertools import count

from .rusmarc import Rusmarc, MalformedRecord


class RusmarcFileIterator(object):
    def __init__(self, filename, encoding=None):
        self.__enc = encoding
        self.__f = open(filename, "rb")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__f.close()

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        rec_len_b = self.__f.read(5)
        if rec_len_b:
            try:
                rec_len = int(rec_len_b)
            except ValueError:
                raise MalformedRecord(f"wrong value {rec_len_b}")
            rec = self.__f.read(rec_len - 5)
            assert isinstance(rec, str)
            if rec[-1] == Rusmarc.IS1:
                return Rusmarc(rec_len_b + rec, encoding=self.__enc)
            else:
                raise MalformedRecord(f"{rec} problems")
        else:
            raise StopIteration()


class MarcFileIterator(object):
    def __init__(self, filename, encoding=None):
        self.__enc = encoding
        self.pos = 0
        self.__f = open(filename, "rb")
        self.last_error = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__f.close()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.next()
        except StopIteration:
            raise
        except Exception as ex:
            print(ex)
            return None

    def next(self, encoding='UTF-8'):
        self.last_error = None
        if self.pos:
            # bad record, Try to rewind it
            self.__f.seek(self.pos, os.SEEK_SET)

            # read by bytes and search for Rusmarc.IS1
            count = 0
            while True:
                data = self.__f.read(1)
                if data == '':
                    raise StopIteration()
                elif data[0] == Rusmarc.IS1:
                    break
                count += 1
            self.pos = 0
            print(f"Skipped: {count} bytes. Need to create a new file, use option -f when start processing.")

        start = self.__f.tell()
        rec_len_b = self.__f.read(5)

        if rec_len_b:
            try:
                rec_len = int(rec_len_b)
                assert rec_len > 5
                rec_len -= 5
            except ValueError as err:
                self.pos = self.__f.tell()
                self.last_error = MalformedRecord(f"{rec_len_b} must be integer at position {start}")
                # raise MalformedRecord(f"{rec_len_b} must be integer at position {start}")
                return None

            rec = self.__f.read(rec_len)
            assert isinstance(rec, bytes)
            assert len(rec) > 5 and len(rec) == rec_len
            if rec[-1] == Rusmarc.IS1:
                return rec_len_b + rec
            else:
                self.pos = self.__f.tell()
                self.last_error = MalformedRecord(f"field borders are wrong {rec.decode(encoding)}")
                return None
        else:
            raise StopIteration()
