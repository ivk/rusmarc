import io

from .rusmarc import Rusmarc, MalformedRecord


class RusmarcFileIterator(object):
    def __init__(self, filename, encoding=None):
        self.__enc = encoding
        self.__f = io.open(filename, "rb")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__f.close()

    def __iter__(self):
        return self

    def next(self):
        rec_len_b = self.__f.read(5)
        if rec_len_b:
            try:
                rec_len = int(rec_len_b)
            except ValueError:
                raise MalformedRecord
            rec = self.__f.read(rec_len - 5)
            assert isinstance(rec, str)
            if rec[-1] == Rusmarc.IS1:
                return Rusmarc(rec_len_b + rec, encoding=self.__enc)
            else:
                raise MalformedRecord
        else:
            raise StopIteration()


class MarcFileIterator(object):
    def __init__(self, filename, encoding=None):
        self.__enc = encoding
        self.__f = io.open(filename, "rb")

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
                raise MalformedRecord
            rec = self.__f.read(rec_len - 5)
            assert isinstance(rec, bytes)
            if rec[-1] == Rusmarc.IS1:
                return rec_len_b + rec
            else:
                raise MalformedRecord
        else:
            raise StopIteration()
