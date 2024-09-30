from __future__ import unicode_literals
class MalformedRecord(Exception):
    pass


class Rusmarc(object):
    IS3 = '\x1f'# Begin of subfield
    IS2 = '\x1e'# End of field
    IS1 = 0x1D  # End of record
    bIS3 = b'\x1F'  # Begin of subfield
    bIS2 = b'\x1E'  # End of field
    bIS1 = b'\x1D'  # End of record

    def __init__(self, bytestr=None, encoding='utf-8'):
        self.fields = {}
        self.status = b'n'       # default = new record
        self.type = b'a'         # default = text (excl. manuscripts)
        self.bib_level = b'm'    # default = monograph
        self.hier_level = b' '   # default = not defined
        self.control = b' '      # default = not defined
        self.coding_level = b'1' # default = sublevel 1
        self.isbd = b' '         # default = record conforms ISBD rules
        if bytestr:
            self.deserialize(bytestr, encoding)

    def deserialize(self, record_str, encoding='utf-8'):
        assert isinstance(record_str, bytes)
        header = record_str[:24]
        try:
            data_start = int(header[12:17])
        except ValueError:
            raise MalformedRecord()
        self.status = record_str[5]
        self.type = record_str[6]
        self.bib_level = record_str[7]
        self.hier_level = record_str[8]
        self.control = record_str[9]
        self.coding_level = record_str[17]
        self.isbd = record_str[18]
        dictionary = record_str[24:data_start]
        data = record_str[data_start:]
        self.__validate(header, dictionary, data)
        self.__get_raw_fields(dictionary, data, encoding)
        self.__parse_raw_fields()

    @classmethod
    def __validate(cls, header, dictionary, data, encoding='utf-8'):
        try:
            int(header[:5])
        except ValueError:
            raise MalformedRecord()
        if (
                # header[20:] != b'450 ' or
                header[10:12] != b'22' or
                data[-1:] != cls.bIS1 or
                dictionary[-1:] != cls.bIS2):
            raise MalformedRecord()

    def __get_raw_fields(self, dictionary, data, encoding):
        i = 0
        dl = len(dictionary)
        while i < dl - 1:
            try:
                fno = int(dictionary[i:i+3])
                flen = int(dictionary[i+3:i+7])
                fstart = int(dictionary[i+7:i+12])
                fval = data[fstart:fstart+flen]
                self.add_field(fno, fval.decode(encoding))
                i += 12
            except ValueError:
                raise MalformedRecord()

    def __parse_raw_fields(self):
        for fno, fval_list in self.fields.items():
            parsed_val_lst = []
            for fval in fval_list:
                if fval[-1:] != self.IS2:
                    raise MalformedRecord()
                parsed_val_lst.append(self.__parse_raw_field(fno, fval[:-1]))
            self.fields[fno] = parsed_val_lst

    def __parse_raw_field(self, fno, fval):
        if fno >= 10:
            val = {
                'i1': fval[0],
                'i2': fval[1],
                'sf': self.__parse_raw_subfields(fval[2:])
            }
        else:
            val = fval
        return val

    def __parse_raw_subfields(self, sfval):
        if sfval == '':
            return {}
        if sfval[1] == '1':
            sfields = self.__parse_emb_fields(sfval)
        else:
            sfields = []
            sfield_list = sfval.split(self.IS3)[1:]
            for sf in sfield_list:
                sfname = sf[0]
                sfval = sf[1:]
                sfields.append((sfname, sfval))
        return sfields

    def __parse_emb_fields(self, fval):
        field_list = fval.split(self.IS3 + '1')[1:]
        val = []
        for field in field_list:
            try:
                fno = int(field[0:3])
            except ValueError:
                raise MalformedRecord()
            fval = self.__parse_raw_field(fno, field[3:])
            val.append(('1', {fno: [fval]}))
        return val

    def add_field(self, fno, fval):
        if fno in self.fields:
            self.fields[fno].append(fval)
        else:
            self.fields[fno] = [fval]

    def serialize(self, encoding='utf-8'):
        dic = []
        data = []
        start = 0
        for fno in sorted(self.fields.keys()):
            packed_flds = self.__pack_field(fno, self.fields[fno], 'marc')
            for p in packed_flds:
                p = p.encode(encoding)
                l = len(p)
                dic.append(b"%03d%04d%05d" % (fno, l, start))
                data.append(p)
                start += l
        dic.append(self.bIS2)
        data.append(self.bIS1)
        dic = b"".join(dic)
        data = b"".join(data)
        header = b"".join(
            (b"%05d" % (24 + len(dic) + len(data)),
            bytes(self.status), bytes(self.type), bytes(self.bib_level), bytes(self.hier_level),
            bytes(self.control), b'22', b"%05d" % (24 + len(dic)),
            bytes(self.coding_level), bytes(self.isbd), b' 450 '))
        return b"".join((header, dic, data))

    def __pack_field(self, fno, fval, delimiter_type):
        delim = self.IS2
        if delimiter_type == 'txt':
            delim = ''
        val_list = []
        if fno >= 10:
            for f in fval:
                val = "".join((
                    f['i1'], f['i2'],
                    self.__pack_subfields(f['sf'], delimiter_type),
                    delim))
                val_list.append(val)
        else:
            for f in fval:
                val = f + delim
                val_list.append(val)
        return val_list

    def __pack_emb_field(self, fno, fval, delimiter_type):
        val_list = []
        if fno >= 10:
            for f in fval:
                val = "".join((
                    f['i1'], f['i2'],
                    self.__pack_subfields(f['sf'], delimiter_type)))
                val_list.append(val)
        else:
            for f in fval:
                val_list.append(f)
        return val_list

    def __pack_subfields(self, sf_arr, delimiter_type):
        """
        :param sf_arr: array
        :param delimiter_type: 'prim', 'txt'
        :return: array
        """
        sf_prefix = self.IS3
        if delimiter_type == 'txt':
            sf_prefix = '$'
        res = []
        for sfn, sfv in sf_arr:
            if sfn == '1':
                for embf_no in sorted(sfv.keys()):
                    packed_flds = self.__pack_emb_field(
                        embf_no, sfv[embf_no], delimiter_type)
                    for p in packed_flds:
                        res.append(
                            "".join((sf_prefix, sfn, "%03d" % embf_no, p)))
            else:
                res.append("".join((sf_prefix, sfn, sfv)))
        return "".join(res)

    def serialize_marc_txt(self, encoding='utf-8'):
        res = "marker: " + self.serialize(encoding)[:24] + '\n'
        for fno in sorted(self.fields.keys()):
            packed_flds = self.__pack_field(
                fno, self.fields[fno], "txt")
            for p in packed_flds:
                res = "".join((res, "%03d: " % fno, p, "\n"))
        return res
