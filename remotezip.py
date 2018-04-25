import requests
import io
import zipfile


class OutOfBound(Exception):
    pass


class NegativeSeek(io.UnsupportedOperation):
    pass


def itercouples(objects):
    it = iter(objects)
    try:
        prev = next(it)
    except StopIteration:
        return
    for l in it:
        yield prev, l
        prev = l
    yield prev, None


class PartialBuffer:
    def __init__(self, buffer, offset, size, stream):
        self.buffer = buffer
        self.offset = offset
        self.size = size
        self.position = offset
        self.position_oob = False
        self.stream = stream

    def __str__(self):
        return "<PartialBuffer off=%s size=%s stream=%s>" % (self.offset, self.size, self.stream)

    __repr__ = __str__

    def read(self, size=0):
        if size == 0:
            size = self.offset + self.size - self.position

        content = self.buffer.read(size)
        self.position = self.offset + self.buffer.tell()
        return content

    def close(self):
        if not self.buffer.closed:
            self.buffer.close()
            if hasattr(self.buffer, 'release_conn'):
                self.buffer.release_conn()

    def seek(self, offset, whence):
        if whence == 2:
            self.position = self.size + self.offset + offset
        elif whence == 0:
            self.position = offset
        else:
            self.position += offset

        relative_position = self.position - self.offset

        if relative_position < 0 or relative_position >= self.size:
            self.position_oob = True
            raise OutOfBound("Position out of buffer bound")

        self.position_oob = False
        if self.stream:
            buff_pos = self.buffer.tell()
            if relative_position < buff_pos:
                self.position_oob = True
                raise NegativeSeek("Negative seek not supported")

            skip_bytes = relative_position - buff_pos
            if skip_bytes == 0:
                return self.position
            self.buffer.read(skip_bytes)
        else:
            self.buffer.seek(relative_position)

        return self.position


class RemoteIO(io.IOBase):
    def __init__(self, fetch_fun, initial_buffer_size=64*1024):
        self.fetch_fun = fetch_fun
        self.initial_buffer_size = initial_buffer_size
        self.buffer = None
        self.file_size = None
        self.position = None
        self.member_pos2size = None

    def set_pos2size(self, pos2size):
        self.member_pos2size = pos2size

    def read(self, size=0):
        if size == 0:
            size = self.file_size - self.buffer.position

        if self.buffer.position_oob:
            if self.member_pos2size is None:
                fetch_size = size
                stream = False
            else:
                fetch_size = self.member_pos2size[self.buffer.position]
                if fetch_size is None:
                    fetch_size = self.file_size - self.buffer.position
                stream = True

            self.buffer.close()
            self.buffer = self.fetch_fun((self.buffer.position, self.buffer.position + fetch_size -1), stream=stream)

        content = self.buffer.read(size)
        return content

    def seek(self, offset, whence=0):
        if whence == 2 and self.file_size is None:
            size = self.initial_buffer_size
            self.buffer = self.fetch_fun((-size, None), stream=False)
            self.file_size = self.buffer.size + self.buffer.position

        try:
            return self.buffer.seek(offset, whence)
        except (OutOfBound, NegativeSeek):
            return self.buffer.position   # we ignore the issue here, we will check if buffer is fine during read

    def tell(self):
        return self.buffer.position

    def close(self):
        if self.buffer:
            self.buffer.close()
            self.buffer = None


class RemoteZip(zipfile.ZipFile):
    def __init__(self, url, initial_buffer_size=64*1024, **kwargs):
        self.kwargs = kwargs
        self.url = url

        rio = RemoteIO(self.fetch_fun, initial_buffer_size)
        super(RemoteZip, self).__init__(rio)
        rio.set_pos2size(self.get_position2size())

    def get_position2size(self):
        position2size = {}
        for m1, m2 in itercouples(self.infolist()):
            size = None if m2 is None else m2.header_offset - m1.header_offset
            position2size[m1.header_offset] = size
        return position2size

    @staticmethod
    def make_buffer(fd_or_bytes, content_range_header, stream):
        range_min, range_max = content_range_header.split("/")[0][6:].split("-")
        range_min, range_max = int(range_min), int(range_max)
        io_buffer = fd_or_bytes if stream else io.BytesIO(fd_or_bytes)
        return PartialBuffer(io_buffer, range_min, range_max - range_min + 1, stream)

    @staticmethod
    def make_header(range_min, range_max):
        if range_max is None:
            if range_min < 0:
                return "bytes=%s" % range_min
            else:
                return "bytes=%s-" % range_min

        return "bytes=%s-%s" % (range_min, range_max)

    def fetch_fun(self, data_range, stream=False):
        range_header = self.make_header(*data_range)
        kwargs = dict(self.kwargs)
        kwargs.update({'stream': stream})
        kwargs['headers'] = headers = dict(kwargs.get('headers', {}))
        headers['Range'] = range_header
        try:
            res = requests.get(self.url, **kwargs)
            res.raise_for_status()
        except Exception as e:
            print(e)
        pb = self.make_buffer(res.raw if stream else res.content, res.headers['Content-Range'], stream=stream)
        print(pb)
        return pb


