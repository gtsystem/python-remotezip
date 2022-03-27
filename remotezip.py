import io
import zipfile

import requests

__all__ = ['RemoteIOError', 'RemoteZip']

class RemoteZipError(Exception):
    pass

class OutOfBound(RemoteZipError):
    pass


class RemoteIOError(RemoteZipError):
    pass


class RangeNotSupported(RemoteZipError):
    pass


class PartialBuffer:
    def __init__(self, buffer, offset, size, stream):
        self.buffer = buffer if stream else io.BytesIO(buffer.read())
        self.offset = offset
        self.size = size
        self.position = offset
        self.stream = stream

    def __repr__(self):
        return "<PartialBuffer off=%s size=%s stream=%s>" % (self.offset, self.size, self.stream)

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
            raise OutOfBound("Position out of buffer bound")

        if self.stream:
            buff_pos = self.buffer.tell()
            if relative_position < buff_pos:
                raise OutOfBound("Negative seek not supported")

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
        self._seek_succeeded = False
        self.member_pos2size = None
        self._last_member_pos = None

    def set_pos2size(self, pos2size):
        self.member_pos2size = pos2size

    def read(self, size=0):
        if size == 0:
            size = self.file_size - self.buffer.position

        if not self._seek_succeeded:
            if self.member_pos2size is None:
                fetch_size = size
                stream = False
            else:
                try:
                    fetch_size = self.member_pos2size[self.buffer.position]
                    self._last_member_pos = self.buffer.position
                except KeyError:
                    if self._last_member_pos and self._last_member_pos < self.buffer.position:
                        fetch_size = self.member_pos2size[self._last_member_pos]
                        fetch_size -= (self.buffer.position - self._last_member_pos)
                    else:
                        raise OutOfBound("Attempt to seek outside boundary of current zip member")
                stream = True

            self._seek_succeeded = True
            self.buffer.close()
            self.buffer = self.fetch_fun((self.buffer.position, self.buffer.position + fetch_size -1), stream=stream)

        return self.buffer.read(size)

    def seekable(self):
        return True

    def seek(self, offset, whence=0):
        if whence == 2 and self.file_size is None:
            size = self.initial_buffer_size
            self.buffer = self.fetch_fun((-size, None), stream=False)
            self.file_size = self.buffer.size + self.buffer.position

        try:
            pos = self.buffer.seek(offset, whence)
            self._seek_succeeded = True
            return pos
        except OutOfBound:
            self._seek_succeeded = False
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
        ilist = self.infolist()
        if len(ilist) == 0:
            return {}

        position2size = {ilist[-1].header_offset: self.start_dir - ilist[-1].header_offset}
        for i in range(len(ilist) - 1):
            m1, m2 = ilist[i: i+2]
            position2size[m1.header_offset] = m2.header_offset - m1.header_offset

        return position2size

    @staticmethod
    def make_buffer(io_buffer, content_range_header, stream):
        range_min, range_max = content_range_header.split("/")[0][6:].split("-")
        range_min, range_max = int(range_min), int(range_max)
        return PartialBuffer(io_buffer, range_min, range_max - range_min + 1, stream)

    @staticmethod
    def make_header(range_min, range_max):
        if range_max is None:
            return "bytes=%s%s" % (range_min, '' if range_min < 0 else '-')
        return "bytes=%s-%s" % (range_min, range_max)

    @staticmethod
    def request(url, range_header, kwargs):
        kwargs['headers'] = headers = dict(kwargs.get('headers', {}))
        headers['Range'] = range_header
        res = requests.get(url, stream=True, **kwargs)
        res.raise_for_status()
        if 'Content-Range' not in res.headers:
            raise RangeNotSupported("The server doesn't support range requests")
        return res.raw, res.headers

    def fetch_fun(self, data_range, stream=False):
        range_header = self.make_header(*data_range)
        kwargs = dict(self.kwargs)
        try:
            res, headers = self.request(self.url, range_header, kwargs)
            return self.make_buffer(res, headers['Content-Range'], stream=stream)
        except IOError as e:
            raise RemoteIOError(str(e))
