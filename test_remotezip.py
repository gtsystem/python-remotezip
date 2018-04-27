import unittest
import zipfile
import tempfile
import os
import shutil
import io

import remotezip as rz


class TmpDir(object):
    """Create a tmp directory that is automatically destroyed when the context exit"""
    def __enter__(self):
        self.tmpdir = tempfile.mktemp()
        os.mkdir(self.tmpdir)
        return self.tmpdir

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.tmpdir)


class LocalRemoteZip(rz.RemoteZip):
    def fetch_fun(self, data_range, stream=False):
        with open(self.url, 'rb') as f:
            f.seek(0, 2)
            fsize = f.tell()

            range_min, range_max = data_range
            if range_min < 0:
                range_max = fsize - 1
                range_min = max(fsize + range_min, 0)
            elif range_max is None:
                range_max = fsize - 1

            content_range="bytes {range_min}-{range_max}/{fsize}".format(**locals())
            f.seek(range_min, 0)

            f = io.BytesIO(f.read(range_max - range_min + 1))
            buff = self.make_buffer(f, content_range, stream=stream)
        return buff


class TestPartialBuffer(unittest.TestCase):
    def verify(self, stream):
        pb = rz.PartialBuffer(io.BytesIO(b'aaaabbcccdd'), 10, 11, stream=stream)
        self.assertEqual(pb.position, 10)
        self.assertEqual(pb.size, 11)
        self.assertEqual(pb.read(5), b'aaaab')
        self.assertEqual(pb.read(3), b'bcc')
        self.assertEqual(pb.read(3), b'cdd')
        pb.close()

    def test_static(self):
        self.verify(stream=False)

    def test_static_seek(self):
        pb = rz.PartialBuffer(io.BytesIO(b'aaaabbcccdd'), 10, 11, stream=False)
        self.assertEqual(pb.seek(10, 0), 10)
        self.assertEqual(pb.read(5), b'aaaab')
        self.assertEqual(pb.seek(12, 0), 12)
        self.assertEqual(pb.read(5), b'aabbc')
        self.assertEqual(pb.seek(20, 0), 20)
        self.assertEqual(pb.read(1), b'd')
        self.assertEqual(pb.seek(10, 0), 10)
        self.assertEqual(pb.seek(2, 1), 12)

    def test_static_read_no_size(self):
        pb = rz.PartialBuffer(io.BytesIO(b'aaaabbcccdd'), 10, 11, stream=False)
        self.assertEqual(pb.read(), b'aaaabbcccdd')
        self.assertEqual(pb.position, 21)
        self.assertEqual(pb.seek(15, 0), 15)
        self.assertEqual(pb.read(), b'bcccdd')
        self.assertEqual(pb.seek(-5, 2), 16)
        self.assertEqual(pb.read(), b'cccdd')
        self.assertEqual(pb.read(), b'')

    def test_static_oob(self):
        pb = rz.PartialBuffer(io.BytesIO(b'aaaabbcccdd'), 10, 11, stream=False)
        with self.assertRaises(rz.OutOfBound):
            pb.seek(21, 0)

    def test_stream(self):
        self.verify(stream=True)

    def test_stream_forward_seek(self):
        pb = rz.PartialBuffer(io.BytesIO(b'aaaabbcccdd'), 10, 11, stream=True)
        self.assertEqual(pb.seek(12, 0), 12)
        self.assertEqual(pb.read(3), b'aab')
        self.assertEqual(pb.seek(2, 1), 17)
        self.assertEqual(pb.read(), b'ccdd')

        with self.assertRaisesRegexp(rz.OutOfBound, "Negative seek not supported"):
            pb.seek(12, 0)
        self.assertEqual(pb.position, 12)


class TestRemoteIO(unittest.TestCase):
    def fetch_fun(self, data_range, stream=False):
        # simulate 200k file
        fsize = 200 * 1024
        min_range, max_range = data_range
        if min_range < 0:
            size = -min_range
            min_range = fsize - size
        else:
            size = max_range - min_range + 1

        if stream:
            data = b's' * size
        else:
            data = b'x' * size
        return rz.PartialBuffer(io.BytesIO(data), min_range, size, stream=stream)

    def test_simple(self):
        rio = rz.RemoteIO(fetch_fun=self.fetch_fun)
        self.assertIsNone(rio.file_size)
        rio.seek(0, 2)  # eof
        self.assertIsNotNone(rio.file_size)
        self.assertEqual(rio.tell(), 200 * 1024)

        curr_buffer = rio.buffer

        rio.seek(-20, 2)
        self.assertEqual(rio.read(2), b'xx')
        self.assertIs(rio.buffer, curr_buffer)  # buffer didn't change
        self.assertEqual(rio.read(), b'x' * 18)
        self.assertEqual(rio.tell(), 200 * 1024)

        rio.seek(120*1024, 0)
        self.assertEqual(rio.read(2), b'xx')
        self.assertEqual(rio.buffer.size, 2)
        self.assertIsNot(rio.buffer, curr_buffer)  # buffer changed
        rio.close()

    def test_file_access(self):
        rio = rz.RemoteIO(fetch_fun=self.fetch_fun)
        rio.seek(0, 2)  # eof
        curr_buffer = rio.buffer
        # we have two file, one at pos 156879 with size 30k and the last at pos
        rio.set_pos2size({15687: 30*1024, 50354: None})
        rio.seek(15687, 0)
        self.assertEqual(rio.tell(), 15687)
        self.assertEqual(rio.read(5), b'sssss')
        self.assertIsNot(rio.buffer, curr_buffer)  # buffer changed
        curr_buffer = rio.buffer

        # re-read the same file
        rio.seek(15687, 0)
        self.assertEqual(rio.read(4), b'ssss')
        self.assertEqual(rio.buffer.size, 30*1024)
        self.assertIsNot(rio.buffer, curr_buffer)  # buffer changed
        curr_buffer = rio.buffer

        # move to next file
        rio.seek(50354, 0)
        self.assertEqual(rio.read(4), b'ssss')
        self.assertEqual(rio.buffer.size, 154446)
        self.assertIsNot(rio.buffer, curr_buffer)  # buffer changed

        rio.close()


class TestRemoteZip(unittest.TestCase):
    @staticmethod
    def make_big_header_zip(fname, entries):
        with zipfile.ZipFile(fname, 'w', compression=zipfile.ZIP_DEFLATED) as zip:
            for i in range(entries):
                zip.writestr('test_long_header_file_{0}'.format(i), 'x')

    def test_big_header(self):
        with TmpDir() as dire:
            fname = os.path.join(dire, 'test.zip')
            self.make_big_header_zip(fname, 2000)

            with LocalRemoteZip(fname) as rz:
                for i, finfo in enumerate(rz.infolist()):
                    self.assertEqual(finfo.filename, 'test_long_header_file_{0}'.format(i))
                    self.assertEqual(finfo.file_size, 1)

                self.assertIsNone(rz.testzip())

    @staticmethod
    def make_zip_file(fname):
        with zipfile.ZipFile(fname, 'w', compression=zipfile.ZIP_DEFLATED) as zip:
            zip.writestr('file1', 'X' + ('A' * 10000) + 'Y')
            zip.writestr('file2', 'short content')
            zip.writestr('file3', '')
            zip.writestr('file4', 'last file')

    def test_interface(self):
        with TmpDir() as dire:
            fname = os.path.join(dire, 'test.zip')
            self.make_zip_file(fname)

            rz = LocalRemoteZip(fname, min_buffer_size=50)
            ilist = rz.infolist()
            self.assertEqual(ilist[0].filename, 'file1')
            self.assertEqual(ilist[0].file_size, 10002)
            self.assertEqual(rz.read('file1'), b'X' + (b'A' * 10000) + b'Y')
            self.assertEqual(rz.read('file1'), b'X' + (b'A' * 10000) + b'Y')

            self.assertEqual(ilist[1].filename, 'file2')
            self.assertEqual(ilist[1].file_size, 13)
            self.assertEqual(rz.read('file2'), b'short content')

            self.assertEqual(ilist[2].filename, 'file3')
            self.assertEqual(ilist[2].file_size, 0)
            self.assertEqual(rz.read('file3'), b'')

            self.assertEqual(ilist[3].filename, 'file4')
            self.assertEqual(ilist[3].file_size, 9)
            self.assertEqual(rz.read('file4'), b'last file')

            self.assertIsNone(rz.testzip())

    def test_zip64(self):
        rz = LocalRemoteZip('test_data/zip64.zip')
        self.assertEqual(rz.read('big_file'), b'\x00' * (1024*1024))
        self.assertIsNone(rz.testzip())

    def test_make_buffer(self):
        content_buff = io.BytesIO(b'aaaabbcccdd')
        buff = LocalRemoteZip.make_buffer(content_buff, 'bytes 0-11/12', stream=False)
        self.assertEqual(buff.size, 12)
        self.assertEqual(buff.position, 0)
        self.assertEqual(buff.offset, 0)
        self.assertFalse(buff.stream)

        content_buff = io.BytesIO(b'aaaabbcccdd')
        buff = LocalRemoteZip.make_buffer(content_buff, 'bytes 10-21/40', stream=False)
        self.assertEqual(buff.size, 12)
        self.assertEqual(buff.position, 10)
        self.assertEqual(buff.offset, 10)
        self.assertFalse(buff.stream)

        content_buff = io.BytesIO(b'aaaabbcccdd')
        buff = LocalRemoteZip.make_buffer(content_buff, 'bytes 10-21/40', stream=True)
        self.assertEqual(buff.size, 12)
        self.assertEqual(buff.position, 10)
        self.assertEqual(buff.offset, 10)
        self.assertTrue(buff.stream)
        self.assertIs(buff.buffer, content_buff)

    def test_make_header(self):
        header = LocalRemoteZip.make_header(0, 10)
        self.assertEqual(header, 'bytes=0-10')

        header = LocalRemoteZip.make_header(80, None)
        self.assertEqual(header, 'bytes=80-')

        header = LocalRemoteZip.make_header(-123, None)
        self.assertEqual(header, 'bytes=-123')

    # TODO: test get_position2size

if __name__ == '__main__':
    unittest.main()
