import unittest
import zipfile
import tempfile
import os
import shutil
import io

from requests import session
import requests_mock

import remotezip as rz


class TmpDir(object):
    """Create a tmp directory that is automatically destroyed when the context exit"""
    def __enter__(self):
        self.tmpdir = tempfile.mktemp()
        os.mkdir(self.tmpdir)
        return self.tmpdir

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self.tmpdir)

class ServerSimulator:
    def __init__(self, fname):
        self._fname = fname

    def serve(self, request, context):
        from_byte, to_byte = rz.RemoteFetcher.parse_range_header(request.headers['Range'])
        with open(self._fname, 'rb') as f:
            if from_byte < 0:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(size + from_byte, 0), 0)
                init_pos = f.tell()
                content = f.read(min(size, -from_byte))
            else:
                f.seek(from_byte, 0)
                init_pos = f.tell()
                content = f.read(to_byte - from_byte + 1)

        context.headers['Content-Range'] = rz.RemoteFetcher.build_range_header(init_pos, init_pos + len(content))
        return content


class LocalFetcher(rz.RemoteFetcher):
    def fetch(self, data_range, stream=False):
        with open(self._url, 'rb') as f:
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
            buff = rz.PartialBuffer(f, range_min, range_max - range_min + 1, stream=stream)
        return buff


class TestPartialBuffer(unittest.TestCase):
    def setUp(self):
        if not hasattr(self, 'assertRaisesRegex'):
            self.assertRaisesRegex = self.assertRaisesRegexp

    def verify(self, stream):
        pb = rz.PartialBuffer(io.BytesIO(b'aaaabbcccdd'), 10, 11, stream=stream)
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
        self.assertEqual(pb.tell(), 21)
        self.assertEqual(pb.seek(15, 0), 15)
        self.assertEqual(pb.read(), b'bcccdd')
        self.assertEqual(pb.seek(-5, 2), 16)
        self.assertEqual(pb.read(), b'cccdd')
        self.assertEqual(pb.read(), b'')

    def test_static_out_of_bound(self):
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

        with self.assertRaisesRegex(rz.OutOfBound, "Negative seek not supported"):
            pb.seek(12, 0)


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
        self.assertIsNone(rio._file_size)
        rio.seek(0, 2)  # eof
        self.assertIsNotNone(rio._file_size)
        self.assertEqual(rio.tell(), 200 * 1024)

        curr_buffer = rio.buffer

        rio.seek(-20, 2)
        self.assertEqual(rio.read(2), b'xx')
        self.assertIs(rio.buffer, curr_buffer)  # buffer didn't change
        self.assertEqual(rio.read(), b'x' * 18)
        self.assertEqual(rio.tell(), 200 * 1024)

        rio.seek(120*1024, 0)
        self.assertEqual(rio.read(2), b'xx')
        self.assertIsNot(rio.buffer, curr_buffer)  # buffer changed
        rio.close()

    def test_file_access(self):
        rio = rz.RemoteIO(fetch_fun=self.fetch_fun)
        rio.seek(0, 2)  # eof
        curr_buffer = rio.buffer
        # we have two file, one at pos 156879 with size 30k and the last at pos
        rio.set_position_to_size({15687: 30 * 1024, 50354: 63000})
        rio.seek(15687, 0)
        self.assertEqual(rio.tell(), 15687)
        self.assertEqual(rio.read(5), b'sssss')
        self.assertIsNot(rio.buffer, curr_buffer)  # buffer changed
        curr_buffer = rio.buffer

        # re-read the same file
        rio.seek(15687, 0)
        self.assertEqual(rio.read(4), b'ssss')
        self.assertEqual(repr(rio.buffer), "<PartialBuffer off=15687 size=30720 stream=True>")
        self.assertIsNot(rio.buffer, curr_buffer)  # buffer changed
        curr_buffer = rio.buffer

        # move to next file
        rio.seek(50354, 0)
        self.assertEqual(rio.read(4), b'ssss')
        self.assertEqual(repr(rio.buffer), "<PartialBuffer off=50354 size=63000 stream=True>")
        self.assertIsNot(rio.buffer, curr_buffer)  # buffer changed
        curr_buffer = rio.buffer

        # seek forward
        rio.seek(60354, 0)
        self.assertEqual(rio.read(4), b'ssss')
        self.assertIs(rio.buffer, curr_buffer)      # buffer didn't change

        # seek backward
        rio.seek(51354, 0)
        self.assertEqual(rio.read(4), b'ssss')
        self.assertIsNot(rio.buffer, curr_buffer)   # buffer changed

        rio.close()


class TestLocalFetcher(unittest.TestCase):
    def test_build_range_header(self):
        header = rz.RemoteFetcher.build_range_header(0, 10)
        self.assertEqual(header, 'bytes=0-10')

        header = rz.RemoteFetcher.build_range_header(80, None)
        self.assertEqual(header, 'bytes=80-')

        header = rz.RemoteFetcher.build_range_header(-123, None)
        self.assertEqual(header, 'bytes=-123')

    def test_parse_range_header(self):
        range_min, range_max = rz.RemoteFetcher.parse_range_header('bytes 0-11/12')
        self.assertEqual(range_min, 0)
        self.assertEqual(range_max, 11)

        range_min, range_max = rz.RemoteFetcher.parse_range_header('bytes 10-21/40')
        self.assertEqual(range_min, 10)
        self.assertEqual(range_max, 21)

        range_min, range_max = rz.RemoteFetcher.parse_range_header('bytes -123')
        self.assertEqual(range_min, -123)
        self.assertIsNone(range_max)


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

            with rz.RemoteZip(fname, fetcher=LocalFetcher) as zfile:
                for i, finfo in enumerate(zfile.infolist()):
                    self.assertEqual(finfo.filename, 'test_long_header_file_{0}'.format(i))
                    self.assertEqual(finfo.file_size, 1)

                self.assertIsNone(zfile.testzip())

    @staticmethod
    def make_unordered_zip_file(fname):
        with zipfile.ZipFile(fname, 'w') as zip:
            zip.writestr("fileA", "A" * 300000 + 'Z')
            zip.writestr("fileB", "B" * 10000 + 'Z')
            zip.writestr("fileC", "C" * 100000 + 'Z')
            info_list = zip.infolist()
            info_list[0], info_list[1] = info_list[1], info_list[0]

    def test_unordered_fileinfo(self):
        """Test that zip file with unordered fileinfo records works as well. Fix #13."""
        with TmpDir() as dire:
            fname = os.path.join(dire, 'test.zip')
            self.make_unordered_zip_file(fname)

            with rz.RemoteZip(fname, fetcher=LocalFetcher) as zfile:
                names = zfile.namelist()
                self.assertEqual(names, ['fileB', 'fileA', 'fileC'])
                with zfile.open('fileB', 'r') as f:
                    self.assertEqual(f.read(), b"B" * 10000 + b'Z')
                with zfile.open('fileA', 'r') as f:
                    self.assertEqual(f.read(), b"A" * 300000 + b'Z')
                with zfile.open('fileC', 'r') as f:
                    self.assertEqual(f.read(), b"C" * 100000 + b'Z')
                self.assertIsNone(zfile.testzip())

    def test_fetch_part(self):
        # fetch a range
        expected_headers = {'Range': 'bytes=10-20'}
        headers = {'Content-Range': 'Bytes 10-20/30'}
        with requests_mock.Mocker() as m:
            m.register_uri("GET", "http://test.com/file.zip", content=b"abc", status_code=200, headers=headers,
                           request_headers=expected_headers)
            fetcher = rz.RemoteFetcher("http://test.com/file.zip")
            buffer = fetcher.fetch((10, 20), stream=True)
            self.assertEqual(buffer.tell(), 10)
            self.assertEqual(buffer.read(3), b"abc")

    def test_fetch_ending(self):
        # fetch file ending
        expected_headers = {'Range': 'bytes=-100'}
        headers = {'Content-Range': 'Bytes 10-20/30'}
        with requests_mock.Mocker() as m:
            m.register_uri("GET", "http://test.com/file.zip", content=b"abc", status_code=200, headers=headers,
                           request_headers=expected_headers)
            fetcher = rz.RemoteFetcher("http://test.com/file.zip")
            buffer = fetcher.fetch((-100, None), stream=True)
            self.assertEqual(buffer.tell(), 10)
            self.assertEqual(buffer.read(3), b"abc")

    def test_fetch_ending_unsupported_suffix(self):
        # fetch file ending
        expected_headers = {'Range': 'bytes=900-999'}
        headers = {'Content-Range': 'Bytes 900-999/1000'}
        with requests_mock.Mocker() as m:
            m.head("http://test.com/file.zip", status_code=200, headers={'Content-Length': '1000'})
            m.get("http://test.com/file.zip", content=b"abc", status_code=200, headers=headers,
                  request_headers=expected_headers)
            fetcher = rz.RemoteFetcher("http://test.com/file.zip", support_suffix_range=False)
            buffer = fetcher.fetch((-100, None), stream=True)
            self.assertEqual(buffer.tell(), 900)
            self.assertEqual(buffer.read(3), b"abc")

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

            zfile = rz.RemoteZip(fname, min_buffer_size=50, fetcher=LocalFetcher)
            ilist = zfile.infolist()
            self.assertEqual(ilist[0].filename, 'file1')
            self.assertEqual(ilist[0].file_size, 10002)
            self.assertEqual(zfile.read('file1'), b'X' + (b'A' * 10000) + b'Y')
            self.assertEqual(zfile.read('file1'), b'X' + (b'A' * 10000) + b'Y')

            self.assertEqual(ilist[1].filename, 'file2')
            self.assertEqual(ilist[1].file_size, 13)
            self.assertEqual(zfile.read('file2'), b'short content')

            self.assertEqual(ilist[2].filename, 'file3')
            self.assertEqual(ilist[2].file_size, 0)
            self.assertEqual(zfile.read('file3'), b'')

            self.assertEqual(ilist[3].filename, 'file4')
            self.assertEqual(ilist[3].file_size, 9)
            self.assertEqual(zfile.read('file4'), b'last file')

            self.assertIsNone(zfile.testzip())

    def test_zip64(self):
        zfile = rz.RemoteZip('test_data/zip64.zip', fetcher=LocalFetcher)
        self.assertEqual(zfile.read('big_file'), b'\x00' * (1024*1024))
        self.assertIsNone(zfile.testzip())

    def test_range_not_supported(self):
        with requests_mock.Mocker() as m:
            m.get("http://test.com/file.zip")
            with self.assertRaises(rz.RangeNotSupported):
                rz.RemoteZip("http://test.com/file.zip")

    def test_custom_session(self):
        custom_session = session()
        custom_session.headers.update({"user-token": "1234"})

        with TmpDir() as dire:
            fname = os.path.join(dire, 'test.zip')
            self.make_zip_file(fname)

            server = ServerSimulator(fname)
            expected_headers = {
                "user-token": "1234"
            }
            with requests_mock.Mocker() as m:
                m.register_uri("GET", "http://test.com/file.zip", content=server.serve, status_code=200, request_headers=expected_headers)
                rz.RemoteZip("http://test.com/file.zip", session=custom_session)



    # TODO: test get_position2size

if __name__ == '__main__':
    unittest.main()
