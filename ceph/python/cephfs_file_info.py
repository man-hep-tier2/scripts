#!/usr/bin/env python3
import pathlib, os, argparse, datetime, rados
from sys import stderr, stdout
from xattr import getxattr
import cephfs as libcephfs
from math import ceil
from struct import unpack
from zlib import adler32
from io import BufferedWriter

def log_stderr(msg):
    print(msg, file=stderr)

def log_stderr_and_exit(msg, rc):
    log_stderr(msg)
    exit(rc)

def rados_connect(conffile=''):
    cluster = rados.Rados(conffile=conffile)
    cluster.connect()
    return cluster

def cephfs_connect(rados, fsname=None):
    fs = libcephfs.LibCephFS(rados_inst=rados)
    fs.mount(filesystem_name=fsname)
    return fs

class XrdXAttrCs:
    def __init__(self, cs):
        # order is network byte order (big endian)
        # 16 bytes cs type name (char)
        # 8  bytes file's mtime when cs was computed (long long)
        # 4  bytes time difference between time when cs was computed and file's mtime, seconds? (int)
        # 2  bytes reserved (short)
        # 1  byte  reserved (char)
        # 1  byte  cs length (char)
        # 64 bytes cs value (char)
        (alg, mtime, self._delta, _, _, cs) = unpack('!16sqihc65p', cs)
        self._alg = alg.decode('ascii').strip("\x00")
        self._mtime = datetime.datetime.fromtimestamp(mtime)
        self._checksum = cs

    @property
    def checksum(self):
        return self._checksum

    @property
    def checksum_hex(self):
        return self._checksum.hex()

    @property
    def algorithm(self):
        return self._alg

    @property
    def mtime(self):
        return self._mtime

    @property
    def delta(self):
        return self._delta

class CephfsFile:

    def __init__(self, file, rados=None, cephfs=None, chunksize=1024**2):
        self._file = file
        self._chunksize = chunksize
        self._rados_object = rados
        self._cephfs_object = cephfs
        self._ioctx_object = None
        self._pool_info = None
        self._stats = None
        self._adler32 = None

    @property
    def _rados(self):
        if self._rados_object is None:
            self._rados_object = rados_connect()
        return self._rados_object

    @property
    def _cephfs(self):
        if self._cephfs_object is None:
            self._cephfs_object = cephfs_connect()
        return self._cephfs_object

    @property
    def _ioctx(self):
        if self._ioctx_object is None:
            self._ioctx_object = self._rados.open_ioctx2(self.pool_id)
        return self._ioctx_object

    def _get_stat(self, name):
        return self.stats[name]

    def _get_pool_info(self, name):
        return self.pool_info[name]

    def update_stats(self):
        self._stats = {}
        stats = self._cephfs.stat(self._file)
        for st in ['st_ino', 'st_size', 'st_blksize', 'st_atime', 'st_mtime', 'st_ctime']:
            self._stats[st.removeprefix('st_')] = getattr(stats, st)
        self._stats['ino_hex'] = f'{stats.st_ino:x}'
        stats = self._cephfs.statx(self._file, libcephfs.CEPH_STATX_VERSION, 0)
        self._stats['version'] = stats['version']

    def update_pool_info(self):
        fd = self._cephfs.open(self._file, os.O_RDONLY)
        self._pool_info = self._cephfs.get_layout(fd)
        self._cephfs.close(fd)

    def update_adler32(self, chunksize=None):
        chunksize = chunksize or self._chunksize
        a32cs = 1
        for o in self.objects:
            for chunk in self.read_object(o, chunksize=chunksize):
                a32cs = adler32(chunk, a32cs)
        self._adler32 = f'{a32cs:0>8x}'

    def list_xattrs(self):
        _ , attrstr = self._cephfs.llistxattr(self._file)
        return [x for x in attrstr.decode('utf-8').split("\x00") if len(x) > 0]

    def get_xattr(self, name):
        return self._cephfs.lgetxattr(self._file, name)

    def object_at_index(self, index):
        if index < 0 or index >= self.object_count:
           raise IndexError(f'index {index} is out of bounds')
        return f'{self.inode_hex}.{index:0>8x}'

    def read_object(self, obj, chunksize=None):
        if self.stripe_count != 1 or self.object_size != self.stripe_unit:
            raise Exception('Reading objects is currently only supported if stripe count == 1 and object size == stripe unit size')
        chunksize = chunksize or self._chunksize
        offset = 0
        read = 1
        while read > 0:
            chunk = self._ioctx.read(obj, chunksize, offset)
            read = len(chunk)
            offset += read
            yield chunk

    def read_object_at_index(self, index, chunksize=None):
        return self.read_object(self.object_at_index(index), chunksize or self._chunksize)

    def stat_object(self, obj):
        return self._ioctx.stat(obj)

    def stat_object_at_index(self, index):
        return self.stat_object(self.object_at_index(index))

    def get(self, fp):
        for o in self.objects:
            for c in self.read_object(o):
                fp.write(c)

    @property
    def stats(self):
        if self._stats is None:
            self.update_stats()
        return self._stats

    @property
    def pool_info(self):
        if self._pool_info is None:
            self.update_pool_info()
        return self._pool_info

    filepath = property(lambda self: self._file)
    inode = property(lambda self: self._get_stat('ino'))
    inode_hex = property(lambda self: self._get_stat('ino_hex'))
    filesize = property(lambda self: self._get_stat('size'))
    blocksize = property(lambda self: self._get_stat('blksize'))
    atime = property(lambda self: self._get_stat('atime'))
    mtime = property(lambda self: self._get_stat('mtime'))
    ctime = property(lambda self: self._get_stat('ctime'))
    version = property(lambda self: self._get_stat('version'))
    stripe_unit = property(lambda self: self._get_pool_info('stripe_unit'))
    stripe_count = property(lambda self: self._get_pool_info('stripe_count'))
    object_size = property(lambda self: self._get_pool_info('object_size'))
    pool_id = property(lambda self: self._get_pool_info('pool_id'))
    pool_name = property(lambda self: self._get_pool_info('pool_name'))

    @property
    def adler32_xattr(self):
        if not 'user.XrdCks.adler32' in self.list_xattrs():
            return None
        return XrdXAttrCs(self.get_xattr('user.XrdCks.adler32'))

    @property
    def object_count(self):
        return ceil(self.filesize / self.object_size)

    @property
    def objects(self):
        for i in range(self.object_count):
            yield f'{self.inode_hex}.{i:0>8x}'

    @property
    def adler32(self):
        if self._adler32 is None:
            self.update_adler32()
        return self._adler32

def parse_cmdline():
    parser = argparse.ArgumentParser()
    parser.add_argument('path', help='CephFS path to check', type=pathlib.Path)
    parser.add_argument('--info', help='Print generic info on file', action='store_true')
    parser.add_argument('--list-objects', help='List the object names of the file', action='store_true')
    parser.add_argument('--missing-objects', help='Check for and list missing object names of the file', action='store_true')
    parser.add_argument('--adler32', help='Calculate adler32 checksum from objects', action='store_true')
    parser.add_argument('--check-adler32', help='Compare the calculated adler32 checksum with the metadata', action='store_true')
    parser.add_argument('--chunksize', help='Specify the default chunk size for read operations', type=int, default=1024**2)
    parser.add_argument('--get', help='Writes the file content to stdout or to the specified file (--out-file option)', action='store_true')
    parser.add_argument('--out-file', help='The name of the file to write to.', type=pathlib.Path)
    args = parser.parse_args()
    return args

def info(file):
    print(f'path: {file.filepath}')
    for p in ['inode', 'filesize', 'blocksize', 'atime', 'mtime', 'ctime', 'version', 'stripe_unit', 'stripe_count', 'pool_name', 'object_size', 'object_count']:
        print(f'{p}: {getattr(file, p)}')
    a32 = file.adler32_xattr
    if a32 != None:
        print(f'adler32 xattr checksum: {a32.checksum_hex}')
        print(f'adler32 xattr mtime: {a32.mtime}')
        print(f'adler32 xattr delta: {a32.delta}')

def get(file):
    if args.out_file:
        if args.out_file.exists():
            log_stderr_and_exit(f'file {str(args.out_file)} already exists, refusing to overwrite it', 1)
        with args.out_file.open('wb') as f:
            file.get(f)
    else:
        file.get(BufferedWriter(stdout.buffer))

def print_missing(file):
    for o in file.objects:
        try:
            file.stat_object(o)
        except rados.ObjectNotFound:
            print(o)

def main():
    args = parse_cmdline()
    cluster = rados_connect()
    fs = cephfs_connect(cluster)
    file = CephfsFile(str(args.path), rados=cluster, cephfs=fs, chunksize=args.chunksize)
    if args.info:
        info(file)
    if args.list_objects:
        for o in file.objects:
            print(o)
    if args.missing_objects:
        print_missing(file)
    if args.adler32:
        print(file.adler32)
    if args.check_adler32:
        print(f'(meta,data,equal): ({file.adler32_xattr.checksum_hex},{file.adler32},{file.adler32_xattr.checksum_hex == file.adler32})')
    if args.get:
        get(file)

if __name__ == '__main__':
    main()
