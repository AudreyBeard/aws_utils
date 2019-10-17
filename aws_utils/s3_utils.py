import os
import multiprocessing as mp
import argparse

import boto3


def urljoin(*args):
    """ Join urls, since os.path.join doesn't work with urls
        It looks ugly, but it's faster than stripping either side of the args
    """
    joined = args[0]
    for i in range(1, len(args)):
        if joined[-1] == '/' and args[i][0] == '/':
            joined += args[i][1:]
        elif joined[-1] == '/' or args[i][0] == '/':
            joined += args[i]
        else:
            joined += ('/' + args[i])
    return joined


class PoolFunctions(object):
    """ Class defining functions for a thread pool to be used in copying/moving
        stuff to an S3 bucket
    """
    def __init__(self, s3_bucket_name=None, dpath_dst=None, dpath_src=None, direction=None):
        self.bucket_name = s3_bucket_name
        self.dpath_dst = dpath_dst
        self.dpath_src = dpath_src
        self.direction = direction
        self.init_src_files()

    def init_src_files(self):
        """ List files in src directory. If the transfer will be an upload, we
            need to know what's in the bucket to avoid unnecessary copying
        """

        # Find files in the bucket
        s3 = boto3.resource('s3')
        files_in_bucket = list(map(
            lambda x: x.key,
            s3.Bucket(self.bucket_name).objects.all()
        ))

        # Create a lookup table  so we don't try to download them again
        self.in_bucket = {k: True for k in files_in_bucket}

        if self.direction == 'down':
            self.src_fpaths = [fp for fp in files_in_bucket if fp.startswith(self.dpath_src)]
        elif self.direction == 'up':
            self.src_fpaths = ls_r(os.path.expandvars(os.path.expanduser(self.dpath_src)))

    def cp_to_bucket(self, fname_src):
        """ Opens connection to desired s3 instance and uploads files
        """

        # Upload local file to same path in bucket
        fname_dst = create_dst_fpath(fname_src, self.dpath_src, self.dpath_dst)

        # Only copy if not in bucket already
        if not self.in_bucket.get(fname_dst):
            s3_client = boto3.client('s3')
            s3_client.upload_file(fname_src, self.bucket_name, fname_dst)
        return self.in_bucket.get(fname_dst)

    def cp_from_bucket(self, fname_src):
        """ Opens connection to desired s3 instance and downloads files
        """

        # Upload local file to same path in bucket
        fname_dst = create_dst_fpath(fname_src, self.dpath_src, self.dpath_dst)

        # Only copy if not in bucket already
        file_exists = os.path.exists(fname_dst)
        if not file_exists:

            # Make intermediate directories as needed
            try:
                os.makedirs(os.path.split(fname_dst)[0])
            except FileExistsError:
                pass

            # Do the actual copying
            s3_client = boto3.client('s3')
            s3_client.download_file(self.bucket_name, fname_src, fname_dst)
        return file_exists

    def mv_to_bucket(self, filename):
        """ Copies and deletes
        """
        self.cp_to_bucket(filename)
        os.remove(filename)


class MultiprocessingS3Interface(object):
    """ Defines a simpler interface for coying to/from S3 buckets.
        Creates a MP pool of a given size and can copy files to/from an S3
        bucket using MP.Pool().map()
    """
    def __init__(self, pool_size=1, verbosity=0):
        self.pool = mp.Pool(pool_size)
        self._v = verbosity
        self.in_bucket = None
        self.pool_size = pool_size

    def __repr__(self):
        s = self.__class__.__name__
        s += ' of size {}'.format(self.pool_size)
        s += ' connected to bucket {}'.format(self.bucket_name)
        return s

    def init_interface(self, src_dpath, dst_dpath):
        """ Initializes the bucket name, source and destination dpaths, and a
            list of items in the bucket for copy and move functions later
        """
        bucket_name, src_dpath, dst_dpath, direction = self._parse_src_dst(
            src_dpath,
            dst_dpath
        )

        funcs = PoolFunctions(bucket_name, dst_dpath, src_dpath, direction)
        return funcs

    def cp(self, src, dst, fnames=None):
        """ Copy function which infers the bucket name and direction. If fnames
            is given, only transfers those files. If not, will transfer all files
            in src to dst.
            Parameters:
                src (str): source directory path - if this is in a bucket,
                    specify BUCKET_NAME:dpath
                dst (str): destination directory path - if this is in a bucket,
                    specify BUCKET_NAME:dpath
                fnames (list of str | str): if list of strings, this is used as
                    the path to files(excluding the prefix denoted by <src>. If
                    <fnames> is a string, it is assumed to have a wildcard
                    character, e.g. "*.jpg" or "file_number*.txt"
            Example:
                >>> self = MultiProcessingS3Interface(4):
                >>> self.cp("bucket_name:src/directory", "~/dst/directory")
                >>> self.cp("~/src/directory", "bucket_name:src/directory",
                ...         fnames=["file_1", "file_2", "file_3"])
                >>> self.cp("~/src/directory", "bucket_name:src/directory",
                ...         fnames='*.jpg')
                >>> self.cp("bucket_name:src/directory", "~/dst/directory",
                ...         fnames='cat*.gif')
        """
        if self._v:
            print("Initializing internal bucket interface...")
        funcs = self.init_interface(src, dst)

        if fnames is None:
            fnames = funcs.src_fpaths

        # If fnames is a string with a wildcard character
        # Can only check the filenames right now, not the paths
        elif isinstance(fnames, str) and '*' in fnames:
            tokens = fnames.split('*')
            fnames = [
                fp
                for fp, fn in map(
                    lambda x: (x, os.path.split(x)[-1]),
                    funcs.src_fpaths
                )
                if fn.startswith(tokens[0]) and fn.endswith(tokens[1])
            ]
        print_head_tail(fnames, self._v)

        if funcs.direction == 'up':
            if self._v:
                print("Uploading {} files from {} to {}:{}".format(
                    len(fnames),
                    funcs.dpath_src,
                    funcs.bucket_name,
                    funcs.dpath_dst), flush=True
                )
            self.pool.map(funcs.cp_to_bucket, fnames)
        elif funcs.direction == 'down':
            if self._v:
                print("Downloading {} files from {}:{} to {}".format(
                    len(fnames),
                    funcs.bucket_name,
                    funcs.dpath_src,
                    funcs.dpath_dst), flush=True
                )
            self.pool.map(funcs.cp_from_bucket, fnames)
        else:
            raise NotImplementedError("No support for cross-bucket transfers yet")

        if self._v:
            print("Done copying")

    def _parse_src_dst(self, src, dst):
        """ Parses bucket name, source dpath, destination dpath, and direction
            of flow from src and dst arguments
        """
        try:
            bucket_name, src = src.split(':')
            direction = 'down'
        except ValueError:
            try:
                bucket_name, dst = dst.split(':')
                direction = 'up'
            except ValueError:
                raise ValueError("Either src or dst must specify bucket name")
        return bucket_name, src, dst, direction


def create_dst_fpath(src_fname, src_dpath, dst_dpath):
    """ Infers a destination filepath given the source filename and directory
        path, and the destination directory path
    """
    subpath = src_fname.split(src_dpath)[-1]
    subpath = subpath[1:] if subpath.startswith(os.sep) else subpath
    return os.path.join(dst_dpath, subpath)


def ls_r(directory):
    """ Recursive list directory, maintaining full paths
    """
    all_files = []
    for root, _, fns in os.walk(directory):
        all_files.extend([os.path.join(root, fn) for fn in fns])
    return all_files


def print_head_tail(iterable, n=5):
    if n > 0:
        # Diagnostic
        print("Found {} files.\nFirst {}:".format(len(iterable), n), flush=True)
        print(*iterable[:n], sep='\n', flush=True)
        print("\nLast {}:".format(n), flush=True)
        print(*iterable[-n:], sep='\n', flush=True)


if __name__ == '__main__':
    """
        python to_s3.py --bucket_name=oscar-datasets --dpath_src=$HOME/data/Moments_in_Time_256x256_30fps --cp --nproc=7
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket_name',
                        help='S3 Bucket Name',
                        action='store',
                        default='oscar-datasets',
                        )
    parser.add_argument('--dpath_src',
                        help='Data path containing files you want to move/copy',
                        action='store',
                        default='.',
                        )
    parser.add_argument('--dpath_dst',
                        help='Prefix for files in bucket',
                        action='store',
                        default=None,
                        )
    parser.add_argument('--cp',
                        help='Copy flag',
                        action='store_true',
                        )
    parser.add_argument('--mv',
                        help='Move flag',
                        action='store_true',
                        )
    parser.add_argument('--nproc',
                        help='Number of processes to use',
                        action='store',
                        default=1,
                        )
    args = parser.parse_args()

    # Default to local datapath's lowest-level folder if destination is not given
    dpath_dst = os.path.split(args.dpath_src)[-1] if args.dpath_dst is None else args.dpath_dst

    # Initialize pool functions with bucket name and datapath
    print("Initializing pool functions and creating table of existing files now...", flush=True)
    funcs = PoolFunctions(s3_bucket_name=args.bucket_name,
                          dpath_dst=dpath_dst,
                          dpath_src=args.dpath_src)

    # Get files in local datapath and construct full filepaths
    print("Finding files now...", flush=True)
    fpaths = ls_r(os.path.expandvars(os.path.expanduser(args.dpath_src)))

    # Diagnostic
    n_files_to_show = 5
    print("Found {} files.\nFirst {}:".format(len(fpaths), n_files_to_show), flush=True)
    print(*fpaths[:n_files_to_show], sep='\n', flush=True)
    print("\nLast {}:".format(n_files_to_show), flush=True)
    print(*fpaths[-n_files_to_show:], sep='\n', flush=True)

    # Init pool
    pool = mp.Pool(int(args.nproc))
    print("\nInitialized pool with {} processes".format(args.nproc), flush=True)

    if args.cp:
        task_str = 'Copying'
        task_func = funcs.cp_to_bucket
    elif args.mv:
        task_str = 'Moving'
        task_func = funcs.mv_to_bucket
    else:
        raise NotImplementedError("No valid operation specified")

    print("{} {} files from {} to {}/{}".format(task_str,
                                                len(fpaths),
                                                args.dpath_src,
                                                args.bucket_name,
                                                args.dpath_dst), flush=True)
    # Farm out task to pool
    pool.map(task_func, fpaths)

    print("Done!")
