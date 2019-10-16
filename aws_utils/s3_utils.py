import os
import multiprocessing as mp
import argparse

import boto3

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
    def __init__(self, s3_bucket_name=None, dpath_dst=None, dpath_src=None):
        self.bucket_name = s3_bucket_name
        self.dpath_dst = dpath_dst
        self.dpath_src = dpath_src

        # Create a lookup table of files already in bucket so we don't try to
        # download them again
        s3 = boto3.resource('s3')
        files_in_bucket = list(map(
            lambda x: x.key,
            s3.Bucket(s3_bucket_name).objects.all()
        ))
        self.in_bucket = {k: True for k in files_in_bucket}

    def cp_to_bucket(self, fname_src):
        """ Opens connection to desired s3 instance and uploads files
        """

        # Upload local file to same path in bucket if not already there
        subpath = fname_src.split(self.dpath_src)[-1]
        if subpath.startswith(os.sep):
            subpath = subpath[1:]
        fname_dst = os.path.join(self.dpath_dst,
                                 subpath)

        # Only copy if not in bucket already
        if not self.in_bucket.get(fname_dst):
            s3_client = boto3.client('s3')
            s3_client.upload_file(fname_src, self.bucket_name, fname_dst)
        return self.in_bucket.get(fname_dst)

    def mv_to_bucket(self, filename):
        """ Copies and deletes
        """
        self.cp_to_bucket(filename)
        os.remove(filename)


def ls_r(directory):
    """ Recursive list directory
    """
    all_files = []
    for root, _, fns in os.walk(directory):
        all_files.extend([os.path.join(root, fn) for fn in fns])
    return all_files


if __name__ == '__main__':
    """
        python to_s3.py --bucket_name=oscar-datasets --dpath_src=$HOME/data/Moments_in_Time_256x256_30fps --cp --nproc=7
    """
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
    fpaths = ls_r(os.path.expandvars(args.dpath_src))

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
