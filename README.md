# aws_utils

Utilities for working with AWS tools

This repo contains functionality that's useful enough for me to want to wrap in
a nice Python API. Currently, it supports the following:
- Copy to S3 bucket from local machine
- Copy from S3 bucket to local machine
- Move to S3 bucket from local machine

---
## Setup
1. First, make sure you have an AWS account and access to all the assets you're
   interested in working with
2. Install this repo from this directory with `pip install .`
3. Run `aws configure` to add your credentials to the AWS client on your
   machine. You'll need:
   - Access Key ID
   - Secret Access Key ID
   - Default Region (I don't usually set this)
   - Default output format (I usually use `json`)
4. Import and use these utilities in your work!

*If you have already entered your credentials, you can skip that step*

---
## Usage
### From Python:
```
from aws_utils import S3Interface

# How many processes do you want to use? I usually choose N - 1, where N is the
# total number of cores my machine has (e.g. an machine with 8 cores means I
# use pool_size = 7.
# More processes means faster up/download, but may impact other processes
# running on your system
pool_size = 7
interface = S3Interface(pool_size)

# A few possible uses
interface.cp("BUCKET_NAME:src_directory", "dst_directory", fnames="*.jpg")
interface.cp("src_directory", "BUCKET_NAME:dst_directory", fnames="*.mov")
interface.mv("BUCKET_NAME:src_directory", "dst_directory", fnames="invoice_*.txt")
interface.mv("src_directory", "BUCKET_NAME:dst_directory")
```

---
## TODO
- [ ] Add to PyPI
- [ ] Transfer from one S3 bucket to another
