# aws_utils
Utilities for working with AWS tools

This repo contains functionality that's useful enough for me to want to wrap in
a nice Python API. Currently, it supports the following:
- Copy to S3 bucket
- Move to S3 bucket

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
## TODO
[ ] Copy _from_ S3 bucket
[ ] Add to PyPI


