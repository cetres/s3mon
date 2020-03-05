#!/usr/bin/env python
"""
s3mon - New files monitoring of public s3 bucket
"""

import os
import sys
import traceback
import logging
import argparse

from s3mon import S3Mon

def main():
    description = u"Identify new files in s3 bucket"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('bucket', metavar='BUCKET', type=str, nargs='+',
                    help='bucket(s) for read')
    parser.add_argument("-p", "--prefix",
                  help=u"Prefix of the key to find")
    parser.add_argument("-r", "--region",
                  help=u"Region of bucket")
    parser.add_argument("-m", "--max_iter", default='100',
                  help=u"Max iteration times for s3 list")
    parser.add_argument("-d", "--debug", action='store_true',
                  help=u"Debug output")
    parser.add_argument("-l", "--log_file", 
                  help=u"Log file")
    parser.add_argument("-c", "--cache_dir", default=S3Mon.default_cache_path(),
                  help=u"Cache path for state store")

    args = parser.parse_args()

    if len(args.bucket) == 0:
        parser.error("At least one bucket need to be informed")

    LOG_LEVEL = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(filename=args.log_file, level=LOG_LEVEL, format='%(asctime)-15s %(levelname)s %(message)s')

    try:
        s3mon = S3Mon(region=args.region, cache_dir=args.cache_dir)
        for bucket in args.bucket:
            diff_list = s3mon.compare(bucket, args.prefix, args.max_iter)
            print('{} new registries identified:'.format(len(diff_list)))
            print("\n".join(diff_list))
    except:
        logging.critical('Unknown error found')
        if args.debug:
            raise
        sys.exit(1)

if __name__ == '__main__':
    main()