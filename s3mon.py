#!/usr/bin/env python
"""
s3mon - New files monitoring of public s3 bucket
"""
__author__ = "Gustavo Oliveira (cetres@gmail.com)"
__version__ = "0.1.0"
__copyright__ = "Copyright (c) 2019 Gustavo Oliveira"
__license__ = "MIT"

import os
import sys
import traceback
import logging
from datetime import datetime, timedelta
import argparse
import logging
import platform
import hashlib

import boto3
import pandas as pd

def load_objects(bucket, prefix=None, region=None, max_iter=0):
    s3client = boto3.client('s3', region_name=region)
    objects = s3client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    ct = 0
    df = None
    while True:
        if df is None:
            df = pd.DataFrame(objects['Contents'])
        else:
            df = pd.concat([df, pd.DataFrame(objects['Contents'])])
        if 'NextContinuationToken' in objects:
            objects = s3client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=objects['NextContinuationToken'])
        else:
            break
        ct += 1
        logging.debug('Iter: {}'.format(ct))
        if max_iter > 0 and ct >= max_iter:
            break
    df['LastModified'] = pd.to_datetime(df['LastModified'], utc=True)
    logging.info('IsTruncated: {}\nQty: {}'.format(objects['IsTruncated'], len(objects['Contents'])))
    return df.set_index('Key')

if __name__ == '__main__':
    platform_name = platform.system()
    if platform_name == 'Linux':
        default_cache = '/var/spool/s3mon'
    elif platform_name == 'Windows':
        default_cache = os.path.expandvars('$TEMP\\s3mon')
    elif platform_name == 'Darwin':
        default_cache = os.path.expandvars('$HOME/Library/s3mon')
    else:
        default_cache = './cache'
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
    parser.add_argument("-c", "--cache_dir", default=default_cache,
                  help=u"Cache path for state store")

    args = parser.parse_args()

    if len(args.bucket) == 0:
        parser.error("At least one bucket need to be informed")

    LOG_LEVEL = logging.DEBUG if args.debug else logging.INFO
    
    logging.basicConfig(filename=args.log_file, level=LOG_LEVEL, format='%(asctime)-15s %(levelname)s %(message)s')

    try:
        if not os.path.isdir(args.cache_dir):
            os.mkdir(args.cache_dir)
        for bucket in args.bucket:
            logging.debug('Starting bucket %s' % bucket)
            filepath = os.path.join(args.cache_dir, hashlib.md5((bucket+args.prefix).encode('utf-8')).hexdigest()+".csv.gz")
            df_new = load_objects(bucket, prefix=args.prefix, region=args.region, max_iter=args.max_iter)
            if os.path.isfile(filepath):
                logging.debug('Cache found: {}'.format(filepath))
                df_old = pd.read_csv(filepath, index_col='Key', compression='gzip', parse_dates=['LastModified'])
                logging.debug('Cache loaded: {} rows'.format(df_old.shape[0]))
                if df_new.equals(df_old):
                    print('No modifications. Exiting...')
                    sys.exit(0)
                diff_list = list(pd.concat([df_old, df_new]).drop_duplicates(keep=False).index)
                print('{} new registries identified:'.format(len(diff_list)))
                print("\n".join(diff_list))
                logging.debug('Diff list [{}]: {}'.format(len(diff_list), "\n".join(diff_list)))
            else:
                logging.debug('Cache not found: {}'.format(filepath))
            df_new.to_csv(filepath, compression='gzip')
    except SystemExit as e:
        raise
    except:
        logging.critical('Unknown error found')
        raise # for debug purpose
        sys.exit(1)
