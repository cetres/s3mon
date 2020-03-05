import hashlib
import json
import logging
import os
import platform

import boto3
import pandas as pd

class S3Mon(object):
    _cache_dir = None
    _logger = None
    _s3client = None

    def __init__(self, region=None, cache_dir=None):
        self._logger = logging.getLogger('s3mon')
        self._s3client = boto3.client('s3', region_name=region)
        if  cache_dir is None:
            cache_dir = S3Mon.default_cache_path()
        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)
        self._cache_dir = cache_dir

    def load_objects(self, bucket, prefix=None, max_iter=0):
        objects = self._s3client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        ct = 0
        df = None
        while True:
            if df is None:
                df = pd.DataFrame(objects['Contents'])
            else:
                df = pd.concat([df, pd.DataFrame(objects['Contents'])])
            if 'NextContinuationToken' in objects:
                objects = self._s3client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=objects['NextContinuationToken'])
            else:
                break
            ct += 1
            self._logger.debug('Iter: {}'.format(ct))
            if max_iter > 0 and ct >= max_iter:
                break
        df['LastModified'] = pd.to_datetime(df['LastModified'], utc=True)
        self._logger.info('IsTruncated: {}\nQty: {}'.format(objects['IsTruncated'], len(objects['Contents'])))
        return df.set_index('Key')

    def cache_file_path(self, bucket, prefix):
        filename = hashlib.md5((bucket+prefix).encode('utf-8')).hexdigest() + ".csv.gz"
        return os.path.join(self._cache_dir, filename)
    
    def load_cache_file(self, bucket, prefix):
        filepath = self.cache_file_path(bucket, prefix)
        if os.path.isfile(filepath):
            df = pd.read_csv(filepath, index_col='Key', compression='gzip', parse_dates=['LastModified'])
            self._logger.debug('Cache loaded: {} rows'.format(df.shape[0]))
            return df
        else:
            self._logger.debug('Cache not found: {}'.format(filepath))
            return None
    
    def save_cache_file(self, bucket, prefix, df):
        filepath = self.cache_file_path(bucket, prefix)
        df.to_csv(filepath, compression='gzip')

    def compare(self, bucket, prefix, max_iter=0):
        diff_list = []
        self._logger.debug('Starting bucket %s' % bucket)
        df_old = self.load_cache_file(bucket, prefix)
        df_new = self.load_objects(bucket, prefix=prefix, max_iter=max_iter)
        if df_old is not None:
            if df_new.equals(df_old):
                self._logger.info('No modifications. Exiting...')
                diff_list = []
            else:
                diff_list = list(pd.concat([df_old, df_new]).drop_duplicates(keep=False).index)
        else:
            diff_list = list(df_new.index)
        self._logger.debug('Diff list [{}]: {}'.format(len(diff_list), "\n".join(diff_list)))
        if len(diff_list) > 0:
            self.save_cache_file(bucket, prefix, df_new)
        return diff_list

            
    @staticmethod
    def default_cache_path():
        platform_name = platform.system()
        if platform_name == 'Linux':
            default_cache = '/var/spool/s3mon'
        elif platform_name == 'Windows':
            default_cache = os.path.expandvars('$TEMP\\s3mon')
        elif platform_name == 'Darwin':
            default_cache = os.path.expandvars('$HOME/Library/s3mon')
        else:
            default_cache = './cache'
        return default_cache

