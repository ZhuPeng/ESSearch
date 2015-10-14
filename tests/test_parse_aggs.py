import unittest
from esearch.query import SearchBodyBuild
from esearch import parse_aggs


class Test_ParseAggs(unittest.TestCase):
    def setUp(self):
        self.body_build = SearchBodyBuild()
        self.body_build.range('time', 'start', 'end')\
                       .must('ip', '10.10.0.0')\
                       .groupby_date('time', '1d')\
                       .groupby('domain')\
                       .sum('pv')

        self.res = {
            'hits': {},
            '_shards': {},
            'took': 100,
            'aggregations': {
                'time-date_histogram': {
                    'buckets': [
                        {
                            'domain-terms': {
                                'buckets': [
                                    {
                                        'pv-sum': {
                                            'value': 2000
                                        },
                                        'key': 'www.xxxx.com',
                                        'doc_count': 1000
                                    }
                                ],
                                'sum_other_doc_count': 0,
                                'doc_count_error_upper_bound': 0
                            },
                            'key_as_string': '1111-11-11',
                            'key': 11111111,
                            'doc_count': 11111111
                        },
                        {
                            'domain-terms': {
                                'buckets': [
                                    {
                                        'pv-sum': {
                                            'value': 2000
                                        },
                                        'key': 'www.xxxx.com',
                                        'doc_count': 1000
                                    }
                                ],
                                'sum_other_doc_count': 0,
                                'doc_count_error_upper_bound': 0
                            },
                            'key_as_string': '2222-22-22',
                            'key': 22222222,
                            'doc_count': 22222222
                        }
                    ],
                }
            }
        }
        self.pre_parser = parse_aggs.parse(self.body_build, self.res)

    def get_date_and_value(self, idx):
        idx = str(idx)
        return '%s-%s-%s' % (idx*4, idx*2, idx*2), int(idx*8)

    def test_get_buckets_metric(self):
        for idx, time_bucket in enumerate(self.pre_parser.get_buckets('time')):
            date, value = self.get_date_and_value(idx+1)
            self.assertEqual(time_bucket.get_key_as_string(), date)
            self.assertEqual(time_bucket.get_key(), value)
            self.assertEqual(time_bucket.get_doc_count(), value)

            for domain_bucket in time_bucket.get_buckets('domain'):
                self.assertEqual(domain_bucket.get_key(), 'www.xxxx.com')
                self.assertEqual(domain_bucket.get_doc_count(), 1000)
                self.assertEqual(domain_bucket.get_sum('pv'), 2000)

                with self.assertRaises(Exception):
                    domain_bucket.get_buckets('xxxxx')
