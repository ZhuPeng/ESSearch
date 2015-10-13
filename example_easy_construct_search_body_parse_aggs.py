import datetime
from esearch.query import SearchBodyBuild
from esearch import parse_aggs


# Construct the search body
body_build = SearchBodyBuild()
end = datetime.datetime.now()
start = end - datetime.timedelta(days=10)

body = body_build.select_range('time', start, end)\
                 .select_must('ip', '10.10.0.0')\
                 .groupby_date('time', '1d')\
                 .groupby('domain')\
                 .add_metric('pv', 'sum')\
                 .get_body()

print body


# Search
# import elasticsearch
# es_client = elasticsearch.Elasticsearch()
# res = es_client.search(index='test', body=body, doc_type='test')
res = {
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

# Parse aggregations
pre_aggs = parse_aggs.parse(body_build, res)

for time_bucket in pre_aggs.get_buckets('time'):
    print '--time bucket'
    print '\tkey:', time_bucket.get_key()
    print '\tkey_as_string:', time_bucket.get_key_as_string()
    print '\tdoc_count:', time_bucket.get_doc_count()

    for domain_bucket in time_bucket.get_buckets('domain'):
        print '--domain bucket'
        print '\tkey:', domain_bucket.get_key()
        print '\tdoc_count:', domain_bucket.get_doc_count()

        print '\tmetric value:', domain_bucket.get_metric_value('pv')

        # raise an Exception: Do not exists this aggregation field
        # domain_bucket.get_buckets('xx')
