import unittest
from esearch.query import SearchBodyBuild


class Test_SearchBodyBuild(unittest.TestCase):
    def setUp(self):
        self.esbody = SearchBodyBuild()
        self.testquery = {
            'query': {
                'bool': {
                    'must': [
                        {
                            'bool': {
                                'should': []
                            }
                        },
                    ]
                }
            }
        }
        self.testaggs = {}

    def test_select_must(self):
        self.esbody.select_must('time1', 'today')
        self.testquery['query']['bool']['must'].append({'term': {'time1': 'today'}})
        self.assertEqual(
            self.esbody.query_build.get_body(), self.testquery
        )

        self.esbody.select_must('time2', 'today')
        self.testquery['query']['bool']['must'].append({'term': {'time2': 'today'}})

        self.assertEqual(
            self.esbody.query_build.get_body(), self.testquery
        )

    def test_select_should(self):
        self.esbody.select_should('time1', 'today')\
            .select_should('time2', 'today')
        self.testquery['query']['bool']['must'][0]['bool']['should'].append({'term': {'time1': 'today'}})
        self.testquery['query']['bool']['must'][0]['bool']['should'].append({'term': {'time2': 'today'}})
        self.assertEqual(
            self.esbody.query_build.get_body(), self.testquery
        )

    def test_select_range(self):
        self.esbody.select_range('time', 'yesterday', 'today')
        self.testquery['query']['bool']['must'].append({'range': {'time': {'gte': 'yesterday', 'lte': 'today'}}})
        self.assertEqual(
            self.esbody.query_build.get_body(), self.testquery
        )

    def test_select_range_should(self):
        self.esbody.select_range('time', 'yesterday', 'today')\
            .select_should('time1', 'today')\
            .select_should('time2', 'today')

        self.testquery['query']['bool']['must'].append({'range': {'time': {'gte': 'yesterday', 'lte': 'today'}}})
        self.testquery['query']['bool']['must'][0]['bool']['should'].append({'term': {'time1': 'today'}})
        self.testquery['query']['bool']['must'][0]['bool']['should'].append({'term': {'time2': 'today'}})

        self.assertEqual(
            self.esbody.query_build.get_body(), self.testquery
        )

    def test_update_aggs(self):
        self.assertEqual(self.esbody.aggs_build.get_body(), self.testaggs)

        self.esbody.groupby('ip')
        self.testaggs['aggs'] = {'ip-terms': {'terms': {'field': 'ip', 'size': 0}}}

        self.assertEqual(self.esbody.aggs_build.get_body(), self.testaggs)

        self.esbody.groupby_date('date', '1h')\
            .add_metric('count', 'avg')
        self.testaggs['aggs']['ip-terms']['aggs'] = {
            'date-date_histogram': {'date_histogram': {'field': 'date', 'interval': '1h'}},
            'count-avg': {'avg': {'field': 'count'}}
        }

        self.assertEqual(self.esbody.aggs_build.get_body(), self.testaggs)

    def test_bodybuild(self):
        body = {}
        body.update(self.testquery)
        body.update(self.testaggs)

        self.assertEqual(self.esbody.get_body(), body)

        self.esbody.select_range('time', 'yesterday', 'today')\
            .select_should('time1', 'today')
        self.testquery['query']['bool']['must'].append({'range': {'time': {'gte': 'yesterday', 'lte': 'today'}}})
        self.testquery['query']['bool']['must'][0]['bool']['should'].append({'term': {'time1': 'today'}})

        self.esbody.groupby_date('date', '1h')\
            .add_metric('count', 'avg')
        self.testaggs['aggs'] = {
            'date-date_histogram': {'date_histogram': {'field': 'date', 'interval': '1h'}},
            'count-avg': {'avg': {'field': 'count'}}
        }

        body = {}
        body.update(self.testquery)
        body.update(self.testaggs)
        self.assertEqual(self.esbody.get_body(), body)
