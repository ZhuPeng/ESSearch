from query import AggregationBuild


def parse(query_build, es_result):
    return AggsBody(query_build.aggs_build, es_result['aggregations'])


class AggsBody():
    def __init__(self, aggs_build, aggs_body):
        self.aggs_body = aggs_body
        self.aggs_build = aggs_build

    def get_buckets(self, field):
        aggs_name = self.aggs_build.get_aggs_name(field)
        is_bucket = AggregationBuild.is_bucket_byname(aggs_name)
        if not is_bucket:
            raise Exception('Need a field of bucket aggregation.')

        for bucket in self.aggs_body[aggs_name]['buckets']:
            yield Bucket(self.aggs_build, bucket)


class Bucket():
    def __init__(self, aggs_build, bucket):
        self.aggs_build = aggs_build
        self.bucket = bucket

    def get_key_as_string(self):
        if 'key_as_string' not in self.bucket:
            raise Exception('key_as_string not exists in this bucket.')
        return self.bucket['key_as_string']

    def get_key(self):
        return self.bucket['key']

    def get_doc_count(self):
        return self.bucket['doc_count']

    def _child_aggs_exists(func):
        def check(self, field):
            if self.aggs_build.is_field_aggs(field):
                return func(self, field)
            else:
                raise Exception('Aggregation of "%s" not exists in this bucket.' % field)
        return check

    @_child_aggs_exists
    def get_buckets(self, field):
        return AggsBody(self.aggs_build, self.bucket).get_buckets(field)

    @_child_aggs_exists
    def get_metric_value(self, field):
        '''Get metric value'''
        aggs_name = self.aggs_build.get_aggs_name(field)
        return self.bucket[aggs_name]['value']


if __name__ == '__main__':
    pass
