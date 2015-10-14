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
        self._create_get_metric_func()

    def get_key_as_string(self):
        if 'key_as_string' not in self.bucket:
            raise Exception('key_as_string not exists in this bucket.')
        return self.bucket['key_as_string']

    def get_key(self):
        return self.bucket['key']

    def get_doc_count(self):
        return self.bucket['doc_count']

    def _check_field(self, field):
        if not self.aggs_build.is_field_aggs(field):
            raise Exception('Aggregation of "%s" not exists in this bucket.' % field)

    def get_buckets(self, field):
        self._check_field(field)
        return AggsBody(self.aggs_build, self.bucket).get_buckets(field)

    def _create_get_metric_func(self):
        def get_metric_func(aggs_type):
            def template(self, field):
                self._check_field(field)
                aggs_name = self.aggs_build.get_aggs_name(field, aggs_type)
                return self.bucket[aggs_name]['value']
            return template

        for metric in AggregationBuild.metric_set:
            setattr(self.__class__, 'get_'+metric, get_metric_func(metric))
        setattr(self.__class__, 'get_count', get_metric_func('cardinality'))


if __name__ == '__main__':
    pass
