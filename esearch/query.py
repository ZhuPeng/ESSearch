class SearchBodyBuild():
    def __init__(self):
        self.query_build = QueryBuild()
        self.aggs_build = AggregationBuild()
        self._create_add_metric_func()

    def must(self, field, value):
        self.query_build.add_condition('must', field, value)
        return self

    def should(self, field, value):
        self.query_build.add_condition('should', field, value)
        return self

    def range(self, field, start, end):
        self.query_build.add_condition('range', field, (start, end))
        return self

    def groupby(self, field, do_nest=True):
        self.aggs_build.update_aggs(field)
        return self

    def groupby_date(self, date_field, interval, do_nest=True):
        self.aggs_build.update_aggs((date_field, interval), 'date_histogram')
        return self

    def _create_add_metric_func(self):
        def metric_func(aggs_type):
            def template(self, field, do_nest=False):
                self.aggs_build.update_aggs(field, aggs_type, do_nest)
                return self
            return template

        for metric in AggregationBuild.metric_set:
            setattr(self.__class__, metric, metric_func(metric))
        setattr(self.__class__, 'count', metric_func('cardinality'))

    def get_body(self):
        self.body = {}
        self.body.update(self.query_build.get_body())
        self.body.update(self.aggs_build.get_body())
        return self.body


class QueryBuild():
    def __init__(self):
        self.query_body = {
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

    def add_condition(self, name, filed, value):
        getattr(self, '_add_' + name)(filed, value)

    def _add_must(self, field, value):
        condition = {"term": {field: value}}
        self.query_body['bool']['must'].append(condition)

    def _add_range(self, field, range_value):
        condition = {"range": {field: {"gte": range_value[0], "lte": range_value[1]}}}
        self.query_body['bool']['must'].append(condition)

    def _add_should(self, field, value):
        condition = {"term": {field: value}}
        self.query_body['bool']['must'][0]['bool']['should'].append(condition)

    def get_body(self):
        return {'query': self.query_body}


class AggregationBuild():
    name_sep = '-'
    metric_set = set(['sum', 'avg', 'stats', 'min', 'max',
                      'value_count', 'cardinality'])

    def __init__(self):
        self.aggs_body = {}
        self.aggs_level = []
        self.aggs_type_dict = {}

    @staticmethod
    def is_metric(aggs_type):
        return aggs_type in AggregationBuild.metric_set

    @staticmethod
    def is_bucket(aggs_type):
        return aggs_type in set(['terms', 'date_histogram'])

    @staticmethod
    def is_bucket_byname(aggs_name):
        return AggregationBuild.is_bucket(aggs_name.split(AggregationBuild.name_sep)[-1])

    def is_field_aggs(self, field):
        return field in self.aggs_type_dict

    def get_aggs_name(self, field, aggs_type=None):
        if not aggs_type:
            aggs_type = self.aggs_type_dict[field]
            if len(aggs_type) > 1:
                raise Exception('There exists multi aggregation type work on this field, please select a certain one.')
            aggs_type = aggs_type[0]
        return field + AggregationBuild.name_sep + aggs_type

    def keep_field_aggs_type(self, field, aggs_type):
        if field not in self.aggs_type_dict:
            self.aggs_type_dict[field] = []
        self.aggs_type_dict[field].append(aggs_type)

    def _build_metric(self, field, metric):
        return {metric: {'field': field}}

    def _build_terms(self, field, terms):
        return {terms: {'field': field, 'size': 0}}

    def _build_date_histogram(self, date_tuple, date_histogram):
        return {
            date_histogram: {
                'field': date_tuple[0],
                'interval': date_tuple[1]
            }
        }

    def update_aggs(self, field, aggs_type='terms', do_nest=True):
        if AggregationBuild.is_metric(aggs_type):
            build_func_sufix = 'metric'
        else:
            build_func_sufix = aggs_type

        if type(field) is tuple:
            new_aggs_name = field[0] + self.name_sep + aggs_type
            self.keep_field_aggs_type(field[0], aggs_type)
        else:
            new_aggs_name = field + self.name_sep + aggs_type
            self.keep_field_aggs_type(field, aggs_type)

        new_aggs_body = getattr(self, '_build_'+build_func_sufix)(field, aggs_type)
        self._update_aggs_body(new_aggs_name, new_aggs_body, do_nest)

    def _update_aggs_body(self, new_aggs_name, new_aggs_body, do_nest):
        last_level_aggs, last_level_name = self.find_last_aggs()
        new_aggs_body = {new_aggs_name: new_aggs_body}

        if do_nest:
            if last_level_aggs:
                last_level_aggs[last_level_name]['aggs'] = new_aggs_body
            else:
                last_level_aggs['aggs'] = new_aggs_body
            self.aggs_level.append([new_aggs_name])

        else:
            depth = len(self.aggs_level) - 1
            if depth < 0:
                self.aggs_level.append([new_aggs_name])
                last_level_aggs['aggs'] = new_aggs_body
            else:
                self.aggs_level[depth].append(new_aggs_name)
                last_level_aggs.update(new_aggs_body)

    def find_last_aggs(self):
        if not self.aggs_level:
            return (self.aggs_body, '')

        traverse = self.aggs_body['aggs']
        last_level_name = self.aggs_level[0][0]
        for level in self.aggs_level[1:]:
            traverse = traverse[last_level_name]['aggs']
            last_level_name = level[0]
        return traverse, last_level_name

    def get_body(self):
        return self.aggs_body

if __name__ == '__main__':
    pass
