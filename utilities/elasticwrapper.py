from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


class ElasticWrapper:
    @staticmethod
    def delete_index(index_name):
        if es.indices.exists(index_name):
            es.indices.delete(index=index_name, params={'timeout': '1000s'})
            return True
        return False

    @staticmethod
    def create_index_if_not_exist(index_name):
        if not es.indices.exists(index_name):
            es.indices.create(index=index_name, params={'timeout': '1000s'})
            return True
        return False

    @staticmethod
    def search(index_name, body):
        docs = es.search(index=index_name, body=body)
        return ElasticWrapper._parse_response_docs(docs)

    @staticmethod
    def bulk_index_docs(docs, index_name):
        bulk_request = [e for doc_operation in map(lambda d: [{
            'index': {
                '_index': index_name,
                '_id': d['id']
            }},
            d
        ], docs) for e in doc_operation]

        partitions_size = 1000
        start = 0
        while True:
            es.bulk(body=bulk_request[start:start +
                                            partitions_size], request_timeout=1000)

            start += partitions_size
            if start > len(bulk_request):
                break

    @staticmethod
    def index_doc(doc, id, index):
        es.index(index=index, id=id, body=doc)

    @staticmethod
    def get_all_documents_recursively(index, body={'size': 10000}):
        '''
            uses the method of scrolling to extract all documents (>10000) of an index with a query
        '''
        outputs = []

        resp = es.search(index=index, body=body, scroll='1m')
        outputs.extend(ElasticWrapper._parse_response_docs(resp))

        while len(resp['hits']['hits']) > 0:
            print(f'{len(outputs)} docs extracted!')
            resp = es.scroll(scroll_id=resp['_scroll_id'], scroll='1m')
            outputs.extend(ElasticWrapper._parse_response_docs(resp))

        return outputs

    @staticmethod
    def _parse_response_docs(resp):
        return [x['_source'] for x in resp['hits']['hits']]


__all__ = ['ElasticWrapper']
