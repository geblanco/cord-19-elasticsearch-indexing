from elasticsearch_dsl import connections


def get_connection(addr="localhost", port=9200):
    connections.create_connection(hosts=[f"{addr}:{port}"])
    return connections.get_connection()
