from elasticsearch_dsl import Text, Keyword, Document, Index, Date


index_settings = {"similarity": {"default": {"type": "BM25"}}}


class Base(Document):
    cord_uid = Keyword()
    body = Text()
    title = Keyword()
    publish_time = Date()
    url = Keyword()
    journal = Keyword()
    authors = Keyword()


class Paper(Base):
    class Index:
        name = "papers"
        settings = index_settings


class Paper_with_abs(Base):
    abstract = Text()

    class Index:
        name = "papers"
        settings = index_settings


class Paragraph(Base):
    paragraph_id = Keyword()

    class Index:
        name = "paragraphs"
        settings = index_settings


class Paragraph_with_abs(Base):
    abstract = Text()
    paragraph_id = Keyword()

    class Index:
        name = "paragraphs"
        settings = index_settings


class Abstract(Base):
    class Index:
        name = "abstracts"
        settings = index_settings


index_map = {
    "papers": Paper,
    "paragraphs": Paragraph,
    "abstracts": Abstract,
}


index_with_abs_map = {
    "papers": Paper_with_abs,
    "paragraphs": Paragraph_with_abs,
}


def init_index(with_abs=False):
    global index_map, index_with_abs_map
    index_dict = index_with_abs_map if with_abs else index_map
    for name, index in index_dict.items():
        if not Index(name).exists():
            index.init()


"""
def get_or_create_index(index_name="papers", addr="localhost", port=9200):
    connections.create_connection(hosts=[f"{addr}:{port}"])
    es = connections.get_connection()  # noqa: 841
    # Check if index already exists
    index = Index(index_name)
    if not index.exists():
        # Define analyzer
        my_analyzer = analyzer(
            'my_analyzer',
            type="standard",
            stopwords='_english_'
        )
        mapping = get_mapping()
        index.settings(**{"similarity.default.type": "BM25"})
        index.analyzer(my_analyzer)
        index.mapping(mapping)
        index.create()
    return index
"""
