import git
import click
import logging
import datetime
import json
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from utils import parse_commits, print_hits, print_hit, print_search_stats, pp

tracer = logging.getLogger("elasticsearch.trace")
tracer.setLevel(logging.INFO)
tracer.addHandler(logging.FileHandler("/tmp/es_trace.log"))


@click.group()
def cli():
    """
    Welcome to the Elasticsearch Python Client example workshop!

    This will be a series of examples we can execute!

    This workshop is broken down into a number of examples: example1, example2, etc..

    \b
    To understand more of each example please run the `--help` for that example.
        ./run_example example1 --help

    \b
    Then you can run the example.
        ./run_example example1

    Each example is idempotent, which means you
    can run these examples over and over again.

    """
    pass


@cli.command()
def example01():
    """
    Create an index for indexing git commits.

    We will use the elasticsearch-py git repo for our example.

    First we will need to create settings and mappings for our index. Since this
    is an example cluster, lets create the cluster with 1 shard and 0 replicas.
    What fields are we going to be indexing from commit data?

    \b
    1. repository: Repository name
      * This should be a keyword field

    2. author: The author of the commit
      * This should be both a keyword and a text field. We want full text and keyword on this field

    3. authored_date: The date the commit was
      * Datetime field

    4. commiter: The person who commited the code on behalf of the author (usually the same person)
      * This should be both a keyword and a text field. We want full text and keyword on this field

    5. committed_date: the date of the commit
      * Datetime field

    6. parent_shas: the SHA of any parents to this commit.
      * This should be a keyword field

    7. description: the commit description
      * We really want full text search on the description

    8. files: the files modified by this commit
      * We want full text search on files, but we also want to use a field_path analyzer on this data.
    """

    client = Elasticsearch()

    # lets not repeat ourselves
    user_mapping = {
        "properties": {
            "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}
        }
    }

    create_index_body = {
        "settings": {
            # just one shard, no replicas for testing
            "number_of_shards": 1,
            "number_of_replicas": 0,
            # custom analyzer for analyzing file paths
            "analysis": {
                "analyzer": {
                    "file_path": {
                        "type": "custom",
                        "tokenizer": "path_hierarchy",
                        "filter": ["lowercase"],
                    }
                }
            },
        },
        "mappings": {
            "properties": {
                "repository": {"type": "keyword"},
                "author": user_mapping,
                "authored_date": {"type": "date"},
                "committer": user_mapping,
                "committed_date": {"type": "date"},
                "parent_shas": {"type": "keyword"},
                "description": {"type": "text", "analyzer": "snowball"},
                "files": {"type": "text", "analyzer": "file_path", "fielddata": True},
            }
        },
    }

    # create empty index
    print("Creating index named `git` with following settings:")
    print(json.dumps(create_index_body, indent=2))
    try:
        resp = client.indices.create(index="git", body=create_index_body)
    except TransportError as e:
        # ignore already existing index
        if e.error == "resource_already_exists_exception":
            pass
        else:
            raise
    print("Complete!")


@cli.command()
def example02():
    """
    Eample using streaming_bulk.

    In this example we're using a function `parse_commits` to parse our commits and
    create a json repersentation of the data for indexing into ES.

    Streaming bulk consumes actions from the iterable passed in and yields
    results per action.

    \b
    How we use streaming_bulk:

    * First we need a `client` object.
    * Next we need an interable. Any iterable will work. A list, a dictionary, or
    in this case a generator.
        A generator is an object that can be looped over and will `yield` the next computed result without
        having to compute all results and store them in memory.
    * Next an `index` to index the data into
    * And finally a `chunk_size`. Which is how many items we want our bulk requests to send.

    In our example, after each bulk request we will print out the results of each request.

    """
    client = Elasticsearch()
    repo_name = "elasticsearch-py"
    repo = git.Repo("/elasticsearch-py")

    from elasticsearch.helpers import bulk, streaming_bulk

    # we let the streaming bulk continuously process the commits as they come
    # in - since the `parse_commits` function is a generator this will avoid
    # loading all the commits into memory
    for ok, result in streaming_bulk(
        client,
        parse_commits(repo.refs.master.commit, repo_name),
        index="git",
        chunk_size=50,  # keep the batch sizes small for appearances only
    ):
        action, result = result.popitem()
        _id = result["_id"]
        doc_id = f"/git/_doc/{_id}"
        # process the information from ES whether the document has been
        # successfully indexed
        if not ok:
            print(f"Failed to {action} document {doc_id}: {result}")
        else:
            print(doc_id)

    # We need to update a few documents for ease of use in later examples
    UPDATES = [
        {
            "_type": "_doc",
            "_id": "20fbba1230cabbc0f4644f917c6c2be52b8a63e8",
            "_op_type": "update",
            "doc": {"initial_commit": True},
        },
        {
            "_type": "_doc",
            "_id": "ae0073c8ca7e24d237ffd56fba495ed409081bf4",
            "_op_type": "update",
            "doc": {"release": "5.0.0"},
        },
    ]

    success, _ = bulk(client, UPDATES, index="git")
    print(f"Performed {success} actions")
    client.indices.refresh(index="git")

    initial_commit = client.get(
        index="git", id="20fbba1230cabbc0f4644f917c6c2be52b8a63e8"
    )
    print(f"{initial_commit['_id']}: {initial_commit['_source']['committed_date']}")

    # and now we can count the documents
    print(client.count(index="git")["count"], "documents in index")


@cli.command()
def example03():
    """
    Load documents into ES from a CSV.

    Lets say we have a CSV file with data that we want to index.
    This CSV is structured in a way that there's a "header" row at the top
    the second row is the "data type" and the rest of the rows are just data.

    \b
    Car;MPG;Cylinders;Displacement;Horsepower;Weight;Acceleration;Model;Origin
    STRING;DOUBLE;INT;DOUBLE;DOUBLE;DOUBLE;DOUBLE;INT;CAT
    Chevrolet Chevelle Malibu;18.0;8;307.0;130.0;3504.;12.0;70;US

    First step will be to actaully create our index with our mappings.
    Using `DictReader` we can map elements in our data rows
    to their field name from the "header" row.

    This allows us to pass our csv reader object into the `bulk` function.
    The `reader` object is an interable which is what the `bulk` function
    expects.

    """
    import csv
    from elasticsearch.helpers import bulk

    client = Elasticsearch()

    with open("cars.csv") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=";")
        header = reader.fieldnames
        types = next(reader)
        create_index_body = {
            "settings": {
                # just one shard, no replicas for testing
                "number_of_shards": 1,
                "number_of_replicas": 0,
                # custom analyzer for analyzing file paths
            },
            "mappings": {"properties": {}},
        }

        for field_name, ftype in types.items():
            if ftype == "STRING":
                mapping_type = "text"
            elif ftype == "CAT":
                mapping_type = "keyword"
            elif ftype == "INT":
                mapping_type = "integer"
            elif ftype == "DOUBLE":
                mapping_type = "double"

            create_index_body["mappings"]["properties"][field_name] = {
                "type": mapping_type
            }
        # create empty index
        print("Creating index named `cars`...")
        try:
            client.indices.create(index="cars", body=create_index_body)
        except TransportError as e:
            # ignore already existing index
            if e.error == "resource_already_exists_exception":
                pass
            else:
                raise

        # Pass in the reader object to `bulk` to send the rest of the
        # data to Elasticsearch.
        ret = bulk(client, reader, index="cars")
        print(f"Indexed {ret[0]}, {len(ret[1])} errored")


@cli.command()
def example04():
    """
    Simple Query

    Sending a query to Elasticsearch via the python client is straight
    forward, if you have ever sent a search query to Elasticsearch.

    The `search` method will expect the following parameters:

    \b
    * `index` - The index we want to query
    * `body` - The query we want to use, represented as a Python dict.
        ie:
            {"query": {}}
    """
    client = Elasticsearch()
    print('Find commits that says "fix" without touching tests:')
    result = client.search(
        index="git",
        body={
            "query": {
                "bool": {
                    "must": {"match": {"description": "fix"}},
                    "must_not": {"term": {"files": "test_elasticsearch"}},
                }
            }
        },
    )
    print_hits(result)


@cli.command()
def example05():
    """
    Iterate over ALL results of a query.

    Sometimes we have a lot of data that returns from a particular query. We
    sometimes also want to see every single result. The Elasticsearch `scroll`
    api is perfect for this. The python client offers a simple way to iterate
    over all your scroll results using the `scan` function.

    \b
    The `scan` function will take at least 3 paramenters:
    1. `client` - an instantiated client object.
    2. `query` - The query you want to run.
    3. `index` - The index to run the query against.

    \b
    ie:
        scan(client, index=index_name, query={})
    """

    import csv
    from elasticsearch.helpers import scan

    client = Elasticsearch()

    results = scan(
        client,
        query={
            "query": {
                "bool": {
                    "must": {"match": {"description": "fix"}},
                    "must_not": {"term": {"files": "test_elasticsearch"}},
                }
            }
        },
        index="git",
    )
    for result in results:
        print_hit(result)


@cli.command()
def example06():
    """
    Example Aggregation

    Creating an aggregation query in the python client is just as easy
    as creating a query.

    The search function is exactly the same, except we are going to send
    an aggregation searh as our `body`.

    Again our `search` method requires at least:

    1. `index` to search
    2. `body` which is our query to send to Elasticsearch.

    \b
    ie:
        body={
            "size": 0,
            "aggs": {
                "committers": {
                    "terms": {"field": "committer.name.keyword"},
                    "aggs": {"line_stats": {"stats": {"field": "stats.lines"}}},
                }
            },

    """

    client = Elasticsearch()
    print("Stats for top 10 committers:")
    result = client.search(
        index="git",
        body={
            "size": 0,
            "aggs": {
                "committers": {
                    "terms": {"field": "committer.name.keyword"},
                    "aggs": {"line_stats": {"stats": {"field": "stats.lines"}}},
                }
            },
        },
    )

    print_search_stats(result)
    for committer in result["aggregations"]["committers"]["buckets"]:
        print(
            "%15s: %3d commits changing %6d lines"
            % (committer["key"], committer["doc_count"], committer["line_stats"]["sum"])
        )
    print("=" * 80)


@cli.command()
def example07():
    """
    Cluster health

    The low level python client also exposes all the api's that Elasticsearch
    supports.

    For example we can get our cluster health.

    \b
        client.cluster.health()

    The `health` function takes all the same parameters that the RESt api
    takes, so we can get the health of an index with:

    \b
        client.cluster.health(index="git")

    """

    client = Elasticsearch()
    print("Cluster health for entire cluser:")
    print("=" * 40)
    for key, value in client.cluster.health().items():
        print(f"\t{key}: {value}")

    client = Elasticsearch()
    print("\n\nCluster health for `git` index:")
    print("=" * 40)
    for key, value in client.cluster.health(index="git").items():
        print(f"\t{key}: {value}")


@cli.command()
def example08():
    """
    Cat API's

    Elasticsearch CAT API's are super useful on the CLI. But they can
    also be really useful in the python client, if you use the `format=json`
    parameter.

    You will get back all the pre-formatted in JSON for you which can be really
    handy to interate over.
    """

    client = Elasticsearch()
    print("Cat health example with headers:")
    print("=" * 80)

    print(
        json.dumps(
            client.cat.health(
                format="json", h="timestamp,status,node.total,active_shards_percent"
            ),
            indent=2,
        )
    )


@cli.command()
def example09():
    """
    XPack APIs

    Using the SQL api we can query the Elasticsearch cluster using SQL query syntax.

    Using the `query` method we can query our cluster with SQL syntax.
    \b
        body={
            "query": "SELECT description,authored_date FROM git WHERE author.name='Nick Lang' LIMIT 5"
        }

    Now that we know how to get the data we want we can easily translate our
    SQL query to Elasticearch DSL using the `translate` funtion, it takes the
    exact same parameters are our `query` method, except this time it will
    retun a json object representing our ES DSL query syntax

    """
    client = Elasticsearch()
    result = client.xpack.sql.query(
        body={
            "query": "SELECT description,authored_date FROM git WHERE author.name='Nick Lang' LIMIT 5"
        }
    )
    columns = result["columns"]
    rows = result["rows"]
    print("=" * 80)
    print("Authored Date            \t| Description")
    print("=" * 80)
    for row in rows:
        print(f"{row[1]}\t| {row[0]}")
        print("=" * 80)

    result = client.xpack.sql.translate(
        body={
            "query": "SELECT description,authored_date FROM git WHERE author.name='Nick Lang' LIMIT 5"
        }
    )

    print("=" * 80)
    print("Translate our SQL query to Elasticearch DSL query syntax")
    print("=" * 80)
    print("")
    print(json.dumps(result, indent=2))


@cli.command()
def example10():
    """
    DSL objects for common entities instead of dict/json.
    All importable from elasticsearch_dsl
    """
    from elasticsearch_dsl import Q, Search

    """
    Straightforward mapping to json - kwargs are translated into keys into json.
    You can use the to_dict() method to see the result json.
    """

    q = Q("terms", tags=["python", "search"])
    q.to_dict()

    """
    All objects can also be constructed using the raw dict.
    """

    q = Q({"terms": {"tags": ["python", "search"]}})
    q.to_dict()

    """
    Query objects support logical operators which result in bool queries
    """
    q = q | Q("match", title="python")
    q.to_dict()

    """
    DSL objects also allow for attribute access instead of ['key']
    """
    q.minimum_should_match = 2
    q.minimum_should_match
    q.to_dict()

    from datetime import date

    q = q & Q("range", **{"@timestamp": {"lt": date(2019, 1, 1)}})
    q.to_dict()

    """
    Configuration is global so no client needs to be passed around.
    """
    from elasticsearch_dsl import connections

    """
    Default connection used where no other connection specified. Any configuration
    methods just pass all parameters to the underlying elasticsearch-py client.
    """
    connections.create_connection(hosts=["localhost"])

    """
    Optionally specify an alias for the connection in case of multiple connections.
    """
    connections.create_connection("prod", hosts=["localhost"])
    s = Search(using="prod")
    s.count()

    """
    You can always just pass in your own client instance
    """
    s = Search(using=Elasticsearch())
    s.count()

    """
    Any method on Search returns a clone so you need to always assign it back to
    the same variable.
    """
    s = Search()
    s = s.params(q="fix")

    """
    Multiple queries are combined together using the AND operator
    """
    s = Search()
    s = s.query("match", description="fix")
    s = s.query("match", author="Honza")

    """
    Filter shortcut to use {bool: {filter: []}}
    """
    s = s.filter("range", committed_date={"lt": date(2016, 1, 1)})
    s.to_dict()

    """
    Exclude as a wrapper around must_not, use __ instead of dots for convenience.
    """
    s = s.exclude("term", committer__name__keyword="Honza Král")

    """
    Search is executed when iterated on or when .execute() is called.
    """
    for hit in s:
        """
        Hit class offers direct access to fields and via .meta any other properties
        on the returned hit (_id, _seq_no, ...)
        """
        print(f"{hit.meta.id[:6]} ({hit.author.name}): {hit.description[:50]}")

    """
    Aggregations are implemented in place to allow for chaining
    """
    s = Search(index="git")
    s.aggs.bucket("tags", "terms", field="terms").metric(
        "lines", "sum", field="stats.lines"
    ).metric("authors", "cardinality", field="author.name.keyword")
    r = s.execute()

    """
    Or modify aggregation in place
    """
    s.aggs["tags"].bucket(
        "months", "date_histogram", field="committed_date", interval="month"
    )

    """
    Analysis
    """

    from elasticsearch_dsl import analyzer, token_filter

    a = analyzer(
        "file_analyzer",
        tokenizer="path_hierarchy",
        filter=[
            "lowercase",
            token_filter(
                "split_ext",
                "pattern_capture",
                preserve_original=True,
                patterns=[r"^([^\.]+)"],
            ),
        ],
    )

    a.simulate("test/integration/search.py")

    """
    """

    from elasticsearch_dsl import Document, Text, Keyword, InnerDoc, Date, Nested

    class FileDiff(InnerDoc):
        filename = Text(analyzer=a)
        patch = Text()

    class Commit(Document):
        description = Text()
        committed_date = Date()
        author = Text(fields={"keyword": Keyword()})

        files = Nested(FileDiff)

        def subject(self):
            return self.description.split("\n", 1)[0][:80]

        class Index:
            name = "git*"
            settings = {"number_of_replicas": 0}

    """
    Create the index
    """

    Commit.init(index="git-v2")

    """
    Search now returns Commit objects
    """
    for c in Commit.search():
        print(f"{c.meta.id}: {c.subject()}")


if __name__ == "__main__":
    cli()
