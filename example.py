import git
import click
import logging
import datetime
import json
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from utils import parse_commits, print_hits, print_hit

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
def example1():
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
    print(json.dumps(create_index_body, indent=4))
    try:
        client.indices.create(index="git", body=create_index_body)
    except TransportError as e:
        # ignore already existing index
        if e.error == "resource_already_exists_exception":
            pass
        else:
            raise
    print("Complete!")


@cli.command()
def example2():
    """
    Process commits and index into our index.

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
def example3():
    """
    Load documents into ES from a CSV.

    Lets say we have a CSV file with data that we want to index.
    This CSV is structured in a way that there's a "header" row at the top
    the second row is the "data type" and the rest of the rows are just data.
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

        ret = bulk(client, reader, index="cars")
        print(f"Indexed {ret[0]}, {len(ret[1])} errored")


@cli.command()
def example4():
    """
    Simple Query
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
def example5():
    """
    Iterate over ALL results of a query.

    Sometimes we have a lot of data that returns from a particular query. We
    sometimes also want to see every single result. The Elasticsearch `scan`
    api is perfect for this. The python client offers a simple way to iterate
    over all your scroll results using the `scan` function.

    \b
    Your scan function will take at least 3 paramenters:
    1. client - an instantiated client object.
    2. query - The query you want to run.
    3. index - The index to run the query against.
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


if __name__ == "__main__":
    cli()
