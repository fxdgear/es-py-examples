import git
import click
import logging
import datetime

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from utils import parse_commits

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
        ./run_example example1 --help

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
    print("Creating index named `git`...")
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
        doc_id = f"/git/doc/{_id}"
        # process the information from ES whether the document has been
        # successfully indexed
        if not ok:
            print(f"Failed to {action} document {doc_id}: {result}")
        else:
            print(doc_id)


if __name__ == "__main__":
    cli()
