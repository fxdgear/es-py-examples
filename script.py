# import these at the begining...

import git
import click
import datetime
import json
from utils import parse_commits, print_hits, print_hit, print_search_stats, pp
from elasticsearch.exceptions import TransportError

##


import logging

tracer = logging.getLogger("elasticsearch.trace")
tracer.setLevel(logging.INFO)
tracer.addHandler(logging.FileHandler("/tmp/es_trace.log"))

from elasticsearch import Elasticsearch

client = Elasticsearch()

# lets not repeat ourselves
user_mapping = {
    "properties": {"name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}}
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

client.indices.create(index="git", body=create_index_body)

from elasticsearch.helpers import bulk, streaming_bulk

repo = git.Repo("/elasticsearch-py")

for ok, result in streaming_bulk(
    client,
    parse_commits(repo.refs.master.commit, "elasticsearch-py"),
    index="git",
    chunk_size=50,
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
client.indices.refresh(index="git")

initial_commit = client.get(index="git", id="20fbba1230cabbc0f4644f917c6c2be52b8a63e8")

# and now we can count the documents
print(client.count(index="git")["count"], "documents in index")

import csv

with open("cars.csv") as csvfile:
    reader = csv.DictReader(csvfile, delimiter=";")
    ret = bulk(client, reader, index="cars")


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

from elasticsearch.helpers import scan

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

for key, value in client.cluster.health().items():
    print(f"\t{key}: {value}")

for key, value in client.cluster.health(index="git").items():
    print(f"\t{key}: {value}")

results = client.cat.health(
    format="json", h="timestamp,status,node.total,active_shards_percent"
)

print(results)

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


######################


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
