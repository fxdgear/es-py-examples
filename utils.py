from datetime import datetime
from dateutil.parser import parse as parse_date


def parse_commits(head, name):
    """
    Go through the git repository log and generate a document per commit
    containing all the metadata.
    """
    for commit in head.traverse():
        yield {
            "_id": commit.hexsha,
            "repository": name,
            "committed_date": datetime.fromtimestamp(commit.committed_date),
            "committer": {
                "name": commit.committer.name,
                "email": commit.committer.email,
            },
            "authored_date": datetime.fromtimestamp(commit.authored_date),
            "author": {"name": commit.author.name, "email": commit.author.email},
            "description": commit.message,
            "parent_shas": [p.hexsha for p in commit.parents],
            # we only care about the filenames, not the per-file stats
            "files": list(commit.stats.files),
            "stats": commit.stats.total,
        }


def print_search_stats(results):
    print("=" * 80)
    print(
        "Total %d found in %dms" % (results["hits"]["total"]["value"], results["took"])
    )
    print("-" * 80)


def print_hit(hit):
    created_at = parse_date(
        hit["_source"].get("created_at", hit["_source"]["authored_date"])
    )
    print(
        "/%s/%s/%s (%s): %s"
        % (
            hit["_index"],
            hit["_type"],
            hit["_id"],
            created_at.strftime("%Y-%m-%d"),
            hit["_source"]["description"].split("\n")[0],
        )
    )


def print_hits(results):
    " Simple utility function to print results of a search query. "
    print_search_stats(results)
    for hit in results["hits"]["hits"]:
        # get created date for a repo and fallback to authored_date for a commit
        print_hit(hit)
    print("=" * 80)


print()
