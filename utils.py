from datetime import datetime


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
