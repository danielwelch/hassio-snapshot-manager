import requests
import arrow
import os
import logging

AUTH_HEADERS = {"X-HASSIO-KEY": os.environ.get("HASSIO_TOKEN")}


def list_snapshots():
    snapshots = hassio_request("snapshots")["snapshots"]
    # Sort them by creation date, and reverse.
    # We want to backup the most recent first
    snapshots.sort(key=lambda x: arrow.get(x["date"]))
    snapshots.reverse()
    return snapshots


def hassio_request(path, post=False):
    if post is False:
        r = requests.get(f"http://hassio/{path}", headers=AUTH_HEADERS)
    else:

        r = requests.post(f"http://hassio/{path}", headers=AUTH_HEADERS)
    r.raise_for_status()
    j = r.json()
    LOG = logging.getLogger("snapshot_manager")
    LOG.debug(j)
    return j["data"]
