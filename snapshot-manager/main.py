# TODO:
# keep_last functionality
# only keep X number of snapshots in dropbox, deleting older ones...
# extensibility of uploading (dropbox, google drive, etc.)

# object oriented
# watch backup dir with watchdog
# readline (always-on add-on that reads commands)

from dropbox import DropboxRemote
import logging
import pathlib
import os
import arrow
import requests
import json
import sys

LOG = logging.getLogger("snapshot_manager")
BACKUP_DIR = pathlib.Path("/backup/")
CHUNK_SIZE = 4 * 1024 * 1024
AUTH_HEADERS = {"X-HASSIO-KEY": os.environ.get("HASSIO_TOKEN")}
DEFAULT_CONFIG = "/data/options.json"


def setup_logging(config):
    log = logging.getLogger("snapshot_manager")
    log.setLevel(logging.DEBUG if config.get("debug") else logging.INFO)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    # Remove existing handlers. This should be an issue in unit tests.
    log.handlers = []
    log.addHandler(ch)
    return log


def hassio_get(path):
    r = requests.get(f"http://hassio/{path}", headers=AUTH_HEADERS)
    r.raise_for_status()
    j = r.json()
    LOG = logging.getLogger("snapshot_manager")
    LOG.debug(j)
    return j["data"]


def list_snapshots():
    snapshots = hassio_get("snapshots")["snapshots"]
    # Sort them by creation date, and reverse. We want to backup the most recent first
    snapshots.sort(key=lambda x: arrow.get(x["date"]))
    snapshots.reverse()
    return snapshots


def load_config(path=DEFAULT_CONFIG):
    with open(path) as f:
        return json.load(f)


class SnapshotManager(object):
    def __init__(self):
        pass
        # instantiate and assign multiple uploaders


def main():

    config = load_config(DEFAULT_CONFIG)
    setup_logging(config)

    # determine which remote destinations to instantiate
    remotes = []
    if config.get("dropbox_access_token", None):
        dbx = DropboxRemote(
            remote_dir=config.get("dropbox_dir", None),
            access_token=config["dropbox_access_token"])
        remotes.push(dbx)

    while True:
        msg_str = sys.stdin.readline()
        msg = json.loads(msg_str)
        try:
            cmd = msg["command"]
        except KeyError:
            LOG.exception("Improperly formatted message: %s", msg)

        if cmd == "backup":
            snapshots = list_snapshots()
            if not snapshots:
                LOG.warning("No snapshots found to backup")
                continue
            LOG.info(f"Backing up {len(snapshots)} snapshots")
            for i, snapshot in enumerate(snapshots, start=1):
                LOG.info(
                    f"Snapshot: {snapshot['name']} ({i}/{len(snapshots)})")
                for r in remotes:
                    r.upload(snapshot)
        else:
            LOG.exception("Unknown command: %s", cmd)
