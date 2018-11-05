# TODO:
# keep_last functionality => clean_local and keep_local option
# only keep X number of snapshots in dropbox, deleting older ones...
# extensibility of uploading (dropbox, google drive, etc.)
# validate input
# expose a string format for file name?

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


def list_snapshots():
    snapshots = hassio_request("snapshots")["snapshots"]
    # Sort them by creation date, and reverse. We want to backup the most recent first
    snapshots.sort(key=lambda x: arrow.get(x["date"]))
    snapshots.reverse()
    return snapshots


def load_config(path=DEFAULT_CONFIG):
    with open(path) as f:
        return json.load(f)


class SnapshotManager(object):
    def __init__(self, config):
        # instantiate and assign multiple uploaders (remotes)
        self.remotes = []
        if config.get("dropbox_access_token", None):
            dbx = DropboxRemote(
                remote_dir=config.get("dropbox_dir", None),
                use_filename=config.get("use_filename", False),
                access_token=config["dropbox_access_token"])
            self.remotes.push(dbx)

    def remotes(self):
        return self.remotes

    def clean_local(self, keep):
        # keep only the latest X number of snapshots, delete the rest
        LOG.info("Cleaning up local snapshots. Keeping latest %d snapshots.",
                 keep)
        snapshots = list_snapshots()
        stale = snapshots[keep + 1:]
        for snapshot in stale:
            LOG.info("Deleting snapshot {slug}".format(snapshot["slug"]))
            path = "snapshots/{slug}/remove".format(snapshot["slug"])
            hassio_request(path, post=True)


def main():

    config = load_config(DEFAULT_CONFIG)
    setup_logging(config)

    manager = SnapshotManager(config)

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
                for r in manager.remotes():
                    r.upload(snapshot)
        elif cmd == "clean_local":
            try:
                manager.clean_local(config["keep_local"])
            except KeyError:
                LOG.exception(
                    "Cannot clean up local backups: keep_local option is not set."
                )
        else:
            LOG.exception("Unknown command: %s", cmd)
