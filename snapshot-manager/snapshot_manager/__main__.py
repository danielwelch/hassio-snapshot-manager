import logging
import json
import pathlib
import sys

from manager import SnapshotManager
from util import list_snapshots

LOG = logging.getLogger("snapshot_manager")
CHUNK_SIZE = 4 * 1024 * 1024
DEFAULT_CONFIG_PATH = "/data/options.json"


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


def load_config(path=DEFAULT_CONFIG_PATH):
    with open(path) as f:
        return json.load(f)


def check_config(config):
    if not config.get(dropbox_access_token, None):  # extend with or
        LOG.exception("No remote destinations found in configuration")


def main():

    config = load_config(DEFAULT_CONFIG_PATH)
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
