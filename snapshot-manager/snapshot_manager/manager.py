import logging

from dbox import DropboxRemote
from util import hassio_request, list_snapshots


class SnapshotManager(object):
    def __init__(self, config):
        # instantiate and assign multiple uploaders (remotes)
        self.LOG = logging.getLogger("snapshot_manager")
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
        self.LOG.info(
            "Cleaning up local snapshots. Keeping latest %d snapshots.", keep)
        snapshots = list_snapshots()
        stale = snapshots[keep + 1:]
        for snapshot in stale:
            self.LOG.info("Deleting snapshot {slug}".format(snapshot["slug"]))
            path = "snapshots/{slug}/remove".format(snapshot["slug"])
            hassio_request(path, post=True)
