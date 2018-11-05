import logging
import os
import pathlib

LOG = logging.getLogger("snapshot_manager")
BACKUP_DIR = pathlib.Path("/backup/")
CHUNK_SIZE = 4 * 1024 * 1024
AUTH_HEADERS = {"X-HASSIO-KEY": os.environ.get("HASSIO_TOKEN")}
DEFAULT_CONFIG = "/data/options.json"


class RemoteInitializationError(Exception):
    pass


class Remote(object):
    def __init__(self, remote_dir, use_filename=False):
        # directory in which to store snapshot files
        self.remote_dir = remote_dir
        # use display name instead of slug for remote file path
        self.use_filename = use_filename
        self.LOG = logging.getLogger("snapshot_manager")

    def upload(self, snapshot):
        raise NotImplementedError

    def clean_remote(self, snapshot):
        # remove certain snapshot from remote location
        # Maybe keep_last instead
        raise NotImplementedError

    def remote_path(self, snapshot):
        if self.use_filename:
            return self.remote_dir / f"{snapshot['name']}.tar"
        else:
            return self.remote_dir / f"{snapshot['slug']}.tar"


def bytes_to_human(nbytes):
    suffixes = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    while nbytes >= 1024 and i < len(suffixes) - 1:
        nbytes /= 1024.
        i += 1
    f = ("%.2f" % nbytes).rstrip("0").rstrip(".")
    return "%s %s" % (f, suffixes[i])


def local_path(snapshot):
    return BACKUP_DIR / f"{snapshot['slug']}.tar"


def remote_path(remote_dir, snapshot, use_filename=False):
    if use_filename:
        return remote_dir / f"{snapshot['name']}.tar"
    else:
        return remote_dir / f"{snapshot['slug']}.tar"
