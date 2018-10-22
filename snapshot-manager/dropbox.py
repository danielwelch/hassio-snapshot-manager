import os
import hashlib

from .upload import (Remote, RemoteInitializationError, local_path, CHUNK_SIZE,
                     bytes_to_human)
import retrace
import arrow
import dropbox
from dropbox import DropboxAPI, exceptions


class DropboxRemote(Remote):
    def __init__(self, access_token):
        super().__init__()
        # initialize with dropbox api object
        self.dbx = DropboxAPI(access_token)
        self.dropbox_dir = self.remote_dir
        try:
            self.dbx.users_get_current_account()
        except exceptions.AuthError:
            self.LOG.error("Invalid Dropbox access token")
            raise RemoteInitializationError

    def _compute_dropbox_hash(self, filename):

        with open(filename, "rb") as f:
            block_hashes = b""
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                block_hashes += hashlib.sha256(chunk).digest()
            return hashlib.sha256(block_hashes).hexdigest()

    def _file_exists(self, file_path, dest_path):
        try:
            metadata = self.dbx.files_get_metadata(dest_path)
        except Exception:
            self.LOG.info("No existing snapshot in dropox with this name")
            return False

        dropbox_hash = metadata.content_hash
        local_hash = self._compute_dropbox_hash(file_path)
        if local_hash == dropbox_hash:
            return True

        # If the hash doesn't match, delete the file so we can re-upload it.
        # We might want to make this optional? a safer mode might be to
        # add a suffix?
        self.LOG.warn(
            "The snapshot conflicts with a file name in dropbox, the contents "
            "are different. The dropbox file will be deleted and replaced. "
            "Local hash: %s, Dropbox hash: %s",
            local_hash,
            dropbox_hash,
        )
        self.dbx.files_delete(dest_path)
        return False

    def _process_snapshot(self, snapshot):
        path = local_path(snapshot)
        created = arrow.get(snapshot["date"])
        size = bytes_to_human(os.path.getsize(path))
        target = str(self.remote_path(self.dropbox_dir, snapshot))
        self.LOG.info(f"Slug: {snapshot['slug']}")
        self.LOG.info(f"Created: {created}")
        self.LOG.info(f"Size: {size}")
        self.LOG.info(f"Uploading to: {target}")
        try:
            if self._file_exists(path, target):
                self.LOG.info("Already found in Dropbox with the same hash")
                return
            self._upload_file(self.dbx, path, target)
        except Exception:
            self.LOG.exception("Upload failed")

    @retrace.retry(limit=4)
    def _upload_file(self, file_path, dest_path):

        f = open(file_path, "rb")
        file_size = os.path.getsize(file_path)
        if file_size <= CHUNK_SIZE:
            return self.dbx.files_upload(f, dest_path)

        upload_session_start_result = self.dbx.files_upload_session_start(
            f.read(CHUNK_SIZE))
        cursor = dropbox.files.UploadSessionCursor(
            session_id=upload_session_start_result.session_id, offset=f.tell())
        commit = dropbox.files.CommitInfo(path=dest_path)
        prev = None
        while f.tell() < file_size:
            percentage = round((f.tell() / file_size) * 100)

            if not prev or percentage > prev + 5:
                self.LOG.info(f"{percentage:3} %")
                prev = percentage

            if (file_size - f.tell()) <= CHUNK_SIZE:
                self.dbx.files_upload_session_finish(
                    f.read(CHUNK_SIZE), cursor, commit)
            else:
                self.dbx.files_upload_session_append(
                    f.read(CHUNK_SIZE), cursor.session_id, cursor.offset)
                cursor.offset = f.tell()
        self.LOG.info("100 %")

    def upload(self, snapshot):
        self.LOG.info(f"Backing up to Dropbox directory: {self.dropbox_dir}")
        self._process_snapshot(self, snapshot)
