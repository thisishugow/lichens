import os
from smbprotocol.connection import Connection
from smbprotocol.open import FilePipePrinterAccessMask
from smbprotocol.header import NtStatus
from smbprotocol.file_info import FileInformationClass, FileNamesInformation
import glob
from fnmatch import fnmatch

class SambaServer:
    """
    A class for interacting with a Samba server for file transfers.

    Args:
        server_name (str): The name or IP address of the Samba server.
        share_name (str): The name of the shared folder on the Samba server.
        username (str): The username for authentication.
        password (str): The password for authentication.

    Attributes:
        server_name (str): The name or IP address of the Samba server.
        share_name (str): The name of the shared folder on the Samba server.
        username (str): The username for authentication.
        password (str): The password for authentication.
        connection (smbprotocol.connection.Connection): The SMB protocol connection.
    """

    def __init__(self, server_name: str, share_name: str, username: str, password: str):
        self.server_name = server_name
        self.share_name = share_name
        self.username = username
        self.password = password
        self.connection:Connection = Connection()

    def connect(self):
        """
        Connect to the Samba server.
        """
        self.connection.connect(server_name=self.server_name, share_name=self.share_name, username=self.username, password=self.password)

    def disconnect(self):
        """
        Disconnect from the Samba server.
        """
        self.connection.disconnect()

    def upload_file(self, local_path: os.PathLike, remote_path: str):
        """
        Upload a single file to the Samba server.

        Args:
            local_path (os.PathLike): The local path of the file to upload.
            remote_path (str): The remote path on the Samba server to save the file.
        """
        with open(local_path, 'rb') as local_file:
            with self.connection.create_file(
                    remote_path,
                    access_mask=FilePipePrinterAccessMask.FILE_WRITE_DATA,
                    disposition=FilePipePrinterAccessMask.FILE_OVERWRITE_IF,
            ) as file_handle:
                file_handle.write(local_file.read())
        print(f"File '{local_path}' uploaded to '{remote_path}'.")

    def download_file(self, remote_path: str, local_path: os.PathLike):
        """
        Download a single file from the Samba server.

        Args:
            remote_path (str): The remote path on the Samba server to download.
            local_path (os.PathLike): The local path to save the downloaded file.
        """
        with self.connection.open_file(
                remote_path,
                access_mask=FilePipePrinterAccessMask.FILE_READ_DATA,
        ) as file_handle:
            with open(local_path, 'wb') as local_file:
                local_file.write(file_handle.read())
        print(f"File '{remote_path}' downloaded to '{local_path}'.")

    def batch_upload(self, src: os.PathLike, dst: str, filter_regex: str = None, *args, **kwargs):
        """
        Upload all files from the source directory to the destination directory on the Samba server.

        Args:
            src (os.PathLike): The local source directory containing files to upload.
            dst (str): The remote destination directory on the Samba server.
            filter_regex (str, optional): A regular expression to filter files. Defaults to None.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        os.chdir(src)
        files_to_upload = glob(filter_regex) if filter_regex else glob('*')

        for file in files_to_upload:
            local_file_path = os.path.join(src, file)
            remote_file_path = os.path.join(dst, file)

            with open(local_file_path, 'rb') as local_file:
                with self.connection.create_file(
                        remote_file_path,
                        access_mask=FilePipePrinterAccessMask.FILE_WRITE_DATA,
                        disposition=FilePipePrinterAccessMask.FILE_OVERWRITE_IF,
                ) as file_handle:
                    file_handle.write(local_file.read())
        print("Batch upload completed.")

    def batch_download(self, src: str, dst: os.PathLike, filter_regex: str = None, *args, **kwargs):
        """
        Download all files from the source directory on the Samba server to the local destination directory.

        Args:
            src (str): The remote source directory on the Samba server.
            dst (os.PathLike): The local destination directory to save downloaded files.
            filter_regex (str, optional): A regular expression to filter files. Defaults to None.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        os.chdir(dst)
        files_to_download = self.list_files(src)

        for file in files_to_download:
            if filter_regex and not fnmatch(file, filter_regex):
                continue

            remote_file_path = os.path.join(src, file)
            local_file_path = os.path.join(dst, file)

            with self.connection.open_file(
                    remote_file_path,
                    access_mask=FilePipePrinterAccessMask.FILE_READ_DATA,
            ) as file_handle:
                with open(local_file_path, 'wb') as local_file:
                    local_file.write(file_handle.read())
        print("Batch download completed.")

    

    def list_files(self, src: str):
        """
        List files in the specified directory on the Samba server.

        Args:
            src (str): The remote directory path on the Samba server.

        Returns:
            list: A list of filenames in the specified directory.
        """
        files = []
        with self.connection.create_file(
                src,
                access_mask=FilePipePrinterAccessMask.FILE_READ_DATA,
                file_info=FileNamesInformation,
        ) as file_handle:
            while True:
                data = file_handle.read()
                if not data:
                    break
                files.extend(entry.file_name for entry in data)
                if NtStatus.STATUS_NO_MORE_FILES in file_handle.status:
                    break
        return files
