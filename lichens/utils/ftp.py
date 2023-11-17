from ftplib import FTP
from fnmatch import fnmatch
from glob import glob
import os 
import asyncio
import aioftp


class FtpServer(FTP):
    def __init__(self, host: str = "", user: str = "", passwd: str = "", acct: str = "", timeout: float = ..., source_address: tuple[str, int] | None = None, *, encoding: str = "utf-8") -> None:
        super().__init__(host, user, passwd, acct, timeout, source_address, encoding=encoding)

    def batch_upload(self, src: os.PathLike, dst: os.PathLike, filter_regex: str = None, *args, **kwargs):
        """Upload all the files matched the regex to the ftp folder. 

        Args:
            src (os.PathLike): Source directory containing files to upload.
            dst (os.PathLike): Destination directory on the FTP server.
            filter_regex (str): Regular expression to filter files. Defaults to None.
        """
        os.chdir(src)  # Change to the source directory
        files_to_upload = glob(filter_regex) if filter_regex else glob('*')
        
        for file in files_to_upload:
            local_file_path = os.path.join(src, file)
            remote_file_path = os.path.join(dst, file)
            
            with open(local_file_path, 'rb') as local_file:
                self.storbinary(f"STOR {remote_file_path}", local_file)
        print("Batch upload completed.")

    def batch_download(self, src: os.PathLike, dst: os.PathLike, filter_regex: str = None, *args, **kwargs):
        """Download all the files matched the regex to the local folder.

        Args:
            src (os.PathLike): Source directory on the FTP server.
            dst (os.PathLike): Destination directory to save downloaded files.
            filter_regex (str, optional): Regular expression to filter files. Defaults to None.
        """
        os.chdir(dst)  # Change to the destination directory
        files_to_download = self.nlst(src)  # Get the list of files in the source directory
        
        for file in files_to_download:
            if filter_regex and not fnmatch(file, filter_regex):
                continue  # Skip files that don't match the filter
            
            local_file_path = os.path.join(dst, file)
            remote_file_path = os.path.join(src, file)
            
            with open(local_file_path, 'wb') as local_file:
                self.retrbinary(f"RETR {remote_file_path}", local_file.write)
        print("Batch download completed.")


class AioFtpServer(aioftp.ClientSession):
    def __init__(self, host: str = "", user: str = "", passwd: str = "", acct: str = "", timeout: float = ..., source_address: tuple[str, int] | None = None, *, encoding: str = "utf-8") -> None:
        super().__init__(host, user, passwd, acct, timeout, source_address, encoding=encoding)

    async def aio_batch_upload(self, src: os.PathLike, dst: os.PathLike, filter_regex: str = None, *args, **kwargs):
        """Upload all the files matched the regex to the ftp folder asynchronously.

        Args:
            src (os.PathLike): Source directory containing files to upload.
            dst (os.PathLike): Destination directory on the FTP server.
            filter_regex (str): Regular expression to filter files. Defaults to None.
        """
        os.chdir(src)  # Change to the source directory
        files_to_upload = glob(filter_regex) if filter_regex else glob('*')

        async with self:
            for file in files_to_upload:
                local_file_path = os.path.join(src, file)
                remote_file_path = os.path.join(dst, file)

                await self.upload(local_file_path, remote_file_path)
        print("Asynchronous batch upload completed.")

    async def aio_batch_download(self, src: os.PathLike, dst: os.PathLike, filter_regex: str = None, *args, **kwargs):
        """Download all the files matched the regex to the local folder asynchronously.

        Args:
            src (os.PathLike): Source directory on the FTP server.
            dst (os.PathLike): Destination directory to save downloaded files.
            filter_regex (str, optional): Regular expression to filter files. Defaults to None.
        """
        os.chdir(dst)  # Change to the destination directory
        files_to_download = await self.list(src)  # Get the list of files in the source directory

        async with self:
            for file in files_to_download:
                if filter_regex and not fnmatch(file, filter_regex):
                    continue  # Skip files that don't match the filter

                local_file_path = os.path.join(dst, file)
                remote_file_path = os.path.join(src, file)

                await self.download(remote_file_path, local_file_path)
        print("Asynchronous batch download completed.")