"""
File store client for jb-mesh's persistent file storage.

Python tools use this to import files into the shared store,
retrieve files by ID, and manage file metadata.

Usage:
    from jb_service import Service, method
    
    class MyTool(Service):
        @method
        def process(self, input_path: str) -> dict:
            # Do some work, create output file
            output_path = self.create_output()
            
            # Import into store with 1 hour TTL
            file_id = self.files.import_file(output_path, name="output.png", ttl=3600)
            
            # Clean up local file
            os.remove(output_path)
            
            return {"file_id": file_id}
        
        @method
        def read_file(self, file_id: str) -> dict:
            # Get the blob path for direct reading
            path = self.files.get_path(file_id)
            with open(path, 'rb') as f:
                data = f.read()
            return {"size": len(data)}
"""
import os
import json
from typing import Optional, List
from dataclasses import dataclass
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode


@dataclass
class FileInfo:
    """Metadata about a stored file."""
    id: str
    name: str
    size: int
    sha256: str
    path: str  # Filesystem path to blob
    created_at: int
    expires_at: int  # 0 = permanent
    
    @classmethod
    def from_dict(cls, data: dict) -> 'FileInfo':
        return cls(
            id=data['id'],
            name=data['name'],
            size=data['size'],
            sha256=data['sha256'],
            path=data.get('path', ''),
            created_at=data['created_at'],
            expires_at=data.get('expires_at', 0),
        )


class FileStoreError(Exception):
    """Error from the file store."""
    pass


class FileStore:
    """
    Client for jb-mesh's persistent file storage.
    
    This is automatically available as `self.files` in Service subclasses.
    """
    
    def __init__(self, base_url: str = None):
        """
        Initialize the file store client.
        
        Args:
            base_url: jb-mesh URL (default: http://localhost:9800)
        """
        self.base_url = base_url or os.environ.get('JB_MESH_FILESTORE_URL', 'http://localhost:9800')
        self._store_url = f"{self.base_url}/v1/store"
    
    def import_file(self, path: str, name: str = None, ttl: int = 0) -> str:
        """
        Import a file into the store.
        
        The file is copied into the store's blob directory. You can delete
        the original file after import if desired.
        
        Args:
            path: Path to the file to import
            name: Display name (default: basename of path)
            ttl: Time to live in seconds (0 = permanent)
        
        Returns:
            UUID of the imported file
        
        Raises:
            FileStoreError: If import fails
            FileNotFoundError: If source file doesn't exist
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"File not found: {path}")
        
        data = {
            'path': os.path.abspath(path),
            'name': name or os.path.basename(path),
            'ttl': ttl,
        }
        
        result = self._request('POST', self._store_url, json_data=data)
        return result['id']
    
    def get_path(self, file_id: str) -> str:
        """
        Get the blob path for a file.
        
        This returns the actual filesystem path to the blob, which you can
        read directly. This is more efficient than downloading via HTTP.
        
        Args:
            file_id: UUID of the file
        
        Returns:
            Filesystem path to the blob
        
        Raises:
            FileStoreError: If file not found
        """
        info = self.info(file_id)
        return info.path
    
    def info(self, file_id: str) -> FileInfo:
        """
        Get metadata for a file.
        
        Args:
            file_id: UUID of the file
        
        Returns:
            FileInfo with metadata
        
        Raises:
            FileStoreError: If file not found
        """
        result = self._request('GET', f"{self._store_url}/{file_id}")
        return FileInfo.from_dict(result)
    
    def list(self, include_expired: bool = False) -> List[FileInfo]:
        """
        List all files in the store.
        
        Args:
            include_expired: Include expired files (default: False)
        
        Returns:
            List of FileInfo objects
        """
        params = {}
        if include_expired:
            params['include_expired'] = 'true'
        
        url = self._store_url
        if params:
            url += '?' + urlencode(params)
        
        result = self._request('GET', url)
        files = result.get('files') or []
        return [FileInfo.from_dict(f) for f in files]
    
    def rename(self, file_id: str, name: str) -> FileInfo:
        """
        Rename a file (update display name).
        
        Args:
            file_id: UUID of the file
            name: New display name
        
        Returns:
            Updated FileInfo
        
        Raises:
            FileStoreError: If file not found
        """
        result = self._request('PATCH', f"{self._store_url}/{file_id}", 
                              json_data={'name': name})
        return FileInfo.from_dict(result)
    
    def set_ttl(self, file_id: str, ttl: int) -> FileInfo:
        """
        Update the TTL for a file.
        
        Args:
            file_id: UUID of the file
            ttl: New TTL in seconds (0 = permanent)
        
        Returns:
            Updated FileInfo
        
        Raises:
            FileStoreError: If file not found
        """
        result = self._request('PATCH', f"{self._store_url}/{file_id}",
                              json_data={'ttl': ttl})
        return FileInfo.from_dict(result)
    
    def delete(self, file_id: str) -> None:
        """
        Delete a file from the store.
        
        Args:
            file_id: UUID of the file
        
        Raises:
            FileStoreError: If file not found
        """
        self._request('DELETE', f"{self._store_url}/{file_id}")
    
    def _request(self, method: str, url: str, json_data: dict = None) -> dict:
        """Make an HTTP request to the store."""
        headers = {'Content-Type': 'application/json'}
        body = None
        
        if json_data is not None:
            body = json.dumps(json_data).encode('utf-8')
        
        req = Request(url, data=body, headers=headers, method=method)
        
        try:
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except HTTPError as e:
            try:
                error = json.loads(e.read().decode('utf-8'))
                raise FileStoreError(error.get('error', str(e)))
            except (json.JSONDecodeError, KeyError):
                raise FileStoreError(f"HTTP {e.code}: {e.reason}")
        except URLError as e:
            raise FileStoreError(f"Connection failed: {e.reason}")


# Convenience function for standalone use
def get_filestore(base_url: str = None) -> FileStore:
    """
    Get a FileStore client.
    
    Args:
        base_url: jb-mesh URL (default: from JB_MESH_FILESTORE_URL env or localhost:9800)
    
    Returns:
        FileStore client
    """
    return FileStore(base_url)
