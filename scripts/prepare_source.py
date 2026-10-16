"""Download the pinned official MinIO source before building its local image."""
import hashlib
from pathlib import Path
import requests

root=Path(__file__).resolve().parents[1]
target=root/'docker/minio/minio-source.tar.gz'
url='https://codeload.github.com/minio/minio/tar.gz/refs/tags/RELEASE.2025-09-07T16-13-09Z'
if not target.exists():
    response=requests.get(url,timeout=(30,180))
    response.raise_for_status()
    target.write_bytes(response.content)
digest=hashlib.sha256(target.read_bytes()).hexdigest()
assert digest=='c9598dcce3440977e79f787f2ba0e7e4d92c8d556bd51e7cef3785bafd6635f3', 'MinIO source checksum mismatch'
print('MinIO source SHA256:',digest)
