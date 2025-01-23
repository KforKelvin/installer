import os
import requests  # If you want to do an HTTP download
import hashlib
import json
import shutil

def download_chunks_from_manifest(
    manifest_url,
    base_download_url,        # e.g. "https://my-bucket.s3.amazonaws.com/my-folder/"
    download_folder="local_spaceteam_0.28",
    reassemble=False          # set to True if you want to stitch chunks into a single .zip
):
    """
    1. Download manifest from S3 (via a presigned URL or if S3 is public).
    2. For each chunk in the manifest, download & verify.
    3. (Optional) Reassemble the chunks into one .zip file.
    """
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
    
    # 1. Download the manifest
    print(f"Downloading manifest from: {manifest_url}")
    r = requests.get(manifest_url)
    if r.status_code != 200:
        raise Exception(f"Failed to download manifest: HTTP {r.status_code}")
    
    manifest = r.json()
    print(f"Manifest contains {len(manifest)} chunks.")
    
    # 2. Download and verify each chunk
    chunk_paths = []
    for chunk_info in manifest:
        chunk_file_name = chunk_info["chunk_file_name"]
        checksum_sha256 = chunk_info["checksum_sha256"]
        
        # Where we'll store the chunk locally
        local_chunk_path = os.path.join(download_folder, chunk_file_name)
        chunk_paths.append(local_chunk_path)
        
        # Check if file already exists and if checksum matches
        if os.path.exists(local_chunk_path):
            if verify_checksum(local_chunk_path, checksum_sha256):
                print(f"Chunk {chunk_file_name} already exists locally with matching checksum. Skipping.")
                continue
            else:
                print(f"Local chunk {chunk_file_name} exists but checksum is wrong. Redownloading.")
                os.remove(local_chunk_path)
        
        # Construct the chunk URL from your S3 base URL + chunk file name
        chunk_url = f"{base_download_url}/{chunk_file_name}"
        print(f"Downloading {chunk_url}...")
        download_file(chunk_url, local_chunk_path)
        
        # Verify checksum
        if not verify_checksum(local_chunk_path, checksum_sha256):
            raise Exception(f"Checksum mismatch for {chunk_file_name} after download.")
        print(f"Chunk {chunk_file_name} checksum verified.")
    
    # 3. Optionally reassemble into a single .zip
    if reassemble:
        # We'll assume the original file was "Spaceteam_0.28.zip"
        output_zip_path = os.path.join(download_folder, "Spaceteam_0.28.zip")
        print(f"Reassembling chunks into {output_zip_path}...")
        
        with open(output_zip_path, "wb") as out_file:
            for chunk_path in sorted(chunk_paths, key=lambda x: x.split("_")[-1]):
                with open(chunk_path, "rb") as c:
                    shutil.copyfileobj(c, out_file)
        print("Reassembly complete.")
    
    print("All chunks downloaded and verified successfully!")


def download_file(url, local_path):
    """
    A simple streaming download that writes to a local file.
    """
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk: 
                    f.write(chunk)

def verify_checksum(file_path, expected_sha256):
    """
    Compare the sha256 of file_path with expected_sha256.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(8192), b""):
            sha256.update(block)
    file_sha256 = sha256.hexdigest()
    return file_sha256 == expected_sha256


if __name__ == "__main__":
    # Example usage:
    # Suppose your manifest is at: https://my-bucket.s3.amazonaws.com/my-folder/manifest_0.28.json
    # And all chunk files are in the same S3 folder.
    manifest_url = "https://my-bucket.s3.amazonaws.com/my-folder/manifest_0.28.json"
    base_download_url = "https://my-bucket.s3.amazonaws.com/my-folder"
    
    download_chunks_from_manifest(
        manifest_url=manifest_url,
        base_download_url=base_download_url,
        download_folder="local_spaceteam_0.28",
        reassemble=True  # Set to True if you want a single .zip at the end
    )
