import os
import hashlib
import json
import shutil

def compute_sha256(file_path):
    """Compute the SHA-256 of a file."""
    sha = hashlib.sha256()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            sha.update(block)
    return sha.hexdigest()

def split_into_chunks(
    input_file_path,
    chunk_size_mb=200,
    output_dir="output_chunks",
    version="0.28"
):
    """
    Splits a large file into equal-sized chunks, zero-pads filenames for correct ordering,
    and returns a manifest.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_size = os.path.getsize(input_file_path)
    chunk_size = chunk_size_mb * 1024 * 1024
    num_chunks = (file_size + chunk_size - 1) // chunk_size  # ceiling division

    manifest = []
    base_name = os.path.basename(input_file_path)

    with open(input_file_path, "rb") as infile:
        for i in range(num_chunks):
            chunk_data = infile.read(chunk_size)
            chunk_name = f"{base_name}_chunk_{str(i).zfill(5)}"  # zero-pad index

            chunk_path = os.path.join(output_dir, chunk_name)
            with open(chunk_path, "wb") as chunkfile:
                chunkfile.write(chunk_data)

            chunk_checksum = hashlib.sha256(chunk_data).hexdigest()

            manifest.append({
                "chunk_file_name": chunk_name,
                "size": len(chunk_data),
                "checksum_sha256": chunk_checksum
            })

    # Write manifest
    manifest_file_path = os.path.join(output_dir, f"manifest_{version}.json")
    with open(manifest_file_path, "w") as mf:
        json.dump(manifest, mf, indent=4)

    print(f"Created {num_chunks} chunks.")
    return manifest_file_path

def reassemble_chunks(
    manifest_file_path,
    output_file="reassembled.zip",
):
    """
    Reads a manifest, sorts chunk filenames in correct order, concatenates them into one file.
    Verifies each chunk's checksum before writing.
    """
    with open(manifest_file_path, "r") as mf:
        manifest = json.load(mf)

    # We assume all chunks are in the same folder as the manifest
    manifest_dir = os.path.dirname(manifest_file_path)

    # Sort by chunk index. We used zero-padding, so lexicographical sorting will work.
    # But if we didn't zero-pad, we could parse numeric indices from the filenames.
    sorted_manifest = sorted(manifest, key=lambda c: c["chunk_file_name"])

    with open(output_file, "wb") as out_f:
        for chunk_info in sorted_manifest:
            chunk_path = os.path.join(manifest_dir, chunk_info["chunk_file_name"])
            # Verify the chunk's checksum before writing
            local_checksum = compute_sha256(chunk_path)
            if local_checksum != chunk_info["checksum_sha256"]:
                raise ValueError(f"Checksum mismatch for {chunk_path}. Aborting reassembly!")

            with open(chunk_path, "rb") as chunk_file:
                shutil.copyfileobj(chunk_file, out_f)

    print(f"Reassembly complete: {output_file}")

if __name__ == "__main__":
    # 1) Split
    large_zip = "Spaceteam_0.28.zip"  # Path to your big file
    manifest_path = split_into_chunks(large_zip, chunk_size_mb=100)

    # 2) Reassemble
    reassemble_output = "reassembled.zip"
    reassemble_chunks(manifest_path, output_file=reassemble_output)

    # 3) Compare checksums
    original_hash = compute_sha256(large_zip)
    reassembled_hash = compute_sha256(reassemble_output)
    print(f"Original ZIP SHA-256:    {original_hash}")
    print(f"Reassembled ZIP SHA-256: {reassembled_hash}")

    if original_hash == reassembled_hash:
        print("SUCCESS: Reassembled file matches the original!")
    else:
        print("ERROR: Checksums differ. Something went wrong.")
