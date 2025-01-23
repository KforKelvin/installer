import os
import hashlib
import json

def split_into_chunks(
    input_file_path, 
    chunk_size_mb=100, 
    output_folder="output_chunks_SpacePro", 
    version="0.28"
):
    """
    Splits a large file into 100MB (by default) chunks and returns a manifest.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        
    chunk_size = chunk_size_mb * 1024 * 1024
    file_size = os.path.getsize(input_file_path)
    file_name = os.path.basename(input_file_path)
    
    manifest = []
    
    with open(input_file_path, "rb") as f:
        chunk_index = 0
        while True:
            data = f.read(chunk_size)
            if not data:
                break  # Reached end of file
            
            # Write this chunk to a separate file
            chunk_file_name = f"{file_name}_chunk_{chunk_index}"
            chunk_file_path = os.path.join(output_folder, chunk_file_name)
            
            with open(chunk_file_path, "wb") as chunk_file:
                chunk_file.write(data)
                
            # Calculate checksum for integrity verification
            checksum = hashlib.sha256(data).hexdigest()
            
            # Build chunk entry for the manifest
            manifest.append({
                "chunk_file_name": chunk_file_name,
                "version": version,
                "size": len(data),
                "checksum_sha256": checksum
            })
            
            chunk_index += 1
    
    # Write the manifest to a JSON file
    manifest_file_path = os.path.join(output_folder, f"manifest_{version}.json")
    with open(manifest_file_path, "w") as mf:
        json.dump(manifest, mf, indent=4)
        
    print(f"Created {chunk_index} chunks in '{output_folder}'.")
    print(f"Manifest written to '{manifest_file_path}'.")
    
    return manifest_file_path

if __name__ == "__main__":
    # Example usage:
    input_file = "Spaceteam_0.28.zip"
    split_into_chunks(input_file_path=input_file, chunk_size_mb=100)
