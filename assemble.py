import os
import json
import shutil

def assemble_chunks(manifest_path, output_file_path):
    """
    Assembles chunks listed in the manifest into a single file.
    """
    with open(manifest_path, "r") as mf:
        manifest = json.load(mf)
    
    with open(output_file_path, "wb") as out_file:
        for chunk_info in sorted(manifest, key=lambda x: x["chunk_file_name"].split("_")[-1]):
            chunk_file_path = os.path.join(os.path.dirname(manifest_path), chunk_info["chunk_file_name"])
            with open(chunk_file_path, "rb") as chunk_file:
                shutil.copyfileobj(chunk_file, out_file)
    
    print(f"Assembled file saved to {output_file_path}")

if __name__ == "__main__":
    manifest_path = "output_chunks_SpacePro/manifest_0.28.json"
    output_file_path = "Spaceteam_0.28_reassembled.zip"
    assemble_chunks(manifest_path, output_file_path)