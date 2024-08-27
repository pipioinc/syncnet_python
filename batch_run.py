""" 
Script to batch process a list of video files from a bucket:
* This script just wraps SyncNet from github.com/joonson/syncnet_python with one mod to write out the offsets.
* Next step should be to repackage the pre-processing and model to be more efficient for Pipio.
"""

import os
import subprocess


# Bucket mount root and file paths:
bucket_files_root = "/your/bucket/mount/avatar_videos/"
bucket_file_paths = [
    "dataset_name_0",
    "dataset_name_1",
]

# Local files root
local_files_root = "/mnt/Data8GB/avatar_videos/"
output_dir = "/mnt/Data8GB/syncnet_output/"

with open(f"{output_dir}/avatar_offsets.txt", "w") as f_offsets:
    for bucket_file_path in bucket_file_paths:
        for ss in [30, 60, 120, 180, 240, 300, 360]:
            # find and replace file extension:
            try:
                bucket_file_path_base, bucket_file_name = os.path.split(bucket_file_path)
                bucket_file_name_noext, bucket_file_name_ext = os.path.splitext(bucket_file_name)
                
                # local_file_name = bucket_file_name_noext + f"_{ss}s.mp4"
                local_file_name = bucket_file_name_noext + f"_{ss}s{bucket_file_name_ext}"

                if not os.path.exists(f"{bucket_files_root}/{bucket_file_path}"):
                    print(f"File does not exist: {bucket_files_root}/{bucket_file_path}")
                    continue

                full_bucket_file_path = f"{bucket_files_root}/{bucket_file_path}"
                full_local_file_path = f"{local_files_root}/{local_file_name}"
                output_reference = f"{bucket_file_name_noext}_{ss}"

                # Trim the video to 30 seconds starting ss seconds in:
                # command = f"ffmpeg -y -ss {ss} -t 30 -i {full_bucket_file_path} -pix_fmt yuv420p -c:v h264 -async 1 {full_local_file_path}"
                command = f"ffmpeg -y -ss {ss} -t 30 -i {full_bucket_file_path} -c copy {full_local_file_path}"
                output = subprocess.call(command, shell=True, stdout=None)

                # ToDo: Another round of tests that doesn't re-encode the video when trimming it.

                # Call run_pipeline.py and run_syncnet.py:
                command = f"python run_pipeline.py --videofile {full_local_file_path} --reference {output_reference} --data_dir {output_dir}"
                output = subprocess.call(command, shell=True, stdout=None)

                command = f"python run_syncnet.py --videofile {full_local_file_path} --reference {output_reference} --data_dir {output_dir}"
                output = subprocess.call(command, shell=True, stdout=None)

                # Write the offset to the offsets file:
                offset_file_path = f"{output_dir}/pywork/{output_reference}/offset.txt"
                with open(offset_file_path, 'r') as f_offset:
                    offset = int(f_offset.readline().strip())

                    f_offsets.write(f"{output_reference} {offset}\n")
                    f_offsets.flush()
            except Exception as e:
                print(f"Error processing {bucket_file_path}_{ss}: {e}")
