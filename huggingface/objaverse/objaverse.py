import json
import os

# 设置环境变量
os.environ['HTTP_PROXY'] = "http://192.168.0.117:10809"
os.environ['HTTPS_PROXY'] = "http://192.168.0.117:10809"


# glbs are too large, only need to download the filtered lvis subset.
# lvis subset can be downloaded from https://huggingface.co/datasets/sidraoa/objaverse-lvis
def gen_glbs_download_cmd():
    Download_Folder = "/home/cano/dataset/objaverse/"
    download_cmd_list = []

    # object-paths.json is downloaded from huggingface.co/allenai/objaverse
    dictionary = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(dictionary, "object-paths.json")
    with open(filepath, "r") as f:
        object_paths = json.load(f)

        for name, relpath in object_paths.items():
            url = os.path.join("https://huggingface.co/datasets/allenai/objaverse/resolve/main/", relpath)
            download_path = os.path.join(Download_Folder, relpath)
            os.makedirs(os.path.dirname(download_path), exist_ok=True)

            download_cmd_list.append(f"wget {url} -O {download_path}")

    command_path = os.path.join(dictionary, "download_objaverse_glbs.sh")
    with open(command_path, "w") as f:
        f.write("\n".join(download_cmd_list))

def gen_metadata_download_cmd():
    Download_Folder = "/home/cano/dataset/objaverse/"
    download_cmd_set = set()

    dictionary = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(dictionary, "object-paths.json")
    with open(filepath, "r") as f:
        object_paths = json.load(f)

        for name, relpath in object_paths.items():
            folder_name = relpath.split("/")[1]
            download_cmd_set.add(folder_name)

    command_path = os.path.join(dictionary, "download_objaverse_metadata.sh")
    with open(command_path, "w") as f:
        for folder_name in download_cmd_set:
            filename = f"metadata/{folder_name}.json.gz"
            url = os.path.join("https://huggingface.co/datasets/allenai/objaverse/resolve/main/", filename)
            download_path = os.path.join(Download_Folder, filename)

            f.write(f"wget {url} -O {download_path}\n")

if __name__ == "__main__":
    # gen_glbs_download_cmd()
    gen_metadata_download_cmd()
