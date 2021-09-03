import os

def _check_valid_path(file_path: str) -> None:
    """
    check the write file's parent directory exists or not,if not exists,create a new directory
    """
    dir_name = os.path.dirname(file_path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)


def _split_name_idx(node: str):
    name, idx = node.split("_", 1)
    idx=int(idx)
    return name, idx

if __name__=="__main__":
    _check_valid_path("shit")
