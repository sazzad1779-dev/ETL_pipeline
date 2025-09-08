import os

def list_directories(base_path):
    dir_paths = []
    dir_names = []

    for root, dirs, files in os.walk(base_path):
        for d in dirs:
            dir_path = os.path.join(root, d)
            dir_paths.append(dir_path)   # full path
            dir_names.append(d)          # directory name only

    return dir_paths, dir_names

# Example usage
base_path = "Sevensix_dropbox/機密レベル3/営業本部/メーカー別"
all_paths, all_names = list_directories(base_path)

print("Directory Paths:")
print(all_paths)

print("\nDirectory Names:")
print(all_names)
