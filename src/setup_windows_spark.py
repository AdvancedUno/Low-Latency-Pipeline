import os
import urllib.request

def download_hadoop_winutils():
    # Switched to a reliable mirror for the Hadoop 3.3.0 binaries
    base_url = "https://raw.githubusercontent.com/kontext-tech/winutils/master/hadoop-3.3.0/bin/"
    files = ["winutils.exe", "hadoop.dll"]
    
    hadoop_bin_dir = os.path.join(os.getcwd(), "hadoop", "bin")
    os.makedirs(hadoop_bin_dir, exist_ok=True)
    
    print("Downloading Windows native binaries for Spark...")
    for file in files:
        file_path = os.path.join(hadoop_bin_dir, file)
        if not os.path.exists(file_path):
            print(f" -> Downloading {file}...")
            urllib.request.urlretrieve(base_url + file, file_path)
            print(f"    Saved to {file_path}")
        else:
            print(f" -> {file} already exists locally.")
            
    print("\n✅ Download complete. You can now run your test script.")

if __name__ == "__main__":
    download_hadoop_winutils()