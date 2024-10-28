from minio import Minio
import urllib.request
import pandas as pd
import sys
import os
import urllib.request

def main():
    grab_data()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method downloads the 2024 New York Yellow Taxi trip records in Parquet format.
    Files are saved into the "../../data/raw" folder.
    """
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    folder_path = "../../data/raw"
    months = [f"{month:02d}" for month in range(1, 13)]  # List of months from 01 to 12

    # Create the directory if it doesn't exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    for month in months:
        file_name = f"yellow_tripdata_2024-{month}.parquet"
        file_url = base_url + file_name
        file_path = os.path.join(folder_path, file_name)

        # Download the file
        print(f"Downloading {file_name}...")
        try:
            urllib.request.urlretrieve(file_url, file_path)
            print(f"Downloaded {file_name} successfully.")
        except Exception as e:
            print(f"Failed to download {file_name}: {e}")


def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "NOM_DU_BUCKET_ICI"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

if __name__ == '__main__':
    sys.exit(main())
