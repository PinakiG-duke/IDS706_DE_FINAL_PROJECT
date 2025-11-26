from kaggle.api.kaggle_api_extended import KaggleApi
import os

DATASET = "olistbr/brazilian-ecommerce"
LOCAL_DIR = "DE_Project_Data"

def download_kaggle_dataset():
    api = KaggleApi()
    api.authenticate()

    os.makedirs(LOCAL_DIR, exist_ok=True)
    api.dataset_download_files(
        DATASET,
        path=LOCAL_DIR,
        unzip=True
    )
    print(f"Downloaded dataset to {LOCAL_DIR}")

if __name__ == "__main__":
    download_kaggle_dataset()
