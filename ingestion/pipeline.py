from download_from_kaggle import download_kaggle_dataset
from upload_to_s3 import upload_directory_to_s3, BUCKET_NAME, LOCAL_DIR, S3_PREFIX

def main():
    print("Step 1: Download from Kaggle")
    download_kaggle_dataset()

    print("Step 2: Upload to S3")
    upload_directory_to_s3(LOCAL_DIR, BUCKET_NAME, S3_PREFIX)

if __name__ == "__main__":
    main()
