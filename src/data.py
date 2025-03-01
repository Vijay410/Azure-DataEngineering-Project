import kagglehub
import os

# Specify the folder where you want to store the dataset
download_folder = "/workspaces/Azure-DataEngineering-Project/src/data"

# Ensure the folder exists
os.makedirs(download_folder, exist_ok=True)

# Download the dataset and specify the folder to store it
path = kagglehub.dataset_download("ukveteran/adventure-works", path=download_folder)

# Print the path where the dataset is saved
print("Path to dataset files:", path)
