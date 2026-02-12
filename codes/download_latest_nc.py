# download_latest_nc_Iraq.py
import requests
import xml.etree.ElementTree as ET
import os
import logging
import re
from datetime import datetime



import shutil
import glob
import os

# 1. Define the pattern (e.g., all folders starting with 'folder')
pattern = '/home/omar/Documents/Dust/*latest'

# 2. Find all matching paths
for path in glob.glob(pattern):
    # 3. Check if it is actually a directory (to avoid deleting files)
    if os.path.isdir(path):
        try:
            shutil.rmtree(path)
            print(f"Removed directory: {path}")
        except OSError as e:
            print(f"Error: {path} : {e.strerror}")
            
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('iraq_latest_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Configuration
USERNAME = "omar.althuwaynee"
PASSWORD = "nah5MooG"
CATALOG_URL = "https://dust.aemet.es/thredds/catalog/restrictedDataRoot/MULTI-MODEL/latest/catalog.xml"
BASE_DOWNLOAD_URL = "https://dust.aemet.es/thredds/fileServer/restrictedDataRoot/MULTI-MODEL/latest/"

def extract_date_from_filename(filename):
    """Extract year and month from filename like YYYYMMDD_3H_MEDIAN.nc"""
    match = re.search(r'(\d{4})(\d{2})\d{2}_3H_MEDIAN\.nc', filename)
    if match:
        year = match.group(1)  # YYYY
        month = match.group(2) # MM
        return year, month
    return None, None

def get_latest_file_info():
    """Get the latest NetCDF file information from catalog"""
    logger.info("Fetching latest catalog...")
    
    try:
        response = requests.get(
            CATALOG_URL,
            auth=(USERNAME, PASSWORD),
            verify=False,
            timeout=30
        )
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to access catalog: {str(e)}")
        return None
    
    # Parse XML
    root = ET.fromstring(response.content)
    namespace = {'ns': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}
    
    # Find all .nc files and get the latest one
    latest_file = None
    for dataset in root.findall('.//ns:dataset', namespace):
        name = dataset.get('name')
        if name and name.endswith('_3H_MEDIAN.nc'):
            latest_file = name
            # Continue to find the most recent (last one in list)
    
    if not latest_file:
        logger.error("No 3H_MEDIAN.nc file found in catalog")
        return None
    
    return latest_file

def download_latest_nc():
    """Download the latest NetCDF file"""
    # Get latest file info
    latest_file = get_latest_file_info()
    if not latest_file:
        return None
    
    logger.info(f"Latest file found: {latest_file}")
    
    # Extract year and month for folder naming
    year, month = extract_date_from_filename(latest_file)
    if not year or not month:
        logger.error(f"Could not extract date from filename: {latest_file}")
        # Use current date as fallback
        now = datetime.now()
        folder_name = f"{now.year:04d}_{now.month:02d}_latest"
    else:
        folder_name = f"{year}_{month}_latest"
    
    # Create folder if it doesn't exist
    os.makedirs(folder_name, exist_ok=True)
    logger.info(f"Created/using folder: {folder_name}")
    
    # Check if file already exists in folder
    local_file_path = os.path.join(folder_name, latest_file)
    if os.path.exists(local_file_path):
        file_size = os.path.getsize(local_file_path)
        if file_size > 1024:  # More than 1KB
            logger.info(f"File already exists: {local_file_path} ({file_size/1024/1024:.1f} MB)")
            return local_file_path
        else:
            logger.warning(f"File exists but suspiciously small, re-downloading: {local_file_path}")
            os.remove(local_file_path)
    
    # Download the file
    download_url = BASE_DOWNLOAD_URL + latest_file
    logger.info(f"Downloading from: {download_url}")
    
    try:
        response = requests.get(
            download_url,
            auth=(USERNAME, PASSWORD),
            verify=False,
            stream=True,
            timeout=60
        )
        response.raise_for_status()
        
        # Get file size for progress tracking
        total_size = int(response.headers.get('content-length', 0))
        
        with open(local_file_path, 'wb') as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Log progress for large files
                    if total_size > 0 and downloaded % (10 * 1024 * 1024) == 0:  # Every 10MB
                        progress = (downloaded / total_size) * 100
                        logger.info(f"Download progress: {progress:.1f}% ({downloaded/1024/1024:.1f}/{total_size/1024/1024:.1f} MB)")
        
        # Verify download
        final_size = os.path.getsize(local_file_path)
        if total_size > 0 and final_size != total_size:
            logger.warning(f"File size mismatch: expected {total_size}, got {final_size} bytes")
        else:
            logger.info(f"Successfully downloaded: {local_file_path} ({final_size/1024/1024:.1f} MB)")
        
        return local_file_path
        
    except requests.exceptions.Timeout:
        logger.error("Download timeout - server took too long to respond")
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
        return None
    except requests.exceptions.ConnectionError:
        logger.error("Connection error - check your internet connection")
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
        return None
    except Exception as e:
        logger.error(f"Download failed: {str(e)}")
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
        return None

def list_downloaded_files():
    """List all downloaded files organized by folder"""
    logger.info("Current downloaded files:")
    
    # Find all folders matching pattern
    for folder in sorted(os.listdir('.')):
        if re.match(r'\d{4}_\d{2}_latest', folder) and os.path.isdir(folder):
            nc_files = [f for f in os.listdir(folder) if f.endswith('_3H_MEDIAN.nc')]
            if nc_files:
                logger.info(f"  {folder}/")
                for nc_file in sorted(nc_files):
                    file_path = os.path.join(folder, nc_file)
                    file_size = os.path.getsize(file_path)
                    logger.info(f"    {nc_file} ({file_size/1024/1024:.1f} MB)")
            else:
                logger.info(f"  {folder}/ (empty)")

def main():
    """Main function"""
    logger.info("=" * 50)
    logger.info("Starting latest NetCDF download for Iraq")
    logger.info("=" * 50)
    
    # Download latest file
    downloaded_file = download_latest_nc()
    
    if downloaded_file:
        logger.info(f"Download successful: {downloaded_file}")
        
        # Show where it was saved
        folder = os.path.dirname(downloaded_file)
        filename = os.path.basename(downloaded_file)
        logger.info(f"File saved to: {folder}/{filename}")
        
        # List all downloaded files
        list_downloaded_files()
    else:
        logger.error("Failed to download latest file")
    
    logger.info("=" * 50)
    logger.info("Download process completed")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
