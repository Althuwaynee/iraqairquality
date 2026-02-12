import requests
import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

lock_file = "/tmp/dust_download_job.lock"

if os.path.exists(lock_file):
    print("Job already running. Exiting.")
    sys.exit()

open(lock_file, 'w').close()

try:
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('dust_download.log'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger()

    # Define user credentials
    USERNAME = "omar.althuwaynee"
    PASSWORD = "nah5MooG"
    
    BASE_URL = "https://dust.aemet.es/thredds/fileServer/restrictedDataRoot/MULTI-MODEL"
    
    # CONFIGURATION: Specify which years/months to download
    # Example: Download September for 2023, 2024, 2025
    DOWNLOAD_CONFIG = [
        # (year, start_month, end_month, start_day, end_day)
    #    (2012, 1, 12, 1, 31), (2013, 1, 12, 1, 31),(2014, 1, 12, 1, 31), (2015, 1, 12, 1, 31),(2016, 1, 12, 1, 31)# All of 2023
    #    (2022, 1, 12, 1, 31),  (2023, 1, 12, 1, 31),(2024, 1, 12, 1, 31),(2025, 1, 12, 1, 31),(2026, 1, 12, 1, 31)# All of 2024
        (2026, 2, 2, 1, 31),  #
    ]
    
    # Or for specific months only:
    # DOWNLOAD_CONFIG = [
    #     (2023, 9, 9, 1, 30),   # Sep 2023 only
    #     (2024, 9, 9, 1, 30),   # Sep 2024 only  
    #     (2025, 9, 9, 1, 30),   # Sep 2025 only
    # ]

    # Download function with retry logic
    def download_file(download_url, output_path, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    download_url,
                    auth=(USERNAME, PASSWORD),
                    headers={'User-Agent': 'OmarResearchBot/1.0'},
                    verify=False,
                    stream=True,
                    timeout=30
                )
                response.raise_for_status()
                
                # Get file size for logging
                file_size = int(response.headers.get('content-length', 0))
                
                with open(output_path, 'wb') as f:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                
                # Verify file was downloaded
                if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                    if file_size > 0:
                        actual_size = os.path.getsize(output_path)
                        if actual_size == file_size:
                            logger.info(f"Successfully downloaded {os.path.basename(output_path)} ({actual_size/1024/1024:.1f} MB)")
                            return True
                        else:
                            logger.warning(f"File size mismatch for {os.path.basename(output_path)}")
                    else:
                        logger.info(f"Downloaded {os.path.basename(output_path)}")
                        return True
                else:
                    logger.error(f"File empty or not created: {output_path}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Attempt {attempt+1}/{max_retries} failed for {download_url}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    return False
            except Exception as e:
                logger.error(f"Unexpected error downloading {download_url}: {str(e)}")
                return False
        return False

    # Track download statistics
    total_files = 0
    downloaded_files = 0
    skipped_files = 0
    failed_files = 0
    
    # Process each year/month in configuration
    for year, start_month, end_month, start_day, end_day in DOWNLOAD_CONFIG:
        for month in range(start_month, end_month + 1):
            MONTH = f"{month:02d}"
            
            # Determine days in month
            if month == 12:
                next_month = datetime(year + 1, 1, 1)
            else:
                next_month = datetime(year, month + 1, 1)
            
            days_in_month = (next_month - datetime(year, month, 1)).days
            actual_end_day = min(end_day, days_in_month)
            
            # Create directory for month data
            output_dir = f"IRQ_{year}_{MONTH}"
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f"Created output directory: {output_dir}")
            
            # Prepare download tasks
            download_tasks = []
            
            for day in range(start_day, actual_end_day + 1):
                date_str = f"{year}{month:02d}{day:02d}"
                filename = f"{date_str}_3H_MEDIAN.nc"
                download_url = f"{BASE_URL}/{year}/{MONTH}/{filename}"
                output_path = os.path.join(output_dir, filename)
                
                total_files += 1
                
                # Skip if file already exists and has reasonable size
                if os.path.exists(output_path):
                    file_size = os.path.getsize(output_path)
                    if file_size > 1024:  # At least 1KB
                        logger.debug(f"File already exists, skipping: {filename} ({file_size/1024/1024:.1f} MB)")
                        skipped_files += 1
                        continue
                    else:
                        logger.warning(f"File exists but suspiciously small: {filename} ({file_size} bytes)")
                        os.remove(output_path)  # Remove corrupt file
                
                download_tasks.append((download_url, output_path, filename))
            
            # Download files with parallel processing
            if download_tasks:
                logger.info(f"Downloading {len(download_tasks)} files for {year}-{MONTH}")
                
                # Use ThreadPoolExecutor for parallel downloads (adjust max_workers based on your network)
                with ThreadPoolExecutor(max_workers=5) as executor:
                    future_to_task = {
                        executor.submit(download_file, url, path): (url, path, name) 
                        for url, path, name in download_tasks
                    }
                    
                    for future in as_completed(future_to_task):
                        url, path, name = future_to_task[future]
                        try:
                            success = future.result()
                            if success:
                                downloaded_files += 1
                            else:
                                failed_files += 1
                        except Exception as e:
                            logger.error(f"Exception downloading {name}: {str(e)}")
                            failed_files += 1
                
                # Brief pause between months to avoid overwhelming server
                time.sleep(2)
    
    # Print summary
    logger.info("=" * 60)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total files considered: {total_files}")
    logger.info(f"Successfully downloaded: {downloaded_files}")
    logger.info(f"Skipped (already existed): {skipped_files}")
    logger.info(f"Failed downloads: {failed_files}")
    logger.info(f"Success rate: {downloaded_files/(total_files-skipped_files)*100:.1f}%")
    
    if failed_files > 0:
        logger.warning(f"Some files failed to download. Check the log for details.")
    
    logger.info("Download process completed!")

except KeyboardInterrupt:
    logger.info("Download interrupted by user")
except Exception as e:
    logger.error(f"Unexpected error in main process: {str(e)}")
    import traceback
    traceback.print_exc()
finally:
    if os.path.exists(lock_file):
        os.remove(lock_file)
