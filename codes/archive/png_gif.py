from PIL import Image
import os
import argparse
from datetime import datetime, timedelta

def create_dust_gif(input_folder, output_file, start_date, end_date, start_time=None, end_time=None, duration=500):
    """
    Create an animated GIF from dust map PNG files within a specified date and time range.
    
    Args:
        input_folder (str): Path to folder containing PNG files
        output_file (str): Output GIF file path
        start_date (str): Start date in format YYYYMMDD
        end_date (str): End date in format YYYYMMDD
        start_time (str): Optional start time in format HHMM (24-hour)
        end_time (str): Optional end time in format HHMM (24-hour)
        duration (int): Frame duration in milliseconds
    """
    # Convert date strings to datetime objects
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    
    # Prepare time filters if provided
    start_time_dt = datetime.strptime(start_time, "%H%M").time() if start_time else None
    end_time_dt = datetime.strptime(end_time, "%H%M").time() if end_time else None
    
    # Collect all PNG files in folder
    png_files = [f for f in os.listdir(input_folder) if f.endswith('.png') and f.startswith('dust_map_IRQ_')]
    
    # Filter files by date and time range
    filtered_files = []
    for filename in png_files:
        try:
            # Extract date and time from filename (format: dust_map_IRQ_YYYYMMDD_HHMM.png)
            parts = filename.split('_')
            file_date = parts[3]
            file_time = parts[4].split('.')[0]  # Remove .png extension
            
            file_dt = datetime.strptime(f"{file_date}_{file_time}", "%Y%m%d_%H%M")
            file_time_dt = file_dt.time()
            
            # Check if file is within date range
            if start_dt.date() <= file_dt.date() <= end_dt.date():
                # Check time if time filters are provided
                if start_time_dt and end_time_dt:
                    if start_time_dt <= file_time_dt <= end_time_dt:
                        filtered_files.append((file_dt, filename))
                else:
                    filtered_files.append((file_dt, filename))
        except (IndexError, ValueError):
            continue
    
    if not filtered_files:
        print("No files found matching the specified criteria.")
        return
    
    # Sort files by datetime
    filtered_files.sort(key=lambda x: x[0])
    
    # Load images
    images = []
    for dt, filename in filtered_files:
        try:
            img_path = os.path.join(input_folder, filename)
            img = Image.open(img_path)
            images.append(img.copy())
            img.close()
        except Exception as e:
            print(f"Error loading {filename}: {e}")
    
    if not images:
        print("No valid images found.")
        return
    
    # Save as animated GIF
    images[0].save(
        output_file,
        save_all=True,
        append_images=images[1:],
        duration=duration,
        loop=0
    )
    print(f"Successfully created animated GIF: {output_file} with {len(images)} frames")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create animated GIF from dust map PNG files.')
    parser.add_argument('input_folder', help='Folder containing PNG files')
    parser.add_argument('output_file', help='Output GIF file path')
    parser.add_argument('start_date', help='Start date (YYYYMMDD)')
    parser.add_argument('end_date', help='End date (YYYYMMDD)')
    parser.add_argument('--start_time', help='Start time (HHMM)', default=None)
    parser.add_argument('--end_time', help='End time (HHMM)', default=None)
    parser.add_argument('--duration', type=int, help='Frame duration in ms', default=500)
    
    args = parser.parse_args()
    
    create_dust_gif(
        args.input_folder,
        args.output_file,
        args.start_date,
        args.end_date,
        args.start_time,
        args.end_time,
        args.duration
    )
