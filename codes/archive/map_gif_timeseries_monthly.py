from PIL import Image
import os
import argparse
from datetime import datetime



######  HOW to RUN me
###### (pythonenv) omar@omardurham:~/Documents/Dust$ python map_gif_timeseries_monthly.py IRQ/monthly_plots IRQ/monthly_plots/output.gif

#############################################




def create_monthly_gif(input_folder, output_file, duration=4500):
    """
    Create an animated GIF from monthly dust concentration timeseries PNG files.
    
    Args:
        input_folder (str): Path to folder containing monthly PNG files
        output_file (str): Output GIF file path
        duration (int): Frame duration in milliseconds
    """
    # Month order for sorting
    month_order = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]
    
    # Collect all monthly PNG files in folder
    png_files = [f for f in os.listdir(input_folder) 
                if f.endswith('.png') and f.startswith('dust_concentration_timeseries_')]
    
    if not png_files:
        print("No monthly plot files found.")
        return
    
    # Create mapping of month names to files
    month_files = {}
    for filename in png_files:
        # Extract month name from filename (format: dust_concentration_timeseries_Month.png)
        month = filename.split('_')[-1].split('.')[0]
        if month in month_order:
            month_files[month] = filename
    
    # Sort files by month order
    sorted_files = []
    for month in month_order:
        if month in month_files:
            sorted_files.append(month_files[month])
    
    if not sorted_files:
        print("No valid monthly plot files found.")
        return
    
    # Load images in order
    images = []
    for filename in sorted_files:
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
    print(f"Successfully created monthly animation GIF: {output_file} with {len(images)} frames")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create animated GIF from monthly dust concentration plots.')
    parser.add_argument('input_folder', help='Folder containing monthly PNG files')
    parser.add_argument('output_file', help='Output GIF file path')
    parser.add_argument('--duration', type=int, help='Frame duration in ms', default=4500)
    
    args = parser.parse_args()
    
    create_monthly_gif(
        args.input_folder,
        args.output_file,
        args.duration
    )
