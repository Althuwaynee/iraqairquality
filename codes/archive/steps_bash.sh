#!/bin/bash

# Load the required Python module (if necessary on your system)
module load python/3.x

# Run the Python script
python grb2_nc_server_autoYEAR_allNC_iraq.py        # To download data from server automatically
python grb2_nc_server_manual_allNC_iraq.py    	    # To process .nc files into maps and png
python timeseries_annual.py 			    # To plot time series 
python ECDF_Box_plots.py 			    # To plot stat summary



