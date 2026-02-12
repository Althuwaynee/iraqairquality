import cdsapi

dataset = "cams-global-atmospheric-composition-forecasts"
request = {
    "variable": [
        "dust_aerosol_optical_depth_550nm",
        "particulate_matter_10um",
        "total_aerosol_optical_depth_550nm"
    ],
    "date": ["2025-06-09/2025-06-11"],
    "time": ["00:00", "12:00"],
    "leadtime_hour": ["0"],
    "type": ["forecast"],
    "data_format": "netcdf_zip"
}

client = cdsapi.Client()
client.retrieve(dataset, request).download()
