#
# config items for era5 downloads (for use with cdsapi)
#

cds = [  
    {   ### ERA5 operative analysis on pressure levels
        'dataset': 'reanalysis-era5-pressure-levels',
        'subdir': 'pressure',
        'time_range': '200001-201812',
        'var_list': {
            'relative_humidity': 'R',
            'temperature': 'T',
            'u_component_of_wind': 'U',
            'v_component_of_wind': 'V',
            # --- not doing yet
            #'divergence': 'D',
            #'fraction_of_cloud_cover': 'CC',
            #'geopotential': 'Z',
            #'ozone_mass_mixing_ratio': 'O3',
            #'potential_vorticity': 'PV',
            #'specific_cloud_ice_water_content': 'CIWC',
            #'specific_cloud_liquid_water_content': 'CLWC',
            #'specific_humidity': 'Q',
            #'specific_rain_water_content': '',
            #'specific_snow_water_content': '',
            #'vertical_velocity': 'W',
            #'vorticity': ''
            # ---
        },
        'qry': {
            'product_type':'reanalysis',
            'format':'netcdf',
            'grid': [0.25, 0.25],
            'area': [20, -140, -57 , 78],       # Australasia CORDEX (default global)
            'pressure_level':['1','2','3','5','7','10','20','30','50','70','100','125','150','175',
                              '200','225','250','300','350','400','450','500','550','600','650','700',
                              '750','775','800','825','850','875','900','925','950','975','1000'],
            #'variable':['geopotential'],
            #'year':['2018'],
            #'month': ['01','02','03','04','05','06','07','08','09','10','11','12'],    # (default all)
            'day': ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16',
                   '17','18','19','20','21','22','23','24','25','26','27','28','29','30','31'],
            'time': ['00:00','01:00','02:00','03:00','04:00','05:00','06:00','07:00','08:00',
                     '09:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00','17:00',
                     '18:00','19:00','20:00','21:00','22:00','23:00']
        }
    },
    {   ### ERA5 operative analysis/forecast at surface level
        'dataset': 'reanalysis-era5-single-levels',
        'subdir': 'surface',
        'time_range': '200001-201812',
        'var_list': {
            '100m_u_component_of_wind': '100U',
            '100m_v_component_of_wind': '100V',
            '10m_u_component_of_wind': '10U',
            '10m_v_component_of_neutral_wind': '10V',
            '10m_v_component_of_wind': 'v10n',
            '10m_wind_gust_since_previous_post_processing': '10FG',
            '2m_dewpoint_temperature': '2D',
            '2m_temperature': '2T',
            'convective_precipitation': 'CP',
            'convective_snowfall': 'SF',
            'mean_sea_level_pressure': 'MSL',
            'surface_net_solar_radiation': 'SSR',
            'surface_net_thermal_radiation': 'STR',
            'surface_pressure': 'SP',
            'surface_solar_radiation_downwards': 'SSR',
            'surface_thermal_radiation_downwards': 'STRD',
            'total_precipitation': 'TP'
        },
        'qry': {
            'product_type':'reanalysis',
            'format':'netcdf',
            #'format':'grib',
            'grid': [0.25, 0.25],
            'day': ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16',
                   '17','18','19','20','21','22','23','24','25','26','27','28','29','30','31'],
            'time': ['00:00','01:00','02:00','03:00','04:00','05:00','06:00','07:00','08:00',
                     '09:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00','17:00',
                     '18:00','19:00','20:00','21:00','22:00','23:00']
        }
    },
    {   ### ERA5 wave model surface
        'dataset': 'reanalysis-era5-single-levels',
        'subdir': 'wave',
        'time_range': '200001-201812',
        'var_list': {
            '10m_wind_direction': 'dwi',
            '10m_wind_speed': 'wind',
            'coefficient_of_drag_with_waves': 'cdww',
            'free_convective_velocity_over_the_oceans': 'wstar',
            'mean_direction_of_total_swell': 'mdts',
            'mean_direction_of_wind_waves': 'mdww',
            'mean_period_of_total_swell': 'mpts',
            'mean_period_of_wind_waves': 'mpww',
            'mean_wave_direction': 'mwd',
            'mean_wave_period': 'mwp',
            'mean_wave_period_based_on_first_moment_for_swell': 'p1ps',
            'mean_wave_period_based_on_second_moment_for_swell': 'p2ps',
            'mean_wave_period_based_on_first_moment_for_wind_waves': 'p1ww',
            'mean_wave_period_based_on_second_moment_for_wind_waves': 'p2ww',
            'peak_wave_period': 'pp1d',     # not sure this is correct for 'Peak period of 1D spectra'
            'significant_height_of_combined_wind_waves_and_swell': 'swh',
            'significant_height_of_total_swell': 'shts',
            'significant_height_of_wind_waves': 'shww',
            'u_component_stokes_drift': 'ust',
            'v_component_stokes_drift': 'vst',
            'wave_spectral_directional_width_for_wind_waves': 'dwww'
        },
        'qry': {
            'product_type':'reanalysis',
            'format':'netcdf',
            'grid': [0.5, 0.5],
            'day': ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16',
                   '17','18','19','20','21','22','23','24','25','26','27','28','29','30','31'],
            'time': ['00:00','01:00','02:00','03:00','04:00','05:00','06:00','07:00','08:00',
                     '09:00','10:00','11:00','12:00','13:00','14:00','15:00','16:00','17:00',
                     '18:00','19:00','20:00','21:00','22:00','23:00']
        }
    },
    {   ### ERAI operative analysis on land surface - not sure these are correct
        'dataset': 'reanalysis-era5-single-levels',
        'subdir': 'static',
        'time_range': '197901-201012',
        'var_list': {
            'skin_temperature': 'SKT',
            'snow_albedo': 'ASN',
            'snow_density': 'RSN',
            'snow_depth': 'SD',
            'soil_temperature_level_1': 'STL1',
            'soil_temperature_level_2': 'STL2',
            'soil_temperature_level_3': 'STL3',
            'soil_temperature_level_4': 'STL4',
            'temperature_of_snow_layer': 'TSN',
            'volumetric_soil_water_layer_1': 'SWVL1',
            'volumetric_soil_water_layer_2': 'SWVL2',
            'volumetric_soil_water_layer_3': 'SWVL3',
            'volumetric_soil_water_layer_4': 'SWVL4'
        },
        'qry': {
            'product_type':'reanalysis',
            'format':'netcdf',
            'grid': [0.25, 0.25],
            'day':['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16',
                   '17','18','19','20','21','22','23','24','25','26','27','28','29','30','31'],
            'time':['00:00','06:00','12:00','18:00']
        }
    },
]

cfg = {
    'nthreads': 8,
    #'datadir': '/g/data/ub4/era5',
    'datadir': '/g/data/gv1/matt/era5',
    #'datadir': './',
    #'staging': '/g/data/gv1/matt/era5/.staging',
    'staging': 'uncompressed',
    'getcmd': 'curl -o',
    'qccmd': 'gdalinfo',
    'nccmd': 'nccopy -k 4 -d5 -s',
    'db': 'era5.sqlite',
}

### END ###
