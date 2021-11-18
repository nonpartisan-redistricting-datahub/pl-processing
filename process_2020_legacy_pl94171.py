#!/usr/bin/env python
# coding: utf-8

# Import all necessary libraries to run the script.

# In[ ]:


import pandas as pd 
import geopandas as gp
import numpy as np
import time
import os
from zipfile import ZipFile
import zipfile
from datetime import date
from datetime import datetime
from pytz import timezone
import shutil
import multiprocessing as mp


# Set the working directory to the current directory.

# In[ ]:


wd = os.getcwd()


# Manually download all of the PL 94-171 zip folders from this location: https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/.

# Move all of the PL 94-171 zip folders into a folder titled "raw-pl", inside the working directory.

# Unzip the PL 94-171 data.

# In[ ]:


start_time = time.time() #set a timer for tracking 
pl_dir = os.path.join(os.getcwd(),'raw-pl') #define PL directory
for zipped_state in os.listdir(pl_dir):
    if '.zip' in zipped_state: 
        zip_path = os.path.join(pl_dir,zipped_state) #establish the zip path
        print('NOW ON STATE: ', zipped_state)
        zip_obj = ZipFile(zip_path) #recognizes data downloaded as a zip file
        folder = zipped_state.split('.zip')[0] 
        new_dir = os.path.join('raw-pl',folder)
        if not os.path.exists(new_dir):
            os.mkdir(new_dir) #make the new directory for unzipped data if it does not yet exist
        zip_obj.extractall(new_dir) #extracts zipped data
        zip_obj.close()
        zip_pl = os.path.join(wd,'zipped-pl') 
        if not os.path.exists(zip_pl):
            os.mkdir(zip_pl) #create directory for zipped data if it does not yet exist
        new_path = os.path.join(zip_pl,zipped_state)
        shutil.move(zip_path,new_path) #move zipped data to new folder
print("All PL unzipped and moved in ", "--- %s seconds ---" % (time.time() - start_time))


# Open the headers data retrieved from the Census (and stored locally) and store the info appropriately in a dictionary so we can retrieve the correct columns and data types later.

# In[ ]:


headers = pd.read_excel('./2020_PLSummaryFile_FieldNames.xlsx',sheet_name=None) #read in headers as pandas dataframe
segs = []
seg_headers = []
desc_dicts = []
for k,v in headers.items(): #for each sheet in the Excel file
    if 'Fields' in k:
        name = str(k).split('2020 P.L. ')[1] 
        name = name.split(' Fields')[0]
        segs.append(name) #adds segment name to segs list
        cols = list(v.columns)
        seg_headers.append(cols) #add list of all the columns in the dataframe to seg_headers list
        
header_dict = dict(zip(segs,seg_headers)) #create header dictionary where the key is segment and the value is the list of columns


# Define header dictionary to ensure columns are read in as the correct data type and no data or leading zeros are omitted.

# In[ ]:


header_dtype_dict = {}
for val in header_dict:
    temp_dict = {}
    wordlist = ['FILEID', 'STUSAB', 'CHARITER', 'CIFSN', 'LOGRECNO'] #all columns that should be strings
    wordlist_2 = ['AREALAND','AREAWATR','POP100','HU100'] #all columns that should be integers
    for column_name in header_dict[val]: 
        if val == "Geoheader": #if the segment is the Geoheader
            if column_name in '\t'.join(wordlist_2): 
                temp_dict[column_name]="int" #if it's in wordlist_2, make it an integer
            else:
                temp_dict[column_name]="object" #else make the column an object
        elif column_name in '\t'.join(wordlist):  
            temp_dict[column_name]="object" #if it's in worldist, make it an object
        else:
            temp_dict[column_name]="int" #else make the column an integer
    header_dtype_dict[val]=temp_dict #create a dictionary for each value in the dict, producing a nested dictionary


# Create a dictionary to iterate over potential geography folders for processing.

# In[ ]:


summary_levels = ['040','160','150','500','050','620','610','140','700','750'] #summary levels are used to query
geogs = ['st','place','bg','cd','cnty','sldl','sldu','t','vtd','b'] #these are the corresponding geographies (in order) to those summary levels
geog_dict = dict(zip(geogs,summary_levels))
#https://www2.census.gov/programs-surveys/decennial/2020/technical-documentation/complete-tech-docs/summary-file/2020Census_PL94_171Redistricting_StatesTechDoc_English.pdf
#see page 70 in link above for more information about summary levels


# Create a function to assign a FIPS code for a state given a postal code.

# In[ ]:


def assign_fips(postal_code):
    values = ['01','02','04','05','06','08','09','10','12','13','15','16','17','18','19','20','21','22','23',
                  '24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','40','41','42','44','45','46',
                  '47','48','49','50','51','53','54','55','56']
    keys = ['al','ak','az','ar','ca','co','ct','de','fl','ga','hi','id','il','in','ia','ks','ky','la','me','md','ma','mi','mn','ms','mo','mt','ne','nv','nh','nj','nm','ny','nc','nd','oh','ok','or','pa','ri','sc','sd','tn','tx','ut','vt','va','wa','wv','wi','wy']
    dictionary = dict(zip(keys,values))
    state_ab = ''
    for k,v in dictionary.items(): 
        if k == postal_code: 
            state_ab=v
    return state_ab


# Create a function to assign a state's full name given a state's postal code.

# In[ ]:


def assign_fullname(state_ab):
    values = ['al','ak','az','ar','ca','co','ct','de','fl',
              'ga','hi','id','il','in','ia','ks','ky','la','me',
              'md','ma','mi','mn','ms','mo','mt','ne',
              'nv','nh','nj','nm','ny','nc','nd','oh',
              'ok','or','pa','ri','sc','sd','tn','tx',
              'ut','vt','va','wa','wv','wi','wy']
    keys = ['Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut','Delaware','Florida',
            'Georgia','Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas','Kentucky','Louisiana','Maine',
            'Maryland','Massachusetts','Michigan','Minnesota','Mississippi','Missouri','Montana','Nebraska',
            'Nevada','New Hampshire','New Jersey','New Mexico','New York','North Carolina','North Dakota','Ohio',
            'Oklahoma','Oregon','Pennsylvania','Rhode Island','South Carolina','South Dakota','Tennessee','Texas',
            'Utah','Vermont','Virginia','Washington','West Virginia','Wisconsin','Wyoming']
    dictionary = dict(zip(keys,values))
    state_name = ''
    for k, v in dictionary.items():
        if v == state_ab:
            state_name = k
    return state_name


# Create a data folder which data will be extracted to.

# In[ ]:


data_folder = os.path.join(wd, 'PL2020')
if not os.path.exists(data_folder):
    os.mkdir(data_folder)


# For this code to work, for all geographies of interest (as defined in the dictionary above), you must have all of the shapefiles for every geography of interest.

# In[ ]:


raw_shp = os.path.join(wd,'raw-shape')
shp_dict = {}
for i in os.listdir(raw_shp):
    if '.' not in i:
        geog_folder = os.path.join(raw_shp,i)
        all_shps = []
        for file in os.listdir(geog_folder):
            if file.endswith('.shp'):
                file_path = os.path.join(geog_folder,file)
                all_shps.append(file_path)
        mini_dict = {i:all_shps}
        shp_dict.update(mini_dict) #updates the dictionary with the key as the geography and the value as a list of local folder paths of shapefiles in that folder.


# The function below is used to retrieve the file path for a shapefile given a FIPS code and level (e.g. block, county, tract, etc.).

# In[ ]:


def get_shp(fip, level, shp_dict = shp_dict):
    for k,v in shp_dict.items(): #for each item in the dictionary defined above 
        if level == k: #if the level input matchs the key
            for i in v: #for each file path (i) that is avaiable for that geography (v is a list)
                basepath = os.path.basename(os.path.normpath(i)) #get the basebath
                fips_match = basepath.split('_')[2] 
                if fip == fips_match: #if the FIP input matches the fips code in the tract
                    return i #return the filepath from the list (v)
                else:
                    continue
        else:
            continue


# Below is a function used to zip files (including the data files, READMEs, and supporting files if a shapefile such as .prj, .cpg, .shx, .dfb) and then delete unzipped files to clean the working directory.

# In[ ]:


def zip_folder(name, readme_path, list_of_file_paths, dtype):
    print(name)
    start_time = time.time()
    dir_path = os.path.dirname(os.path.realpath(list_of_file_paths[0]))
    print(dir_path)
    root = dir_path
    zipObj = ZipFile(name, 'w',zipfile.ZIP_DEFLATED) #instantiate the zip object 
    readme = readme_path
    readme_dir = os.path.dirname(readme) 
    readme_root = os.path.dirname(os.path.realpath(readme))
    zipObj.write(readme,os.path.relpath(readme,readme_root))
    if dtype == 'csv':
        for i in list_of_file_paths:
            zipObj.write(i, os.path.relpath(i,root))
        zipObj.close()
        for i in list_of_file_paths: 
            os.remove(i)
    if dtype == 'shp':
        for i in list_of_file_paths:
            basename = os.path.basename(os.path.normpath(i))
            basename = basename.split('.')[0]
            dir_path = os.path.dirname(os.path.realpath(i))
            cpg_path = os.path.join(dir_path,basename+'.cpg')
            dbf_path = os.path.join(dir_path,basename+'.dbf')
            prj_path = os.path.join(dir_path,basename+'.prj')
            shx_path = os.path.join(dir_path,basename+'.shx')
            zipObj.write(i, os.path.relpath(i,root))
            if os.path.exists(cpg_path):
                zipObj.write(cpg_path, os.path.relpath(cpg_path,root))
            if os.path.exists(dbf_path):
                zipObj.write(dbf_path, os.path.relpath(dbf_path,root))
            if os.path.exists(prj_path):
                zipObj.write(prj_path, os.path.relpath(prj_path,root))
            if os.path.exists(shx_path):
                zipObj.write(shx_path,os.path.relpath(shx_path,root))
        zipObj.close()
        for i in list_of_file_paths: 
            basename = os.path.basename(os.path.normpath(i))
            basename = basename.split('.')[0]
            dir_path = os.path.dirname(os.path.realpath(i))
            cpg_path = os.path.join(dir_path,basename+'.cpg')
            dbf_path = os.path.join(dir_path,basename+'.dbf')
            prj_path = os.path.join(dir_path,basename+'.prj')
            shx_path = os.path.join(dir_path,basename+'.shx')
            os.remove(i)
            if os.path.exists(cpg_path):
                os.remove(cpg_path)
            if os.path.exists(dbf_path):
                os.remove(dbf_path)
            if os.path.exists(prj_path):
                os.remove(prj_path)
            if os.path.exists(shx_path):
                os.remove(shx_path)
    with open(readme, 'r') as r:
        print(r.read())
    r.close()
    shutil.rmtree(readme_dir)
    print("Zipping complete in ","--- %s seconds ---" % (time.time() - start_time))


# The function below is used to write all output (what would otherwise be print statements) to a text file that can be referenced for validation purposes and to check processing time and time stamps.

# In[ ]:


def write_output(print_string,sa,wd=wd): 
    print(print_string)
    process_path = os.path.join(wd,'PROCESSING_DOCUMENTATION')
    fname = sa.upper()+'_DOCUMENTATION.txt'    
    process_path_state = os.path.join(process_path,fname)
    with open(process_path_state,'a+') as f:
        f.write(str(datetime.now(timezone('US/Eastern'))) + "\t " + print_string)
        f.write('\n')
    f.close()


# This function makes a copy of the metadata for a given file that is used for the external metadata files that are viewable on our site, https://redistrictingdatahub.org/

# In[ ]:


def copy_metadata(readme_path,zip_name, dtype,sa):
    dest = '_'.join(['readme',zip_name, dtype])
    dest = '.'.join([dest,'txt'])
    readmes = os.path.join(wd,'READMES')
    if not os.path.exists(readmes):
        os.mkdir(readmes)
    shp_readmes = os.path.join(readmes,'SHP')
    if not os.path.exists(shp_readmes):
        os.mkdir(shp_readmes)
    csv_readmes = os.path.join(readmes,'CSV')
    if not os.path.exists(csv_readmes):
        os.mkdir(csv_readmes)
    if dtype == 'shp':
        dest_path = os.path.join(shp_readmes,dest)
        shutil.copy(readme_path,dest_path)
    if dtype == 'csv':
        dest_path = os.path.join(csv_readmes,dest)
        shutil.copy(readme_path,dest_path)
    write_output('Metadata copied to media folder.',sa)


# This function retrieves a 'full' name of a geography, given a shortened version.

# In[ ]:


def full_geog(geog):
    geog_short = ['st','place','bg','cd','cnty','sldl','sldu','t','vtd','b']
    geog_full = ['State','Place','Block Group','Congressional District','County','State Legislative District Lower (SLDL)','State Legislative District Upper (SLDU)','Census Tract','Voting Tabulation District (VTD)','Block']
    dictionary = dict(zip(geog_short,geog_full))
    state_name = ''
    for k, v in dictionary.items():
        if k == geog:
            full_geog = v
    return full_geog


# The function below write the README for the dataset that is being processed. Its inputs are the README path, name of the zip folder that the dataset will live in, the state abbreviation, the geography type, and the data type (e.g. SHP or CSV).

# In[ ]:


def write_readme(readme_path, zip_name, sa, geog, dtype):
    start_time = time.time()
    state_name = assign_fullname(sa) #get full name of the state
    geog_full = full_geog(geog) #get full geography name
    list_of_sections =  ['Redistricting Data Hub (RDH) Retrieval Date','Sources','Fields','Processing','Additional Notes']
    new_section = '\n'+'\n'+'##'
    header_section = '2020 PL 94-171 Data Summary File for ' + state_name + ' based on the Decennial Census at the ' + geog_full + ' level' 
    if dtype == 'shp':
        header_section = header_section + ' on 2020 Census Redistricting Data (P.L. 94-171) Shapefiles'
    header_section = header_section + '\n' + '\n' + 'Please note that we have NOT validated against the official data used by your state’s redistricting body or bodies. Some states reallocate incarcerated persons and/or exclude non-permanent residents from the PL 94-171 data file for redistricting. Other states may make additional modifications. For more information about state modifications, visit our PL 94-171 Modifications article included in https://redistrictingdatahub.org/data/about-our-data/'
    if geog in ['sldu','sldl','cd']:
        header_section = header_section + '\n' + '\n' + "Please note that the Census Bureau does not account for changes to congressional, and state legislative district boundaries since the 2018 election. For more information, contact your state's redistricting body or check out All About Redistricting https://redistricting.lls.edu/"
    date_section = new_section + list_of_sections[0] + '\n' + '08/12/21'
    sources_section = new_section + list_of_sections[1] + '\n' + '2020 PL 94-171 Legacy Format Data was retrieved from the US Census FTP site https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/'
    if dtype == 'shp':
        sources_section = sources_section + '\n' + '2020 Census Redistricting Data (P.L. 94-171) Shapefiles were retrieved on 05/24/21 from https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html'
    fields_section = new_section + list_of_sections[2] + '\n' + 'For a full list of fields and descriptions, review the 2020 Census State Redistricting Data (Public Law 94-171) Summary File Technical Documentation at https://www2.census.gov/programs-surveys/decennial/2020/technical-documentation/complete-tech-docs/summary-file/2020Census_PL94_171Redistricting_StatesTechDoc_English.pdf'
    process_section = new_section + list_of_sections[3] + '\n' + 'The legacy format 2020 PL 94-171 Data was downloaded from the Census. \nThe legacy format data is provided in one zip file per state. Each zip file contains four files: 3 “segments” containing the data for 1 or more standard redistricting tables, and 1 “geographic header” file. \nThe first segment contains the data for Tables P1 (Race) and P2 (Hispanic or Latino, and Not Hispanic or Latino by Race). The second segment contains data for Tables P3 (Race for the Population 18 Years and Over), P4 (Hispanic or Latino, and Not Hispanic or Latino by Race for the Population 18 Years and Over), and H1 (Occupancy Status). The third segment contains Table P5 (Group Quarters Population by Major Group Quarters Type), which was not part of the 2010 PL 94-171 data release.\nThe files were imported into Python as pipe-delimited data frames and the columns renamed. The segments were joined together and then to the geo file, using the logical record number, or LOGRECNO.\nThe 10 different summary levels that the RDH processed were queried from the data, each corresponding to a particular unit of geography: state, county, tract, block group, block, congressional district, state legislative district - lower, state legislative district - upper, minor civil division, and census place.\nThen the corresponding geographies were merged with the 2020 Census Redistricting Data (P.L. 94-171) shapefiles based on Census GEOIDs. \nFinally, the tabulated data were exported in CSV and shapefile formats.\nThe RDH verified results internally and externally with partner organizations. \nFor additional processing information, review our GitHub https://github.com/nonpartisan-redistricting-datahub'    
    if dtype == 'shp':
        #this section deals with two dataset where the number of cases in the CSV and SHP do not align
        if str(zip_name).endswith('wv_pl2020_vtd'):
            add_section = new_section + list_of_sections[4] + "Note that in our validation of this file we found a mismatch between the VTD CSV and SHP length. List of missing GEOIDs in the final joined shapefile are:{'54009000000', '54077000000', '54033000000', '54051000000', '54063000000', '54049000000', '54055000000', '54027000000', '54081000000', '54065000000', '54039000000', '54047000000', '54107000000'}"
            add_section = add_section + '\n' + '\nFor more information about this data set, visit our PL 94-171 article included in https://redistrictingdatahub.org/data/about-our-data/. \n\nFor additional questions, email info@redistrictingdatahub.org.' 
        if str(zip_name).endswith('me_pl2020_vtd'):
            add_section = new_section + list_of_sections[4] + "Note that in our validation of this file we found a mismatch between the VTD CSV and SHP length. List of missing GEOIDs in the final joined shapefile are: {'23021000000', '23025000000', '23007000000', '23013000000', '23003000000', '23019000000', '23017000000', '23029000000', '23009000000', '23015000000'}"
            add_section = add_section + '\n' + '\nFor more information about this data set, visit our PL 94-171 article included in https://redistrictingdatahub.org/data/about-our-data/. \n\nFor additional questions, email info@redistrictingdatahub.org.' 
        else:
            add_section = new_section + list_of_sections[4] + '\n' + 'For more information about this data set, visit our PL 94-171 article included in https://redistrictingdatahub.org/data/about-our-data/. \n\nFor additional questions, email info@redistrictingdatahub.org.' 
    else:
        add_section = new_section + list_of_sections[4] + '\n' + 'For more information about this data set, visit our PL 94-171 article included in https://redistrictingdatahub.org/data/about-our-data/. \n\nFor additional questions, email info@redistrictingdatahub.org.'
    to_write = header_section + date_section + sources_section + fields_section + process_section + add_section
    to_write = str(to_write)
    with open(readme_path, 'w') as metadata:
        metadata.write(to_write)
    metadata.close()
    copy_metadata(readme_path, zip_name, dtype,sa)
    write_output("Metadata was created and coppied in "+"--- %s seconds ---" % (time.time() - start_time),sa)


# The function below creates the documentation text file for a given state. 

# In[ ]:


def make_doc_file(sa, wd = wd):
    process_path = os.path.join(wd,'PROCESSING_DOCUMENTATION')
    if not os.path.exists(process_path):
        os.mkdir(process_path)
    fname = sa.upper()+'_DOCUMENTATION.txt'    
    process_path_state = os.path.join(process_path,fname)
    with open(process_path_state,'a+') as f:
        f.truncate(0)
    f.close()
    return process_path_state


# Create a tracker file that can be used to view what states are complete and which are not.

# In[ ]:


def make_tracker_file(wd = wd):
    fname = 'STATE_TRACKER.txt'    
    tracker_path = os.path.join(wd,fname)
    with open(tracker_path,'a+') as f:
        f.truncate(0)
    return tracker_path


# In[ ]:


tracker_path = make_tracker_file(wd)


# The function below is used to add data to the state tracker.

# In[ ]:


def progress_output(sa,time_hr,time_min, tracker_path = tracker_path):
    sa = sa.upper()
    with open(tracker_path,'a+') as f:
        text = str('\n'+ sa + ' WAS COMPLETED IN '+str(time_hr)+' HOURS ('+str(time_min)+' minutes).')
        print(text)
        f.write(text)


# Below is the main function for processing the 2020 PL 94-171 data. The only required input (the others are defined in this script) is a folder path for the unzipped PL 94-171 data.

# In[ ]:


def process_segs(state_folder, header_dtype_dict = header_dtype_dict, tracker_path = tracker_path, geog_dict = geog_dict, header_dict = header_dict, cwd = data_folder, shp_dict = shp_dict):  
    state_start_time = time.time()
    #Get just the basename of the unzipped folder
    state_folder_name = os.path.basename(os.path.normpath(state_folder))
    # Make it a string
    state_folder_name = str(state_folder_name)
    #Get the state abbreviation
    sa = state_folder_name[:2]
    sa = str(sa)
    process_path_state = make_doc_file(sa)
    write_output('\nStarting state: ' + sa.upper(),sa)
    #Identify the full paths for all files in the state's folder
    seg1_path = os.path.join(state_folder,sa+'000012020.pl')
    seg2_path = os.path.join(state_folder,sa+'000022020.pl')
    seg3_path = os.path.join(state_folder,sa+'000032020.pl')
    geo_path = os.path.join(state_folder, sa+'geo2020.pl')
    #Read in all file files from their full path
    write_output('Reading in segments for ' + sa.upper(),sa)
    write_output('SEG 1',sa)    
    #each segment should read in the column names from the header_dict and the data types given the dtype header dict
    seg1 = pd.read_csv(seg1_path,delimiter = '|',header=None,names=header_dict['Segment 1'],dtype=header_dtype_dict['Segment 1'])
    write_output('SEGMENT 1 LENGTH:'+str(len(seg1)),sa)
    seg2 = pd.read_csv(seg2_path,delimiter = '|',header=None,names=header_dict['Segment 2'],dtype=header_dtype_dict['Segment 2'])
    write_output('SEGMENT 2 LENGTH:'+str(len(seg2)),sa)
    seg3 = pd.read_csv(seg3_path,delimiter = '|',header=None,names=header_dict['Segment 3'],dtype=header_dtype_dict['Segment 3'])
    write_output('SEGMENT 3 LENGTH:'+str(len(seg3)),sa)
    try:
        geo = pd.read_csv(geo_path,delimiter = '|', header = None,names=header_dict['Geoheader'],dtype=header_dtype_dict['Geoheader'])
    except:
        #the exception is used to deal with particular characters that may appear in some City, VTD, County, or other names, such as ~
        geo = pd.read_csv(geo_path,delimiter = '|', header = None,names=header_dict['Geoheader'],dtype=header_dtype_dict['Geoheader'],encoding = 'latin-1')
    write_output('GEO LENGTH:'+str(len(geo)),sa)
    list_of_lens = [len(geo),len(seg1),len(seg2),len(seg3)]
    set_lens = set(list_of_lens)
    if len(set_lens)==1:
        #if all of the lengths of the datasets are the same
        write_output('LENGTH OF ALL SEGMENTS IS THE SAME BEFORE QUERYING.',sa)
    else:
        write_output('LENGTH OF ALL SEGMENTS IS NOT THE SAME BEFORE QUERYING.',sa)
    write_output('Done reading in segments for ' + sa.upper(),sa)
    
    #Identify columns to removed from the segments (as to not replicate in the join)
    drop_cols = ['FILEID','STUSAB','CHARITER','CIFSN']
    geo_mod = geo.drop(columns = ['CIFSN'])
    seg1_mod = seg1.drop(columns = drop_cols)
    seg2_mod = seg2.drop(columns = drop_cols)
    seg3_mod = seg3.drop(columns = drop_cols)
    
    #Create GEOID field for the merge to the shapefiles later
    geo_mod['GEOID'] = geo_mod['GEOID'].astype(str)
    geo_mod['GEOID20'] = geo_mod['GEOID'].apply(lambda x: x.split('US',maxsplit=1)[1])
    
    #Merge all segments with the geo file
    write_output('Merging segments and geo file for ' + sa.upper(),sa)
    seg1_mod = pd.merge(geo_mod,seg1_mod, on = 'LOGRECNO')
    seg1_2 = pd.merge(seg1_mod,seg2_mod, on = 'LOGRECNO')
    seg1_2_3 = pd.merge(seg1_2,seg3_mod, on = 'LOGRECNO')
    write_output('JOINED TOGETHER',sa)
    write_output('Done merging segments and geo file for ' +  sa.upper(),sa)
    
    #Assign FIPS from the state abbreviation
    fip = assign_fips(sa)
    write_output('FIPS for ' + sa.upper() + ' is ' + fip,sa)
    write_output('Starting production of files for '+ sa.upper(),sa)

    state_sums = []
    cols4comp = []  
    #for each item in the geography dictionary, process the data
    #NOTE: This code works for validation *if* the geography for State (040) is listed first.
    for k,v in geog_dict.items():
        geog_start_time = time.time()
        level = k
        sum_lev = v
        write_output('On level '+ level+ ' which is summary level '+ str(sum_lev),sa)
        unique_sum_levs = list(seg1_2_3['SUMLEV'].unique())
        if sum_lev not in unique_sum_levs:
            write_output('THERE IS NO DATA AT THIS LEVEL FOR '+sa,sa)
            continue
        
        seg1_2_3_geog = seg1_2_3[seg1_2_3['SUMLEV']==sum_lev].copy()
        len_queried = len(seg1_2_3_geog)
        write_output('LENGTH OF QUERIED DATA FOR '+ level + ' is: ' + str(len_queried),sa)
        str_check = ['H0','P0']
        if level == 'st':
            for i in list(seg1_2_3_geog.columns):
                if any(string in i for string in str_check):
                    seg1_2_3_geog[i] = seg1_2_3_geog[i].astype(int)
                    i_sum = seg1_2_3_geog[i].sum()
                    cols4comp.append(i)
                    state_sums.append(i_sum)
            state_dict = dict(zip(cols4comp,state_sums))
        else:
            i_vals = []
            i_cols = []
            for i in list(seg1_2_3_geog.columns):
                if any(string in i for string in str_check):
                    i_sum = seg1_2_3_geog[i].sum()
                    i_vals.append(i_sum)
                    i_cols.append(i)
            i_dict = dict(zip(i_cols,i_vals))
            matching = i_dict == state_dict
            if matching == False:
                if level == 'place':
                    for key,value in i_dict.items():
                        for x,y in state_dict.items():
                            if key == x:
                                if int(value) < int(y):
                                    write_output('The total for '+ str(key) + ' is LESS than the state total.', sa)
                                    write_output('The place has a total of '+str(value)+' for '+key,sa)
                                    write_output('The state has a total of '+str(y) + ' for '+ x,sa)
                                elif int(value) > int(y):
                                    write_output('The total for ' + str(key) + ' is MORE than the state total.',sa)
                                    write_output('The place has a total of '+str(value)+' for '+key,sa)
                                    write_output('The state has a total of '+str(y) + ' for '+ x,sa)
                                else:
                                    write_output('The total for '+ str(key) + ' should be the SAME as the state total.', sa)
                                    write_output('The place has a total of '+str(value)+' for '+key,sa)
                                    write_output('The state has a total of '+str(y) + ' for '+ x,sa)
                else:
                    write_output('The totals of '+level+' do not match the state totals.',sa)
                    for key,value in i_dict.items():
                        for x,y in state_dict.items():
                            if key == x:
                                if value == y:
                                    write_output(x+' has the same total in both the state sum and '+level+' sum: '+str(y),sa)
                                else:
                                    write_output(x+' does NOT have the same total in both the state sum and '+level+' sum.',sa)
                                    write_output(level+ ' has a total of '+str(value)+' for '+key,sa)
                                    write_output('The state has a total of '+str(y)+ ' for '+x,sa)
            else:
                write_output('ALL P1, P2, P3, P4, P5, H1 TOTALS ARE THE SAME FOR '+level+' AS THEY ARE FOR THE STATE',sa)
        
        #drop any columns that may have entirely null data (this happens as not all GEOHEADER fields are applicable to all geographies)
        cols_to_drop = []
        for i in list(seg1_2_3_geog.columns):
            null_sum = int(seg1_2_3_geog[i].isnull().sum())
            if null_sum == int(len(seg1_2_3_geog)):
                cols_to_drop.append(i)

        seg1_2_3_geog = seg1_2_3_geog.drop(columns = cols_to_drop)
        seg1_2_3_geog.dropna(axis=1, how='all')

        #Organize and set up folders for extraction
        file_name = '_'.join([sa,'pl2020',level])
        csv_folder = os.path.join(cwd,'CSV')
        if not os.path.exists(csv_folder):
            os.mkdir(csv_folder)
        lev_csv = '_'.join([level,'csv'])
        lev_csv_folder = os.path.join(csv_folder,lev_csv)
        if not os.path.exists(lev_csv_folder):
            os.mkdir(lev_csv_folder)
        file_path = os.path.join(lev_csv_folder,file_name+'.csv')
        write_output('Making CSV at the '+level+' level for '+sa.upper(),sa)
        if len(seg1_2_3_geog) == 0:
            write_output('LENGTH OF CSV IS 0 SO THERE IS NO DATA AVAILABLE AT THIS LEVEL FOR THIS STATE.',sa)
            continue
        seg1_2_3_geog.to_csv(file_path,index=False)
        write_output('CSV FILE COMPLETE',sa)

        #start the process of zipping the CSVs
        zip_name = file_name+'.zip'
        zip_path = os.path.join(lev_csv_folder,zip_name)
        new_readme_holder = os.path.join(lev_csv_folder,'README_HOLD_'+sa)
        if not os.path.exists(new_readme_holder):
            os.mkdir(new_readme_holder)
        readme_path = os.path.join(new_readme_holder,'README.txt')
        zip_name = zip_name.split('.')[0]
        write_readme(readme_path, zip_name, sa, level, 'csv')
        write_output('Starting to zip CSV..',sa)
        file_paths_list = [file_path]
        zip_folder(zip_path, readme_path, file_paths_list, 'csv')
        write_output('CSV ZIP FOLDER COMPLETE',sa)
        write_output(zip_path,sa)
        
        #start the process for Shapefiles
        shp_folder = os.path.join(cwd,'SHP')
        if not os.path.exists(shp_folder):
            os.mkdir(shp_folder)
        lev_shp = '_'.join([level,'shp'])
        lev_shp_folder = os.path.join(shp_folder,lev_shp)
        if not os.path.exists(lev_shp_folder):
            os.mkdir(lev_shp_folder)
            
        #get the shapefile path
        shp_path = get_shp(fip,level,shp_dict)
        
        if shp_path is not None:
            write_output('Reading in shapefile for '+sa.upper()+' at the '+level+ ' level..',sa)
            
            #read in the shapefile with no rows to get the columns and set the datatypes, then read in again
            shp_geog = gp.read_file(shp_path,rows=0)
            shp_geog_cols = list(shp_geog.columns)
            int_cols = ['ALAND20','AWATER20']
            temp_dict = {}
            for col_name in shp_geog_cols:
                if col_name in int_cols:
                    temp_dict[col_name]='int'
                else:
                    temp_dict[col_name] = 'object'
                    
            shp_geog = gp.read_file(shp_path,dtype=temp_dict)
            
            #remove columns that may be entirely null
            cols_to_drop = []
            for i in list(shp_geog.columns):
                null_sum = int(shp_geog[i].isnull().sum())
                if null_sum == int(len(shp_geog)):
                    cols_to_drop.append(i)
            shp_geog = shp_geog.drop(columns = cols_to_drop)
            shp_geog.dropna(axis=1, how='all')
        
            #merge the shapefile and PL data
            write_output('SHP GEOG',sa)
            write_output('Merging data and shapefiles...',sa)
            match = len(shp_geog) == len_queried
            if match == True:
                write_output('There are the same number of rows in the shapefile and the PL data.',sa)
            else:
                write_output('There are NOT the same number of rows in the shapefile and the PL data.',sa)
                write_output('There are '+ str(len(shp_geog))+' rows in the shapefile.',sa)
                write_output('There are '+str(len(seg1_2_3_geog))+ ' row in the PL data.',sa)
            write_output('Merging shapefile and joined segments...',sa)
            
            geo1_2_3 = pd.merge(shp_geog,seg1_2_3_geog,on='GEOID20')
            write_output('Done merging data and shapefiles.',sa)
            write_output('JOINED SHP AND DATA',sa)

            file_path = os.path.join(lev_shp_folder,file_name+'.shp')
            list_of_shp_lens = [len_queried,len(shp_geog),len(geo1_2_3)]
            set_shp_lens = set(list_of_shp_lens)
            #confirm that the shapefile is the same length as the queried PL data
            if len(set_shp_lens) == 1:
                write_output('THE QUERIED DATA, SHAPEFILE, AND CREATED GEODATAFRAME ARE ALL THE SAME LENGTH WHICH IS '+str(len(shp_geog)),sa)
            else:
                write_output('THE QUERIED DATA, SHAPEFILE, AND CREATE GEODATAFRAME ARE NOT ALL THE SAME LENGHT.',sa)
            write_output('Making SHPs at the '+level+ ' level for '+ sa.upper(),sa)
            #extract the shapefile
            geo1_2_3.to_file(file_path)
            write_output('Join shapefile and data complete',sa)
                    
          
            file_paths_list = [file_path]
            zip_name = file_name+'.zip'
            zip_path = os.path.join(lev_shp_folder,zip_name)
            zip_name = zip_name.split('.')[0]
            
            #Write meta-data
            new_readme_holder = os.path.join(lev_shp_folder,'README_HOLD_'+sa)
            if not os.path.exists(new_readme_holder):
                os.mkdir(new_readme_holder)
            readme_path = os.path.join(new_readme_holder,'README.txt')
            write_readme(readme_path, zip_name,sa, level, 'shp')
            write_output('Starting to zip SHPs..',sa)
            zip_folder(zip_path, readme_path, file_paths_list, 'shp')
            write_output('SHP ZIP FOLDER COMPLETE',sa)
        else:
            write_output('There is no shapefile available at the ' + level + ' for '+sa.upper(),sa)
            write_output(k.upper() + " files for "+sa.upper()+" were processed in "+"--- %s seconds ---" % (time.time() - geog_start_time),sa)
            continue
        write_output(k.upper()+ " files for "+ sa.upper()+ " were processed in "+ "--- %s seconds ---" % (time.time() - geog_start_time),sa)
    state_end_time = time.time()
    diff_sec = state_end_time - state_start_time
    diff_min = diff_sec/60
    diff_hr = diff_min/60
    write_output('\nTotal time difference (minutes) for '+sa.upper()+': '+str(diff_min),sa)
    write_output('Total time difference (hours) for '+sa.upper()+': '+str(diff_hr),sa)
    completed_raw_pl = os.path.join(wd,'completed-raw-pl')
    if not os.path.exists(completed_raw_pl):
        os.mkdir(completed_raw_pl)
    new_state_folder_path = os.path.join(completed_raw_pl,state_folder_name)
    shutil.move(state_folder,new_state_folder_path)
    progress_output(sa,diff_hr,diff_min, tracker_path = tracker_path)


# Set up folder list.

# In[ ]:


def get_folder_list(pl_dir = pl_dir):
    state_folder_list = []
    for i in os.listdir(pl_dir):
        if '.pl' in i:
            state_folder = os.path.join(pl_dir,i)
            state_folder_list.append(state_folder)
    return state_folder_list


# In[ ]:


Run the processing using multiprocessing library.


# In[ ]:


if __name__ == '__main__':
    priority_states = ['TX','AL','AZ','FL','GA','LA','MI','MO','MS','NC','PA','OH','VA','WI']
    other_states = ['CA','NJ','AK','WA','MT','NV','NM','TN','SC','DE','WV','HI','WY','ND','NE','NY','NH','CO','AR','VT','IL','IN','IA','MA','ID','CT','ME','MD','OK','UT','MN','RI','KS','KY','OR','SD'] # priority, starting with TX
    states = priority_states + other_states
    priority_states = [i.lower() for i in states]
    priority_states = [os.path.join(pl_dir,str(key)+'2020.pl') for i,key in enumerate(states)]
    
    no_path = []
    for i in steates:
        if not os.path.exists(i):
            print('THE FILE PATH DOES NOT EXIST FOR ', i)
            no_path.append(i)
    if len(no_path)==0:
        print('ALL PRIORITY FILE PATHS EXIST!')
    
    p = mp.Pool(10)
    p.map(process_segs, states)
    p.close()
    p.join()

