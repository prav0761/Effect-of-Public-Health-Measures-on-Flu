import pandas as pd
import numpy as np
import time
import json
from   datetime import date, timedelta
from   dateutil.parser import parse
from   glob import glob
from   collections import Counter
from   pathlib import Path
from   tqdm import tqdm
import dask
import dask.dataframe as dd
from   dask.distributed import Client
from   dask.distributed import LocalCluster
import multiprocessing
import us
import os

#######################################################################################################################
##                                                                                                                   ##
## create_visits_with_day: Create the string version of the day of the week with the visit count, separated by :     ##
##                                                                                                                   ##
#######################################################################################################################
def create_visits_with_day(v):
    return ['V'+str(i)+':'+str(v[i]) for i in range(len(v))]

#######################################################################################################################
##                                                                                                                   ##
## main: The main function that reads in data and cycles over the files that need to be built                        ##
##                                                                                                                   ##
#######################################################################################################################
def main():
    total_start = time.time()
    PRINT_TIME  = True 
    DEBUG       = False

    ###################################################################################################################
    ##                                                                                                               ##
    ## Set the start and end date (year,month,day) for building our visits matrix                                    ##
    ##                                                                                                               ##
    ###################################################################################################################
    #start_date          = date(2019,1,7) #First Monday of the year
    start_date          = date(2020,9,2) #First Monday of the year (it ran out of time)
    end_date            = date(2022,4,18) #Closing on Sunday
    num_weeks           = (int)((abs(end_date-start_date).days)/7)+1
    dates_list          = [start_date+timedelta(days = i*7) for i in range(num_weeks)]
    month_day_year_list = [d.strftime("%m%d%y") for d in dates_list]
    print("=================================================================")
    print("=================================================================")
    print("START_DATE:   ",start_date)
    print("END_DATE:     ",end_date)
    print("NUM_WEEKS:    ",num_weeks)
    print("DATE STRINGS: ",month_day_year_list)
    print("=================================================================")
    print("=================================================================")

    ###################################################################################################################
    ##                                                                                                               ##
    ## Get all the dates in the format needed for access in the files list                                           ##
    ##                                                                                                               ##
    ###################################################################################################################
    start = time.time()
    dates =  [str(d.year)+"-"+str(d.month).zfill(2)+"-"+str(d.day).zfill(2) for d in dates_list]

    ###################################################################################################################
    ##                                                                                                               ##
    ## Set the msa name as a string value                                                                            ##
    ##                                                                                                               ##
    ###################################################################################################################
    #STNAME  = 'TX'
    #STCODE  = '48'
    ###################################################################################################################
    ##                                                                                                               ##
    ## Read in the fips places data                                                                                  ##
    ##                                                                                                               ##
    ###################################################################################################################
    print('3.  READING FIPS DATA: ')
    start          = time.time()
    zip_df         = pd.read_csv('/work2/projects/utprojections/safegraph_data/CONVERSION_TOOLS/ZIP_TRACT_CBG_02032022.csv',
                                usecols = ['ZipCode', 'StateFIPS', 'fips', 'cbg'],
                                keep_default_na=False,dtype = 'object')
    zip_df         = zip_df.rename(columns = {'ZipCode':'zipcode'})
    zip_df         = zip_df.rename(columns = {'StateFIPS':'statefips'})
    zip_df.zipcode = zip_df.zipcode.apply(lambda x: str(x).zfill(5))
    zip_df.statefips = zip_df.statefips.apply(lambda x: str(x).zfill(2))
    zip_df.fips    = zip_df.fips.apply(lambda x: str(x).zfill(5))
    zip_df.cbg     = zip_df.cbg.apply(lambda x: str(x).zfill(12))
    if (PRINT_TIME):
        print('    ----Time in seconds: ',time.time()-start)
    ###################################################################################################################
    ## Extract out all the fips and cbgs                                                                             ##
    ###################################################################################################################
    print('    EXTRACING FIPS LIST AND CBG LIST FOR STATE: ')
    start     = time.time()
    z_df = zip_df
#    z_df      = zip_df.loc[zip_df.StateFIPS == STCODE]
    start     = time.time()
    #fips_list    = sorted(list(z_df.statefips.unique()))
    fips_list    = sorted(list(z_df.zipcode.unique()))
    dcbg_list = sorted(list(z_df.cbg.unique()))
    cbg_list  = dcbg_list+['999999999999']
    if (PRINT_TIME):
        print('    ----: ', time.time() - start)
    ###################################################################################################################
    ## Create a dictionary mapping cbg to fips                                                                    ##
    ###################################################################################################################
    print('    CREATE MAP FROM CBG TO STATES: ')
    start = time.time()
    z = z_df[['zipcode','cbg']]
    cbgz  = dict(zip(z_df.cbg, z_df.zipcode))
    if (PRINT_TIME):
        print("    ----: ", time.time()-start)
    ###################################################################################################################
    ## Create an index for each CBG for later use with the numpy array used for calculating visits                   ##
    ###################################################################################################################
    print('    CREATE AN INDEXED MAPPING OF ALL THE CBS IN MSA: ') 
    start = time.time()
    cbgm  = {c[1]: c[0] for c in enumerate(cbg_list)}
    if (PRINT_TIME):
        print('    ----: ', time.time() - start)

    ###################################################################################################################
    ##                                                                                                               ##
    ## Read in the open census file that contains the population information per CBG                                 ##
    ##                                                                                                               ##
    ###################################################################################################################
    print("4.  READ CBG POPULATION: ")
    start   = time.time()
    fname   = "/work2/projects/utprojections/safegraph_data/safegraph_open_census_data/data/cbg_b01.csv"
    b01     = pd.read_csv(fname, usecols = ['census_block_group', 'B01001e1'])
    b01     = b01.rename(columns = {'census_block_group':'cbg', 'B01001e1':'population'})
    b01.cbg = b01.cbg.apply(lambda x: str(x).zfill(12))
    b001    = b01.loc[b01.cbg.isin(cbg_list)]
    if (PRINT_TIME):
        print("    ----: ", time.time()-start)

    ###################################################################################################################
    ##                                                                                                               ##
    ## Read in the file directory with all the paths for the files                                                   ##
    ##                                                                                                               ##
    ###################################################################################################################
    print("5.  READ IN FILE MAPPING: ")
    start = time.time()
    fdf   = pd.read_csv("/work2/projects/utprojections/safegraph_data/FileList.csv")
    if (PRINT_TIME):
        print("    ----: ", time.time() - start)

    ###################################################################################################################
    ##                                                                                                               ##
    ## Cycle over all the dates in the specified time range                                                          ##
    ##                                                                                                               ##
    ###################################################################################################################
    print("6.  CYCLE OVER DATES AND WRITE OUT FILES")
    for d in dates:
        ftime = time.time()
        print("    PROCESSING DATE: ",d)
        ###############################################################################################################
        ##                                                                                                           ##
        ## Create a NumPy array of size len(cbg_list)xlen(cbg_list) for calculations                                 ##
        ##                                                                                                           ##
        ###############################################################################################################
        print("    6.1  CREATE EMPTY CBG TO CBG NUMPY ARRAY: ")
        start = time.time()
        total_visitors  = np.zeros(shape=(len(dcbg_list),len(cbg_list))).astype('float64')
        total_visits    = np.zeros(shape=(len(dcbg_list),len(cbg_list))).astype('float64')
        total_rvisitors = np.zeros(shape=(len(dcbg_list),len(cbg_list))).astype('int')
        total_rvisits   = np.zeros(shape=(len(dcbg_list),len(cbg_list))).astype('int')
        raw_visitors    = np.zeros(shape=(len(dcbg_list))).astype('int')
        raw_visits      = np.zeros(shape=(len(dcbg_list))).astype('int')
        num_days        = 7
        V               = np.zeros(shape=(num_days,len(dcbg_list),len(cbg_list))).astype('float64')
        if (PRINT_TIME):
            print("         -----------------------: ", time.time()-start)
    
        ###############################################################################################################
        ##                                                                                                           ##
        ## Get a list of home panel summary file names needed for date = d                                           ##
        ##                                                                                                           ##
        ###############################################################################################################
        print('    6.2  HOME PANEL SUMMARY FILES: ')               
        start  = time.time()
        hfiles = list(fdf.loc[fdf.date.str.contains(d)].home_panel_summary.unique())
        if (PRINT_TIME):
            print('         -------------------: ', time.time() - start)
        ###############################################################################################################
        ## Read in the home panel summary files and keep only those in matching cbgs in cbg_list                     ##
        ###############################################################################################################
        print('         READ IN THE HOME PANEL SUMMARY FILES: ')
        start   = time.time()
        hhf     = dd.read_csv(hfiles, usecols = ['date_range_start', 'census_block_group', 'number_devices_residing', 'number_devices_primary_daytime']).rename(columns = {'census_block_group':'cbg'})
        hhf.cbg = hhf.cbg.apply(lambda x: str(x).zfill(12), meta = ('cbg', 'str'))
        hf      = hhf.loc[hhf.cbg.isin(cbg_list)]
        if (PRINT_TIME):
            print("         -----------------------: ", time.time()-start)
        ###############################################################################################################
        ## Merge the home panel summary file with the census information                                             ##
        ###############################################################################################################
        print("         MERGE HOME PANEL SUMMARY WITH CENSUS: ")
        start  = time.time()
        hf     = hf.merge(b001, on='cbg')
        if (PRINT_TIME):
            print("         -----------------------: ", time.time()-start)
        ###############################################################################################################
        ## Compute the scale factor for estimating true visits and visitors                                          ##
        ###############################################################################################################
        print("         COMPUTE SCALE FACTOR:")
        start              = time.time()
        hf['scale_factor'] = hf.apply(lambda x: 0.0 if x.number_devices_residing == 0 else float(x.population/x.number_devices_residing), axis = 1, meta = ('hf', 'float'))
        if (PRINT_TIME):
            print("         -----------------------: ", time.time()-start)
    
        ###############################################################################################################
        ##                                                                                                           ##
        ## Get a list of weekly panel files needed for date = d                                                      ##
        ##                                                                                                           ##
        ###############################################################################################################
        print('    6.3  WEEKLY PANEL FILES: ')               
        start  = time.time()
        dfiles = '/work2/projects/utprojections/safegraph_data/MOBILITY/'+str(d)+'-weekly-patterns.csv'
        if (PRINT_TIME):
            print('         -------------------: ', time.time() - start)
        ###############################################################################################################
        ## Read in the weekly pattern file and only keep those rows that match our list of valid placekeys           ##
        ###############################################################################################################
        print("         READING WEEKLY PATTERNS: ")
        start                = time.time()
        ddf                  = dd.read_csv(dfiles, 
                                           usecols = ['placekey', 
                                                      'postal_code', 
                                                      'date_range_start', 
                                                      'raw_visit_counts', 
                                                      'raw_visitor_counts', 
                                                      'visits_by_day', 
                                                      'poi_cbg',
                                                      'visitor_home_cbgs'],
                                           dtype={'parent_placekey': 'object', 'poi_cbg': 'object', 'postal_code': 'object'})
        ddf                  = ddf.rename(columns={'poi_cbg':'cbg'})
        ddf.cbg              = ddf.cbg.apply(lambda x: str(x).zfill(12), meta = ('cbg', 'str'))
        ddf.postal_code      = ddf.postal_code.astype(str).apply(lambda x: x.zfill(5), meta = ('postal_code', 'str'))
        ddf                  = ddf.loc[ddf.cbg.isin(cbg_list)]
        if (PRINT_TIME):
            print("         -----------------------: ", time.time()-start)
        ###############################################################################################################
        ##  Merge the hf dataframe with the weekly dataframe to get what is needed for scaling                       ##
        ###############################################################################################################
        print("         MERGE WEEKLY WITH  HOME PATTERNS: ")
        start                = time.time()
        ddf                  = dd.merge(ddf,hf,on=['date_range_start', 'cbg'])
        ddf.date_range_start = dd.to_datetime(ddf.date_range_start,format='%Y-%m-%dT%H:%M:%S%z')
        if (PRINT_TIME):
            print("         -----------------------: ", time.time()-start)
        ###############################################################################################################
        ##  Convert the visits_by_day and visitor_home_cbgs from json to something we can deal with                  ##
        ###############################################################################################################
        print("         JSON LOADS VISITS: ")
        start                 = time.time()
        ddf.visits_by_day     = ddf.visits_by_day.apply(json.loads, meta=pd.Series(dtype='int', name='visits_by_day'))
        ddf.visitor_home_cbgs = ddf.visitor_home_cbgs.apply(json.loads, meta=pd.Series(dtype='int', name='visitor_home_cbgs'))
        if (PRINT_TIME):
            print("         -----------------------: ", time.time()-start)
        ##############################################################################################################
        ## Group by groupvars and sum visits_by_day and true_visits_by_day in those groupings                       ##
        ##############################################################################################################
        print("         COMPUTING")                           
        start = time.time()
        df    = ddf.compute()
        print(df.head(100))
        if (PRINT_TIME):
            print("         -----------------------: ", time.time()-start)
    
        ###############################################################################################################
        ##                                                                                                           ##
        ## Cycle over the visitor home cbgs and compute the true visitors from one cbg to another                    ##
        ##                                                                                                           ##
        ###############################################################################################################
        print("    6.4  TRANSFORM TO DICT")                   
        start = time.time()
        ##############################################################################################################
        ## Create a dictionary from the dataframe for fast processing                                               ##
        ##############################################################################################################
        tf    = df[['cbg','visitor_home_cbgs','scale_factor','raw_visitor_counts','raw_visit_counts','visits_by_day']].to_dict('records')
        if (PRINT_TIME):
            print("         -----------------------: ", time.time()-start)
        ##############################################################################################################
        ## Cycle over the rows of the dataframe turned to a dictionary to compute the total visitors,               ##
        ## total visits, total raw visitor counts and total raw visits for every dest_cbg                           ##
        ##############################################################################################################
        print("    6.5  COMPUTE TOTALS VISITORS, VISITS, RAW_VISITS AND RAW_VISITORS")
        start = time.time()
        for row in tqdm(tf):
            ##########################################################################################################
            ## Cycle over the visitor home cbgs dictionary for this row                                             ##
            ##########################################################################################################
            rindex   = cbgm[row['cbg']]
            vhcbg    = row['visitor_home_cbgs']
            v2V      = row['raw_visit_counts']/row['raw_visitor_counts']
            v_by_day = row['visits_by_day']
            if row['cbg'] in cbgz:
                zr = cbgz[row['cbg']]
            else:
                zr = '99999'

            ##########################################################################################################
            ## Sum the total raw visitors to cbg and raw_visits to cbg                                              ##
            ##########################################################################################################
            raw_visitors[rindex] = raw_visitors[rindex]+row['raw_visitor_counts']
            raw_visits[rindex]   = raw_visits[rindex]+row['raw_visit_counts']
            for v in vhcbg:
                visitors = vhcbg[v]
                if (v in cbg_list):
                    cindex = cbgm[v]
                    zc = cbgz[v]
                else:
                    cindex = cbgm['999999999999']
                    zc = '99999'
                ######################################################################################################
                ## Add the raw visitor from cbg at cindex that travelled to cbg at rindex by scaling that number    ##
                ## by the population in the cbg divided by the sample size of cell phones in that cbg               ##
                ##                total_visitor = visitor_from_cbg*(population_cbg/sample_size_cbg)                 ##
                ######################################################################################################
                total_visitors[rindex,cindex]  = total_visitors[rindex,cindex]  + (visitors*row['scale_factor'])
                total_rvisitors[rindex,cindex] = total_rvisitors[rindex,cindex] + visitors
                ######################################################################################################
                ## Estimate total visit contribution from cbg at cindex to cbg at rindex by multiplying             ##
                ## total_visitors from cbg at cindex to cbg at rindex by the ratio of raw_visitor counts to place   ##
                ## that we are currently processing by the raw_visit counts to this place                           ##
                ##    total_visits = total_visitor_to_cbg*(raw_visitor_to_cbg_count)/(raw_visits_to_cbg_count)      ##
                ######################################################################################################
                total_visits[rindex,cindex]    = total_visits[rindex,cindex]   + (total_visitors[rindex,cindex]*v2V)
                total_rvisits[rindex,cindex]   = total_rvisits[rindex,cindex]  + (total_rvisitors[rindex,cindex]*v2V)
        if (PRINT_TIME):
            print("         -----------------------: ", time.time()-start)

        ##############################################################################################################
        ## Cycle over the rows and compute the visits by day from source cbg to dest_cbg                            ##
        ##############################################################################################################
        print("    6.6  COMPUTE VISITS BY DAY FROM SOURCE CBG TO DESTINATION CBG")    
        start = time.time()
        for row in tqdm(tf):
            ##########################################################################################################
            ## Cycle over the visitor home cbgs dictionary for this row                                             ##
            ##########################################################################################################
            rindex   = cbgm[row['cbg']]
            vhcbg    = row['visitor_home_cbgs']
            v2V      = row['raw_visit_counts']/row['raw_visitor_counts']
            v_by_day = row['visits_by_day']
            if row['cbg'] in cbgz:
                zr = cbgz[row['cbg']]
            else:
                zr = '99999'
            for v in vhcbg:
                visitors = vhcbg[v]
                if (v in cbg_list):
                    cindex = cbgm[v]
                    zc = cbgz[v] 
                else:
                    cindex = cbgm['999999999999']
                    zc = '99999' 

                ######################################################################################################
                ## Estimate total visits by day from cbg at cindex to cbg at rindex by multiplying the visit        ##
                ## by the ratio of total_visits to cbg at rindex divided by the raw_visit_counts to cbg             ##
                ##          visit = total_visits_to_cbg*(raw_visitor_to_cbg_count)/(raw_visits_to_cbg_count)        ##
                ######################################################################################################
                AF                            = total_rvisits[rindex,cindex]/raw_visits[rindex]
                V[0,rindex,cindex]            = V[0,rindex,cindex]+v_by_day[0]*AF
                V[1,rindex,cindex]            = V[1,rindex,cindex]+v_by_day[1]*AF
                V[2,rindex,cindex]            = V[2,rindex,cindex]+v_by_day[2]*AF
                V[3,rindex,cindex]            = V[3,rindex,cindex]+v_by_day[3]*AF
                V[4,rindex,cindex]            = V[4,rindex,cindex]+v_by_day[4]*AF
                V[5,rindex,cindex]            = V[5,rindex,cindex]+v_by_day[5]*AF
                V[6,rindex,cindex]            = V[6,rindex,cindex]+v_by_day[6]*AF
        if (PRINT_TIME):
            print("         -----------------------: ", time.time()-start)
    
        ###############################################################################################################
        ##                                                                                                           ##
        ## Write the numpy array to a pandas dataframe for final processing                                          ##
        ##                                                                                                           ##
        ###############################################################################################################
        print("    6.7   WRITE FINAL COUNTS TO DATAFRAME: ")                 
        start = time.time()
        fcolumns      = ['date','dest_fips','dest_cbg']+cbg_list
        ##############################################################################################################
        ## Dataframe for total visitors from source cbg to destination cbg                                          ##
        ##############################################################################################################
        TVdf          = pd.DataFrame(total_visitors.astype('int'),columns = cbg_list)
        ##############################################################################################################
        ## Dataframe for total visits from source cbg to destination cbg                                            ##
        ##############################################################################################################
        Tvdf          = pd.DataFrame(total_visits.astype('int'), columns = cbg_list)
        ##############################################################################################################
        ## Dataframe for total visits by day from source to destination cbg                                         ##
        ##############################################################################################################
        Vdf           = pd.DataFrame(columns = fcolumns)
        Vdf.date      = Vdf.date.astype('str')
        Vdf.dest_fips  = Vdf.dest_fips.astype('str')
        Vdf.dest_cbg  = Vdf.dest_cbg.astype('str')
        sdate         = (df.date_range_start.unique())[0]
        ccol          = pd.Series(dcbg_list).astype('str')
        zclist        = [cbgz[c] for c in dcbg_list]
        zcol          = pd.Series(zclist).astype('str')
        ###############################################################################################################
        ## Make all the values integer values                                                                        ##
        ###############################################################################################################
        for c in cbg_list:
            Vdf[c]  = Vdf[c].astype('int')
            TVdf[c] = TVdf[c].astype('int')
            Tvdf[c] = Tvdf[c].astype('int')
        ###############################################################################################################
        ## Insert the columns for the total visitors dataframe                                                       ##
        ###############################################################################################################
        dlist = [sdate.strftime("%Y/%m/%d") for j in range(len(cbg_list))]
        dcol  = pd.Series(dlist)
        TVdf.insert(0,'date',dcol)
        TVdf.insert(1,'dest_fips',zcol)
        TVdf.insert(2,'dest_cbg',ccol)
        ###############################################################################################################
        ## Insert the columns for the total visits dataframe                                                         ##
        ###############################################################################################################
        Tvdf.insert(0,'date',dcol)
        Tvdf.insert(1,'dest_fips',zcol)
        Tvdf.insert(2,'dest_cbg',ccol)
        ###############################################################################################################
        ## Insert the columns for each day of the week for the visits by day dataframe                               ##
        ###############################################################################################################
        for i in range(7):
            dlist = [(sdate+timedelta(days=i)).strftime("%Y/%m/%d") for j in range(len(cbg_list))]
            dcol  = pd.Series(dlist)
            tdf   = pd.DataFrame((V[i]).astype('int'),columns=cbg_list)
            tdf.insert(0,'date',dcol)
            tdf.insert(1,'dest_fips',zcol)
            tdf.insert(2,'dest_cbg',ccol)
            Vdf   = Vdf.append(tdf, ignore_index=True)
        if (PRINT_TIME):
            print("         -----------------------: ", time.time()-start)
    
        ###############################################################################################################
        ##                                                                                                           ##
        ## Convert all the cbgs to fips and rename rows and columns and add columns with the same fips together      ##
        ##                                                                                                           ##
        ###############################################################################################################
        print("    6.8   MAP THE CBGS TO FIPS FOR FINAL PROCESSING: ")
        start = time.time()
        ###############################################################################################################
        ## Create the new column names to replace cbgs with fips                                                     ##
        ###############################################################################################################
        zcols = ['99999' if c not in cbgz else cbgz[c] for c in cbg_list]
        ncols = dict(zip(cbg_list,zcols))
        Vdf   = Vdf.rename(columns = ncols)
        TVdf  = TVdf.rename(columns = ncols)
        Tvdf  = Tvdf.rename(columns = ncols)
        ###############################################################################################################
        ## Sum rows by the dest_fips (fips include multiple cbgs)                                                    ##
        ###############################################################################################################
        Vdf   = Vdf.groupby(['date','dest_fips']).sum()[zcols]
        TVdf  = TVdf.groupby(['date','dest_fips']).sum()[zcols]
        Tvdf  = Tvdf.groupby(['date','dest_fips']).sum()[zcols]
        ###############################################################################################################
        ## Sum cols by the source_zip (multiple columns with the same fips value)                                    ##
        ###############################################################################################################
        Vdf   = Vdf.groupby(Vdf.columns, axis=1).sum().reset_index()
        TVdf  = TVdf.groupby(TVdf.columns, axis=1).sum().reset_index()
        Tvdf  = Tvdf.groupby(Tvdf.columns, axis=1).sum().reset_index()

        print("-------------------------------------------------------------------------------------")
        print("-------------------------------------------------------------------------------------")
        print("-------------------------------------------------------------------------------------")
        print("-------------------------------------------------------------------------------------")
        print("TOTAL VISITORS FROM SOURCE FIPS TO DESTINATION FIPS: ")
        print(TVdf.head(25))

        print("-------------------------------------------------------------------------------------")
        print("-------------------------------------------------------------------------------------")
        print("-------------------------------------------------------------------------------------")
        print("-------------------------------------------------------------------------------------")
        print("TOTAL VISITS FROM SOURCE FIPS TO DESTINATION FIPS: ")
        print(Tvdf.head(25))

        print("-------------------------------------------------------------------------------------")
        print("-------------------------------------------------------------------------------------")
        print("-------------------------------------------------------------------------------------")
        print("-------------------------------------------------------------------------------------")
        print("TOTAL VISITS BY DAY FROM SOURCE FIPS TO DESTINATION FIPS: ")
        print(Vdf.head(25))

        if (PRINT_TIME):
            print("-----------------------: ", time.time()-start)
    
        ###############################################################################################################
        ##                                                                                                           ##
        ## Write the result to a file                                                                                ##
        ##                                                                                                           ##
        ###############################################################################################################
        print("    6.9  WRITE THE RESULT TO A CSV FILE: ")        
        start   = time.time()
        ###############################################################################################################
        ## Write out the file for the week designated by date_range_start                                            ##
        ###############################################################################################################
        print("         WRITING VISITS BY DAY")
        outfile   = '/work2/projects/utprojections/safegraph_data/JLTests/STATE_TO_STATE_USA/visits_by_day_by_zip_'+d+'.csv'
        Vdf.to_csv(outfile, index=False)

        print("         WRITING TOTAL VISITORS")
        TVoutfile = '/work2/projects/utprojections/safegraph_data/JLTests/STATE_TO_STATE_USA/VISITORS/total_visitors_by_zip_'+d+'.csv'
        TVdf.to_csv(TVoutfile, index=False)

        print("         WRITING TOTAL VISITS")
        Tvoutfile = '/work2/projects/utprojections/safegraph_data/JLTests/STATE_TO_STATE_USA/VISITS/total_visits_by_zip_'+d+'.csv'
        Tvdf.to_csv(Tvoutfile, index=False)
        if (PRINT_TIME):
            print("-----------------------: ", time.time()-start)

        print("-----------------------------------------------------")
        print("----PROCESSING TIME FOR DATE: ",time.time()-ftime)
        print("-----------------------------------------------------")
    
    ###################################################################################################################
    ##                                                                                                               ##
    ## Calculate the total time to run all files                                                                     ##
    ##                                                                                                               ##
    ###################################################################################################################
    total_end = time.time()
    print("=================================================================")
    print("=================================================================")
    print('TOTAL RUN TIME: ',time.time() - total_start)
    print("=================================================================")
    print("=================================================================")

if __name__ == '__main__':
    cpus = multiprocessing.cpu_count()
    print('CPUS ON NODE: ',cpus)
    print('SETTING UP CLUSTER: ',(int)(cpus/2), 2)
    cluster = LocalCluster(n_workers=(int)(cpus/2), threads_per_worker=2)
    client  = Client(cluster, asynchronous=True)
    client.get_versions(check=True)
    main()
    print('SHUTTING DOWN: ')
    client.shutdown()
