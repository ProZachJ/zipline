#
# Ingest stock csv files to create a zipline data bundle

import os

import numpy  as np
import pandas as pd
import datetime
from zipline.utils.calendars import register_calendar_alias, register_calendar

boDebug=True # Set True to get trace messages

from zipline.utils.cli import maybe_show_progress

def viacsv(symbols,start=None,end=None):

    # strict this in memory so that we can reiterate over it.
    # (Because it could be a generator and they live only once)
    tuSymbols = tuple(symbols)
 
    if boDebug:
        print "entering viacsv.  tuSymbols=",tuSymbols

    # Define our custom ingest function
    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,  # unused
               daily_bar_writer,
               adjustment_writer,
               calendar,
               cache,
               show_progress,
               output_dir,
               # pass these as defaults to make them 'nonlocal' in py2
               start=start,
               end=end):

        if boDebug:
             
            print calendar
            print "entering ingest and creating blank dfMetadata"

        dfMetadata = pd.DataFrame(np.empty(len(tuSymbols), dtype=[
            ('start_date', 'int64'),
            ('end_date', 'int64'),
            ('auto_close_date', 'int64'),
            ('symbol', 'object'),
        ]))

        if boDebug:
            print "dfMetadata",type(dfMetadata)
            print dfMetadata.describe
            print

        # We need to feed something that is iterable - like a list or a generator -
        # that is a tuple with an integer for sid and a DataFrame for the data to
        # daily_bar_writer

        liData=[]
        iSid=0
        
        dateparse = lambda x: pd.to_datetime(x, unit='s')
        
        for S in tuSymbols:
            IFIL="/tmp/"+S+".csv"
            
            if boDebug:
               print "S=",S,"IFIL=",IFIL
            
            dfData=pd.read_csv(
                IFIL,
                date_parser=dateparse,
                index_col='date',
                usecols=[
                    'open',
                    'high',
                    'low',
                    'close',
                    'volume',
                    'date',
                    'quoteVolume',
                    'weightedAverage',
                ],
                na_values=['NA']).sort_index()
            
            if boDebug:
               print "read_csv dfData",type(dfData),"length",len(dfData)
               print
            
            dfData.rename(
                columns={
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'volume': 'volume',
                    'weightedAverage': 'price',
                },
                inplace=True,
            )
            dfData['volume']=dfData['volume']/1000
            liData.append((iSid,dfData))

            # the start date is the date of the first trade and
            start_date = dfData.index[0]
            
            # the end date is the date of the last trade
            end_date = dfData.index[-1]
            
            # The auto_close date is the day after the last trade.
            ac_date = end_date + pd.Timedelta(days=1)
            
            # Update our meta data
            dfMetadata.iloc[iSid] = start_date, end_date, ac_date, S
            
            if boDebug:
                print "start_date",type(start_date),start_date
                print "end_date",type(end_date),end_date
                print "ac_date",type(ac_date),ac_date            

            iSid += 1
            
            #End Symbol Loop

        if boDebug:
            print "liData",type(liData),"length",len(liData)
            print liData
            print
            print "Now calling daily_bar_writer"

        daily_bar_writer.write(liData, show_progress=True)


        dfMetadata['exchange'] = "POLONIEX"

        if boDebug:
            print "returned from daily_bar_writer"
            print "calling asset_db_writer"
            print "dfMetadata",type(dfMetadata)
            print dfMetadata
            print


        symbol_map = pd.Series(dfMetadata.symbol.index, dfMetadata.symbol)
        if boDebug:
            print "symbol_map",type(symbol_map)
            print symbol_map
            print

        asset_db_writer.write(equities=dfMetadata)

        if boDebug:
            print "returned from asset_db_writer"
            print "calling adjustment_writer"

        adjustment_writer.write()

        if boDebug:
            print "returned from adjustment_writer"
            print "now leaving ingest function"

    if boDebug:
       print "about to return ingest function"
    return ingest

#register_calendar_alias("POLO", "POLONIEX")