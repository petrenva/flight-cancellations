import os
import re
import requests
from invoke import run
import pandas as pd
import numpy as np
from datetime import datetime


def remove_non_ascii(fname_in, fname_out):
    # https://www.utf8-chartable.de/unicode-utf8-table.pl?unicodeinhtml=hex
    print("reading file", fname_in)
    with open(fname_in, 'rb') as f:
        binary = f.read()
    res = re.sub(b'[\xa0-\xff]', b'', binary)
    with open(fname_out, 'wb') as f:
        f.write(res)


def memory_usage(df):
    res = df.memory_usage(index=True).sum() / 1024 ** 2
    return "Memory usage: {0:.1f} Mb".format(res)


class AirflightDelayDataLoader(object):

    datatypes = {
        "Year": np.int16,
        "Month": np.int8,
        "DayofMonth": np.int8,
        "DayOfWeek": np.int8,
        # "DepTime": object,
        "CRSDepTime": np.int16,
        # "ArrTime": object,
        "CRSArrTime": np.int16,
        "UniqueCarrier": "category",
        "FlightNum": "category",
        "TailNum": "category",
        # "ActualElapsedTime": object, #np.int16,
        # "CRSElapsedTime": object, #np.int16,
        # "AirTime": object, #np.int16,
        # "ArrDelay": object, #np.int16,
        # "DepDelay": object, #np.int16,
        "Origin": "category",
        "Dest": "category",
        # "Distance": object, #np.int16,
        # "TaxiIn": object, #np.int16,
        # "TaxiOut": object, #np.int16,
        "Cancelled": np.int8,
        "CancellationCode": "category",
        "Diverted": np.int8,
        # "CarrierDelay": object, #np.int16,
        # "WeatherDelay": object, #np.int16,
        # "NASDelay": object, #np.int16,
        # "SecurityDelay": object, #np.int16,
        # "LateAircraftDelay": object, #np.int16
    }

    columns = ['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'DepTime', 'CRSDepTime', 'ArrTime', 'CRSArrTime', 'UniqueCarrier',
         'FlightNum', 'TailNum', 'ActualElapsedTime', 'CRSElapsedTime', 'AirTime', 'ArrDelay', 'DepDelay', 'Origin',
         'Dest', 'Distance', 'TaxiIn', 'TaxiOut', 'Cancelled', 'CancellationCode', 'Diverted', 'CarrierDelay',
         'WeatherDelay', 'NASDelay', 'SecurityDelay', 'LateAircraftDelay']

    def __init__(self, data_dir, start_year=1987, end_year=2009):
        self.start_year = start_year
        self.end_year = end_year
        self.data_dir = data_dir

    def info(self):
        x = """
            Airflights delay and cancelation data for ({}-{})
            data description: http://stat-computing.org/dataexpo/2009/the-data.html
            """.format(self.start_year, self.end_year)
        print(x)

    def download(self):
        print("get data from online for years {}-{}".format(self.start_year, self.end_year))
        for i in range(self.start_year, self.end_year):
            x = "http://stat-computing.org/dataexpo/2009/{}.csv.bz2".format(i)
            fname = os.path.join(os.getcwd(), "data", x.split('/')[-1])
            if not os.path.exists(fname) and not os.path.exists(fname.rstrip('.bz2')):
                print("accessing url {}".format(x))
                r = requests.get(x)
                with open(fname, 'wb') as f:
                    print("Saving to", fname)
                    f.write(r.content)

    def unpack(self):
        for i in range(self.start_year, self.end_year):
            fn = os.path.join("data", "{}.csv".format(i))
            if not os.path.exists(fn):
                fname = "{}.bz2".format(fn)
                print("unpack data {}".format(fname))
                run("bzip2 -d {}".format(fname))

    def clean(self):
        # 2001, 2002 are corrupted, they have non-ascii symbols
        # rename them to 2001.csv-orig and 2002.csv-orig
        # remove non-ascii characters
        for i in range(2001, 2003):
            fname = os.path.join(os.getcwd(), "data", "{}.csv".format(i))
            if not os.path.exists(fname):
                remove_non_ascii("{}-orig".format(fname), fname)

    def read_data(self, year, cols, dtypes):
        fname = os.path.join(self.data_dir, "{}.csv".format(year))
        df = pd.read_csv(fname, na_values=['NA'], keep_default_na=False, usecols=cols, dtype=dtypes)
        memus = memory_usage(df)
        print(fname, memus, df.shape)
        return df

    def read_data_years(self, years, cols, dtypes):
        df = pd.DataFrame()
        for year in years:
            dfx = self.read_data(year, cols, dtypes)
            df = pd.concat([df, dfx])
        return df

    def get_data(self, years, cols):
        self.info()
        print("check if we need to get data")
        if not os.path.exists(self.data_dir):
            self.download()
            self.unpack()
            self.clean()
        else:
            print("data dir exists {}".format(self.data_dir))
            res = run("ls {}*.csv".format(self.data_dir))
        if cols:
            dtypes = {k: self.datatypes[k] for k in cols if k in self.datatypes.keys()}
        else:
            cols = self.columns
            dtypes = self.datatypes
        print("using columns: {}".format(cols))
        print("using dtypes: {}".format(dtypes))
        return self.read_data_years(years, cols, dtypes)


