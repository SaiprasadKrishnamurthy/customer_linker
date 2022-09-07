"""
    Parses Sales and extracts the GSTINs and dumps it to a corresponding output file.
"""
import logging
import multiprocessing
import os

import dask.dataframe as ddf
import pandas as pd
import requests
import itertools

count = itertools.count()


def get_legal_name(gstin, api_key):
    url = "https://www.knowyourgst.com/developers/gstincall/"
    querystring = {"gstin": gstin}
    headers = {
        'passthrough': f"{api_key}",
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    json = response.json()
    return json


def parse_sales(folder, sales_out_file, api_key):
    num_partitions = multiprocessing.cpu_count()
    absolute = os.path.abspath(folder)
    results = list()
    for x in os.listdir(folder):
        if x.endswith(".txt"):
            filename = x
            df = pd.read_csv(absolute + os.sep + filename, sep='\t')
            gst_col = [col for col in df.columns if str('gst').lower() in str(col).lower()]
            gst_df = df[gst_col].drop_duplicates()
            df_dask = ddf.from_pandas(gst_df, npartitions=num_partitions)
            p = df_dask.map_partitions(process_gst_in, api_key, meta=('result', str))
            res = p.compute()
            for chunk in res:
                for obj in chunk:
                    results.append(obj)

    with open(sales_out_file, 'w') as f:
        f.write('gstin\tsales_name\ttrade_name\tlocation\n')
        for obj in results:
            f.write(f'{obj[0]}\t{obj[1]}\t{obj[2]}\t{obj[3]}\n')

    print(f"Extracted all GSTINs From Sales Files to {sales_out_file}")


def process_gst_in(df, api_key):
    gstin_to_legal_names = list()
    for i in df.itertuples():
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        if len(i) == 2:
            gst_in = str(i[1])
            try:
                json = get_legal_name(gst_in.strip(), api_key)
                if 'legal-name' in json:
                    gstin_to_legal_names.append(
                        (gst_in.strip(), json['legal-name'], json['trade-name'],
                         f"{json['adress']['city']} {json['adress']['pincode']} {json['adress']['state']}"))
                    print(
                        f"\r\t{next(count)} Processed GST : {gst_in} and obtained the legal name as: {json['legal-name']}",
                        end='', flush=True)
            except Exception as ex:
                logger.error(ex)
                gstin_to_legal_names.append(
                    (gst_in.strip(), 'Error', 'Error', f"Error"))
    return gstin_to_legal_names
