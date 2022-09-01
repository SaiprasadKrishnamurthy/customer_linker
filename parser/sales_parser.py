"""
    Parses Sales and extracts the GSTINs and dumps it to a corresponding output file.
"""
import os

import requests


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
    gst_ins = set()
    gstin_to_legal_names = list()
    absolute = os.path.abspath(folder)
    for x in os.listdir(folder):
        if x.endswith(".txt"):
            filename = x
            with open(absolute + os.sep + filename) as fp:
                line = fp.readline()
                index = 1
                while line:
                    if index > 1:
                        parts = line.split("\t")
                        for part in parts:
                            if len(part.strip()) == 15 and part.strip().isalnum() and not gst_ins.__contains__(
                                    part.strip()) and part.strip()[2:7].isalpha():
                                gst_ins.add(part.strip())
                                json = get_legal_name(part.strip(), api_key)
                                if 'legal-name' in json:
                                    gstin_to_legal_names.append((part.strip(), json['legal-name']))
                    index += 1
                    line = fp.readline()
    with open(sales_out_file, 'w') as f:
        f.write('gstin\tsales_name\n')
        for obj in gstin_to_legal_names:
            f.write(f'{obj[0]}\t{obj[1]}\n')

    print(f"Extracted all GSTINs From Sales Files to {sales_out_file}")
