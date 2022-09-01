"""
    Main script that does the following:
      - Parses the Sales files and 26AS files in the respective directories.
      - For every GSTIN in the sales file, calls the GSTIN API to get the Legal Name.
      - Fuzzy Matches this Legal Name with the Legal Name in 26AS.
      - Establishes a Link between GSTIN - LegalName - TAN No.
      - Dumps this linked data into an output tab delimited text file
"""
import os

from matching.match import match
from parser.sales_parser import parse_sales
from parser.twentysix_as_parser import parse_26as

salesDir = os.path.abspath('./sales')
twSixAsDir = os.path.abspath('./26as')
sales_out_file = os.path.abspath('gstins_to_legal_names.txt')
twenty_six_as_out_file = os.path.abspath('legal_names_to_tans.txt')
match_out_file = os.path.abspath('matches.txt')

api_key = input('What is your GST API Key? ')

print('Processing .... ')
parse_sales(salesDir, sales_out_file, api_key)
parse_26as(twSixAsDir, twenty_six_as_out_file)
match(sales_out_file, twenty_six_as_out_file, match_out_file)
