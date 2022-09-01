# customer_linker
A Fuzzy Name Matching utility to match different company names and link them from "sales" to "taxbook".

## sales
A Sales Source data is typically a txt file with tab delimited values.
An example looks like this:
| sales_name                 | gstin           |
|----------------------------|-----------------|
| 10 muffins                 | 31AAOFT0781F1Z4 |
| 2k engineering corporation | 09AAAFZ9208K1ZJ |
| 3 an telecom               | 13ANYPT9196P1ZP |

## taxbook
A Taxbook Source data is typically a 26as txt 

## Running the utility

* Install Python (Latest).
* Run `pip3 install -r requirements.txt`
* After the dependencies are installed, run `python3 main.py`
* You'll be prompted for the GST API key.
* Finally the output will be exported into a tab delimited text file called matches.txt
* Change the directories in main.py if you want to.
