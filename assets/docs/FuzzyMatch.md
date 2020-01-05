#### This is a tool to increase successful matches when joing two tables of messy data - in this case, address data.
#### Instead of matching my list of addresses against a table of 3 million records one at a time (this would take a whole day), I decided to create two new subset dataframes based on concatenated address+zipcode or address+city to speed up matching (One hour!)
#### The fuzzy matching algorithms from fuzzywuzzy matches the % of a string that matches. I found this to be sufficient for my address data.


```python
import os, warnings
import pandas as pd, numpy as np
from fuzzywuzzy import process, fuzz
```


```python
path = r'D:\data\taxdata'
big_list = 'tax_data.csv'
little_list = 'addresses_to_match.csv'
# Set the threshold for percent string match to 75%
thresh = 75

big_listdata = os.path.join(path, big_list)
little_listdata = os.path.join(path, little_list)
```


```python
# Import the data and create dataframes
big_list = pd.read_csv(big_listdata,delimiter=',', low_memory=False)
little_list = pd.read_csv(little_listdata,delimiter=',', low_memory=False)

big_list  = pd.DataFrame(big_list)
little_list = pd.DataFrame(little_list)
```


```python
# I also decided to trim the larger file to just the necessary columns - the file had over 100 fields.
big_list = big_list[["assessors_parcel_number_apn_pin","property_full_street_address","property_city_name","property_state","property_zip_code"]]
```


```python
# Define fields for matching and concatenate them with the address
# Pick address or city, depending on the quality of data. Seperate fields with a ',' delimeter
big_list ['fulladdress'] = big_list['property_full_street_address'].astype(str) + "," + big_list['property_city_name'].astype(str)
little_list ['fulladdress'] = little_list['Address'].astype(str) + "," + little_list['city'].astype(str)

# Make all values lowercase for better matching
big_list = big_list.apply(lambda x: x.astype(str).str.lower())
little_list = little_list.apply(lambda x: x.astype(str).str.lower())
```


```python
# Create a seperate list of the cities (or zipcodes) - later subset the dataframes based on this list
citylist = list(little_list.city)
```


```python
# a function to remove the duplicates in the list of cities - don't need to do the same city twice
def Remove(duplicate): 
    final_list = [] 
    for num in duplicate: 
        if num not in final_list: 
            final_list.append(num) 
    return final_list
```


```python
citylist = Remove(citylist)
print citylist
```


```python
# Here is the function that will do the matching. 
def match_address(name, list_addresses, min_score=0):
    # -1 score if no match
    max_score = -1
    # Return empty address for no match
    max_address = ""
    # Iternating over all addresses in the other file
    for address2 in list_addresses:
        # Finding fuzzy match score
        score = fuzz.ratio(address, address2)
        # Checking if the match is above the threshold
        if (score > min_score) & (score > max_score):
            max_address = address2
            max_score = score
    return (max_address, max_score)
```


```python
# Time to create some matches:
new_list = []
for city in citylist:
    # create df for each city to speed up matching
    big_list2 = big_list.loc[big_list['property_city_name'] == city] # Change the field name used for zipcode matching
    little_list2 = little_list.loc[little_list['city'] == city]
    print big_list2.head()
    for address in little_list2.fulladdress:
        # Find the best match using a set threshold - 75% seems to yield the best results
        print address
        match = match_address(address, big_list2.fulladdress, thresh)
        # New dict for storing data
        newaddress = {}
        newaddress.update({"db_address" : address})
        newaddress.update({"big_list_address" : match[0]})
        newaddress.update({"score" : match[1]})
        new_list.append(newaddress)
```


```python
# create the new table
merge_table = pd.DataFrame(new_list)
```


```python
# I output these to a csv to review them manually
# This can be expanded upon further to join the data post-matching, depending on needs
merge_table.to_csv('output_matches.csv')
```
