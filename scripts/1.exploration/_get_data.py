import pandas as pd
import os
from urllib.request import urlretrieve

# Define headers for the CSV files
headers = ['transaction_unique_identifier', 'price', 'date_of_transfer', 'postcode',
           'property_type', 'old_or_new', 'duration', 'primary_addressable_object_name', 'secondary_addressable_object_name', 'street',
           'locality', 'town_or_city', 'district', 'county', 'ppd_category_type',
           'record_status']


# Loop through years from 2016 to 2022
for year in range(2016, 2023):
    # Create URL for the current year
    url = f"http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-{year}.csv"
    
    # Download the CSV file
    filename = f"data/csv/pp-{year}.csv"
    urlretrieve(url, filename)
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(filename)
    
    # Set the headers for the DataFrame
    df.columns = headers
    
    # Save the DataFrame back to a CSV file with the headers
    df.to_csv(filename, index=False)
    
    print(f"File for year {year} downloaded and processed.")