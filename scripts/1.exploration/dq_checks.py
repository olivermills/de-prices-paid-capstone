import pandas as pd
import os

# Get list of files in data folder
folder_path = 'data/csv/'
file_list = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
#file_list = os.listdir(folder_path)

# Loop through each file
for file in file_list:
    # Read file into pandas dataframe
    df = pd.read_csv(os.path.join(folder_path, file))
    #df = pd.read_csv(folder_path + file)

    # Check if 'transaction_unique_identifier' column exists in dataframe
    if 'transaction_unique_identifier' not in df.columns:
        print(f"File: {file}, Error: 'transaction_unique_identifier' column not found in dataframe.")
    else:
        # Check that 'transaction_unique_identifier' column is non-null and unique
        if not (df['transaction_unique_identifier'].notnull().all() and df['transaction_unique_identifier'].is_unique):
            print(f"File: {file}, Error: 'transaction_unique_identifier' column is not non-null and unique")

        #Summary statistics for 'Price' column
        if 'price' in df.columns:
            price_stats = df['price'].describe()
            # Format price values to be displayed as integers with commas
            price_stats['min'] = '{:,.0f}'.format(price_stats['min'])
            price_stats['25%'] = '{:,.0f}'.format(price_stats['25%'])
            price_stats['50%'] = '{:,.0f}'.format(price_stats['50%'])
            price_stats['75%'] = '{:,.0f}'.format(price_stats['75%'])
            price_stats['max'] = '{:,.0f}'.format(price_stats['max'])
            price_stats['mean'] = '{:,.0f}'.format(price_stats['mean'])
            price_stats['std'] = '{:,.0f}'.format(price_stats['std'])
            print(f"File: {file}, 'Price' column summary statistics:\n{price_stats}")

        # Summary statistics for 'date_of_transfer' column
        if 'date_of_transfer' in df.columns:
            df['date_of_transfer'] = pd.to_datetime(df['date_of_transfer'])
            date_stats = df['date_of_transfer'].describe(datetime_is_numeric=True)
            print(f"File: {file}, 'date_of_transfer' column summary statistics:\n{date_stats}")


    
    # Loop through each column in dataframe
    for column in df.columns:
        # Count the number of null values in the column
        null_count = df[column].isnull().sum()
        
        # Calculate the percentage of null values in the column
        null_pct = null_count / len(df) * 100
        
        # Check if there are any null values in the column
        if null_count > 0:
            # Print the name of the file, the column name, the count of null values, and the percentage of null values
            print(f"File: {file}, Column: {column}, Null Count: {null_count}, Null Percentage: {null_pct:.2f}%")
