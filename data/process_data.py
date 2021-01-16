import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def load_data(messages_filepath, categories_filepath):
    '''
    PURPOSE: read in the data
    INPUTS:
        messages_filepath: string for path to messages.csv
        categories_filepath: string for path to categories.csv
    OUTPUT
        df: a merged datframe using the common id column for 
            messages and categories
    '''
    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)
    df = pd.merge(messages, categories, left_on='id', right_on='id', how='left')
    return df


def clean_data(df):
    '''
    PURPOSE: clean dataframe so it can be loaded into a SQL db
    INPUT: 
        df: the dataframe output of load_data()
    OUTPUT:
        df: the cleaned dataframe
    STEPS:
        1. Create dataframe of 36 categories by splitting string values
        2. Extract column names from first row of category frame
        3. Rename columns with extracted names
        4. Strip values in category frame so that they are 
            integer values (0 and 1)
        5. Drop categories column from original dataframe
        6. Concatenate original dataframe and categories dataframe
        7. Drop duplicates
    '''
    # create a dataframe of the 36 individual category columns
    categories = df.categories.str.split(pat=';', expand=True)

    # select the first row of the categories dataframe
    row = categories.iloc[0,].tolist()

    # use this row to extract a list of new column names for categories.
    category_colnames = [cat[:-2] for cat in row]
    
    # rename the columns of `categories`
    categories.columns = category_colnames
    
    # convert categorie values to numbers (last character of each str)
    for column in categories:
        # set each value to be the last character of the string
        categories[column] = [x.strip()[-1] for x in categories[column]]
        
        # convert column from string to numeric
        categories[column] = categories[column].astype('int')
    
    # drop the original categories column from `df`
    df = df.drop(columns='categories', inplace=True)
    
    # concatenate the original dataframe with the new `categories` dataframe
    df = pd.concat([df, categories], axis=1)

    # drop duplicates
    df = df.drop_duplicates()

    return df

def save_data(df, database_filename):
    pass  


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()