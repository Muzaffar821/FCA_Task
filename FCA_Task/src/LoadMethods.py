import json
import pandas as pd
from snowflake.snowpark.session import Session, FileOperation
import os
import csv
import requests

from Globals import *

def excel_to_csv(folder, excel_file):
    """
    This function exports given Excel into csv files

    Parameters:
    folder: subfolder where excel files can be found
    excel_file: name of the source excel file

    Returns:
    None
    """    

    # Create temp folder if does not exist
    if not os.path.exists(folder + 'temp/'):
        os.mkdir(path)
    
    # Read input excel file
    read_file = pd.read_excel(folder + excel_file, sheet_name=None)  
    
    # Loop through all sheets of the excel file
    for sheet in read_file: 

        # Remove spaces from sheet name
        csv_file = ''.join(sheet.split())

        # Create csv file name
        csv_file = folder + 'temp/' + csv_file + '.csv'

        #remove file if it exists 
        try:
            os.remove(csv_file)
        except OSError:
            pass

        #write to csv format
        read_file[sheet].to_csv (csv_file, 
                  index = None,
                  header=True,
                  quotechar='"',
                  quoting=csv.QUOTE_ALL) 

def Load_File_Sources(folder):
    """
    This function loads given excel file sources to Bronze: Stage 1

    Parameters:
    folder: sub folder where input excel files can be found

    Returns:
    None
    """
    # Read file configuration    
    with open('config/files.json','r') as file:
        file_params = json.load(file)    
    
    # Read database connection configuration
    with open('config/connections.json','r') as file:
        connection_params = json.load(file)
    
    # Create a database connection
    session = Session.builder.configs(connection_params['Bronze']).create()

    # Create internal stage if it does not exists
    session.sql("create or replace stage csv ").collect()

    # Loop through each file specified in the file configuration
    for file_name in file_params:

        # Get target table name from the configuration        
        table_name = file_params[file_name]['table']

        # Get the number of rows to skip as specified in the configuration
        skip_header = file_params[file_name]['skip_header']

        # Log output to screen
        print(f"...loading {file_name} into {table_name}")

        #Upload file to stage
        FileOperation(session).put(folder + 'temp/' + file_name + '.csv', '@csv/' + file_name + ".csv")    

        #Truncate table 
        session.sql("truncate table " + table_name)

        #Load table from stage
        session.sql("copy into " + table_name + " from @csv/" + file_name + ".csv" + " file_format= (type=csv field_delimiter=',' skip_header=" + skip_header + " field_optionally_enclosed_by='\"')").collect()

    #Drop stage
    session.sql("drop stage csv").collect()

    #Close the database session
    session.close()

def Load_Firms_Of_Interest():
    """
    This function loads the Firms of interest (in-scope of the analysis)

    Parameters:
    None

    Returns:
    None
    """
    
    # Get connection configuration
    with open('config/connections.json','r') as file:
        connection_params = json.load(file)

    # Open a session on the database
    session = Session.builder.configs(connection_params['Bronze']).create()
    
    #Load Firms list
    session.call("LOAD_FIRMS_OF_INTEREST")  
    
    #Load Firms of interest by calling the stored procedure
    df = session.sql("SELECT DISTINCT TWITTER_HANDLE FROM FIRMS_OF_INTEREST").to_pandas()

    oRow = {} # A row of output data
    oList = [] # A list to hold rows of data
    
    # Loop through each row in the data
    for index, drow in df.iterrows():
        try:
            # Prepare parameters for API Call
            params['q'] = drow['TWITTER_HANDLE']

            # Call the Web API to get the Firm name details
            Jdata = requests.get(SearchAPIurl, params=params, headers=headers).json()

            # Initialize loop control variables
            N = len(Jdata['Data'])
            I = 0

            # Loop through each record in the output json
            while I < N:

                # Create a database row for the Firm information
                oRow['TWITTER_HANDLE'] = drow['TWITTER_HANDLE']
                oRow['BUSINESS_NAME'] = Jdata['Data'][I]['Name']
                oRow['BUSINESS_TYPE'] = Jdata['Data'][I]['Type of business or Individual']
                oRow['REFERENCE_NUMBER'] = Jdata['Data'][I]['Reference Number']
                oRow['STATUS'] = Jdata['Data'][I]['Status']

                # Append the row to the list of directories
                oList.append(oRow.copy())
                I += 1

        except Exception:
            pass

    #Create Pandas data frame
    oFrame = pd.DataFrame(oList)

    #Truncate table 
    session.sql("truncate table FIRM_DETAILS ")
    
    #Write to database
    session.write_pandas(oFrame, "FIRM_DETAILS", auto_create_table=False, overwrite=False)

    #Close the database session
    session.close()      

def Load_Firm_Details():
    """
    This function loads Firm Details to Stage 2

    Parameters:
    None

    Returns:
    None
    """
    
    # Get connection configuration
    with open('config/connections.json','r') as file:
        connection_params = json.load(file)

    # Open a session on the database
    session = Session.builder.configs(connection_params['Bronze']).create()
    
    #Load Firm Details
    session.call("LOAD_FIRM_DETAILS")  
    
    #Close the database session
    session.close() 

def Load_Twitter_Data():
    """
    This function loads the data from Twitter to Stage 2

    Parameters:
    None

    Returns:
    None
    """
    
    # Get connection configuration
    with open('config/connections.json','r') as file:
        connection_params = json.load(file)

    # Open a session on the database
    session = Session.builder.configs(connection_params['Bronze']).create()
    
    #Load Twitter Data
    session.call("LOAD_TWITTER_DATA")  
    
    #Close the database session
    session.close()     

def Load_New_Cases_Data():
    """
    This function loads the data from New Cases to Stage 2

    Parameters:
    None

    Returns:
    None
    """
    
    # Get connection configuration
    with open('config/connections.json','r') as file:
        connection_params = json.load(file)

    # Open a session on the database
    session = Session.builder.configs(connection_params['Bronze']).create()
    
    #Load New Cases Data
    session.call("LOAD_NEW_CASES")  
    
    #Close the database session
    session.close() 

def Load_Resolved_Cases_Data():
    """
    This function loads the data from Resolved Cases to Stage 2

    Parameters:
    None

    Returns:
    None
    """
    
    # Get connection configuration
    with open('config/connections.json','r') as file:
        connection_params = json.load(file)

    # Open a session on the database
    session = Session.builder.configs(connection_params['Bronze']).create()
    
    #Load Resolved Cases Data
    session.call("LOAD_RESOLVED_CASES")  
    
    #Close the database session
    session.close() 

def Load_Twitter_Stats():
    """
    This function loads the data from Twitter to Stage 3

    Parameters:
    None

    Returns:
    None
    """
    
    # Get connection configuration
    with open('config/connections.json','r') as file:
        connection_params = json.load(file)

    # Open a session on the database
    session = Session.builder.configs(connection_params['Bronze']).create()
    
    #Load Twitter Data
    session.call("LOAD_TWITTER_STATS")  
    
    #Close the database session
    session.close()     

def Load_New_Cases_Stats():
    """
    This function loads the data from New Cases to Stage 3

    Parameters:
    None

    Returns:
    None
    """
    
    # Get connection configuration
    with open('config/connections.json','r') as file:
        connection_params = json.load(file)

    # Open a session on the database
    session = Session.builder.configs(connection_params['Bronze']).create()
    
    #Load New Cases Data
    session.call("LOAD_NEW_CASES_STATS")  
    
    #Close the database session
    session.close() 

def Load_Resolved_Cases_Stats():
    """
    This function loads the data from Resolved Cases to Stage 2

    Parameters:
    None

    Returns:
    None
    """
    
    # Get connection configuration
    with open('config/connections.json','r') as file:
        connection_params = json.load(file)

    # Open a session on the database
    session = Session.builder.configs(connection_params['Bronze']).create()
    
    #Load Resolved Cases Data
    session.call("LOAD_RESOLVED_CASES_STATS")  
    
    #Close the database session
    session.close() 