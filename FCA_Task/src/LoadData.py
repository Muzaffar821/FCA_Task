from LoadMethods import *

def Process_Data():

    print('#### Section: Data Load ####')
    print('**** Bronze: Stage 1 Load ****')

    Opt=input("Do you want to execute Bronze: Stage 1 Load [y/n]: ") 
    if(Opt == 'y'):
        Opt=input("Do you want to prepare csv files from source 1 excel files [y/n]: ") 
        if(Opt == 'y'):
			#Prepare csv files from source 1 excel file
            print('Processing Source 1 Excel files...', end="")
            excel_to_csv('data/','Business-complaints-data-H1-2022.xlsx')
            excel_to_csv('data/','Early-resolution-data-H1-2022.xlsx')
            print('Done')
            
        Opt=input("Do you want to prepare csv files from source 3 excel file [y/n]: ") 
        if(Opt == 'y'):
			#Prepare csv files from source 3 excel file
            print('Processing Source 3 Excel file...', end="")
            excel_to_csv('data/','Data Tables.xlsx')
            print('Done')
            
        Opt=input("Do you want to load source 1 and source 3 files to database [y/n]: ") 
        if(Opt == 'y'):
			#Load source 1 and source 3 files to database
            print('Loading to database...')
            Load_File_Sources('data/')
            print('...Done')
            
        Opt=input("Do you want to load Firms of Interest [y/n]: ") 
        if(Opt == 'y'):
			#Load firms of interest
            print('Loading Firms of interest to database...')
            Load_Firms_Of_Interest()
            print('...Done')        

    print('**** Silver: Stage 2 Load ****')
	
    Opt=input("Do you want to execute Silver: Stage 2 Load [y/n]: ") 
    if(Opt == 'y'):
        Opt=input("Do you want to load Firm Data [y/n]: ") 
        if(Opt == 'y'):
			#Load firms of interest
            print('Loading Firm Details to database...')
            Load_Firm_Details()
            print('...Done')
            
        Opt=input("Do you want to load Twitter Data [y/n]: ") 
        if(Opt == 'y'):
			#Load Twitter Data
            print('Loading Twitter Data to database...')
            Load_Twitter_Data()
            print('...Done')
            
        Opt=input("Do you want to load New Cases Data [y/n]: ") 
        if(Opt == 'y'):
			#Load New Cases Data
            print('Loading New Cases Data to database...')
            Load_New_Cases_Data()
            print('...Done')
            
        Opt=input("Do you want to load Resolved Cases Data [y/n]: ")
        if(Opt == 'y'):
			#Load Resolved Data
            print('Loading Resolved Cases Data to database...')
            Load_Resolved_Cases_Data()
            print('...Done')                      

    print('**** Gold: Stage 3 Load ****')
	
    Opt=input("Do you want to execute Gold: Stage 3 Load [y/n]: ") 
    if(Opt == 'y'):
        Opt=input("Do you want to load Twitter Stats [y/n]: ")
        if(Opt == 'y'):
			#Load Twitter Stats
            print('Loading Twitter Stats to database...')
            Load_Twitter_Stats()
            print('...Done')
            
        Opt=input("Do you want to load New Cases Stats [y/n]: ")
        if(Opt == 'y'):
			#Load New Cases Stats
            print('Loading New Cases Stats to database...')
            Load_New_Cases_Stats()
            print('...Done')
        
        Opt=input("Do you want to load Resolved Cases Stats [y/n]: ")
        if(Opt == 'y'):
			#Load Resolved Stats
            print('Loading Resolved Cases Stats to database...')
            Load_Resolved_Cases_Stats()
            print('...Done') 