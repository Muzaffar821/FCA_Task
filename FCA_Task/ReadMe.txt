::::::::::::::::::::::::
Deployment and Execution
::::::::::::::::::::::::

1. Database Deployment

	Execute the following scripts in the order as given
	
	sql/CreateDatabases.sql
	sql/DDLBronze.sql
	sql/DDLSilver.sql
	sql/DDLGold.sql	
	sql/DBProcedures.sql
	
2. Python project is created for data loading/processing. Use the supplied project folder. Python 3.10.10 Version is used for testing this project.

3. Data Analytical report is created in Microsoft Power BI. Use the report files supplied in the bi folder of the package

4. To execute python project, run the following command and follow the prompts 

python src

5. Presentation files are supplied as part of ppt folder in the package

6. Snowflake Database credentials can be found in the following configuration files

	config/connections.json
	
