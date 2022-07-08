# SynapseServerlessSQLExamples
This repository shows how to leverage Synapse's Serverless SQL Pool to get the optimal partition elimination when working with parquet and delta.

# Environment Setup Steps

In order to execute these examples you need to have a synapse envioronment.  To create the environment:
1. Create a synapse resoure in the azure portal.
1. Add a Spark pool
1. Open the Synapse Studio

# Loading the sample data and executing queries

To work through the example you'll need to use a combination of spark and serverless sql.
1. Import the CreateParquetPartitionedByDate.ipynb file.
1. In the first code cell, change the end date for the data load to today's month and day in 2018.
1. Execute all the cells in the notebook in order.
1. Import the .sql files.
1. Execute the 01-CreateTaxiDataDatabase.sql script to create the database (this is required for creating views)
1. Execute 02a-CreateViewOnDatePartitionedParquet.sql and 02b-CreateViewOnDatePartitionedDelta.sql files to create the necessary views.
1. Execute the various commands in the 03a-Query examples over parquet.sql and 03b-Query Examples over delta.sql files.
1. Examine the "messages" after query execution to see the amount of data read and returned for various query patterns.

# Leveraging the Power BI Template

The power bi template illustrates how to take advantage of the partitioned structure in the serverless sql pool.
1. Open the YellowTaxiData.pbit file in Power BI Desktop.
1. Enter the uri to the on demand query endpoint.
1. Enter "TaxiData" for the database name.
1. Enter Start and end dates (note make the date range small to minimize the amount of initial load time).
1. Right click on the table to create the incremental refresh policy, good options are
    - Archive data starting 1 year...
    - Inremental refresh data starting 3 days...
    - Get the latest data in real time with Direct Query
1. Save and deploy the model to your Power BI tenant.
1. After the model is loaded update the credentials for the database in the service (this do not populate automatically for security reasons), and schedule refresh or manually refresh the data.