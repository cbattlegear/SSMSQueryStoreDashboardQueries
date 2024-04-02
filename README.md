# SSMS Query Store Dashboard Queries
After learning the queries ran for the Query Store dashboards in SSMS aren't documented publicly I made it my personal mission to fully record and document all of the queries used so that they can be used in automation or just for your day to day needs. 

These queries are not my own and are directly copied from what SSMS is running. All credit to the SQL Server Tools team who are way better at writing queries against DMVs than myself. 

## Usage
These are just standard T-SQL queries against the QDS DMVs. I have arranged them as each folder is the dashboard and each file is the individual view. 

To run the queries, copy and paste into SSMS and change the variable values if needed for your use case. 

By default all queries will get the last hour of statistics.

## Methodology

All of these queries were gathered by running XEvents against SQL Azure DB and capturing RPC Completed events to get the SQL Text of the queries as they ran. I will be adding more queries as time goes on. 

### SSMS Version
Queries From SSMS Build 20.0.70.0