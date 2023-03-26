# Project: Data Warehouse

<br>

This project is considered as an extension of the Project: Data Modeling with Postgres, the 1st project of the Data Engineering Nanodegree program. It shares the same purpose and analytical goals in context of the company Sparkify, as well as the design of database schemas, but with the following differences in ETL pipelines:

* The main database system used in the project is Redshift, an [analytics](https://aws.amazon.com/big-data/datalakes-and-analytics/)/[database](https://aws.amazon.com/products/databases/) service offered by AWS. One can achieve most of the essential functionality of Redshift by creating an in-house installation of PostgreSQL combined with [cstore_fdw](https://www.postgresql.org/about/news/cstore_fdw-13-release-for-columnar-store-postgresql-1601/) extension, but Redshift is better integrated with AWS environments.
* The staging area of the intermediate data in the project is user-created database tables which reside in Redshift. This is also different when compared with the 1st project, which used in-memory data structures (Pandas DataFrames and Python lists) to hold intermediate data (actually, if we chose to create staging tables in PostgreSQL in the 1st project, these two projects would be quite similar).
* The unprocessed JSON raw files are placed in several S3 buckets, instead of some directories in the local filesystems.

Due to these differences, the ETL pipelines of this project will be placed in AWS environments, and the Udacity workspace will be used to initiate these pipelines remotely.

<br>

The choice of using Redshift as the main database system in this project also introduced some additional problems when performing data population. While using [IDENTITY](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_examples.html#r_CREATE_TABLE_NEW-create-a-table-with-an-identity-column) column feature to replace [SERIAL](https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-SERIAL) pseudo datatype and [creating staging tables with a defined distribution style to speed up loading](https://aws.amazon.com/premiumsupport/knowledge-center/redshift-fix-copy-analyze-statupdate-off/) are relatively minor issues, [most of the table constraints are not enforced by Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html) and [the absence of merge feature in Redshift](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-upsert.html) are problematic when we want to load data into Redshift tables in the same way as what we did in the 1st project. In this project we will have to rewrite SQL queries to make them deliver the equivalent functionality of PRIMARY KEY constraints and MERGE statements, and the details of these modifications can be found in the script files of the project.

Alternatively, it may be feasible to make use of the table constraint enforcement and merging of PostgreSQL database by using another PostgreSQL database as the staging area, and then load the processed data into Redshift final tables, but this approach is beyond the scope of this project, and by doing so will also increase the operational costs to implement the solution.

For more information about the differences between Redshift and PostgreSQL, check the URL list below:
* [How Redshift differs from PostgreSQL](https://www.stitchdata.com/blog/how-redshift-differs-from-postgresql/)
* [Amazon Redshift – What you need to think before defining primary key](http://www.sqlhaven.com/amazon-redshift-what-you-need-to-think-before-defining-primary-key/)
* [Redshift Pitfalls And How To Avoid Them](https://heap.io/blog/redshift-pitfalls-avoid)

<br>

To give the project a test run, open a new launcher in the project workspace, click on the "Terminal" icon to start a new terminal session. Under the directory path /home/workspace , first we will need to run the create_tables.py script (which will import the sql_queries.py behind the scenes) to create new Redshift tables (unlike the 1st project, we will not create any Redshift database in these scripts - the Redshift database needs to be manually created in advance). This command can also reset the Redshift tables if you have to do it.

    $ python3 create_tables.py

Then we can proceed with importing data into Redshift tables by running the etl.py script.

    $ python3 etl.py

Notice that the total execution time of the etl.py script will heavily depend on the **node type** as well as the **number of nodes** we choose when creating the Redshift cluster. It takes more than 30 minutes to finish the script execution in a dc2.large 1-node cluster, but only about 10 minutes in a dc2.large 2-node cluster. But be wary about the extra AWS service charges you have to pay when you create a Redshift cluster with 2 or more nodes in it. Try to be as economical as possible when using the AWS services.