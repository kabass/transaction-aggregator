# Transaction Aggregation

## Description
This application enable to aggregate transaction from carrefour store to get the most important product ( sales  and turnover )

## Functional description
The functional description is [here](https://github.com/Carrefour-Group/phenix-challenge) 

## Technical description 

The application is implemented with spark framework with scala API. Here we use one spark component :
 * spark-sql : to load data, some data processing, saving data

In the application we have two services :
* ***FileManager*** : This service process all files task ( read, write ...). Each operation of this service is well documented in the code.
* ***Processor*** : This service contains general steps of the application. The application has three main steps 
  * 1) **buildAndSaveDailyAggregatedTransaction** : This operation allows to aggregate all transaction for a product in a day 
  and then link the price from referential
  * 2) **generateTopDailyAggreated** : This operation allows to generate a top daily product and save it to filesystem
  * 3) **generateTopPeriodAggreated** : This operation allows to generate a top period ( J-7) product and save it to filesystem
 
We have an implicit class (**EnrichedDataFrame.TransactionDataFrame**) for all dataframe action related to the transaction. 
All operation are well documented.

## Package description
In the package we have the following files or directories :
 
1. jars
This directory contains the application jars with all dependencies ( it's an uber jar)

2. sample 
This directory contains the sample file for test application installation

3. shell
This direction contains shell script to run application . This script is just an example to check the installation. 
In production you have to change some configuration ( cf Configuration / Running )

## Installation and running
### Package downloading
  You can find the package [here](https://www.dropbox.com/s/3osf8zwz42jmtrt/transaction-aggregator-1.0-SNAPSHOT.zip?dl=0)
  
### Configuration / Running
* 1 - Go to shell/ directory
* 2 - Launch run script : sh run.sh
  * The application has some external configuration by command line argument. Here is the list of command line argument 
     *  -a, --archive
          the archive directory
          
     *  -d, --date
          the date to process : default current day
          Default: 20190424
          
     *  -h, --help
    
     * -i, --input
          the input directory
          
     * -o, --output
          the ouput directory
          
     *  -pa, --partionNumber
          the data partition number : default 8
          Default: 8
          
     *  -p, --period
          the number of previous days to calculate the top sales: default 7
          Default: 7
          
     *  -t, --top
          the the number of top elements : default 100
          Default: 100

### Input/ Output directories strutures
#### Input
  * **referential/** : In this directory we have all referential files
  * **transaction/** : In this directory we have the transaction file

#### Ouput
  * **global/** : In this directory we have the generated related to all stores
    * ..
  * **store/** : In this directory we have per store file
    * **top_n_sales/**: Here we have the top sales files
        *...
    * **top_n_turnover/**: Here we have the top turnover files
        * **daily/** : The per day files 
        * **period/** : The period files ( J-7)
  
### Job scheduling
The job can be scheduled using OOZIE tool in hadoop platform. The task will be to a oozie coordinator linked to a coordinator qith an spark action.


### Performance Test Result
Les données de test de performance sont générées de manière aléatoire avec la classe **src/test/com.carrefour.phenix.DataGenerator**
#### Input
The application take 20 minutes to run with the following input :
 - 1200 stores : 
 - 50000 products per store per days : 60 000 000  rows 
 - 10000 sales per days per store : 12 000 000 rows
 
#### Environnement configuration 
  --conf spark.executor.memory=512m \
  --conf spark.driver.memory=512m \
  --conf spark.executor.cores=2 \

