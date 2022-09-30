### Dependencies
- Please install pyspark and run this locally by creating a new project and copying the delta_upsert.py
- Your project src folder should have spark-warehouse folder (create it)

### Run
python3 delta_upsert.py

### Input file schema
<trade_date, book, security, quantity, price>

### Possible input files:
1. All trades for all books for a given date.
2. All trades for a specific book for all dates.
3. All trades for a specific book for a given dates.

### Module 1
#### Preparing and Updating Delta Table:
On the first iteration, we create a delta table for input data.
 - I have aggregated same security for given book for a given day in one row to deal with duplicate data.
We add 3 additional column <last_updated, key, change_key> for debugging and terse conditions during merge.
Next, when there are updates for the existing data, we get a new file.
The data in these files needs to be upserted in to the delta table.
We will use date, book, and security as a key value to determine if its a new transaction or existing one
  1. For all new transactions, we create a new entry in delta table (insert)
  2. For all existing transactions, if the change_key is different, we update its amount and price.
  3. All entries that already exist in delta table but are missing in the updates will be deleted as a separate merge
     - This is done by left anti joined data of delta table and source data in that order; which gives all the
       missing data in source


### Module 2
#### Creating an aggregated view or table
In this module we will create an aggregated view using delta table and store it as parquet file.
Assuming it is done for reporting purpose and need separate report files for each day for customer view.

### Aggregation Choice:
For aggregated view, I have decided to have a daily report indicating securities average price and total quantity
across all books. That is, the aggregation will be at <date, security> level. The output will loose book info.


### Note
 * Some additional info is added in the code related to the shortcoming of the current delta table implementation
 * Delta table versioning can be controlled by periodic vacuum on delta table. Alternatively, we can keep only the
   latest version if history is not desired for the application use cases.







