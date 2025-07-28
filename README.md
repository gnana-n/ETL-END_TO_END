drop database sales_dwh;

Step-1 Create User & Virtual Warehouse
Lets create a virtual warehouse and user account that will be used to run Snowpark ETL workload.

-- create a virtual warehouse

create warehouse snowpark_etl_wh 
    with 
    warehouse_size = 'xsmall' 
    warehouse_type = 'standard' 
    auto_suspend = 60 
    auto_resume = true 
    min_cluster_count = 1
    max_cluster_count = 1 
    scaling_policy = 'standard';

use role accountadmin;


--Validate if we are able to use the user and virtual warehouse created in step-1

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    
    df = session.sql("select * from information_schema.packages where language='python' ")
    
    # Print a sample of the dataframe to standard output.
    df .show()

    # Return value will appear in the Results tab.
    return df 
    

import snowflake.snowpark as snowpark
def main(session: snowpark.Session): 
    # Your code goes here, inside the "main" handler.
    
    context_df = session.sql("select current_role(), current_database(), current_schema(), current_warehouse()")
    context_df.show(2)

    customer_df = session.sql("select c_custkey,c_name,c_phone,c_mktsegment from snowflake_sample_data.tpch_sf1.customer limit 10")
    customer_df.show(5)

    # Return value will appear in the Results tab.
    return context_df


    
Step-2 Database & Schema Object
Creating sales_dwh and 5 schemas under the sales_dwh.

-- create database
create database if not exists sales_dwh;

use database sales_dwh;

create schema if not exists source; -- will have source stage etc
create schema if not exists curated; -- data curation and de-duplication
create schema if not exists consumption; -- fact & dimension
create schema if not exists common; -- for file formats sequence object etc


Step-3.1 Internal Stage In Source Schema
Creating internal stage that will host all the data setup available in our local machine.

-- creating internal stage within source schema.
use schema source;
create or replace stage my_internal_stg;

ls @my_internal_stg;
rm  @my_internal_stg;


Step 3.2 Loading Data To Internal Stage 

snowsql -a yp20234.us-east-2.aws -u denzel
use database sales_dwh;
use schema source;
-- following put command can be executed
/*
-- csv example

 put file://C:/Users/HP/Desktop/snowflake/orders-in.csv @my_internal_stg auto_compress=False overwrite=True, parallel=3 ;


-- json example
 put file://C:/Users/HP/Desktop/snowflake/orders-us.json @my_internal_stg auto_compress=False overwrite=True, parallel=3 ;
*/

Step-4 File Format Objects Within Common Schema
Following file formate will be created under common schema and will be used to read and process the data from the internal stage location.

use schema common;
-- create file formats csv (India), json (France), Parquet (USA)
create or replace file format my_csv_format
  type = csv
  field_delimiter = ','
  skip_header = 1
  null_if = ('null', 'null')
  empty_field_as_null = true
  field_optionally_enclosed_by = '\042'
  compression = auto;

-- json file format with strip outer array true
create or replace file format my_json_format
  type = json
  strip_outer_array = true
  compression = auto;



-- Internal Stage - Query The CSV Data File Format

use schema source;
select 
    t.$1::text as order_id, 
    t.$2::text as customer_name, 
    t.$3::text as mobile_key,
    t.$4::number as order_quantity, 
    t.$5::number as unit_price, 
    t.$6::number as order_valaue,  
    t.$7::text as promotion_code , 
    t.$8::number(10,2)  as final_order_amount,
    t.$9::number(10,2) as tax_amount,
    t.$10::date as order_dt,
    t.$11::text as payment_status,
    t.$12::text as shipping_status,
    t.$13::text as payment_method,
    t.$14::text as payment_provider,
    t.$15::text as mobile,
    t.$16::text as shipping_address
 from 
   @my_internal_stg/orders-in.csv
   (file_format => 'sales_dwh.common.my_csv_format') t; 

-- Internal Stage - Query The Parquet Data File Format

-- Internal Stage - Query The JSON Data File Format
select                                                       
    $1:"Order ID"::text as orde_id,                   
    $1:"Customer Name"::text as customer_name,          
    $1:"Mobile Model"::text as mobile_key,              
    to_number($1:"Quantity") as quantity,               
    to_number($1:"Price per Unit") as unit_price,       
    to_decimal($1:"Total Price") as total_price,        
    $1:"Promotion Code"::text as promotion_code,        
    $1:"Order Amount"::number(10,2) as order_amount,    
    to_decimal($1:"Tax") as tax,                        
    $1:"Order Date"::date as order_dt,                  
    $1:"Payment Status"::text as payment_status,        
    $1:"Shipping Status"::text as shipping_status,      
    $1:"Payment Method"::text as payment_method,        
    $1:"Payment Provider"::text as payment_provider,    
    $1:"Phone"::text as phone,                          
    $1:"Delivery Address"::text as shipping_address
from                                                
@sales_dwh.source.my_internal_stg/orders-us
(file_format => sales_dwh.common.my_json_format);


Step-5 Foreign Exchange Rate Data
Create Foreign exchange rate data to convert the local currency data (like INR or Euro) to US Dollar so when we create total sales, at global level, so we can build PKI in a single currency and compare the performance.

-- put file://C:/Users/HP/Desktop/snowflake/exchange.csv @my_internal_stg auto_compress=False overwrite=True, parallel=3 ;
list @my_internal_stg;

use schema common;
create or replace transient table exchange_rate(
    date date, 
    usd2usd decimal(10,7),
    usd2eu decimal(10,7),
    usd2can decimal(10,7),
    usd2uk decimal(10,7),
    usd2inr decimal(10,7),
    usd2jp decimal(10,7)
);

copy into sales_dwh.common.exchange_rate
from 
(
select 
    t.$1::date as exchange_dt,
    to_decimal(t.$2) as usd2usd,
    to_decimal(t.$3,12,10) as usd2eu,
    to_decimal(t.$4,12,10) as usd2can,
    to_decimal(t.$4,12,10) as usd2uk,
    to_decimal(t.$4,12,10) as usd2inr,
    to_decimal(t.$4,12,10) as usd2jp
from 
     @sales_dwh.source.my_internal_stg/exchange.csv
     (file_format => 'sales_dwh.common.my_csv_format') t
);

select * from sales_dwh.common.exchange_rate;

Step-6.1 Loading Data From Internal Stage to Source Tables
Every time the data moves from internal stage location to source layer within permanent tables, it will add a sequence number that will help to de-duplicate the data set.

-- order table
use schema source;

create or replace sequence in_sales_order_seq 
  start = 1 
  increment = 1 
comment='This is sequence for India sales order table';

create or replace sequence us_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for USA sales order table';


  
6.2 Source Table DDL Script
show sequences;
-- India Sales Table in Source Schema (CSV File)
create or replace transient table in_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_valaue number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 mobile varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

-- US Sales Table in Source Schema (Parquet File)
create or replace transient table us_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_valaue number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 phone varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);



6.3 Snowpark Python Example To Load To Stage
Here is the example of snowpark python example to load the data from local machine, where all amazon mobile order data in 3 different formats, CSV, Parquet & JSON can be moved to snowflake internal stage.

# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
import sys
import logging

from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType
from snowflake.snowpark.functions import col,lit,row_number, rank
from snowflake.snowpark import Window

   

def ingest_in_sales(session)-> None:
    session.sql(" \
            copy into sales_dwh.source.in_sales_order from ( \
            select \
            in_sales_order_seq.nextval, \
            t.$1::text as order_id, \
            t.$2::text as customer_name, \
            t.$3::text as mobile_key,\
            t.$4::number as order_quantity, \
            t.$5::number as unit_price, \
            t.$6::number as order_valaue,  \
            t.$7::text as promotion_code , \
            t.$8::number(10,2)  as final_order_amount,\
            t.$9::number(10,2) as tax_amount,\
            t.$10::date as order_dt,\
            t.$11::text as payment_status,\
            t.$12::text as shipping_status,\
            t.$13::text as payment_method,\
            t.$14::text as payment_provider,\
            t.$15::text as mobile,\
            t.$16::text as shipping_address,\
            metadata$filename as stg_file_name,\
            metadata$file_row_number as stg_row_numer,\
            metadata$file_last_modified as stg_last_modified\
            from \
            @sales_dwh.source.my_internal_stg/orders-in.csv \
            (                                                             \
                file_format => 'sales_dwh.common.my_csv_format'           \
            ) t  )  on_error = 'Continue'     \
            "
            ).collect()

def ingest_us_sales(session)-> None:
    session.sql(' \
            copy into sales_dwh.source.us_sales_order                \
            from                                    \
            (                                       \
                select                              \
                us_sales_order_seq.nextval, \
                $1:"Order ID"::text as order_id,   \
                $1:"Customer Name"::text as customer_name,\
                $1:"Mobile Model"::text as mobile_key,\
                to_number($1:"Quantity") as quantity,\
                to_number($1:"Price per Unit") as unit_price,\
                to_decimal($1:"Total Price") as total_price,\
                $1:"Promotion Code"::text as promotion_code,\
                $1:"Order Amount"::number(10,2) as order_amount,\
                to_decimal($1:"Tax") as tax,\
                $1:"Order Date"::date as order_dt,\
                $1:"Payment Status"::text as payment_status,\
                $1:"Shipping Status"::text as shipping_status,\
                $1:"Payment Method"::text as payment_method,\
                $1:"Payment Provider"::text as payment_provider,\
                $1:"Phone"::text as phone,\
                $1:"Delivery Address"::text as shipping_address,\
                metadata$filename as stg_file_name,\
                metadata$file_row_number as stg_row_numer,\
                metadata$file_last_modified as stg_last_modified\
                from                                \
                    @sales_dwh.source.my_internal_stg/orders-us.json\
                    (file_format => sales_dwh.common.my_json_format)\
                    ) on_error = continue \
            '
            ).collect()
    

def main(session: snowpark.Session): 
    #ingest in sales data
    ingest_in_sales(session)

    #ingest in sales data
    ingest_us_sales(session) 
    return "success"


call STAGE_TO_SOURCE_TABLE_PROC() ;

select * from sales_dwh.source.us_sales_order;
truncate table sales_dwh.source.us_sales_order;
select * from sales_dwh.source.in_sales_order;
truncate table sales_dwh.source.in_sales_order;
     



    
Step-7 Loading Data From Source To Curated Layer
7.1 Sequence Object Under Curated Schema Layer
-- Following are for curated schema
-- -----------------------------------
use schema curated;
create or replace sequence in_sales_order_seq 
  start = 1 
  increment = 1 
comment='This is sequence for India sales order table';

create or replace sequence us_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for USA sales order table';



  
7.2 Curated Layer DDL
use schema curated;
-- curated India sales order table
create or replace table in_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 order_dt date,
 customer_name varchar(),
 mobile_key varchar(),
 country varchar(),
 region varchar(),
 order_quantity number(38,0),
 local_currency varchar(),
 local_unit_price number(38,0),
 promotion_code varchar(),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number(15,7),
 us_total_order_amt number(23,8),
 usd_tax_amt number(23,8),
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 conctact_no varchar(),
 shipping_address varchar()
);

-- curated US sales order table
create or replace table us_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 order_dt date,
 customer_name varchar(),
 mobile_key varchar(),
 country varchar(),
 region varchar(),
 order_quantity number(38,0),
 local_currency varchar(),
 local_unit_price number(38,0),
 promotion_code varchar(),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number(15,7),
 us_total_order_amt number(23,8),
 usd_tax_amt number(23,8),
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 conctact_no varchar(),
 shipping_address varchar()
);

7.3 Snowpark Python — Source To Curated (IN)
use schema source;

import sys
import logging
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType
from snowflake.snowpark.functions import col,lit,row_number, rank
from snowflake.snowpark import Window


 
    

def filter_dataset(df, column_name, filter_criterian) -> DataFrame:
    # Payment Status = Paid
    # Shipping = Delivered
    return_df = df.filter(col(column_name) == filter_criterian)

    return return_df

def main(session: snowpark.Session):
    sales_df = session.sql("select * from in_sales_order")

    # apply filter to select only paid and delivered sale orders
    #select * from in_sales_order where PAYMENT_STATUS = 'Paid' and SHIPPING_STATUS = 'Delivered'
    paid_sales_df = filter_dataset(sales_df,'PAYMENT_STATUS','Paid')
    shipped_sales_df = filter_dataset(paid_sales_df,'SHIPPING_STATUS','Delivered')

    # adding country and region to the data frame
    # select *, 'IN' as Country, 'APAC' as Region from in_sales_order where PAYMENT_STATUS = 'Paid' and SHIPPING_STATUS = 'Delivered'
    country_sales_df = shipped_sales_df.with_column('Country',lit('IN')).with_column('Region',lit('APAC'))

    # join to add forex calculation
    forex_df = session.sql("select * from sales_dwh.common.exchange_rate")
    sales_with_forext_df = country_sales_df.join(forex_df,country_sales_df['order_dt']==forex_df['date'],join_type='outer')
    #sales_with_forext_df.show(2)

    #de-duplication
    #print(sales_with_forext_df.count())
    unique_orders = sales_with_forext_df.with_column('order_rank',rank().over(Window.partitionBy(col("order_dt")).order_by(col('_metadata_last_modified').desc()))).filter(col("order_rank")==1).select(col('SALES_ORDER_KEY').alias('unique_sales_order_key'))
    final_sales_df = unique_orders.join(sales_with_forext_df,unique_orders['unique_sales_order_key']==sales_with_forext_df['SALES_ORDER_KEY'],join_type='inner')
    final_sales_df = final_sales_df.select(
        col('SALES_ORDER_KEY'),
        col('ORDER_ID'),
        col('ORDER_DT'),
        col('CUSTOMER_NAME'),
        col('MOBILE_KEY'),
        col('Country'),
        col('Region'),
        col('ORDER_QUANTITY'),
        lit('INR').alias('LOCAL_CURRENCY'),
        col('UNIT_PRICE').alias('LOCAL_UNIT_PRICE'),
        col('PROMOTION_CODE').alias('PROMOTION_CODE'),
        col('FINAL_ORDER_AMOUNT').alias('LOCAL_TOTAL_ORDER_AMT'),
        col('TAX_AMOUNT').alias('local_tax_amt'),
        col('USD2INR').alias("Exhchange_Rate"),
        (col('FINAL_ORDER_AMOUNT')/col('USD2INR')).alias('US_TOTAL_ORDER_AMT'),
        (col('TAX_AMOUNT')/col('USD2INR')).alias('USD_TAX_AMT'),
        col('payment_status'),
        col('shipping_status'),
        col('payment_method'),
        col('payment_provider'),
        col('mobile').alias('conctact_no'),
        col('shipping_address')
    )

    #final_sales_df.show(5)
    final_sales_df.write.save_as_table("sales_dwh.curated.in_sales_order",mode="append")
    return "Success"

 call SOURCE_CURRATED_TABLE_IN_PROC();
    select * from sales_dwh.curated.in_sales_order;
    truncate table sales_dwh.curated.in_sales_order;
    
7.4 Snowpark Python — Source To Curated (US)
import sys
import logging
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType
from snowflake.snowpark.functions import col,lit,row_number, rank
from snowflake.snowpark import Window


 
    

def filter_dataset(df, column_name, filter_criterian) -> DataFrame:
    # Payment Status = Paid
    # Shipping = Delivered
    return_df = df.filter(col(column_name) == filter_criterian)

    return return_df

def main(session: snowpark.Session):
    sales_df = session.sql("select * from us_sales_order")
    paid_sales_df = filter_dataset(sales_df,'PAYMENT_STATUS','Paid')
    shipped_sales_df = filter_dataset(paid_sales_df,'SHIPPING_STATUS','Delivered')

    # adding country and region to the data frame
    # select *, 'IN' as Country, 'APAC' as Region from us_sales_order where PAYMENT_STATUS = 'Paid' and SHIPPING_STATUS = 'Delivered'
    country_sales_df = shipped_sales_df.with_column('Country',lit('US')).with_column('Region',lit('NA'))

    # join to add forex calculation
    forex_df = session.sql("select * from sales_dwh.common.exchange_rate")
    sales_with_forext_df = country_sales_df.join(forex_df,country_sales_df['order_dt']==forex_df['date'],join_type='outer')
    #sales_with_forext_df.show(2)

    #de-duplication
    print(sales_with_forext_df.count())
    unique_orders = sales_with_forext_df.with_column('order_rank',rank().over(Window.partitionBy(col("order_dt")).order_by(col('_metadata_last_modified').desc()))).filter(col("order_rank")==1).select(col('SALES_ORDER_KEY').alias('unique_sales_order_key'))
    final_sales_df = unique_orders.join(sales_with_forext_df,unique_orders['unique_sales_order_key']==sales_with_forext_df['SALES_ORDER_KEY'],join_type='inner')
    final_sales_df = final_sales_df.select(
        col('SALES_ORDER_KEY'),
        col('ORDER_ID'),
        col('ORDER_DT'),
        col('CUSTOMER_NAME'),
        col('MOBILE_KEY'),
         col('Country'),
        col('Region'),
        col('ORDER_QUANTITY'),
        lit('USD').alias('LOCAL_CURRENCY'),
        col('UNIT_PRICE').alias('LOCAL_UNIT_PRICE'),
        col('PROMOTION_CODE').alias('PROMOTION_CODE'),
        col('FINAL_ORDER_AMOUNT').alias('LOCAL_TOTAL_ORDER_AMT'),
        col('TAX_AMOUNT').alias('local_tax_amt'),
        col('USD2INR').alias("Exhchange_Rate"),
        (col('FINAL_ORDER_AMOUNT')/col('USD2USD')).alias('US_TOTAL_ORDER_AMT'),
        (col('TAX_AMOUNT')/col('USD2USD')).alias('USD_TAX_AMT'),
        col('payment_status'),
        col('shipping_status'),
        col('payment_method'),
        col('payment_provider'),
        col('phone').alias('conctact_no'),
        col('shipping_address')
    )

    #final_sales_df.show(5)
    final_sales_df.write.save_as_table("sales_dwh.curated.us_sales_order",mode="append")
    return "Success"


    call SOURCE_CURRATED_TABLE_US_PROC() ;

select * from sales_dwh.curated.us_sales_order;
truncate table  sales_dwh.curated.us_sales_order;
    

    
Step-8 Working on Consumption Layer
Step-8.1 Dimension Tables & Sequence Object
-- region dimension
use schema consumption;
create or replace sequence region_dim_seq start = 1 increment = 1;

create or replace transient table region_dim(
    region_id_pk number primary key,
    Country text, 
    Region text,
    isActive text(1)
);

select * from product_dim;
-- product dimension
use schema consumption;
create or replace sequence product_dim_seq start = 1 increment = 1;
create or replace transient table product_dim(
    product_id_pk number primary key,
    Mobile_key text,
    Brand text, 
    Model text,
    Color text,
    Memory text,
    isActive text(1)
);

-- customer dimension
use schema consumption;
create or replace sequence customer_dim_seq start = 1 increment = 1;
create or replace transient table customer_dim(
    customer_id_pk number primary key,
    customer_name text,
    CONCTACT_NO text,
    SHIPPING_ADDRESS text,
    country text,
    region text,
    isActive text(1)
);
-- payment dimension
use schema consumption;
create or replace sequence payment_dim_seq start = 1 increment = 1;
create or replace transient table payment_dim(
    payment_id_pk number primary key,
    PAYMENT_METHOD text,
    PAYMENT_PROVIDER text,
    country text,
    region text,
    isActive text(1)
);

-- fact tables


create or replace table sales_fact (
 order_code varchar(),
 region_id_fk number(38,0),
 customer_id_fk number(38,0),
 payment_id_fk number(38,0),
 product_id_fk number(38,0),
 order_quantity number(38,0),
 local_total_order_amt number(10,2),
 local_tax_amt number(10,2),
 exhchange_rate number,
 us_total_order_amt number,
 usd_tax_amt number
);

select * from sales_dwh.consumption.sales_fact;

-- Table Containts
alter table sales_fact add
    constraint fk_sales_region FOREIGN KEY (REGION_ID_FK) REFERENCES region_dim (REGION_ID_PK) NOT ENFORCED;



alter table sales_fact add
    constraint fk_sales_customer FOREIGN KEY (CUSTOMER_ID_FK) REFERENCES customer_dim (CUSTOMER_ID_PK) NOT ENFORCED;
--
alter table sales_fact add
    constraint fk_sales_payment FOREIGN KEY (PAYMENT_ID_FK) REFERENCES payment_dim (PAYMENT_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_product FOREIGN KEY (PRODUCT_ID_FK) REFERENCES product_dim (PRODUCT_ID_PK) NOT ENFORCED;


8.2 Curated To Model Snowpark Python Example
import sys
import logging
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType
from snowflake.snowpark.functions import col,lit,row_number, rank,split,cast
from snowflake.snowpark import Window


def create_region_dim(all_sales_df,session)-> None:
    region_dim_df = all_sales_df.groupBy(col("Country"),col("Region")).count()
    region_dim_df.show(2)
    region_dim_df = region_dim_df.with_column("isActive",lit('Y'))
    region_dim_df = region_dim_df.selectExpr("sales_dwh.consumption.region_dim_seq.nextval as region_id_pk","Country", "Region", "isActive") 
    #region_dim_df.write.save_as_table('sales_dwh.consumption.region_dim',mode="append")   

    region_dim_df.show(5)
    # part 2 where delta data will be processed 
    
    existing_region_dim_df = session.sql("select Country, Region from sales_dwh.consumption.region_dim")

    region_dim_df = region_dim_df.join(existing_region_dim_df,region_dim_df['Country']==existing_region_dim_df['Country'],join_type='leftanti')
    region_dim_df.show(5)
    intsert_cnt = int(region_dim_df.count())
    if intsert_cnt>0:
        region_dim_df.write.save_as_table("sales_dwh.consumption.region_dim",mode="append")
        print("save operation ran...")
    else:
        print("No insert ...Opps...")


    # have exclude key


def create_product_dim(all_sales_df,session)-> None:

    product_dim_df = all_sales_df.with_column("Brand",split(col('MOBILE_KEY'),lit('/'))[0])     \
                                .with_column("Model",split(col('MOBILE_KEY'),lit('/'))[1])      \
                                .with_column("Color",split(col('MOBILE_KEY'),lit('/'))[2])      \
                                .with_column("Memory",split(col('MOBILE_KEY'),lit('/'))[3])     \
                                .select(col('mobile_key'),col('Brand'),col('Model'),col('Color'),col('Memory'))
    
    product_dim_df = product_dim_df.select(col('mobile_key'),                    \
                                        cast(col('Brand'), StringType()).as_("Brand"),\
                                        cast(col('Model'), StringType()).as_("Model"),\
                                        cast(col('Color'), StringType()).as_("Color"),\
                                        cast(col('Memory'), StringType()).as_("Memory")\
                                        )
    
    product_dim_df = product_dim_df.groupBy(col('mobile_key'),col("Brand"),col("Model"),col("Color"),col("Memory")).count()
    product_dim_df = product_dim_df.with_column("isActive",lit('Y'))

    #fetch existing product dim records.
    existing_product_dim_df = session.sql("select mobile_key, Brand, Model, Color, Memory from sales_dwh.consumption.product_dim")
    existing_product_dim_df.count()

    product_dim_df = product_dim_df.join(existing_product_dim_df,["mobile_key", "Brand", "Model", "Color", "Memory"],join_type='leftanti')

    product_dim_df.show(5)

    product_dim_df = product_dim_df.selectExpr("sales_dwh.consumption.product_dim_seq.nextval as product_id_pk","mobile_key","Brand", "Model","Color","Memory", "isActive") 

    product_dim_df.show(5)
    intsert_cnt = int(product_dim_df.count())
    if intsert_cnt>0:
        product_dim_df.write.save_as_table("sales_dwh.consumption.product_dim",mode="append")
        print("save operation ran...")
    else:
        print("No insert ...Opps...")


    
def create_customer_dim(all_sales_df, session) -> None:
    customer_dim_df = all_sales_df.groupBy(col("COUNTRY"),col("REGION"),col("CUSTOMER_NAME"),col("CONCTACT_NO"),col("SHIPPING_ADDRESS")).count()
    customer_dim_df = customer_dim_df.with_column("isActive",lit('Y'))
    customer_dim_df = customer_dim_df.selectExpr("customer_name", "conctact_no","shipping_address","country","region" ,"isactive") 
    #region_dim_df.write.save_as_table('sales_dwh.consumption.region_dim',mode="append")   

    customer_dim_df.show(5)
    # part 2 where delta data will be processed 
    
    existing_customer_dim_df = session.sql("select customer_name,conctact_no,shipping_address,country, region from sales_dwh.consumption.customer_dim")

    customer_dim_df = customer_dim_df.join(existing_customer_dim_df,["customer_name","conctact_no","shipping_address","country", "region"],join_type='leftanti')

    customer_dim_df = customer_dim_df.selectExpr("sales_dwh.consumption.customer_dim_seq.nextval as customer_id_pk","customer_name", "conctact_no","shipping_address","country","region", "isActive") 

    customer_dim_df.show(5)

    intsert_cnt = int(customer_dim_df.count())
    if intsert_cnt>0:
        customer_dim_df.write.save_as_table("sales_dwh.consumption.customer_dim",mode="append")
        print("save operation ran...")
    else:
        print("No insert ...Opps...")
    
def create_payment_dim(all_sales_df, session) -> None:
    payment_dim_df = all_sales_df.groupBy(col("COUNTRY"),col("REGION"),col("payment_method"),col("payment_provider")).count()
    payment_dim_df = payment_dim_df.with_column("isActive",lit('Y'))

    #region_dim_df.write.save_as_table('sales_dwh.consumption.region_dim',mode="append")   

    payment_dim_df.show(5)
    # part 2 where delta data will be processed 
    
    existing_payment_dim_df = session.sql("select payment_method,payment_provider,country, region from sales_dwh.consumption.payment_dim")

    payment_dim_df = payment_dim_df.join(existing_payment_dim_df,["payment_method","payment_provider","country", "region"],join_type='leftanti')

    payment_dim_df = payment_dim_df.selectExpr("sales_dwh.consumption.payment_dim_seq.nextval as payment_id_pk","payment_method", "payment_provider","country","region", "isActive") 


    intsert_cnt = int(payment_dim_df.count())
    if intsert_cnt>0:
        payment_dim_df.write.save_as_table("sales_dwh.consumption.payment_dim",mode="append")
        print("save operation ran...")
    else:
        print("No insert ...Opps...")


def main(session: snowpark.Session): 
    #get the session object and get dataframe
    

    in_sales_df = session.sql("select * from sales_dwh.curated.in_sales_order")
    us_sales_df = session.sql("select * from sales_dwh.curated.us_sales_order")
  

    all_sales_df = in_sales_df.union(us_sales_df)

   
    create_region_dim(all_sales_df,session)     #region dimension
    create_product_dim(all_sales_df,session)    #product dimension
   
    create_customer_dim(all_sales_df,session)   #customer dimension
    create_payment_dim(all_sales_df,session)    #payment dimension
 

    customer_dim_df = session.sql("select customer_id_pk, customer_name, country, region from sales_dwh.consumption.CUSTOMER_DIM")
    payment_dim_df = session.sql("select payment_id_pk, payment_method, payment_provider, country, region from sales_dwh.consumption.PAYMENT_DIM")
    product_dim_df = session.sql("select product_id_pk, mobile_key from sales_dwh.consumption.PRODUCT_DIM")
    region_dim_df = session.sql("select region_id_pk,country, region from sales_dwh.consumption.REGION_DIM")

        
    all_sales_df = all_sales_df.join(customer_dim_df, ["customer_name","region","country"],join_type='inner')
    all_sales_df = all_sales_df.join(payment_dim_df, ["payment_method", "payment_provider", "country", "region"],join_type='inner')
    #all_sales_df = all_sales_df.join(product_dim_df, ["brand","model","color","Memory"],join_type='inner')
    all_sales_df = all_sales_df.join(product_dim_df, ["mobile_key"],join_type='inner')
    
    all_sales_df = all_sales_df.join(region_dim_df, ["country", "region"],join_type='inner')
    all_sales_df= all_sales_df[['order_id','region_id_pk','customer_id_pk','payment_id_pk','product_id_pk','order_quantity'\
                                    ,'local_total_order_amt'  ,'local_tax_amt'  ,'exhchange_rate','us_total_order_amt' ,'usd_tax_amt'  ]]
   
                                         
    all_sales_df.write.save_as_table("sales_dwh.consumption.sales_fact",mode="append")
    return all_sales_df


 CALL CURRATED_MODEL_DIM_FACT_TABLE_PROC();

select * from sales_dwh.consumption.sales_fact;
truncate table sales_dwh.consumption.sales_fact;
