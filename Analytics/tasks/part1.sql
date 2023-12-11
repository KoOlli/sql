-- drop table Personal_Information;
-- drop table Cards;
-- drop table Transactions;
-- drop table Checks;
-- drop table Product_Grid;
-- drop table Stores;
-- drop table SKU_Group;
-- drop table Date_Of_Analysis_Formation;

create table Personal_Information(
  Customer_ID varchar primary key,
  Customer_Name varchar check(Customer_Name ~* '^[A-ZА-Я][a-zа-я]+$'),
  Customer_Surname varchar check(Customer_Surname ~* '^[A-ZА-Я][a-zа-я]+$'),
  Customer_Primary_Email varchar check(Customer_Primary_Email ~* '^([a-zA-Z0-9_\-\.]+)@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.)|(([a-zA-Z0-9\-]+\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\]?)$'),
  Customer_Primary_Phone varchar check(Customer_Primary_Phone ~* '^\+7\d{10}$')
);
COPY Personal_Information
FROM '/opt/goinfre/karleenk/sql_2_group/datasets/Personal_Data.tsv'
DELIMITER E'\t'
CSV;

create table Cards(
Customer_Card_ID varchar primary key,
Customer_ID varchar
);
COPY Cards
FROM '/opt/goinfre/karleenk/sql_2_group/datasets/Cards.tsv'
DELIMITER E'\t'
CSV;

create table Transactions (
Transaction_ID varchar primary key,
Customer_Card_ID varchar,
Transaction_Summ varchar check(Transaction_Summ ~* '^\d+(\.\d+)?$'),
Transaction_DateTime varchar check(Transaction_DateTime ~* '^0[1-9]|[12][0-9]|3[01].0[0-1]|1[0-2].20[0-9][0-9] 0[0-9]|1[0-9]|2[0-3]:0[0-9]|[1-5][0-9]:0[0-9]|[1-5][0-9]'),
Transaction_Store_ID varchar
);
COPY Transactions
FROM '/opt/goinfre/karleenk/sql_2_group/datasets/Transactions.tsv'
DELIMITER E'\t'
CSV;

create table Checks (
Transaction_ID varchar,
SKU_ID varchar,
SKU_Amount varchar check(SKU_Amount ~* '^\d+(\.\d+)?$'),
SKU_Summ varchar check(SKU_Summ ~* '^\d+(\.\d+)?$'),
SKU_Summ_Paid varchar check(SKU_Summ_Paid ~* '^\d+(\.\d+)?$'),
SKU_Discount varchar check(SKU_Discount ~* '^\d+(\.\d+)?$')
);
COPY Checks
FROM '/opt/goinfre/karleenk/sql_2_group/datasets/Checks.tsv'
DELIMITER E'\t'
CSV;

create table Product_Grid (
SKU_ID varchar primary key,
SKU_Name varchar check(SKU_Name ~* '^(?![\d+_@.-]+$)[a-zа-яA-ZА-Я0-9\W_]*$'),
Group_ID varchar check(Group_ID ~* '^\d+$')
);
COPY Product_Grid
FROM '/opt/goinfre/karleenk/sql_2_group/datasets/SKU.tsv'
DELIMITER E'\t'
CSV;

create table Stores (
Transaction_Store_ID varchar,
SKU_ID varchar,
SKU_Purchase_Price varchar check(SKU_Purchase_Price ~* '^\d+(\.\d+)?$'),
SKU_Retail_Price varchar check(SKU_Retail_Price ~* '^\d+(\.\d+)?$')
);
COPY Stores
FROM '/opt/goinfre/karleenk/sql_2_group/datasets/Stores.tsv'
DELIMITER E'\t'
CSV;

create table SKU_Group (
Group_ID varchar,
Group_Name varchar check(Group_Name ~* '^(?![\d+_@.-]+$)[a-zа-яA-ZА-Я0-9\W_]*$')
);
COPY SKU_Group
FROM '/opt/goinfre/karleenk/sql_2_group/datasets/Groups_SKU.tsv'
DELIMITER E'\t'
CSV;

create table Date_Of_Analysis_Formation (
Analysis_Formation varchar check(Analysis_Formation ~* '0[0-9]|1[0-9]|2[0-3]:0[0-9]|[1-5][0-9]:0[0-9]|[1-5][0-9]')
);
COPY Date_Of_Analysis_Formation
FROM '/opt/goinfre/karleenk/sql_2_group/datasets/Date_Of_Analysis_Formation.tsv'
DELIMITER E'\t'
CSV;