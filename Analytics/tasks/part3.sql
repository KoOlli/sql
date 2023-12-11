-- Администратор имеет полные права на редактирование и просмотр
-- любой информации, запуск и остановку процесса обработки.
drop role if exists administrator;
create role administrator;

grant ALL on Personal_Information to administrator;
grant ALL on Cards to administrator;
grant ALL on Transactions to administrator;
grant ALL on Checks to administrator;
grant ALL on Product_Grid to administrator;
grant ALL on Stores to administrator;
grant ALL on SKU_Group to administrator;
grant ALL on Date_Of_Analysis_Formation to administrator;

grant ALL on v_customers to administrator;
grant ALL on v_purchase_History to administrator;
grant ALL on v_periods to administrator;
grant ALL on v_groups to administrator;

-- Только просмотр информации из всех таблиц.
drop role if exists visitor;
create role visitor;

grant select on Personal_Information to visitor;
grant select on Cards to visitor;
grant select on Transactions to visitor;
grant select on Checks to visitor;
grant select on Product_Grid to visitor;
grant select on Stores to visitor;
grant select on SKU_Group to visitor;
grant select on Date_Of_Analysis_Formation to visitor;

grant select on v_customers to visitor;
grant select on v_purchase_History to visitor;
grant select on v_periods to visitor;
grant select on v_groups to visitor;

