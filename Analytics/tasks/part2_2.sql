-- drop materialized view if exists v_purchase_history cascade;

-- refresh materialized view v_purchase_history;

create materialized view v_purchase_history as
select p.Customer_ID,
       t.Transaction_ID,
       t.Transaction_DateTime::date as Transaction_DateTime,
       sku.Group_ID,
       sum(s.SKU_Purchase_Price::numeric * ch.SKU_Amount::numeric) as group_cost,
       sum(ch.SKU_Summ::numeric) as group_summ,
       sum(ch.SKU_Summ_Paid::numeric) as group_sum_paid
from Personal_Information p
  join Cards c on p.Customer_ID = c.Customer_ID
  join Transactions t on t.Customer_Card_ID = c.Customer_Card_ID
  join Checks ch on ch.Transaction_ID = t.Transaction_ID
  join Product_Grid sku on sku.SKU_ID = ch.SKU_ID
  join Stores s on t.Transaction_Store_ID = s.Transaction_Store_ID and s.SKU_ID = sku.Group_ID
group by 1, 2, 3, 4;

-- SELECT *
-- FROM v_purchase_history
-- ORDER BY transaction_datetime;
