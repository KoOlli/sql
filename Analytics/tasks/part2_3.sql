-- drop materialized view if exists v_periods;

-- refresh materialized view v_periods;

create materialized view v_periods as
  with Group_Purchase_Date as
  (
    select vph.Customer_ID as Customer_ID,
           vph.Group_ID as Group_ID,
           min(vph.Transaction_DateTime) as First_Group_Purchase_Date,
           max(vph.Transaction_DateTime) as Last_Group_Purchase_Date,
           count(distinct vph.Transaction_ID) as Group_Purchase,
           coalesce(min(ch.SKU_Discount::numeric / ch.SKU_Summ::numeric), 0) as Group_Min_Discount
    from v_purchase_history vph
    join Product_Grid s on s.Group_ID = vph.Group_ID
    left join Checks ch on ch.Transaction_ID = vph.Transaction_ID and ch.SKU_Discount::numeric > 0 and ch.SKU_ID = s.Group_ID
    group by 1, 2
  )
  select Customer_ID,
         Group_ID,
         First_Group_Purchase_Date,
         Last_Group_Purchase_Date,
         Group_Purchase,
         (Last_Group_Purchase_Date::date - First_Group_Purchase_Date::date + 1) / Group_Purchase::numeric as Group_Frequency,
         Group_Min_Discount
  from Group_Purchase_Date
  order by 1, 2;
