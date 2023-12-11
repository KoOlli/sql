create or replace function fnc_get_offers_to_increase_visits(
  first_date timestamp,
  last_date timestamp,
  added_transactions integer,
  max_churn_rate numeric,
  max_discount_share numeric,
  max_margin numeric
) returns table (
  "Customer_ID" varchar,
  "Start_Date" timestamp,
  "End_Date" timestamp,
  "Required_Transactions_Count" int,
  "Group_Name" varchar,
  "Offer_Discount_Depth" numeric
) as $$
  with full_group as (
    select distinct
      vc.Customer_ID,
      vg.Group_ID,
      vg.Group_Affinity_Index,
      round((last_date::date - first_date::date) / vc.Customer_Frequency) + added_transactions as Required_Transactions_Count,
      avg(vph.group_sum_paid - vph.group_cost) over (partition by vph.Customer_ID, vph.Group_ID) / 100 * max_margin as Margin,
      case
        when (vg.Group_Minimum_Discount * 100 % 5) = 0
        then vg.Group_Minimum_Discount * 100
        else 5 - (vg.Group_Minimum_Discount * 100 % 5) + (vg.Group_Minimum_Discount * 100)
      end as Offer_Discount_Depth
    from v_customers vc
    join v_groups vg using(Customer_ID)
    join v_purchase_history vph using (Customer_ID, Group_ID)
    where Group_Churn_Rate <= max_churn_rate
      and Group_Discount_Share * 100 < max_discount_share
      and Group_Minimum_Discount > 0
  )
  select
    Customer_ID,
    first_date,
    last_date,
    Required_Transactions_Count,
    Group_Name,
    Offer_Discount_Depth
  from full_group fg
  join SKU_Group using(Group_ID)
  where fg.Group_Affinity_Index = (
    select distinct max(Group_Affinity_Index) from full_group
    where Customer_ID = fg.Customer_ID
    and Offer_Discount_Depth < Margin
  )
  order by Customer_ID
$$ language sql;

SELECT * FROM fnc_get_offers_to_increase_visits('2021-02-15 00:00:00', '2022-06-01 00:00:00', 1, 0.5, 35., 30.);
