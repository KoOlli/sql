create or replace function fnc_get_cross_selling_offers(
  groups_amount bigint default 1000,
  max_churn_rate_index numeric default 0.5,
  max_stability_index numeric default 0.5,
  max_sku_share numeric default 500,
  allowable_margin_share numeric default 1
) returns table (
    "Customer_ID" varchar,
    "SKU_Name" varchar,
    "Offer_Discount_Depth" numeric) as $$
BEGIN
return query
  with sku_list as (
    select vg.Customer_ID,
           vg.Group_ID,
           s.SKU_ID,
           dense_rank() over (partition by Customer_ID, Group_ID
              order by (s.SKU_Retail_Price::numeric - SKU_Purchase_Price::numeric) desc) as sku_rank,
           dense_rank() over (partition by Customer_ID
              order by Group_Affinity_Index desc, Group_ID) as group_rank,
              (max_sku_share * (SKU_Retail_Price::numeric - SKU_Purchase_Price::numeric) / SKU_Retail_Price::numeric) as per_margin,
              Group_Minimum_Discount,
              case
                when (Group_Minimum_Discount * 100 % 5) = 0
                then Group_Minimum_Discount * 100
                else 5 - (Group_Minimum_Discount * 100 % 5) + (Group_Minimum_Discount * 100)
              end as Offer_Discount_Depth
    from v_groups vg
    join v_customers vc using (Customer_ID)
    join Product_Grid sku using (Group_ID)
    join Stores s on vc.Customer_Primary_Store = s.Transaction_Store_ID and s.SKU_ID = sku.SKU_ID
    where Group_Minimum_Discount > 0
      and Group_Churn_Rate <= max_churn_rate_index
      and Group_Stability_Index < max_stability_index
    order by 1, Group_Affinity_Index desc
  )
  select
    vp.Customer_ID,
    SKU_Name,
    Offer_Discount_Depth
  from v_periods vp
  join Product_Grid sku using (Group_ID)
  join v_purchase_history vph using (Customer_ID, Group_ID)
  join Checks ch using (Transaction_ID, SKU_ID)
  join sku_list sl on sl.Customer_ID = vp.Customer_ID
    and sl.Group_ID = vp.Group_ID
    and sl.SKU_ID = sku.SKU_ID
    and sl.sku_rank = 1
    and sl.group_rank <= groups_amount
  where
  per_margin >= Offer_Discount_Depth
  group by 1, 2, 3, vp.Group_Purchase
  having (count(distinct vph.Transaction_ID) / vp.Group_Purchase::numeric) <= allowable_margin_share
  order by 1, 2;
end;
$$ language plpgsql;

select * from fnc_get_cross_selling_offers(100, 0.5, 1, 500, 1);
