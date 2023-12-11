drop index if exists idx_v_periods;
drop index if exists idx_v_purchase_history;

create index if not exists idx_v_periods on v_periods(Customer_ID, Group_ID);
create index if not exists idx_v_purchase_history on v_purchase_history(Customer_ID, Group_ID);

create domain mode as integer check (value between 1 and 2);
create domain uint as integer check (value between 0 and 32767);

create or replace function fnc_create_groups_view(selector_ mode default 1, amount_ uint default 1000)
returns table (
    "Customer_ID" varchar,
    "Group_ID" varchar,
    "Group_Affinity_Index" numeric,
    "Group_Churn_Rate" numeric,
    "Group_Stability_Index" numeric,
    "Group_Margin" numeric,
    "Group_Discount_Share" numeric,
    "Group_Min_Discount" numeric,
    "Group_Average_Discount" numeric
)
as
$$
declare
  date_analysis_formation date:= (select Analysis_Formation::date from Date_Of_Analysis_Formation);
BEGIN
  if selector_ in (1, 2) and amount_ > 0 then
  return query
    with transactions_at_discount as (
      select vph.Customer_ID,
             vph.Group_ID,
             count(distinct vph.Transaction_ID) as count_transactions
      from v_purchase_history vph
      join Product_Grid s on s.Group_ID = vph.Group_ID
      join Checks ch on ch.Transaction_ID = vph.Transaction_ID and ch.SKU_ID = s.Group_ID and ch.SKU_Discount::numeric > 0
      group by vph.Customer_ID, vph.Group_ID
      order by 1, 2
    ),
    Cte_Affinity_Index as (
      select vp.Customer_ID,
             vp.Group_ID,
             (vp.Group_Purchase / count(distinct vph.Transaction_ID)::numeric) as Group_Affinity_Index
      from v_periods vp
      join v_purchase_history vph using (Customer_ID)
      where vph.Transaction_DateTime::date between vp.First_Group_Purchase_Date and vp.Last_Group_Purchase_Date
      group by vp.Customer_ID, vp.Group_ID, vp.Group_Purchase
    ),
    Relative_Deviation as
    (
      select vp.Customer_ID,
             vp.Group_ID,
             vph.Transaction_DateTime,
             vph.group_sum_paid,
             vph.group_cost,
             row_number() over(partition by vph.Customer_ID, vph.Group_ID order by Transaction_DateTime desc) as row_count,
             ((date_analysis_formation - vp.Last_Group_Purchase_Date::date) / vp.Group_Frequency::numeric) as Group_Churn_Rate,
             abs(Transaction_DateTime::date - lag(Transaction_DateTime) over (partition by vph.Customer_ID, vph.Group_ID order by Transaction_DateTime)::date - vp.Group_Frequency) / vp.Group_Frequency as Deviation,
             avg(vph.group_sum_paid / vph.group_summ::numeric) over (partition by Customer_ID, Group_ID) as Group_Average_Discount,
             vp.Group_Min_Discount as Group_Minimum_Discount,
             coalesce((td.count_transactions / vp.Group_Purchase::numeric), 0) as Group_Discount_Share
      from v_purchase_history vph
      join v_periods vp using (Customer_ID, Group_ID)
      left join transactions_at_discount td using (Customer_ID, Group_ID)
    )
    select distinct Customer_ID,
                    Group_ID,
                    cai.Group_Affinity_Index,
                    rd.Group_Churn_Rate,
                    (coalesce(avg(rd.Deviation) over w_part_customer_id_group_id, 0)) as Group_Stability_Index,
                    sum(
                      case
                        when selector_ = 1 and Transaction_DateTime between date_analysis_formation - amount_ and date_analysis_formation
                          then group_sum_paid - group_cost
                        when selector_ = 2 and row_count <= amount_
                          then group_sum_paid - group_cost
                        else 0
                      end
                    ) over w_part_customer_id_group_id,
                    Group_Discount_Share,
                    Group_Minimum_Discount,
                    Group_Average_Discount
    from Relative_Deviation rd
    join Cte_Affinity_Index cai using (Customer_ID, Group_ID)
    window w_part_customer_id_group_id as (partition by rd.Customer_ID, rd.Group_ID);
  end if;
END;
$$
  language plpgsql;

-- drop materialized view if exists v_groups;

create materialized view v_groups (
  Customer_ID,
  Group_ID,
  Group_Affinity_Index,
  Group_Churn_Rate,
  Group_Stability_Index,
  Group_Margin,
  Group_Discount_Share,
  Group_Minimum_Discount,
  Group_Average_Discount
) as
select * from fnc_create_groups_view();
