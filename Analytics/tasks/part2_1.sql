create or replace view v_customers as
  with customer_average as
  (
    select distinct p.Customer_ID as Customer_ID,
                    avg(t.Transaction_Summ::numeric) as Customer_Average_Check,
                    (max(t.Transaction_DateTime::date) - min(t.Transaction_DateTime::date))::numeric / count(*) as Customer_Frequency,
                    round(extract(epoch from ((select Analysis_Formation::timestamp from Date_Of_Analysis_Formation) - max(t.Transaction_DateTime::timestamp))) / 86400, 2) as Customer_Inactive_Period
    from Personal_Information p
      inner join Cards c on c.Customer_ID = p.Customer_ID
      inner join Transactions t on t.Customer_Card_ID = c.Customer_Card_ID
    where t.Transaction_DateTime::date <= (select Analysis_Formation::date
                                     from Date_Of_Analysis_Formation)
    group by p.Customer_ID
  ),
  Customer_Segments as
  (
    select ca.Customer_ID,
    ca.Customer_Average_Check,
    case
      when row_number() over (order by Customer_Average_Check desc) <= count(*) over () * 0.1 then 'High'
      when row_number() over (order by Customer_Average_Check desc) between (count(*) over() * 0.1) and (count(*) over() * 0.35) then 'Medium'
      else 'Low'
    end as Customer_Average_Check_Segment,
    ca.Customer_Frequency,
    case
      when row_number() over(order by Customer_Frequency) <= count(*) over() * 0.1 then 'Often'
      when row_number() over(order by Customer_Frequency) between (count(*) over() * 0.1) and (count(*) over() * 0.35) then 'Occasionally'
      else 'Rarely'
    end as Customer_Frequency_Segment,
    ca.Customer_Inactive_Period,
    Customer_Inactive_Period / Customer_Frequency as Customer_Churn_Rate,
    case
      when (Customer_Inactive_Period / Customer_Frequency) between 0 and 2 then 'Low'
      when (Customer_Inactive_Period / Customer_Frequency) between 2 and 5 then 'Medium'
      else 'High'
    end as Customer_Churn_Segment
  from Customer_Average ca
  order by 3
  ),
  Customer_Store as
  (
    select
      p.Customer_ID,
      t.Transaction_Store_ID,
      count(*) over(partition by p.Customer_ID, t.Transaction_Store_ID) / count(*) over (partition by p.Customer_ID)::numeric as Share_Of_Transactions,
      t.Transaction_DateTime
    from Personal_Information p
      inner join Cards c on c.Customer_ID = p.Customer_ID
      inner join Transactions t on t.Customer_Card_ID = c.Customer_Card_ID
    where t.Transaction_DateTime::date <= (select Analysis_Formation::date
                                     from Date_Of_Analysis_Formation)
    order by 1, 4 desc
  )
select
  *,
  (
    case
      when Customer_Average_Check_Segment = 'Low' then 1
      when Customer_Average_Check_Segment = 'Medium' then 10
      else 19
    end
  )
  +
  (
    case
      when Customer_Frequency_Segment = 'Rarely' then 0
      when Customer_Frequency_Segment = 'Occasionally' then 3
      else 6
    end
  )
  +
  (
    case
      when Customer_Churn_Segment = 'Low' then 0
      when Customer_Churn_Segment = 'Medium' then 1
      else 2
    end
  ) as Customer_Segment,
  case
    when
      (
        select count(distinct Transaction_Store_ID) = 1 from Customer_Store
        where Customer_ID = cs.Customer_ID
        limit 3
      ) then
      (
        select Transaction_Store_ID from Customer_Store
        where Customer_ID = cs.Customer_ID
        limit 1
      )
    else
      (
        select Transaction_Store_ID from Customer_Store
        where Customer_ID = cs.Customer_ID
        order by Share_Of_Transactions desc, Transaction_DateTime desc
        limit 1
      )
    end as Customer_Primary_Store
from Customer_Segments cs
order by 1;
