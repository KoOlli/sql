CREATE OR REPLACE FUNCTION suggest_for_increa_t_average_check(
    metod_for_average_check numeric,
    first_date date,
    last_date date,
    average_ticket_increase_factor numeric,
    max_churn_index numeric,
    max_share_discount_transact numeric,
    allow_margin_share numeric
) RETURNS TABLE (
    "Customer_ID" varchar,
    "Required_Check_Measure" numeric,
    "Group_Name" varchar,
    "Offer_Discount_Depth" numeric
) AS $$ 
BEGIN 
IF (metod_for_average_check = 2) THEN 
    RETURN QUERY 
        SELECT customer_id,
            subquery.Required_Check_Measure,
            sku_group.group_name,
            subquery.Offer_Discount_Depth * 100
        FROM (
            SELECT v_groups.customer_id, (avg(t.transaction_summ)::numeric(10, 2) * average_ticket_increase_factor) as Required_Check_Measure, group_id, (ROUND(group_minimum_discount / 0.05) * 0.05) as Offer_Discount_Depth,
            group_affinity_index::numeric(10, 2), ROW_NUMBER() OVER (PARTITION BY v_groups.customer_id ORDER BY group_affinity_index DESC) AS rm
            FROM v_groups
            JOIN (SELECT Cards.customer_id, transactions.customer_card_id, transaction_summ, transaction_datetime,
                    ROW_NUMBER() OVER (PARTITION BY transactions.customer_card_id ORDER BY transactions.customer_card_id ASC, transaction_datetime DESC) as rn
                FROM transactions 
                JOIN Cards ON cards.customer_card_id = transactions.customer_card_id
                ) t ON v_groups.customer_id = t.customer_id
            WHERE group_churn_rate < max_churn_index AND group_discount_share < max_share_discount_transact AND (Group_Margin / 100 * allow_margin_share) > (ROUND(group_minimum_discount / 0.05) * 0.05) AND transaction_datetime >= first_date and transaction_datetime <= last_date
            GROUP BY v_groups.customer_id, v_groups.group_id, v_groups.group_minimum_discount, v_groups.group_affinity_index
        ) subquery 
        JOIN sku_group ON sku_group.group_id = subquery.group_id
        WHERE rm = 1
        ORDER BY customer_id, Offer_Discount_Depth DESC;
END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION suggest_for_increa_t_average_check(
    metod_for_average_check numeric,
    second_paramert numeric,
    average_ticket_increase_factor numeric,
    max_churn_index numeric,
    max_share_discount_transact numeric,
    allow_margin_share numeric
) RETURNS TABLE (
    "Customer_ID" varchar,
    "Required_Check_Measure" numeric,
    "Group_Name" varchar,
    "Offer_Discount_Depth" numeric
) AS $$ 
BEGIN 
IF (metod_for_average_check = 2) THEN 
    RETURN QUERY 
        SELECT customer_id,
            subquery.Required_Check_Measure,
            sku_group.group_name,
            subquery.Offer_Discount_Depth * 100
        FROM (
            SELECT v_groups.customer_id, (avg(t.transaction_summ)::numeric(10, 2) * average_ticket_increase_factor) as Required_Check_Measure, group_id, (ROUND(group_minimum_discount / 0.05) * 0.05) as Offer_Discount_Depth,
            group_affinity_index::numeric(10, 2), ROW_NUMBER() OVER (PARTITION BY v_groups.customer_id ORDER BY group_affinity_index DESC) AS rm
            FROM v_groups
            JOIN (SELECT Cards.customer_id, transactions.customer_card_id, transaction_summ, transaction_datetime,
                    ROW_NUMBER() OVER (PARTITION BY transactions.customer_card_id ORDER BY transactions.customer_card_id ASC, transaction_datetime DESC) as rn
                FROM transactions 
                JOIN Cards ON cards.customer_card_id = transactions.customer_card_id
                ) t ON v_groups.customer_id = t.customer_id
            WHERE group_churn_rate < max_churn_index AND group_discount_share < max_share_discount_transact AND (Group_Margin / 100 * allow_margin_share) > (ROUND(group_minimum_discount / 0.05) * 0.05) AND t.rn <= second_paramert
            GROUP BY v_groups.customer_id, v_groups.group_id, v_groups.group_minimum_discount, v_groups.group_affinity_index
        ) subquery 
        JOIN sku_group ON sku_group.group_id = subquery.group_id
        WHERE rm = 1
        ORDER BY customer_id, Offer_Discount_Depth DESC;
END IF;
END;
$$ LANGUAGE plpgsql;



select * from suggest_for_increa_t_average_check(2, 3, 1.2, 25, 16, 0.6);
select * from suggest_for_increa_t_average_check(2, '2018-01-08', '2022-09-09', 1.2, 25, 16, 0.6);
