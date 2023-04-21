-- Calculating aim value of average check by first method
DROP FUNCTION IF EXISTS avgCheckM1(character varying, real);
CREATE FUNCTION avgCheckM1 (fst_n_lst_date_m1 varchar, k_check_incr real)
    RETURNS TABLE (Customer_ID bigint, Required_Check_Measure real)
    LANGUAGE plpgsql AS
    $$
    DECLARE
        lower_date date := split_part(fst_n_lst_date_m1, ' ', 1)::date;
        upper_date date := split_part(fst_n_lst_date_m1, ' ', 2)::date;
    BEGIN
        IF (lower_date < getKeyDates(1)) THEN
            lower_date = getKeyDates(1);
        ELSEIF (upper_date > getKeyDates(2)) THEN
            upper_date = getKeyDates(2);
        ELSEIF (lower_date >= upper_date) THEN
            RAISE EXCEPTION
                'last date of the specified period must be later than the first one';
        END IF;
        RETURN QUERY
            WITH pre_query AS (
                SELECT cards.customer_id AS Customer_ID, (t.transaction_summ) AS trans_summ
                FROM cards
                JOIN transactions t on cards.customer_card_id = t.customer_card_id
                WHERE t.transaction_datetime BETWEEN lower_date and upper_date)
            SELECT pq.Customer_ID, avg(trans_summ)::real * k_check_incr AS Avg_check
            FROM pre_query pq
            GROUP BY pq.Customer_ID
            ORDER BY 1;
    END;
    $$;

-- Calculating aim value of average check by second method
DROP FUNCTION IF EXISTS avgCheckM2(bigint, real);
CREATE FUNCTION avgCheckM2 (transact_num bigint, k_check_incr real)
    RETURNS TABLE (Customer_ID bigint, Required_Check_Measure real)
    LANGUAGE plpgsql AS
    $$
    BEGIN
        RETURN QUERY
        WITH pre_query AS (
            SELECT customer_card_id, transaction_summ
            FROM transactions
            ORDER BY transaction_datetime DESC LIMIT transact_num)
        SELECT c.Customer_ID, avg(transaction_summ)::real * k_check_incr AS Avg_check
        FROM pre_query pq
        JOIN cards c ON c.customer_card_id = pq.customer_card_id
        GROUP BY c.Customer_ID
        ORDER BY 1;
    END;
    $$;

-- Getting dates of first or last transactions - depends by key(argument value)
DROP FUNCTION IF EXISTS getKeyDates(integer);
CREATE FUNCTION getKeyDates(key integer)
    RETURNS SETOF date
    LANGUAGE plpgsql AS
    $$
    BEGIN
        IF (key = 1) THEN
            RETURN QUERY
            SELECT transaction_datetime::date
            FROM transactions
            ORDER BY 1 LIMIT 1;
        ELSEIF (key = 2) THEN
            RETURN QUERY
            SELECT transaction_datetime::date
            FROM transactions
            ORDER BY 1 DESC LIMIT 1;
        END IF;
    END;
    $$;

--Creation of view with average margin and affinity rank
DROP VIEW IF EXISTS full_groups_view CASCADE;
CREATE VIEW full_groups_view AS
WITH avg AS (
    SELECT customer_id, group_id, avg(SGM.margin)::real AS Avegage_Margin
    FROM (SELECT customer_id, group_id, ("Summ_Paid" - "Group_Cost") as margin
        FROM Groups_View_Support) as SGM
    GROUP BY 1, 2)
SELECT gv.*, avg.Avegage_Margin,
       row_number() over (partition by gv.customer_id order by group_affinity_index DESC) as rank
FROM groups_view gv
JOIN avg ON avg.customer_id = gv.customer_id AND avg.group_id = gv.group_id
ORDER BY customer_id, rank;

-- Function for detemine offer discount depth
DROP FUNCTION IF EXISTS rewardGroupDetermination(real, real, real);
CREATE FUNCTION rewardGroupDetermination (churn_idx real, trans_share_max real, marge_share_avl real)
RETURNS TABLE (Customer_ID bigint, Group_ID bigint, Offer_Discount_Depth real)
LANGUAGE plpgsql AS
    $$
    DECLARE
        cust_id bigint := 0;
        flag bool := false;
        curr_row record;
        gv_extended CURSOR FOR (SELECT * FROM full_groups_view);
    BEGIN
    FOR curr_row IN gv_extended LOOP
        IF (flag = true AND cust_id = curr_row.customer_id) THEN CONTINUE;
        END IF;
        IF (curr_row.group_churn_rate <= churn_idx AND
            curr_row.group_discount_share <= trans_share_max AND
            curr_row.Avegage_Margin * marge_share_avl / 100 >=
            CEIL((curr_row.group_minimum_discount * 100) / 5.0) * 0.05 * curr_row.Avegage_Margin) THEN
                Customer_ID = curr_row.customer_id;
                Group_ID = curr_row.group_id;
                Offer_Discount_Depth = CEIL((curr_row.group_minimum_discount * 100) / 5.0) * 5;
                flag = true;
                cust_id = Customer_ID;
                RETURN NEXT;
        ELSE
            flag = false;
        END IF;
        END LOOP;
    END;
    $$;

-- Main function
DROP FUNCTION IF EXISTS offersGrowthCheck(integer, varchar, bigint, real, real, real, real);
CREATE FUNCTION offersGrowthCheck
    (calc_method integer, fst_n_lst_date_m1 varchar,
    transact_cnt_m2 bigint, k_check_incs real, churn_idx real,
    trans_share_max real, marge_share_avl real)
    RETURNS table (Customer_ID bigint, Required_Check_Measure real,
                    Group_Name varchar, Offer_Discount_Depth real)
    LANGUAGE plpgsql AS
    $$
    BEGIN
--      Выбор метода расчета среднего чека
        IF (calc_method = 1) THEN
            RETURN QUERY
                SELECT ch.Customer_ID, ch.Required_Check_Measure, gs.group_name, rd.Offer_Discount_Depth
                FROM avgCheckM1(fst_n_lst_date_m1, k_check_incs) AS ch
                JOIN rewardGroupDetermination(churn_idx, trans_share_max, marge_share_avl) rd ON
                    ch.Customer_ID = rd.Customer_ID
                JOIN groups_sku gs ON gs.group_id = rd.Group_ID
                ORDER BY Customer_ID;
        ELSEIF (calc_method = 2) THEN
            RETURN QUERY
                SELECT ch.Customer_ID, ch.Required_Check_Measure, gs.group_name, rd.Offer_Discount_Depth
                FROM avgCheckM2(transact_cnt_m2, k_check_incs) AS ch
                JOIN rewardGroupDetermination(churn_idx, trans_share_max, marge_share_avl) rd ON
                    ch.Customer_ID = rd.Customer_ID
                JOIN groups_sku gs ON gs.group_id = rd.Group_ID
                ORDER BY Customer_ID;
        ELSE
            RAISE EXCEPTION
                'Average check calculation method must be 1 or 2 (1 - per period, 2 - per quantity)';
        END IF;
    END;
    $$;

-- Check
SELECT *
from offersGrowthCheck(2, '10.10.2018 10.10.2022', 200,  1.15, 3, 70, 30);