SET enable_seqscan = OFF;
--Customers view
DROP TABLE IF EXISTS segments CASCADE;
CREATE TABLE segments (
    Segment integer PRIMARY KEY NOT NULL,
    Average_check varchar NOT NULL,
    Frequency_of_purchases varchar NOT NULL,
    Churn_probability varchar NOT NULL
);

-- Data import into Segments table
SET DATESTYLE to iso, DMY;
-- Please, paste path to datasets/ folder
SET imp_path.txt TO '/Users/aleksei/Documents/GitLab/SQL3_RetailAnalitycs_v1.0-2/datasets/';
CALL import('segments', (current_setting('imp_path.txt') || 'Segments.tsv'));

--Сustomers view Creation
DROP VIEW IF EXISTS Customers_View CASCADE;
CREATE VIEW Customers_View (
    Customer_ID,
    Customer_Average_Check,
    Customer_Average_Check_Segment,
    Customer_Frequency,
    Customer_Frequency_Segment,
    Customer_Inactive_Period,
    Customer_Churn_Rate,
    Customer_Churn_Segment,
    Customer_Segment,
    Customer_Primary_Store) AS

    WITH transactions_plus AS (
        SELECT c.customer_id, c.customer_card_id, t.transaction_id ,t.transaction_summ,
               t.transaction_datetime, t.transaction_store_id
        FROM transactions t
        JOIN cards c on c.customer_card_id = t.customer_card_id
        WHERE t.transaction_datetime <=
              (SELECT da.analysis_formation FROM date_of_analysis_formation da)
    ),
    avg_check AS (
        WITH temp AS (
            SELECT customer_id, sum(transaction_summ) / count(transaction_id)::real
                AS Customer_Average_Check
            FROM transactions_plus
            GROUP BY customer_id)
        SELECT row_number() over (ORDER BY Customer_Average_Check DESC) AS row,
               customer_id, Customer_Average_Check
        FROM temp),
    avg_check_seg AS (
        SELECT row, customer_id, Customer_Average_Check,
            (CASE
                WHEN row <= (SELECT (max(row) * 0.1)::bigint FROM avg_check) THEN 'High'
                WHEN row <= (SELECT (max(row) * 0.35)::bigint FROM avg_check)
                   AND row > (SELECT (max(row) * 0.10)::bigint FROM avg_check) THEN 'Medium'
                ELSE 'Low' END)::varchar AS Customer_Average_Check_Segment
        FROM avg_check),
    cus_freq AS (
        WITH temp1 AS (
            SELECT t2.customer_id, (round((
                extract(year from (max(transaction_datetime) - min(transaction_datetime))) * 365) +
                extract(day from (max(transaction_datetime) - min(transaction_datetime))) + (
                extract(hour from (max(transaction_datetime) - min(transaction_datetime))) / 24), 0) / count(transaction_id))::real AS freq
            FROM transactions_plus t2 GROUP BY t2.customer_id
        )
        SELECT row_number() over (ORDER BY temp1.freq) AS row, ac.customer_id, ac.Customer_Average_Check, ac.Customer_Average_Check_Segment,
                temp1.freq AS Customer_Frequency
        FROM avg_check_seg ac
        JOIN temp1 ON temp1.customer_id = ac.customer_id
    ),
    cus_freq_seg AS (
        SELECT *,
            (CASE
                WHEN row <= (SELECT (max(row) * 0.1)::bigint FROM avg_check) THEN 'Often'
                WHEN row <= (SELECT (max(row) * 0.35)::bigint FROM avg_check)
                   AND row > (SELECT (max(row) * 0.10)::bigint FROM avg_check) THEN 'Occasionally'
                ELSE 'Rarely' END)::varchar AS Customer_Frequency_Segment
        FROM cus_freq),
    cus_inact_per AS (
        WITH get_diffrence AS (
                SELECT customer_id,
                       ((SELECT analysis_formation FROM date_of_analysis_formation) -
                       max(t.transaction_datetime)) AS difference
                FROM transactions_plus t
                GROUP BY 1),
            convert_to_days AS (
                SELECT gd.customer_id, ((
                    extract(year from (gd.difference)) * 365) +
                    extract(day from (gd.difference)) + (
                    extract(hour from (gd.difference)) / 24) +
                    extract(minute from (gd.difference)) / 1440)::real AS difference_c
                FROM get_diffrence gd)
        SELECT fs.customer_id, fs.Customer_Average_Check, fs.Customer_Average_Check_Segment, fs.Customer_Frequency,
             fs.Customer_Frequency_Segment, df.difference_c AS Customer_Inactive_Period
        FROM cus_freq_seg fs
        JOIN convert_to_days df ON df.customer_id = fs.customer_id
    ),
    cus_churn_rate AS (
        SELECT *, (cp.Customer_Inactive_Period / cp.Customer_Frequency)::real AS Customer_Churn_Rate
        FROM cus_inact_per cp
    ),
    cus_churn_rate_seg AS (
        SELECT *,
            (CASE
                WHEN Customer_Churn_Rate < 2 THEN 'Low'
                WHEN Customer_Churn_Rate >= 2 AND
                     Customer_Churn_Rate < 5 THEN 'Medium'
                ELSE 'High' END) AS Customer_Churn_Segment
        FROM cus_churn_rate),
    cus_seg AS (
        SELECT crs.customer_id, crs.Customer_Average_Check, crs.Customer_Average_Check_Segment,
               crs.Customer_Frequency, crs.Customer_Frequency_Segment, crs.Customer_Inactive_Period,
               crs.Customer_Churn_Rate, crs.Customer_Churn_Segment, s.Segment AS Customer_Segment
        FROM cus_churn_rate_seg crs
        JOIN segments s ON  s.average_check = crs.Customer_Average_Check_Segment AND
                            s.frequency_of_purchases = crs.Customer_Frequency_Segment AND
                            s.churn_probability = crs.Customer_Churn_Segment),
    cus_p_store AS (
        WITH stores_trans_total AS (
            SELECT customer_id, count(transaction_id) AS total_trans
            FROM transactions_plus
            GROUP BY 1),
        stores_trans_cnt AS (
            SELECT tp.customer_id, tp.transaction_store_id, count(transaction_store_id) AS trans_cnt, max(transaction_datetime) AS last_date
            FROM transactions_plus tp
            GROUP BY 1, 2),
        stores_trans_share AS (
            SELECT stc.customer_id, stc.transaction_store_id, stc.trans_cnt, (stc.trans_cnt::real / stt.total_trans)::real AS trans_share, stc.last_date
            FROM stores_trans_cnt stc
            JOIN stores_trans_total stt ON stt.customer_id = stc.customer_id
            ORDER BY 1, 3 DESC),
        stores_trans_share_rank AS (
            SELECT *, row_number() over (partition by customer_id order by trans_share DESC, last_date DESC) AS row_share_date
            FROM stores_trans_share),
        trans_num AS (
            SELECT t1.customer_id, t1.transaction_store_id, t1.transaction_datetime, t1.row
            FROM (SELECT *, row_number() over (partition by customer_id ORDER BY transaction_datetime DESC) row FROM transactions_plus t1) t1
            ORDER BY 1, transaction_datetime DESC),
        last_stores_trans AS (
            SELECT tn.customer_id, tn.transaction_store_id, tn.transaction_datetime
            FROM trans_num tn
            WHERE tn.row <= 3
            ORDER BY 1),
        last_store_trans AS (
            SELECT tn.customer_id, tn.transaction_store_id, tn.transaction_datetime
            FROM trans_num tn
            WHERE tn.row <= 1
            ORDER BY 1),
        customers_with_same_stores AS (
            SELECT customer_id
            FROM last_stores_trans
            GROUP BY customer_id
            HAVING count(distinct transaction_store_id) = 1),
        req1_customers AS (
            SELECT customer_id, transaction_store_id AS Customer_Primary_Store
            FROM stores_trans_share_rank
            WHERE row_share_date = 1 AND customer_id IN (SELECT * FROM customers_with_same_stores)),
        req23_customers AS (
            SELECT customer_id, transaction_store_id AS Customer_Primary_Store
            FROM stores_trans_share_rank
            WHERE row_share_date = 1 AND customer_id NOT IN (SELECT * FROM customers_with_same_stores)),
        union_tables AS (
            SELECT * FROM req23_customers
            UNION
            SELECT * FROM req1_customers)
    SELECT cs.*, ut.Customer_Primary_Store
    FROM cus_seg cs
    JOIN union_tables ut ON ut.customer_id = cs.customer_id)
        SELECT *
        FROM cus_p_store
        ORDER BY 1;

-- -- Purchase history View
DROP VIEW IF EXISTS Purchase_History_Support CASCADE;
CREATE VIEW Purchase_History_Support AS
SELECT CR.Customer_ID,
       TR.Transaction_ID,
       TR.Transaction_DateTime,
       TR.Transaction_Store_ID,
       SKU.Group_ID,
       CK.SKU_Amount,
       SR.SKU_ID,
       SR.SKU_Retail_Price,
       SR.SKU_Purchase_Price,
       CK.SKU_Summ_Paid,
       CK.SKU_Summ,
       CK.SKU_Discount
FROM Transactions AS TR
JOIN Cards AS CR ON CR.Customer_Card_ID = TR.Customer_Card_ID
JOIN Personal_data AS PD ON PD.Customer_ID = CR.Customer_ID
JOIN Checks AS CK ON TR.Transaction_ID = CK.Transaction_ID
JOIN SKU AS SKU ON SKU.SKU_ID = CK.SKU_ID
JOIN Stores AS SR ON SKU.SKU_ID = SR.SKU_ID
AND TR.Transaction_Store_ID = SR.Transaction_Store_ID;

DROP VIEW IF EXISTS Purchase_History_View CASCADE;
CREATE VIEW Purchase_History_View AS
SELECT Customer_ID,
       Transaction_ID,
       Transaction_DateTime,
       Group_ID,
       sum(SKU_Purchase_Price * SKU_Amount) AS "Group_Cost",
       sum(SKU_Summ) AS "Group_Summ",
       sum(SKU_Summ_Paid) AS "Summ_Paid"
FROM Purchase_History_Support
GROUP BY Customer_ID, Transaction_ID, Transaction_DateTime, Group_ID;

-- Periods View
DROP VIEW IF EXISTS Periods_View CASCADE;
CREATE VIEW Periods_View AS
SELECT Customer_ID,
       Group_ID,
       MIN(Transaction_DateTime) AS "First_Group_Purchase_Date",
       MAX(Transaction_DateTime) AS "Last_Group_Purchase_Date",
       COUNT(*) Group_Purchase,
       (((TO_CHAR((MAX(Transaction_DateTime)::timestamp - MIN(Transaction_DateTime)::timestamp), 'DD'))::int + 1)*1.0) / COUNT(*)*1.0 AS Group_Frequency,
       COALESCE((SELECT MIN(c1.SKU_Discount / c1.SKU_Summ) AS Group_Min_Discount FROM Checks c1
       JOIN Purchase_History_Support ph2 ON ph2.Transaction_ID = c1.Transaction_ID
       WHERE (c1.SKU_Discount / c1.SKU_Summ) > 0 AND ph2.Customer_ID = t1.Customer_ID
       AND ph2.Group_ID = t1.Group_ID), 0) AS Group_Minimum_Discount
  FROM (SELECT DISTINCT Customer_ID, t.Transaction_DateTime, c.SKU_Discount, SKU.Group_ID, c.SKU_Summ
          FROM Cards
                JOIN Transactions t ON cards.Customer_Card_ID = t.Customer_Card_ID
                 JOIN Checks c ON t.Transaction_ID = c.Transaction_ID
                  JOIN SKU ON SKU.SKU_ID = c.SKU_ID) AS t1
 GROUP BY Group_ID, Customer_ID;

-- -- Groups View
DROP VIEW IF EXISTS Groups_View_Support CASCADE;
CREATE VIEW Groups_View_Support AS
SELECT supp.Customer_ID,
       supp.Group_ID,
       supp.Transaction_ID,
       supp.Transaction_DateTime,
       supp."Group_Cost",
       supp."Group_Summ",
       supp."Summ_Paid",
       VP."First_Group_Purchase_Date",
       VP."Last_Group_Purchase_Date",
       VP.Group_Purchase,
       VP.Group_Frequency,
       VP.group_minimum_discount
FROM Periods_View AS VP
         JOIN Purchase_History_View AS supp ON supp.Customer_ID = VP.Customer_ID AND
                                 supp.Group_ID = VP.Group_ID;

DROP FUNCTION IF EXISTS fnc_create_Groups_View(integer,interval,integer) CASCADE;
CREATE FUNCTION fnc_create_Groups_View(IN int default 1, IN interval default '5000 days'::interval,
                                              IN int default 100)
    RETURNS TABLE
            (
                Customer_ID            bigint,
                Group_ID               bigint,
                Group_Affinity_Index   float,
                Group_Churn_Rate       float,
                Group_Stability_Index  float,
                Group_Margin           float,
                Group_Discount_Share   float,
                Group_Minimum_Discount numeric,
                Group_Average_Discount float
            )
    AS
    $$
BEGIN
    RETURN QUERY
        SELECT VMI.Customer_ID,
               VMI.Group_ID,
               "Group_Affinity_Index",
               "Group_Churn_Rate",

coalesce(avg("Group_Stability_Index"), 0),

coalesce(CASE
            WHEN ($1 = 1) THEN
            sum("Group_Margin"::float)
            FILTER (WHERE Transaction_DateTime BETWEEN (SELECT Analysis_Formation FROM Date_Of_Analysis_Formation) - $2 AND
            (SELECT Analysis_Formation FROM Date_Of_Analysis_Formation) )
                WHEN ($1 = 2) THEN
                     (SELECT sum(GM)::float
                     FROM (SELECT "Summ_Paid" - "Group_Cost" as GM FROM Groups_View_Support
                           WHERE VMI.Customer_ID = Groups_View_Support.Customer_ID
                           AND VMI.Group_ID = Groups_View_Support.Group_ID
                           ORDER BY Transaction_DateTime DESC LIMIT $3) as SGM)
END, 0) AS "Group_Margin", "Group_Discount_Share",

coalesce((SELECT min(SKU_Discount / SKU_Summ) FROM Purchase_History_Support AS VB
                         WHERE VB.customer_id = VMI.Customer_ID AND VB.group_id = VMI.Group_ID
                         AND sku_discount / sku_summ > 0.0), 0)::numeric AS "Group_Minimum_Discount",
                         avg(VMI."Group_Minimum_Discount") / avg(VMI."Group_Average_Discount")::float AS "Group_Average_Discount"
        FROM (SELECT Groups_View_Support.Customer_ID,
                     Groups_View_Support.Group_ID,
                     Groups_View_Support.Group_Purchase::float /
                     (SELECT count(Transaction_ID)
                      FROM Groups_View_Support AS GP
                      WHERE GP.Customer_ID = Groups_View_Support.Customer_ID
                        AND GP.Transaction_DateTime
                          BETWEEN Groups_View_Support."First_Group_Purchase_Date"
                          AND Groups_View_Support."Last_Group_Purchase_Date") AS "Group_Affinity_Index",

                     extract(EPOCH from (SELECT Analysis_Formation FROM Date_Of_Analysis_Formation) -
                                        max(Transaction_DateTime)
                                        OVER (PARTITION BY Groups_View_Support.Customer_ID, Groups_View_Support.Group_ID))::float / 86400.0 /
                     Group_Frequency                                      AS "Group_Churn_Rate",

                     abs(extract(epoch from Transaction_DateTime - lag(Transaction_DateTime, 1)
                                                                     over (partition by Groups_View_Support.Customer_ID, Groups_View_Support.Group_ID
                                                                         order by Transaction_DateTime))::float /
                         86400.0 - Group_Frequency) / Group_Frequency   as "Group_Stability_Index",

                     "Summ_Paid" - "Group_Cost"                       AS "Group_Margin",
                     Transaction_DateTime, -- вот это выводит, но убирать нельзя

                     (SELECT count(transaction_id)
                      FROM Purchase_History_Support AS VB
                      WHERE Groups_View_Support.Customer_ID = VB.Customer_ID
                        AND Groups_View_Support.Group_ID = VB.Group_ID
                        AND VB.SKU_Discount != 0)::float / Group_Purchase AS "Group_Discount_Share",
                     "Summ_Paid" as "Group_Minimum_Discount",
                     "Group_Summ" as "Group_Average_Discount"

              FROM Groups_View_Support) as VMI
        GROUP BY VMI.Customer_ID, VMI.Group_ID, "Group_Affinity_Index", "Group_Churn_Rate", "Group_Discount_Share";
END ;
$$ LANGUAGE plpgsql;

DROP VIEW IF EXISTS Groups_View CASCADE;
CREATE VIEW Groups_View AS
select *
from fnc_create_Groups_View();