DROP FUNCTION IF EXISTS offersAimedFrequencyV(varchar, bigint, real, real, real);
-- Main_function
CREATE FUNCTION offersAimedFrequencyV (fst_n_lst_dates varchar, transactions_num bigint,
                                        churn_idx_max real, disc_trans_share real,
                                        allow_margin_share real)
RETURNS TABLE (Customer_ID bigint, Start_Date timestamp, End_Date timestamp, Required_Transactions_Count real,
                Group_Name varchar, Offer_Discount_Depth real)
LANGUAGE plpgsql AS
    $$
    DECLARE
        Start_Date timestamp := split_part(fst_n_lst_dates, ' ', 1)::timestamp;
        End_Date timestamp := split_part(fst_n_lst_dates, ' ', 2)::timestamp;
    BEGIN
        RETURN QUERY
        WITH freq_visits AS (
            SELECT cv.customer_id,
                   (round((End_Date::date - Start_Date::date) / cv.customer_frequency) + transactions_num) AS Required_Transactions_Count
            FROM customers_view cv)
        SELECT fv.customer_id, Start_Date, End_Date, fv.Required_Transactions_Count::real, gs.group_name, rd.offer_discount_depth
        FROM freq_visits fv
        JOIN rewardgroupdetermination(churn_idx_max, disc_trans_share, allow_margin_share) rd ON
            fv.customer_id = rd.customer_id
        JOIN groups_sku gs ON gs.group_id = rd.group_id
        ORDER BY 1;
    END;
    $$;

-- Check
SELECT * FROM offersAimedFrequencyV('11-08-2020 11-08-2022', 200, 3, 70, 30);
