DROP FUNCTION IF EXISTS cross_selling(integer, numeric, numeric, numeric, numeric);
-- Main_function
CREATE FUNCTION cross_selling(
    IN count_group integer,
    IN max_churn_rate numeric,
    IN max_stability_index numeric,
    IN max_sku numeric,
    IN max_margin numeric
)
RETURNS TABLE
        (
            Customer_ID          bigint,
            SKU_Name             varchar,
            Offer_Discount_Depth int
        )
AS $$
BEGIN
RETURN QUERY
    WITH MD AS (
        SELECT dense_rank() OVER (PARTITION BY GV.customer_id ORDER BY GV.group_id) AS DR, -- ранг текущей строки без пропусков
            first_value(sku.sku_name) OVER ( PARTITION BY GV.customer_id, GV.group_id ORDER BY (VB.sku_retail_price - VB.sku_purchase_price) DESC) AS SN,
            GV.group_id AS GI, GV.customer_id, GV.group_minimum_discount, VB.sku_retail_price, VB.sku_purchase_price
        FROM groups_view AS GV
        JOIN purchase_history_support AS VB ON VB.customer_id = GV.customer_id AND VB.group_id = GV.group_id
        JOIN customers_view AS CV ON CV.customer_id = GV.customer_id
        JOIN sku ON sku.group_id = GV.group_id AND sku.sku_id = VB.sku_id WHERE CV.customer_primary_store = VB.transaction_store_id
            AND GV.group_churn_rate <= max_churn_rate AND GV.group_stability_index < max_stability_index),
        count_ AS (
            SELECT count(*) FILTER ( WHERE sku.sku_name = MD.SN)::numeric / count(*)
            FROM purchase_history_support AS VB
            JOIN MD ON MD.customer_id = VB.customer_id
            JOIN sku ON sku.sku_id = VB.sku_id
            WHERE (VB.customer_id = MD.customer_id) AND VB.group_id = MD.GI)
    SELECT DISTINCT MD.customer_id, MD.SN,
        CASE
           WHEN (MD.group_minimum_discount*1.05*100)::integer = 0 THEN 5
           ELSE (MD.group_minimum_discount*1.05*100)::integer
        END
    FROM MD
    WHERE DR <= count_group
      AND (SELECT * FROM count_) < max_sku
      AND (MD.sku_retail_price - MD.sku_purchase_price) * max_margin / 100.0 / MD.sku_retail_price >= MD.group_minimum_discount * 1.05;
END;
$$
LANGUAGE plpgsql;

SELECT * FROM cross_selling(100, 100, 100, 2, 10);