WITH NEXT_DAY_GAS AS (
    SELECT  

        EXTRACT(YEAR FROM trade_date::DATE) as year
        ,EXTRACT(MONTH FROM trade_date::DATE) as month
        ,trade_date::DATE
        
        -- HH
        ,AVG(CASE WHEN symbol='XGF D1-IPG' THEN value ELSE NULL END) AS hh_cash

        -- SOUTHEAST
        ,AVG(CASE WHEN symbol='XVA D1-IPG' then value END) AS transco_st85_cash
        
        -- EAST TEXAS
        ,AVG(CASE WHEN symbol='XT6 D1-IPG' then value END) AS waha_cash

        -- NORTHEAST
        ,AVG(CASE WHEN symbol='YFF D1-IPG' then value END) AS transco_zone_5_south_cash
        ,AVG(CASE WHEN symbol='XZR D1-IPG' then value END) AS tetco_m3_cash
        ,AVG(CASE WHEN symbol='X7F D1-IPG' then value END) AS agt_cash
        ,AVG(CASE WHEN symbol='YP8 D1-IPG' then value END) AS iroquois_z2_cash
        
        -- WEST
        ,AVG(CASE WHEN symbol='XKF D1-IPG' then value END) AS socal_cg_cash
        ,AVG(CASE WHEN symbol='XGV D1-IPG' then value END) AS pge_cg_cash

        -- Rockies/Northwest
        ,AVG(CASE WHEN symbol='YKL D1-IPG' then value END) AS cig_cash

    from ice_python.next_day_gas_v1_2025_dec_16
    GROUP BY trade_date
)

SELECT * FROM NEXT_DAY_GAS
ORDER BY trade_date desc