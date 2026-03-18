{{
    config(
        materialized='table'
    )
}}

with line_items as (

    -- Bad: references staging directly instead of going through intermediate layer
    select
        order_key,
        line_number,
        supplier_key,
        ship_date,
        extended_price,
        discount,
        quantity,
        return_flag,
        extended_price * (1 - discount) as net_revenue

    from {{ ref('stg_tpch__line_items') }}

),

suppliers as (

    select
        supplier_key,
        supplier_name,
        nation_key

    from {{ ref('stg_tpch__suppliers') }}

),

nations as (

    select
        nation_key,
        nation_name

    from {{ ref('stg_tpch__nations') }}

),

-- Intentionally static bracket table via cross join rather than a CASE expression.
-- This forces a 3x row multiplication before the window functions execute.
revenue_brackets as (

    select 'LOW'    as bracket, cast(0    as double) as min_price, cast(1000 as double) as max_price
    union all
    select 'MEDIUM' as bracket, cast(1000 as double) as min_price, cast(5000 as double) as max_price
    union all
    select 'HIGH'   as bracket, cast(5000 as double) as min_price, cast(1e15  as double) as max_price

),

supplier_line_items as (

    select
        li.order_key,
        li.line_number,
        li.supplier_key,
        li.ship_date,
        li.extended_price,
        li.net_revenue,
        li.quantity,
        li.return_flag,
        s.supplier_name,
        n.nation_name as supplier_nation

    from line_items li
    inner join suppliers s
        on li.supplier_key = s.supplier_key
    inner join nations n
        on s.nation_key = n.nation_key

),

-- Cross join multiplies ~6M rows to ~18M before window functions
supplier_line_items_bracketed as (

    select
        sli.order_key,
        sli.line_number,
        sli.supplier_key,
        sli.supplier_name,
        sli.supplier_nation,
        sli.ship_date,
        sli.extended_price,
        sli.net_revenue,
        sli.quantity,
        sli.return_flag,
        rb.bracket as revenue_bracket

    from supplier_line_items sli
    cross join revenue_brackets rb

),

-- Two expensive window functions over the 18M-row inflated dataset
windowed as (

    select
        order_key,
        line_number,
        supplier_key,
        supplier_name,
        supplier_nation,
        ship_date,
        extended_price,
        net_revenue,
        quantity,
        return_flag,
        revenue_bracket,
        -- Running cumulative revenue: full sort per (supplier, bracket)
        sum(net_revenue) over (
            partition by supplier_key, revenue_bracket
            order by ship_date
            rows between unbounded preceding and current row
        ) as cumulative_revenue,
        -- Monthly rank: sort per (supplier, bracket, month)
        rank() over (
            partition by supplier_key, revenue_bracket, date_trunc('month', ship_date)
            order by extended_price desc
        ) as monthly_rank

    from supplier_line_items_bracketed

),

aggregated as (

    select
        supplier_key,
        supplier_name,
        supplier_nation,
        date_trunc('month', ship_date)                                    as ship_month,
        revenue_bracket,
        count(*)                                                           as line_item_count,
        sum(quantity)                                                      as total_quantity,
        sum(extended_price)                                                as gross_revenue,
        sum(net_revenue)                                                   as net_revenue,
        max(cumulative_revenue)                                            as cumulative_revenue_to_month,
        sum(case when return_flag = 'R' then 1    else 0          end)    as returned_item_count,
        sum(case when return_flag = 'R' then net_revenue else 0   end)    as returned_revenue

    from windowed
    where monthly_rank <= 10

    group by
        supplier_key,
        supplier_name,
        supplier_nation,
        date_trunc('month', ship_date),
        revenue_bracket

)

select * from aggregated
