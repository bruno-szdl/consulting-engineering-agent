with line_items as (

    select * from {{ ref('stg_tpch__line_items') }}

),

parts as (

    select * from {{ ref('stg_tpch__parts') }}

),

suppliers as (

    select * from {{ ref('stg_tpch__suppliers') }}

),

nations as (

    select * from {{ ref('stg_tpch__nations') }}

),

joined as (

    select
        line_items.order_key,
        line_items.line_number,
        line_items.quantity,
        line_items.extended_price,
        line_items.discount,
        line_items.tax,
        line_items.return_flag,
        line_items.line_status,
        line_items.ship_date,
        line_items.commit_date,
        line_items.receipt_date,
        line_items.ship_mode,
        parts.part_key,
        parts.part_name,
        parts.manufacturer,
        parts.brand,
        parts.part_type,
        parts.retail_price,
        suppliers.supplier_key,
        suppliers.supplier_name,
        nations.nation_name as supplier_nation,
        line_items.extended_price * (1 - line_items.discount) as discounted_price,
        line_items.extended_price * (1 - line_items.discount) * (1 + line_items.tax) as charge_amount

    from line_items
    inner join parts
        on line_items.part_key = parts.part_key
    inner join suppliers
        on line_items.supplier_key = suppliers.supplier_key
    inner join nations
        on suppliers.nation_key = nations.nation_key

)

select * from joined
