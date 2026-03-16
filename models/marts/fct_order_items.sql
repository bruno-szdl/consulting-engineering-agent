{{
    config(
        materialized='incremental',
        unique_key=['order_key', 'line_number'],
        incremental_strategy='merge'
    )
}}

with line_items as (

    select * from {{ ref('int_line_items_with_parts') }}

    {% if is_incremental() %}
    where ship_date > (select max(ship_date) from {{ this }})
    {% endif %}

),

orders as (

    select * from {{ ref('int_orders_with_customers') }}

),

final as (

    select
        line_items.order_key,
        line_items.line_number,
        orders.order_date,
        orders.order_status,
        orders.customer_key,
        orders.customer_name,
        orders.market_segment,
        orders.customer_nation,
        orders.customer_region,
        line_items.part_key,
        line_items.part_name,
        line_items.brand,
        line_items.part_type,
        line_items.supplier_key,
        line_items.supplier_name,
        line_items.supplier_nation,
        line_items.quantity,
        line_items.extended_price,
        line_items.discount,
        line_items.tax,
        line_items.discounted_price,
        line_items.charge_amount,
        line_items.return_flag,
        line_items.line_status,
        line_items.ship_date,
        line_items.commit_date,
        line_items.receipt_date,
        line_items.ship_mode,
        datediff(day, orders.order_date, line_items.ship_date) as days_to_ship

    from line_items
    inner join orders
        on line_items.order_key = orders.order_key

)

select * from final
