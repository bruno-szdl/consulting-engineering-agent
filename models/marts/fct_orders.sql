with orders as (

    select * from {{ ref('int_orders_with_customers') }}

),

line_items as (

    select * from {{ ref('int_line_items_with_parts') }}

),

order_line_agg as (

    select
        order_key,
        count(*) as item_count,
        sum(extended_price) as gross_amount,
        sum(discounted_price) as discounted_amount,
        sum(charge_amount) as total_charge,
        sum(case when return_flag = 'R' then 1 else 0 end) as returned_item_count

    from line_items
    group by order_key

),

final as (

    select
        orders.order_key,
        orders.order_date,
        orders.order_status,
        orders.order_priority,
        orders.clerk,
        orders.customer_key,
        orders.customer_name,
        orders.market_segment,
        orders.customer_nation,
        orders.customer_region,
        order_line_agg.item_count,
        order_line_agg.gross_amount,
        order_line_agg.discounted_amount,
        order_line_agg.total_charge,
        order_line_agg.returned_item_count,
        order_line_agg.returned_item_count > 0 as has_returns

    from orders
    inner join order_line_agg
        on orders.order_key = order_line_agg.order_key

)

select * from final
