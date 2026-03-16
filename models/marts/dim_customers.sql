with customers as (

    select * from {{ ref('stg_tpch__customers') }}

),

nations as (

    select * from {{ ref('stg_tpch__nations') }}

),

regions as (

    select * from {{ ref('stg_tpch__regions') }}

),

orders as (

    select * from {{ ref('stg_tpch__orders') }}

),

customer_orders as (

    select
        customer_key,
        count(*) as total_orders,
        sum(total_price) as lifetime_revenue,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date

    from orders
    group by customer_key

),

final as (

    select
        customers.customer_key,
        customers.customer_name,
        customers.customer_address,
        customers.phone_number,
        customers.account_balance,
        customers.market_segment,
        nations.nation_name,
        regions.region_name,
        coalesce(customer_orders.total_orders, 0) as total_orders,
        coalesce(customer_orders.lifetime_revenue, 0) as lifetime_revenue,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date

    from customers
    inner join nations
        on customers.nation_key = nations.nation_key
    inner join regions
        on nations.region_key = regions.region_key
    left join customer_orders
        on customers.customer_key = customer_orders.customer_key

)

select * from final
