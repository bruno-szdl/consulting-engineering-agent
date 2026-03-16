with orders as (

    select * from {{ ref('stg_tpch__orders') }}

),

customers as (

    select * from {{ ref('stg_tpch__customers') }}

),

nations as (

    select * from {{ ref('stg_tpch__nations') }}

),

regions as (

    select * from {{ ref('stg_tpch__regions') }}

),

joined as (

    select
        orders.order_key,
        orders.order_date,
        orders.order_status,
        orders.total_price,
        orders.order_priority,
        orders.clerk,
        customers.customer_key,
        customers.customer_name,
        customers.market_segment,
        customers.account_balance as customer_account_balance,
        nations.nation_name as customer_nation,
        regions.region_name as customer_region

    from orders
    inner join customers
        on orders.customer_key = customers.customer_key
    inner join nations
        on customers.nation_key = nations.nation_key
    inner join regions
        on nations.region_key = regions.region_key

)

select * from joined
