with parts as (

    select * from {{ ref('stg_tpch__parts') }}

),

part_suppliers as (

    select * from {{ ref('stg_tpch__part_suppliers') }}

),

supply_summary as (

    select
        part_key,
        count(distinct supplier_key) as supplier_count,
        avg(supply_cost) as avg_supply_cost,
        min(supply_cost) as min_supply_cost,
        sum(available_quantity) as total_available_quantity

    from part_suppliers
    group by part_key

),

final as (

    select
        parts.part_key,
        parts.part_name,
        parts.manufacturer,
        parts.brand,
        parts.part_type,
        parts.part_size,
        parts.container,
        parts.retail_price,
        coalesce(supply_summary.supplier_count, 0) as supplier_count,
        supply_summary.avg_supply_cost,
        supply_summary.min_supply_cost,
        coalesce(supply_summary.total_available_quantity, 0) as total_available_quantity

    from parts
    left join supply_summary
        on parts.part_key = supply_summary.part_key

)

select * from final
