with line_items as (

    select * from {{ ref('int_line_items_with_parts') }}

),

final as (

    select
        order_key,
        line_number,
        quantity,
        extended_price,
        discount,
        tax,
        return_flag,
        line_status,
        ship_date,
        commit_date,
        receipt_date,
        ship_mode,
        part_key,
        part_name,
        manufacturer,
        brand,
        part_type,
        retail_price,
        supplier_key,
        supplier_name,
        supplier_nation,
        discounted_price,
        charge_amount,
        case
            when extended_price < 1000  then 'LOW'
            when extended_price < 5000  then 'MEDIUM'
            else                             'HIGH'
        end as revenue_bracket

    from line_items

)

select * from final
