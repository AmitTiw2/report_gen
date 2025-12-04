select
    sum(
    case
    when trans_type_name in ('Sale', 'Return', 'Rewash')
      and trans_item_state_name in (
        'Normal',
        'Price Override',
        'Comp Sale'
      )
      and item_department_name in (
        'Wash',
        'Detail',
        'Service',
        'Merchandise',
        'Vacuum Sales',
        'Food',
        'Beverage',
        'Ingredient'
      )
      then
        case
          when trans_state_name != 'Completed'
          then 0
          else item_quantity * item_amount
        end
      else 0
    end
  ) as gross_sales
from
  denormalized_transaction
where
  client_id = 'luvs'
  and complete_date >= '2025-01-01 00:00:00'
  and complete_date <= '2025-01-31 23:59:59';