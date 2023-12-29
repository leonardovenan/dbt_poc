with calc_employees as (
select
    (cast(date_part(year, current_date) as int) - cast(date_part(year, birth_date) as int))  as age
    ,(cast(date_part(year, current_date) as int) - cast(date_part(year, hire_date) as int))  as length_of_service
    ,concat(concat(first_name, ' '), last_name)                                              as name -- first_name || ' ' || last_name
    ,*
from {{source('sources', 'employees')}}
)
select * from calc_employees