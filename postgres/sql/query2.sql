with tbl3 as
(select
	t.product,
	sum(t.amount) as total_amount
from
	tbl1 as t
group by
	t.product)

update
	tbl2
set
	total_amount = tbl3.total_amount
from
	tbl3
where
	tbl3.product = tbl2.product;
