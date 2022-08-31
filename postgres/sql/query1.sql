insert
	into
	tbl1
select
	t.product,
	t.quality,
	t.price,
	t.quality * t.price as amount
from
	(
	select
		concat('pr', floor(random()* 10 + 1)) as product,
		floor(random()* 10 + 1) as quality,
		floor(random()* 10 + 1) as price) as t;