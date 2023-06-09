SELECT [id] 'rank'
	,[address]
	,convert(float, replace(left([balance], charindex(' ', balance)-1), ',', '')) 'balance'
	,convert
		(
			float,
			replace
				(
				substring
					(
						balance, 
						charindex('$', balance)+1, 
						charindex(')', balance) - (charindex('$', balance)+1)
					)
				, ','
				, ''
				)
		) 'balance_usd'
	,convert(float, replace([percent_of_coins], '%', '')) 'percent_of_coins'
	,convert(date, left([first_in], 10)) 'first_in'
	,convert(date, left([last_in], 10)) 'last_in'
	,convert(int, [ins]) 'ins'
	,convert(date, left([first_out], 10)) 'first_out'
	,convert(date, left([last_out], 10)) 'last_out'
	,convert(int, [outs]) 'outs'
into btc_pivot_data_upd
FROM [btc_pivot_data]

drop table if exists [btc_pivot_data]

select * 
into [btc_pivot_data]
from btc_pivot_data_upd

drop table if exists btc_pivot_data_upd