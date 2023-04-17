insert into [trezor_txn_data_upd]
SELECT [date]
    ,convert(float, replace([val], ' BTC', ''))
    ,[address]
    ,[page_num]
FROM [trezor_db].[dbo].[trezor_txn_data]


SELECT *
into txn_data_before_2017
FROM [trezor_txn_data_upd]
where date < '20170101'

delete
FROM [trezor_txn_data_upd]
where date < '20170101'