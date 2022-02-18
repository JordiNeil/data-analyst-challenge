
with transfers_in as (
	select 
		account_id, 
		dm.action_month,
		sum(amount) as t_in
	from transfer_ins ti 
	join d_time dt 
	on ti.transaction_requested_at = dt.time_id 
	join d_month dm 
	on dm.month_id = dt.month_id 
	where dt.action_timestamp >= '2020-01-01'
	and dt.action_timestamp <= '2020-12-31'	
	group by ti.account_id, dm.action_month
	order by ti.account_id, dm.action_month 
), --extract incoming standard transfers by month and account
transfers_out as (
	select 
		account_id, 
		dm.action_month,	
		sum(amount) as t_out
	from transfer_outs t_o
	join d_time dt 
	on t_o.transaction_requested_at = dt.time_id 
	join d_month dm 
	on dm.month_id = dt.month_id 
	where dt.action_timestamp >= '2020-01-01'
	and dt.action_timestamp <= '2020-12-31'	
	group by t_o.account_id, dm.action_month
	order by t_o.account_id, dm.action_month
), --extract outgoing standard transfers by month and account
pix_transfers_in as (
	select 
		account_id, 
		dm.action_month,
		sum(pix_amount) as pix_amount 
	from pix_movements pm
	join d_time dt 
	on pm.pix_requested_at = dt.time_id 
	join d_month dm 
	on dm.month_id = dt.month_id 
	where dt.action_timestamp >= '2020-01-01' and in_or_out = 'pix_in'
	and dt.action_timestamp <= '2020-12-31'	
	group by pm.account_id, dm.action_month
	order by pm.account_id, dm.action_month
), --extract incoming pix transfers by month and account
pix_transfers_out as (
	select 
		account_id, 
		dm.action_month,
		sum(pix_amount) as pix_amount 
	from pix_movements pm
	join d_time dt 
	on pm.pix_requested_at = dt.time_id 
	join d_month dm 
	on dm.month_id = dt.month_id 
	where dt.action_timestamp >= '2020-01-01' and in_or_out = 'pix_out'
	and dt.action_timestamp <= '2020-12-31'	
	group by pm.account_id, dm.action_month
	order by pm.account_id, dm.action_month
), --extract outgoing pix transfers by month and account
normal_trans_summary as (
	select 
		case 
		when transfers_in.account_id is null then transfers_out.account_id
		else transfers_in.account_id
		end as account_id,
		case 
		when transfers_in.action_month is null then transfers_out.action_month
		else transfers_in.action_month
		end as action_month,
		transfers_in.t_in,
		transfers_out.t_out,
		(case when transfers_in.t_in is null then 0 else transfers_in.t_in end) -
		(case when transfers_out.t_out is null then 0 else transfers_out.t_out end) as monthly_revenue
	from transfers_in
	full outer join transfers_out
	on transfers_in.account_id = transfers_out.account_id 
	and transfers_in.action_month = transfers_out.action_month	
), --extract summary for normal transfers
pix_trans_summary as (
	select 
		case 
		when pix_transfers_in.account_id is null then pix_transfers_out.account_id
		else pix_transfers_in.account_id
		end as account_id,
		case 
		when pix_transfers_in.action_month is null then pix_transfers_out.action_month
		else pix_transfers_in.action_month
		end as action_month,	
		pix_transfers_in.pix_amount as pix_in,
		pix_transfers_out.pix_amount as pix_out,
		(case when pix_transfers_in.pix_amount is null then 0 else pix_transfers_in.pix_amount end) -
		(case when pix_transfers_out.pix_amount is null then 0 else pix_transfers_out.pix_amount end) as monthly_revenue
	from pix_transfers_in
	full outer join pix_transfers_out
	on pix_transfers_in.account_id = pix_transfers_out.account_id 
	and pix_transfers_in.action_month = pix_transfers_out.action_month
), --extract summary for pix transfers
final_summary as (
select 
	case 
		when normal_transf.account_id is null then pix_transf.account_id
		else normal_transf.account_id
	end as account_id,
	case 
		when normal_transf.action_month  is null then pix_transf.action_month
		else normal_transf.action_month
	end as action_month,
	normal_transf.monthly_revenue as normal_monthly_revenue,
	pix_transf.monthly_revenue as pix_monthly_revenue,
	(case when normal_transf.monthly_revenue is null then 0 else normal_transf.monthly_revenue end) +
	(case when pix_transf.monthly_revenue is null then 0 else pix_transf.monthly_revenue end) as total_monthly_revenue	
from normal_trans_summary normal_transf
full outer join pix_trans_summary pix_transf
on normal_transf.account_id = pix_transf.account_id
and normal_transf.action_month = pix_transf.action_month
) -- extract general summary for each account and month
select *, sum(total_monthly_revenue) over (partition by account_id order by action_month) as account_monthly_balance
from final_summary;