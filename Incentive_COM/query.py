import pandas as pd
import numpy as np
import os
import pandas.io.sql as sqlio
import psycopg2
import calendar
import json


# Query NMV
def query_nmv(start_date, end_date, db):
    with psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(db["host"], db["port"], db["dbname"], db["user"], db["password"])) as conn:
        sql_nmv = f"""
        SELECT groupbrand, sum(NMV) as nmv
        FROM sales.v_sales_mtd_v2
        WHERE date(orderdate + interval '7 hours') between '{start_date}'::date AND '{end_date}'::date
        GROUP BY groupbrand;
        """
        df_nmv = pd.read_sql_query(sql_nmv, conn)
        return df_nmv

# Query SLOB
def query_slob(date, db):
    with psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(db["host"], db["port"], db["dbname"], db["user"], db["password"])) as conn:
        sql_slob = f"""
        --- SKU Demand Doi
With date as	(
					SELECT 			cast ('{date}' as date) as start_date, cast ('2021-03-31' as date) as end_date	),
		 t1_1  as  (
								With t1 as	(
					SELECT 			warehouse_platform as warehouse,
											Case When warehouse_platform = 'tnc' and  length(w.sku) > 15 and w.sku like '%-%'	Then SPLIT_PART(w.sku,'-',2) Else w.sku End sku, 
											available_stock as qty_stock, date(updated_at_log) as updated_at,1 as number,warehouse_code	,sm.groupbrand||warehouse_platform map2,sm.groupbrand||warehouse_code map	,sm.groupbrand
					FROM				athena_warehouse.ims_centralize_inventory_timeseries w
					--- COUNT SHOPBOSCH RA KHOI DOI&SLOB 
					LEFT JOIN		supplychain.supplychain_master_sku sm ON w.sku = sm.sku
					WHERE 			date(updated_at_log) = (SELECT  start_date from date )
					AND					warehouse_platform not in ('octopos','vinculum','shopee')
					AND					date(updated_at_log) = date(w.updated_at + interval '8 hours')
					AND					w.sku not like '%F') 
						SELECT 				warehouse,sku,qty_stock,updated_at,number,warehouse_code
						FROM 					t1  
						WHERE 				map != 'BOSCHTNL|OP2'
						AND						map2 != 'LVN ACDlazada'
					UNION ALL
					SELECT		  'shopee' as warehouse,item_sku as sku_code, 0 as avail_qty,created_at + interval '8 hour' as created_at,
											row_number() OVER(PARTITION BY concat(item_sku, date(created_at),shop_account) ORDER BY created_at desc) as number, null as warehouse_code
					FROM 				athena_warehouse.supplychain_shopee_timeseries
					WHERE 			date(created_at + interval '8 hour') = (SELECT  start_date from date ) and item_sku <>''
					AND 				shop_account in ('larocheposay_officialstore','vichyofficialstore')
					UNION ALL
					SELECT			'vinculum' as warehouse,sku_code, available_qty as qty_stock, date(created_at+ interval '2 hours') as updated_at, 1 as number,site_location as warehouse_code
					FROM 				athena_warehouse.supplychain_vinculum_timeseries 
					WHERE 			date(created_at+ interval '2 hours') =  (SELECT  start_date from date )
					AND					site_location in ('Keppel Land Warehouse') and inv_bucket ='Good' 
					UNION ALL
					SELECT			'vinculum1' as warehouse,sku_code, available_qty as qty_stock, date(created_at+ interval '2 hours') as updated_at, 1 as number,site_location as warehouse_code
					FROM 				athena_warehouse.supplychain_vinculum_timeseries 
					WHERE 			date(created_at+ interval '2 hours') =  (SELECT  start_date from date )
					AND					site_location in ('GHNLOG Q12') and inv_bucket ='Good'
					UNION ALL
					SELECT			'vinculum2' as warehouse,sku_code, available_qty as qty_stock, date(created_at+ interval '2 hours') as updated_at, 1 as number,site_location as warehouse_code
					FROM 				athena_warehouse.supplychain_vinculum_timeseries 
					WHERE 			date(created_at+ interval '2 hours') =  (SELECT  start_date from date )
					AND					site_location in ('Moira Warehouse') and inv_bucket ='Good' )  ,
		t1 	AS		(	
					SELECT			sku,date(updated_at) updated_at,warehouse,qty_stock ,warehouse_code
					FROM 				t1_1  WHERE number =1 and qty_stock >= 0 ) , 
		t2 as 		(  
					SELECT 			parent_sku::text, child_sku::text, quantity
					FROM 				opollo_onpoint.product_bundles 
					WHERE 			bundle_type = 'virtual'),
		t3 as (
					SELECT 			COALESCE (t2.child_sku, t1.sku) as sku,warehouse,updated_at,warehouse_code,
											t1.qty_stock * COALESCE(t2.quantity, 1) AS qty_stock,parent_sku
					FROM 				t1
					LEFT JOIN 	t2 ON t1.sku = t2.parent_sku	),
		t3_1 as (
					Select 			sku,updated_at, Case when warehouse= 'vinculum' then qty_stock else 0 End  keppel_qty,
											Case when warehouse= 'vinculum1' then qty_stock else 0 End  ghnlog12_vin_qty,
											Case when warehouse= 'vinculum2' then qty_stock else 0 End  moira_vin_qty,
											Case when warehouse= 'ghn_ffm' then qty_stock else 0 End  ghn_qty,
											Case when warehouse= 'tnc' then qty_stock else 0 End  tnc_qty,
											Case when warehouse= 'lazada' or warehouse= 'mcl'  then qty_stock else 0 End  lzd_qty,
											Case when warehouse= 'shopee' then qty_stock else 0 End  shopee_qty,
											Case when warehouse= 'tiki' then qty_stock else 0 End  tiki_qty
					From 				t3 		WHERE sku not like '%T'),
		t4 as ( 
					Select    	sku,updated_at,sum(keppel_qty) keppel_qty,sum(ghnlog12_vin_qty) ghnlog12_vin_qty , sum(moira_vin_qty) moira_vin_qty, sum(ghn_qty) ghn_qty, sum(tnc_qty) tnc_qty, sum(lzd_qty) lzd_qty,
																		 sum(shopee_qty) shopee_qty, sum(tiki_qty) tiki_qty
				  FROM        t3_1  Group by 1,2),
		t4_1 as (
					Select      sku,updated_at,(keppel_qty + ghnlog12_vin_qty + moira_vin_qty + ghn_qty + tnc_qty + lzd_qty + shopee_qty + tiki_qty ) stock_qty ,
											keppel_qty , ghnlog12_vin_qty , moira_vin_qty , ghn_qty , tnc_qty , lzd_qty , shopee_qty , tiki_qty
					FROM				t4),
		t5 as ( 
					SELECT 			Case When sm.groupbrand in (Select groupbrand From supplychain.doi_target) then sm.groupbrand Else 'OTHERS' END groupbrand,
											Case When sm.brand is not null then sm.brand Else 'OTHERS' END brand ,sm.name,t4_1.*,sm.basecost
					FROM 				t4_1			LEFT JOIN supplychain.supplychain_master_sku  sm ON t4_1.sku=sm.sku ),
 t6 as (
					SELECT			ri.transfer_request_id,ri.product_sku as sku ,ri.request_quantity::numeric transit_qty
					FROM   			opollo_onpoint.ims_transfer_requests r
					LEFT JOIN  	opollo_onpoint.ims_transfer_request_items  ri
					ON					r.id =ri.transfer_request_id
					WHERE 			r.status = 'in_transit'), 
t6_2 as (
					SELECT			transfer_request_id,sku,SUM(transit_qty) AS transit_qty
					FROM				t6				Group by 1,2) ,
e1 as (
					SELECT 			ibr.id,ibr.transfer_request_id,ibri.product_sku, ibri.request_quantity,ibri.actual_quantity
					FROM 				opollo_onpoint.ims_inbound_requests ibr
					LEFT JOIN		opollo_onpoint.ims_inbound_request_items ibri
					ON					ibr.id = ibri.inbound_request_id
					WHERE				transfer_request_id is not null ),
e2 as (
		SELECT 			transfer_request_id,product_sku, sum(request_quantity) request_quantity, 
								sum( actual_quantity) actual_quantity
		FROM				e1 
		GROUP BY		1,2 ),
t6_1 as (
		SELECT 			t6_2.sku,sum(request_quantity - actual_quantity) transit_qty
		FROM 				t6_2 
		LEFT JOIN 	e2		On t6_2.transfer_request_id=e2.transfer_request_id And t6_2.sku=e2.product_sku 
		GROUP BY		t6_2.sku) ,		
		t7 as (
					SELECT			t5.*,t6_1.transit_qty
					FROM				t5 	
					LEFT JOIN 	t6_1 ON t5.sku=t6_1.sku) ,
		t8_1 as (
					SELECT			groupbrand,brand,name,sku,updated_at, stock_qty,keppel_qty , ghnlog12_vin_qty , moira_vin_qty  , ghn_qty ,	tnc_qty , lzd_qty , shopee_qty , 
											tiki_qty ,Case When transit_qty is not null then transit_qty Else 0 End  transit_qty, basecost
					FROM 				t7) ,
		t8 as (
					SELECT			groupbrand,brand,name,sku,updated_at, (stock_qty + transit_qty) stock_qty,keppel_qty , ghnlog12_vin_qty , moira_vin_qty  , ghn_qty ,	tnc_qty , lzd_qty , 
											shopee_qty , tiki_qty , transit_qty, basecost
					FROM				t8_1 ),			
		t9 as (
					Select 			t8.* , doi_target, slob_threshold
					FROM  			t8
					LEFT JOIN		supplychain.doi_target  On t8.groupbrand=doi_target.groupbrand) ,
-----------------------------------------

		lvn as (	
					SELECT 			sd.sku,date(date) as calendar_date,round(sum(qty::numeric),5) as forecast_sale,
											Case When sm.groupbrand in (Select groupbrand From supplychain.doi_target) then sm.groupbrand Else 'OTHERS' END groupbrand
					FROM 				supplychain.supplychain_demand sd
					LEFT JOIN 	supplychain.supplychain_master_sku  sm ON sd.sku=sm.sku
					WHERE  			sd.insert_date = (select max(insert_date) from supplychain.supplychain_demand) 
					AND 				platform not in ('SHOPEE RETAIL','TIKI TRADING')
					GROUP BY 		1,2,4 ),
					
		lvn1 as (
					SELECT 			lvn.*,date(now() + interval '7 hours') today,doi_target,concat(slob_threshold, ' days') slob_threshold
					FROM 		 		lvn
					LEFT JOIN		supplychain.doi_target  On lvn.groupbrand=doi_target.groupbrand ),
					
		lvn2 as (
					SELECT			sku,calendar_date,forecast_sale,groupbrand,today,slob_threshold,date(date(today) + slob_threshold::interval) as date_checking
					FROM				lvn1) ,
		lvn3 as (
					SELECT 			*, Case When  calendar_date >= today AND calendar_date < date_checking Then 1 else 0 End check1
					FROM 				lvn2) ,
		lvn4 as (			
					SELECT 			* 
					FROM 				lvn3 WHERE check1 =1 ),
		breakcombo AS ( -- Breakdown combo for demand file 
					SELECT 			COALESCE (t2.child_sku, lvn4.sku) as sku,lvn4.calendar_date,
											lvn4.forecast_sale * COALESCE(t2.quantity, 1) AS forecast_sale
					FROM 				lvn4
					LEFT JOIN 	t2 ON lvn4.sku = t2.parent_sku),
		lvn5 AS (
					SELECT			sku, calendar_date,SUM(forecast_sale) AS forecast_sale								FROM 				breakcombo
					GROUP BY 		1,2) ,
					
		t10 as (
					SELECT			t9.*,lvn5.sku as sku1, lvn5.calendar_date,lvn5.forecast_sale 					FROM 				t9 
					FULL JOIN 	lvn5 on t9.sku = lvn5.sku),
		t10_1 as (
					SELECT 			COALESCE(sku, sku1) AS sku_code, updated_at, stock_qty  inv_qty, keppel_qty , ghnlog12_vin_qty , moira_vin_qty, ghn_qty,tnc_qty, lzd_qty, 
											shopee_qty, tiki_qty, transit_qty intransit_qty, calendar_date, forecast_sale 
					FROM 				t10 )  ,
		t11 as (
					SELECT			t10_1.*,sm.basecost
					FROM				t10_1
					LEFT JOIN 	supplychain.supplychain_master_sku sm ON t10_1.sku_code=sm.sku),
		map1 as (
					SELECT 			* 																																		FROM 				t11) ,
					
		map2 as (					
					SELECT 			*, concat(sku_code,calendar_date) as map_sku  												FROM 				map1 ),
					 
		map3 as (					
					SELECT 			*, basecost*inv_qty as inv_value 																			FROM 				map2 ) ,
					
		f3 	as (
					SELECT 			*,	sum(forecast_sale) OVER (partition by sku_code ORDER BY map_sku asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as accumulative_fc_sale
					FROM 				map3),
-- inventory value
					
		f4 as(
					SELECT 			*,(inv_qty::numeric-accumulative_fc_sale::numeric)::numeric as left_
					FROM 				f3),
					
		f5 as (
					SELECT 			*,	case	when calendar_date is null then 0	when left_ >0 then 1	else 0	end as count_DOI
					FROM 				f4),
					
		f6 as (
					SELECT 			*, 	sum(forecast_sale) over (partition by sku_code) as rr_sale 				FROM 				f5),
					
		f7 as (
					SELECT 			*,	Case	when calendar_date is null then 0	when inv_qty > rr_sale and rr_sale <> 0 then round(inv_qty::numeric / rr_sale::numeric,5)	else 0	end as doi
					FROM				 f6),
					
		f8 as (
					SELECT 			*,	Case 	when doi > 0 then doi	else count_doi	end as doi_final			FROM 				f7),  
					
		f9 as (
					SELECT 			*,sum ( doi_final ) over (partition by sku_code) as DOI_final1 
					FROM 				f8 	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23 ORDER BY sku_code asc) ,
					
		f11 as (
					SELECT 			DISTINCT sku_code,updated_at,inv_qty,keppel_qty ,ghnlog12_vin_qty, moira_vin_qty,ghn_qty,tnc_qty,lzd_qty,shopee_qty,tiki_qty,intransit_qty,rr_sale,basecost,
											Case when rr_sale =0 then 0 else doi_final1 end as doi
					FROM 				f9)	,
					
		f10 as (
					SELECT 			*,	Case when doi = 0 and rr_sale is null then 'NO FORECAST'		when rr_sale = 0 then 'NO FORECAST'else 'FORECAST' end as forecast_y_n 
					FROM 				f11	) ,
					
					
		m_sku as (Select sku, name as sku_name,created_at, brand as brand_com,brand,  groupbrand,basecost
						From supplychain.supplychain_master_sku ),
	------------------------------------------------
	--- sales previous X day (X = SLOB threshold days)
		p_lvn as (	
					SELECT 			sd.sku,date(date) as calendar_date,round(sum(qty::numeric),3) as forecast_sale,
											Case When sm.groupbrand in (Select groupbrand From supplychain.doi_target) then sm.groupbrand Else 'OTHERS' END groupbrand
					FROM 				supplychain.supplychain_demand sd
					LEFT JOIN 	supplychain.supplychain_master_sku  sm ON sd.sku=sm.sku
					WHERE  			platform not in ('SHOPEE RETAIL','TIKI TRADING')
					AND					date(date) BETWEEN  (date(now()+interval '7hour') - interval '58 days') And  date(now()+interval '7hour')
					GROUP BY 		1,2,4 ),				
		p_lvn1 as (
					SELECT 			p_lvn.*,date(now() + interval '7 hours') today,doi_target,concat(slob_threshold, ' days') slob_threshold
					FROM 		 		p_lvn
					LEFT JOIN		supplychain.doi_target  On p_lvn.groupbrand=doi_target.groupbrand ),					
		p_lvn2 as (
					SELECT			sku,calendar_date,forecast_sale,groupbrand,today,slob_threshold,date(date(today) - slob_threshold::interval) as date_checking
					FROM				p_lvn1)  ,
		p_lvn3 as (
					SELECT 			*, Case When  calendar_date <= today AND calendar_date >= date_checking Then 1 else 0 End check1
					FROM 				p_lvn2) ,
		p_lvn4 as (			
					SELECT 			* 
					FROM 				p_lvn3 WHERE check1 =1 )  ,
		p_breakcombo AS ( -- Breakdown combo for demand file 
					SELECT 			COALESCE (t2.child_sku, p_lvn4.sku) as sku,p_lvn4.calendar_date,
											p_lvn4.forecast_sale * COALESCE(t2.quantity, 1) AS forecast_sale_previous
					FROM 				p_lvn4
					LEFT JOIN 	t2 ON p_lvn4.sku = t2.parent_sku),
		p_lvn5 AS (
					SELECT			sku,SUM(forecast_sale_previous) AS forecast_sale_previous								FROM 				p_breakcombo 
					GROUP BY 		1)   ,
					

-----------------------------------------------------------


		N1 as (	with t1 as(
					SELECT 			fo.channel_code,	date(orderdate::timestamp + interval '7 hour') as orderdate,
											date(order_updated_at::timestamp + interval '7 hour') order_updated_at,
											orderid,	fo.brand_id,	b.name as brand,	dc.groupbrand_name as groupbrand,
											original_status,	final_status,	fo.platform,	sku,	sku_name,	SUM(quantity) quantity
					FROM 				sales.fact_orders fo 
					LEFT JOIN 	opollo_onpoint.brands b on 	fo.brand_id = b.id::text  
					LEFT JOIN 	sales.dim_channels dc on fo.channel_code = dc.channel_code
					WHERE 			date(orderdate::timestamp + interval '7 hour') >= date(now() - interval '3 month')
					AND 				final_status in ( 'completed','processing','proccessing')
					GROUP BY 		1,2,3,4,5,6,7,8,9,10,11,12	),
					t2 as (  
					SELECT 			parent_sku::text, child_sku::text, quantity
					FROM 				opollo_onpoint.product_bundles 
					WHERE 			bundle_type = 'virtual')
	
					SELECT 			t1.*,	COALESCE (child_sku, sku) as child_sku,
											t1.quantity * COALESCE (mopb.quantity,1) as child_sku_quantity
					FROM 				t1 
					LEFT JOIN 	t2 mopb on t1.sku = mopb.parent_sku)   ,
		N2 as ( --check no sale dựa trên main_order2019
					SELECT 		child_sku as sku, max(date(orderdate)) as last_selling_date 
					FROM 			N1
					WHERE 		date(orderdate) BETWEEN ( (SELECT  start_date from date ) - interval '60 days') AND  (SELECT  start_date from date )
					group by 	1 ),

		N2_1 as (
					SELECT 		*, (SELECT  start_date from date )- date(last_selling_date) as Last_day_selling_to_current,
										CASE when ((SELECT  start_date from date ) - date(last_selling_date))::int <= 30 THEN 'Yes' else 'No' end as sale_in_30_days,
										CASE when ((SELECT  start_date from date ) - date(last_selling_date))::int <= 45 THEN 'Yes' else 'No' end as sale_in_45_days,
										CASE when ((SELECT  start_date from date ) - date(last_selling_date))::int <= 60 THEN 'Yes' else 'No' end as sale_in_60_days
					FROM 			N2) , 
	
		N2_2 as (			
					SELECT 		m_sku.sku,m_sku.brand,m_sku.created_at,N2_1.Last_day_selling_to_current, N2_1.sale_in_30_days,N2_1.sale_in_45_days,N2_1.sale_in_60_days 
					FROM 			m_sku 
					LEFT JOIN N2_1 on  m_sku.sku = N2_1.sku),
					
		N3 as (
					SELECT 		DISTINCT sku, created_at,Last_day_selling_to_current,
										CASE	when sale_in_30_days is null then 'No' else sale_in_30_days end as sale_in_30_days,
										CASE	when sale_in_45_days is null then 'No' else sale_in_45_days end as sale_in_45_days,
										CASE	when sale_in_60_days is null then 'No' else sale_in_60_days end as sale_in_60_days,
										CASE 	when ((SELECT  start_date from date )::date- date(created_at))::int <= 60 then 'New Sku' else 'Old Sku' end as Sku_type
					FROM 			N2_2	),
		N4 as (
					SELECT 		f10.*,p_lvn5.forecast_sale_previous rr_sale_previous,N3.sale_in_30_days,N3.sale_in_45_days,N3.sale_in_60_days,N3.Sku_type,N3.created_at from f10 
					LEFT JOIN N3 			on f10.sku_code = N3.sku
					LEFT JOIN	p_lvn5	On f10.sku_code	= p_lvn5.sku),
				
				
		N4_1 as ( 
					SELECT 		sku_code,updated_at,CASE WHEN inv_qty IS NULL THEN 0 ELSE inv_qty END AS inv_qty,keppel_qty , ghnlog12_vin_qty , moira_vin_qty ,ghn_qty,tnc_qty ,
										lzd_qty,shopee_qty,	tiki_qty,intransit_qty,rr_sale,rr_sale_previous,basecost,doi,forecast_y_n,
										CASE		when sale_in_30_days is null then 'No' else sale_in_30_days end as sale_in_30_days,
										CASE		when sale_in_45_days is null then 'No' else sale_in_45_days end as sale_in_45_days,
										CASE		when sale_in_60_days is null then 'No' else sale_in_60_days end as sale_in_60_days,	Sku_type,created_at 
					FROM 			N4),
		
		N5 as (
					SELECT 		*, CASE when sku_type = 'New Sku' then inv_qty*basecost else 0 end as New_launch_value
					FROM		 	N4_1),
		N5_1 as (
					SELECT		*,CASE 	when sale_in_30_days ='No' and New_launch_value = 0 and forecast_y_n ='NO FORECAST' then inv_qty*basecost 
														when sale_in_30_days ='Yes' and New_launch_value = 0 and forecast_y_n ='NO FORECAST' then inv_qty*basecost
														Else 0 end as no_forecast_value 
					FROM 			N5),
					
		N5_2 as (
					SELECT 		*,CASE 	--when sku_type = 'New Sku' then 0
																	when New_launch_value = 0 and inv_qty*basecost =0 and forecast_y_n ='NO FORECAST' then 0
														--when no_forecast_value  > 0 then 0	
																	Else doi  end as doi_final ,	basecost::numeric * inv_qty::numeric as inv_value
					FROM			 N5_1)		,
		N6 as (
					SELECT 		*, Case 			when doi_final <> 0 then round(inv_value::numeric / doi_final::numeric,2)  else 0 end as DOI_value_1D,
											 Case 			when doi_final / 15 >= 1  then 15  else doi_final end as DOI_0_15D
					FROM 			N5_2),
		N7 as (
					SELECT 		*,Case 			when doi_final / 15 >= 2  then 15  else doi_final - DOI_0_15D end as DOI_15_30D
					FROM 			N6),
		N8 as (
					SELECT 		*,Case 			when doi_final / 15 >= 3  then 15  else doi_final - DOI_0_15D - DOI_15_30D end as DOI_30_45D
					FROM 			N7),
		N9 as (
					SELECT 		*, doi_final - DOI_0_15D -DOI_15_30D - DOI_30_45D  as DOI_over_45D,
											case 			when doi_final - 40 <= 0 then 0 else doi_final - 40 end AS DOI_over_40D,
											case 			when doi_final - 58 <= 0 then 0 else doi_final - 58 end AS DOI_over_58D
					FROM 			N8),
		N10 as (
						SELECT 			*,	Case 			when no_forecast_value + new_launch_value >0 then 0
																			else doi_value_1d * doi_0_15d end as DOI_0_15value,
														Case 			when no_forecast_value + new_launch_value >0 then 0
																			else doi_value_1d * doi_15_30d end as DOI_15_30value,
														Case 			when no_forecast_value + new_launch_value >0 then 0
																			else doi_value_1d * doi_30_45d end as DOI_30_45value,
														Case 			when no_forecast_value + new_launch_value >0 then 0
																			else doi_value_1d * DOI_over_40D  end as DOI_over40_value,
														Case 			when no_forecast_value + new_launch_value >0 then 0
																			else doi_value_1d * doi_over_45d end as DOI_over45_value,
														Case 			when no_forecast_value + new_launch_value >0 then 0
																			else doi_value_1d * DOI_over_58D  end as DOI_over58_value
						FROM 			N9) ,
		N11 as (
						SELECT 			N10.*,Case When sm.groupbrand in (Select groupbrand From supplychain.doi_target) then sm.groupbrand Else 'OTHERS' END groupbrand,
															Case When sm.brand is not null then sm.brand Else 'OTHERS' END brand ,sm.name
						FROM  			N10
						LEFT JOIN		supplychain.supplychain_master_sku  sm ON N10.sku_code=sm.sku ),
		N12 AS (
						SELECT			N11.*,dt.doi_target, dt.slob_threshold,dt.is_active,dt.model
						FROM				N11
						LEFT JOIN		supplychain.doi_target dt On N11.groupbrand = dt.groupbrand ),
		N13 AS (
						SELECT			N12.* ,	CASE 	when doi_final-slob_threshold::numeric > 0 Then  doi_final-slob_threshold::numeric Else 0 End as slob_qty , 
												CASE 	when doi_final-slob_threshold::numeric > 0 Then  (doi_final-slob_threshold::numeric)*doi_value_1d  Else 0 End as slob1
						FROM				N12 ),
		N14 AS (
						SELECT			*,Case when sku_type = 'New Sku' Then 0 Else slob1 + no_forecast_value::numeric End AS slob
						FROM				N13),
	-----------------------
	----Historical sale
		H1 as ( 
						SELECT 			n1.orderdate,n1.child_sku,Case When sm.groupbrand in (Select groupbrand From supplychain.doi_target) then sm.groupbrand Else 'OTHERS' END groupbrand,
												Case When sm.brand is not null then sm.brand Else 'OTHERS' END brand , 
												Case When dt.slob_threshold is not null Then dt.slob_threshold Else '30' End  slob_threshold, sum(n1.child_sku_quantity) child_sku_quantity
						FROM 				N1  	
						LEFT JOIN 	supplychain.supplychain_master_sku  sm		ON 		N1.child_sku = sm.sku
						LEFT JOIN		supplychain.doi_target dt 								On 		sm.groupbrand = dt.groupbrand
						WHERE 			date(orderdate) BETWEEN ( (SELECT  start_date from date ) - interval '60 days') AND  (SELECT  start_date from date )	
						GROUP BY 		1,2,3,4,5)   ,
				
		H2 as (
						SELECT 			H1.*,date(now() + interval '7 hours') today,concat(slob_threshold, ' days') slob_threshold1
						FROM 		 		H1 )   ,					
		H3 as (
						SELECT			child_sku,orderdate,child_sku_quantity,groupbrand,today,slob_threshold1,date(date(today) - slob_threshold1::interval) as date_checking
						FROM				H2)  ,				
		H4 as (
						SELECT 			*, Case When  orderdate <= today AND orderdate >= date_checking Then 1 else 0 End check1
						FROM 				H3),
		H5 as (			
						SELECT 			* 
						FROM 				H4 WHERE check1 =1 ),
						
		H6 AS (
						SELECT			child_sku sku,SUM(child_sku_quantity) AS historical_sale_quantity							
						FROM 				H5 
						GROUP BY 		1) 
						
		SELECT 				N14.groupbrand, sum(N14.slob) AS slob
		FROM 					N14
		LEFT JOIN 		H6 			ON N14.sku_code =H6.sku
		GROUP BY N14.groupbrand
        """
        df_slob = pd.read_sql_query(sql_slob, conn)
        return df_slob