{{ config(materialized='table') }}

WITH final_liveability AS (
WITH CHOICES AS (
		SELECT SPLIT(ui2.What_are_you_interested_in_the_area,',') as OPTION
					,RANK() over (
						ORDER BY ui2.Create_Date DESC
								) AS RANK_NO2
								FROM `streamdata.user_input` ui2
)

SELECT * 
FROM
	(

	SELECT
		 ccare.name as name   
		,ccare.Categories as category    
		,ccare.Address as address    
		,ccare.City as city     
		,ccare.Postcode as postcode    
		,ccare.Latitude as Latitude
		,ccare.Longitude as longitude
	FROM transform_batchdata.childcarecentre ccare
	UNION ALL
	SELECT
		 hosp.name as name   
		,hosp.Categories as category    
		,hosp.Address as address    
		,hosp.City as city     
		,hosp.Postcode as postcode    
		,hosp.Latitude as Latitude
		,hosp.Longitude as longitude
	FROM transform_batchdata.hospitals hosp
	UNION ALL 
	SELECT
		 rel.name as name   
		,rel.Category as category    
		,rel.Address as address    
		,rel.Suburb as city     
		,rel.Postcode as postcode    
		,rel.Latitude as Latitude
		,rel.Longitude as longitude
	FROM transform_batchdata.religiousorganizations rel 
	UNION ALL 
	SELECT
		 res.name as name   
		,res.Categories as category    
		,res.Address as address    
		,res.City as city     
		,res.Postcode as postcode    
		,res.Latitude as Latitude
		,res.Longitude as longitude
	FROM transform_batchdata.restaurants res 
	UNION ALL 
	SELECT
		 sch.name as name   
		,sch.Categories as category    
		,sch.Address as address    
		,sch.City as city     
		,sch.Postcode as postcode    
		,sch.Latitude as Latitude
		,sch.Longitude as longitude
	FROM transform_batchdata.schools sch
	UNION ALL  
	SELECT
		 shop.name as name
		,shop.Categories as category
		,shop.Address as adress
		,shop.City as city
		,shop.Postcode as postcode
		,shop.Latitude as latitude
		,shop.Longitude as longitude
	FROM transform_batchdata.shoppingcentre shop
	UNION ALL 
	SELECT
		 sport.name as name
		,sport.Categories as category
		,sport.Address as adress
		,sport.City as city
		,sport.Postcode as postcode
		,sport.Latitude as latitude
		,sport.Longitude as longitude
	FROM transform_batchdata.sportsclubs sport

)
WHERE postcode = (
    WITH LOCATION AS (
		SELECT  ui1.New_Postcode as Postcode			
				   ,RANK() over (
						 ORDER BY ui1.Create_Date DESC
      					) AS RANK_NO1
								FROM `streamdata.user_input` ui1
		)
						
		SELECT LOCATION.Postcode
		FROM LOCATION
		WHERE LOCATION.RANK_NO1 = 5
	)

AND category IN UNNEST((SELECT CHOICES.OPTION 
												FROM CHOICES
												WHERE CHOICES.RANK_NO2 = 5
												) )

)
select * from final_liveability

