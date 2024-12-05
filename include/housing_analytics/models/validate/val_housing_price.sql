
{{ config(materialized='table') }}

select distinct a.suburb,a.address,
    COALESCE(a.rooms, b.rooms) as rooms,
    COALESCE(a.type, b.type) as type,
    COALESCE(a.price, b.price) as price,
    COALESCE(a.method, b.method) as method,
    COALESCE(a.sellerg, b.sellerg) as sellerg,
    a.date,
    COALESCE(a.distance, b.distance) as distance,
    a.postcode,
    COALESCE(a.bedroom2, b.bedroom2) as bedroom2,
    COALESCE(a.bathroom, b.bathroom) as bathroom,
    COALESCE(a.car, b.car) as car,
    COALESCE(a.landsize, b.landsize) as landsize,
    COALESCE(a.buildingarea, b.buildingarea) as buildingarea,
    COALESCE(a.yearbuilt, b.yearbuilt) as yearbuilt,
    COALESCE(a.councilarea, b.councilarea) as councilarea,
    COALESCE(a.lattitude, b.lattitude) as lattitude,
    COALESCE(a.longtitude, b.longtitude) as longitude,
    COALESCE(a.regionname, b.regionname) as regionname,
    COALESCE(a.propertycount, b.propertycount) as propertycount,
    a.source_file as source_1,
    b.source_file as source_2,
    a.insert_datetime,
    a.batch_id
from raw.raw_housing_price a 

left join  raw.raw_housing_price b
on a.source_file = 'HOUSE_PRICE'
and b.source_file ='HOUSING_FULL'
and a.address = b.address
and a.suburb = b.suburb
and a.postcode = b.postcode
and a.type = b.type
and a.sellerg = b.sellerg
and a.date = b.date
