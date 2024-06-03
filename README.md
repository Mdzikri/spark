ini adalah before sebelum program jalan 

![alt text](<Screenshot from 2024-06-03 18-41-16.png>)


## dan ini adalah setelah di jalankan bisa munculkan console dan postgresql bisa write


![alt text](<Screenshot from 2024-06-03 18-47-30.png>)




# dan ini adalah loga yang kudapat dengan mengambil data 10 teerakhir dan dia masuk ke DB posgrest

## warehouse=# SELECT * FROM public.real_time_data2 ORDER BY event_time DESC LIMIT 10;



               order_id               | customer_id | furniture | color  | price  |     ts    
 |       event_time        
--------------------------------------+-------------+-----------+--------+--------+-----------
-+-------------------------
 2c5b12f0-1503-48cb-be8b-40fc6b6bc0cc |          34 | Desk      | white  |   2046 | 1717415119
 | 1970-01-20 21:03:35.119
 d6a66104-9a0e-43cf-a825-226a4578e9c0 |          59 | Table     | teal   |  91542 | 1717415097
 | 1970-01-20 21:03:35.097
 d849949a-6d89-4471-bf75-649ac62f3290 |          12 | Table     | green  |  71111 | 1717415092
 | 1970-01-20 21:03:35.092
 dcf63ec1-2e8d-4d6f-a783-e8e79c40faf8 |           4 | Table     | teal   | 128156 | 1717415037
 | 1970-01-20 21:03:35.037
 7a62bdf3-7a18-40de-a169-f2a91cf8865d |          73 | Desk      | gray   | 134219 | 1717415007
 | 1970-01-20 21:03:35.007
 102b2d4d-fe36-46e5-9268-1fd835e44162 |          71 | Table     | black  |  94555 | 1717415004
 | 1970-01-20 21:03:35.004
 8d1a7c55-4d1d-4acf-995d-92fe96a912b1 |          80 | Table     | yellow |   8050 | 1717415000
 | 1970-01-20 21:03:35
 77823c24-9132-4577-9edf-8a5daa0bccf0 |          45 | Sofa      | lime   |  65376 | 1717414998
 | 1970-01-20 21:03:34.998
 c0bee690-1bea-4309-877b-8b818b41fe4a |          91 | Desk      | purple |  96078 | 1717414996
 | 1970-01-20 21:03:34.996
 50499217-f90b-4c86-a253-bccad7da1cf3 |           7 | Desk      | silver |  44232 | 1717414992
 | 1970-01-20 21:03:34.992
(10 rows)






## warehouse=# SELECT * FROM public.real_time_data2 ORDER BY event_time DESC LIMIT 10;




               order_id               | customer_id | furniture | color  | price  |     ts     |  
     event_time        
--------------------------------------+-------------+-----------+--------+--------+------------+--
-----------------------
 2c5b12f0-1503-48cb-be8b-40fc6b6bc0cc |          34 | Desk      | white  |   2046 | 1717415119 | 1
970-01-20 21:03:35.119
 d6a66104-9a0e-43cf-a825-226a4578e9c0 |          59 | Table     | teal   |  91542 | 1717415097 | 1
970-01-20 21:03:35.097
 d849949a-6d89-4471-bf75-649ac62f3290 |          12 | Table     | green  |  71111 | 1717415092 | 1
970-01-20 21:03:35.092
 31aa83b4-b1e6-4583-aca6-ec700840307d |          69 | Sofa      | blue   |   4667 | 1717415062 | 1
970-01-20 21:03:35.062
 dcf63ec1-2e8d-4d6f-a783-e8e79c40faf8 |           4 | Table     | teal   | 128156 | 1717415037 | 1
970-01-20 21:03:35.037
 7a62bdf3-7a18-40de-a169-f2a91cf8865d |          73 | Desk      | gray   | 134219 | 1717415007 | 1
970-01-20 21:03:35.007
 102b2d4d-fe36-46e5-9268-1fd835e44162 |          71 | Table     | black  |  94555 | 1717415004 | 1
970-01-20 21:03:35.004
 8d1a7c55-4d1d-4acf-995d-92fe96a912b1 |          80 | Table     | yellow |   8050 | 1717415000 | 1
970-01-20 21:03:35
 77823c24-9132-4577-9edf-8a5daa0bccf0 |          45 | Sofa      | lime   |  65376 | 1717414998 | 1
970-01-20 21:03:34.998
 c0bee690-1bea-4309-877b-8b818b41fe4a |          91 | Desk      | purple |  96078 | 1717414996 | 1
970-01-20 21:03:34.996
(10 rows)

warehouse=# 