config = {
    "consumer_settings": {
        "bootstrap.servers": 'kafka:29092',
        "auto.offset.reset": 'latest',
        "group.id": 'FactConsinmentGroup-1',
        "enable.auto.commit": False
    },

    
    "dest_Connections": {
        'host': '10.10.25.46',
        'user': 'foc_user',
        'password': 'Foc_user@#4321',
        'database': 'foc_db',
        'port': 3306
    },

    "source_connection": {
        'host': '10.10.25.130',
        'user': 'log_stream',
        'password': '10g236!shdhjaF',
        'database': 'tracking_live',
        'port': 3307
    },

    "producer_settings": {
        "bootstrap.servers": 'kafka:29092'
    },

    "topics": [
        'tracking_live.tracking_live.BOOKING_DETAILS',
        'tracking_live.tracking_live.CONSIGNMENT',
        'tracking_live.tracking_live.PAYMENT_DETAILS',
        'tracking_live.tracking_live.ADDRESS_DETAILS',
        'tracking_live.tracking_live.CONSIGNMENT_HISTORY'
    ],

    "Consignment_Final_Schema": [
        'foc_cnno', 'foc_booking_date', 'foc_current_status_code', 'foc_bkdate', 'foc_bkoffice_id', 'foc_bkdate_id',
        'foc_bkservice_code', 'foc_bkmode_code', 'foc_bkoffice_code', 'foc_edd', 'foc_bkgpin_code', 'foc_reddsla_date',
        'foc_customer_promise_edd', 'foc_cust_ref_no', 'foc_edd_booked_status', 'foc_bkpin_code', 'foc_current_datetime', 'foc_bkserv_code'
    ],

    "Booking_Details_Final_Schema": [
        'foc_cnno', 'foc_cpdp_code',  'foc_bkcusttype_code', 'foc_bkcust_code', 'foc_bookchannel',
        'foc_bkdocument_code', 'foc_Booked_Wt', 'foc_Vol_Wt', 'foc_Billed_Wt', 'foc_pcs', 'foc_nop', 'foc_booking_app'
    ],

    "Adress_Details_Final_Schema": [
        'foc_cnno', 'foc_receiver_name', 'foc_receiver_phno', 'foc_receiver_address', 'foc_receiver_city', 
        'foc_receiver_state', 'foc_receiver_pincode', 'foc_sender_name', 'foc_sender_phno', 'foc_sender_address', 'foc_sender_city', 
        'foc_sender_state', 'foc_sender_pincode', 'foc_receiver_company_name', 'foc_sender_company_name'
    ],

    "Payment_Details_Final_Schema": [
        'foc_cnno',  'foc_vas_prod_code'
    ],

    "Consignment_History_Final_Schema": [
        'foc_cnno',  'foc_non_dlv_attempts', 'foc_rto_non_dlv_attempts'
    ],

    "Address_query": f"""
        SELECT 
            -- Receiver Details
            r.Tracking_Number AS foc_cnno,
            CONCAT(IFNULL(r.First_name, ''), ' ', IFNULL(r.Middle_name, ''), ' ', IFNULL(r.Last_name, '')) AS foc_receiver_name,
            CASE 
                WHEN r.First_name LIKE '%LTD%' THEN r.First_name
                ELSE NULL
            END AS foc_receiver_company_name,
            CONCAT(IFNULL(r.Phone, ''), '/', IFNULL(r.Mobile, '')) AS foc_receiver_phno,
            CONCAT(IFNULL(r.Street_1, ''), ',', IFNULL(r.Street_2, ''), ',', IFNULL(r.Street_3, '')) AS foc_receiver_address,
            IFNULL(r.City, '') AS foc_receiver_city,
            IFNULL(r.State, '') AS foc_receiver_state,
            IFNULL(r.Pincode, '') AS foc_receiver_pincode,

            -- Sender Details
            CONCAT(IFNULL(s.First_name, ''), ' ', IFNULL(s.Middle_name, ''), ' ', IFNULL(s.Last_name, '')) AS foc_sender_name,
            CASE 
                WHEN s.First_name LIKE '%LTD%' THEN s.First_name
                ELSE NULL
            END AS foc_sender_company_name,
            CONCAT(IFNULL(s.Phone, ''), '/', IFNULL(s.Mobile, '')) AS foc_sender_phno,
            CONCAT(IFNULL(s.Street_1, ''), ',', IFNULL(s.Street_2, ''), ',', IFNULL(s.Street_3, '')) AS foc_sender_address,
            IFNULL(s.City, '') AS foc_sender_city,
            IFNULL(s.State, '') AS foc_sender_state,
            IFNULL(s.Pincode, '') AS foc_sender_pincode,

            -- Record updated date
            NOW() AS record_updated_date
        FROM 
            -- Receiver subquery
            (SELECT * FROM ADDRESS_DETAILS WHERE Address_Type = 'Receiver') r
        INNER JOIN 
            -- Sender subquery
            (SELECT * FROM ADDRESS_DETAILS WHERE Address_Type = 'Sender') s
        ON r.Tracking_Number = s.Tracking_Number
        WHERE r.Tracking_Number = '{0}';
    """,

    "Booking_query": """SELECT
                        Tracking_Number AS foc_cnno,
                        Booked_By_Cust_Type AS foc_bkcusttype_code,
                        Booked_By_Cust_Code AS foc_bkcust_code,
                        Source_Application AS foc_bookchannel,
                        Package_Type AS foc_bkdocument_code,
                        Booking_Weight AS foc_Booked_Wt,
                        Volumetric_Weight AS foc_Vol_Wt,
                        Chargeable_Weight AS foc_Billed_Wt,
                        CASE WHEN Number_of_Pieces IS NOT NULL THEN 1 ELSE 0 END AS foc_pcs,
                        Number_of_Pieces AS foc_nop,
                        Validation AS foc_booking_app,
                        NOW() AS record_updated_date,
                        CASE WHEN Booked_By_Cust_Code = 'CPDP' THEN 'CPDP' ELSE NULL END AS foc_cpdp_code
FROM BOOKING_DETAILS where Tracking_Number = '{0}';""",

  "Payments_query": """SELECT
                 Tracking_Number AS foc_cnno,
                 Vas_Prod_Code AS foc_vas_prod_code,
                 NOW() AS record_updated_date
FROM PAYMENT_DETAILS where Tracking_Number = '{0}';""",

"History_query": """SELECT 
       Tracking_Number AS foc_cnno,
       COUNT(CASE WHEN Status_Code = 'NONDLV' THEN 1 END) AS foc_non_dlv_attempts,
       COUNT(CASE WHEN Status_Code = 'RTONONDLV' THEN 1 END) AS foc_rto_non_dlv_attempts,
       NOW() AS record_updated_date
FROM 
    CONSIGNMENT_HISTORY where Tracking_Number = '{0}';"""



}
