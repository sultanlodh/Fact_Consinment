
import pandas as pd
import numpy as np   
from datetime import datetime
def transform_consignments(self, df):
        return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
            foc_cnno=lambda df: df['foc_cnno'].fillna('Unknown'),
            timestamp = lambda df:  df['BookingTs'].apply(convert_to_timestamp),
            foc_booking_date = lambda df: df['timestamp'].apply(lambda x: x.date() if pd.notna(x) else None),
            foc_current_status_code=lambda df: df['Current_Status_Code'],
            foc_bkdate=lambda df: pd.to_datetime(df['BookingTs'].astype(float) / 1000, unit='s', errors='coerce'),
            foc_bkoffice_id=lambda df: df['Origin_Branch_Code'],
            foc_bkdate_id=lambda df:  df['BookingTs'].apply(convert_to_timestamp),
            foc_bkserv_code=lambda df: df['Service_Code'],
            foc_bkservice_code=lambda df: df['Service_Name'], 
            foc_bkmode_code=lambda df: df['Mode'],
            foc_bkoffice_code=lambda df: df['Origin_Branch_Name'],
            timestamp_edd=lambda df:  df['Ops_EDD'].apply(convert_to_timestamp),
            foc_edd = lambda df: df['timestamp_edd'].apply(lambda x: x.date() if pd.notna(x) else None),
            foc_bkgpin_code=lambda df: df['Destination_Pincode'],
            foc_reddsla_date = lambda df: pd.to_datetime(df['Ops_REDD'].astype(float) / 1000, unit='s', errors='coerce').apply(lambda x: None if pd.isna(x) else x),
            foc_customer_promise_edd = lambda df: pd.to_datetime(pd.to_numeric(df['Cust_Prom_EDD'], errors='coerce').div(1000), unit='s', errors='coerce').dt.date,
            foc_cust_ref_no=lambda df: df['Reference_No'],
            foc_current_location_code=lambda df: df['Current_Location_Code'],
            foc_edd_booked_status = np.where(df['Ops_EDD_Parameters'].str[8] == '0', 'Booked Before Cut-Off', 'Booked After Cut-Off'),
            foc_bkpin_code=lambda df: df['Origin_Pincode'],
            foc_current_datetime=lambda df: pd.to_datetime(df['Current_StatusTs'].astype(float) / 1000, unit='s', errors='coerce')
        )

def transform_booking_details(self, df): 
        return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
            foc_bkcusttype_code=lambda df: df['Booked_By_Cust_Type'],
            foc_bkcust_code=lambda df: df['Booked_By_Cust_Code'],
            foc_bookchannel=lambda df: df['Source_Application'],
            foc_bkdocument_code=lambda df: df['Package_Type'],
            foc_Booked_Wt=lambda df: df['Booking_Weight'],
            foc_Vol_Wt=lambda df: df['Volumetric_Weight'],
            foc_Billed_Wt=lambda df: df['Chargeable_Weight'],
            foc_pcs=lambda df: np.where(df['Number_of_Pieces'].notnull(), 1, 0),
            foc_nop=lambda df: df['Number_of_Pieces'],
            foc_booking_app=lambda df: df['Validation'],
            foc_cpdp_code=lambda df: np.where(df['Booked_By_Cust_Code'] == 'CPDP', 'CPDP', None)
        )

def transform_payment_details(self, df):
        return df.rename(columns={'Tracking_Number': 'foc_cnno', 'Vas_Prod_Code': 'foc_vas_prod_code'})
    

def transform_address_dtails(self, df):
        return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
            foc_receiver_name = np.where(df['Address_Type'] == 'Receiver', df['First_name'].fillna('') + ' ' + df['Middle_name'].fillna('') + ' ' + df['Last_name'].fillna(''), None),
            foc_receiver_company_name = np.where((df['Address_Type'] == 'Receiver') & (df['First_name'].str.contains('LTD', case=False, na=False)),df['First_name'],None),
            foc_receiver_phno = np.where(df['Address_Type'] == 'Receiver', df['Phone'].fillna('') + '/' + df['Mobile'].fillna(''), None),
            foc_receiver_address = np.where(df['Address_Type'] == 'Receiver', df['Street_1'].fillna('') + ',' + df['Street_2'].fillna('') + ',' + df['Street_3'].fillna(''), None),
            foc_receiver_city = np.where(df['Address_Type'] == 'Receiver', df['City'].fillna(''), None),
            foc_receiver_state = np.where(df['Address_Type'] == 'Receiver', df['State'].fillna(''), None),
            foc_receiver_pincode = np.where(df['Address_Type'] == 'Receiver', df['Pincode'].fillna(''), None),
            foc_sender_name = np.where(df['Address_Type'] == 'Sender', df['First_name'].fillna('') + ' ' + df['Middle_name'].fillna('') + ' ' + df['Last_name'].fillna(''), None),
            foc_sender_company_name = np.where((df['Address_Type'] == 'Sender') & (df['First_name'].str.contains('LTD', case=False, na=False)),df['First_name'],None),
            foc_sender_phno = np.where(df['Address_Type'] == 'Sender', df['Phone'].fillna('') + '/' + df['Mobile'].fillna(''), None),
            foc_sender_address = np.where(df['Address_Type'] == 'Sender', df['Street_1'].fillna('') + ',' + df['Street_2'].fillna('') + ',' + df['Street_3'].fillna(''), None),
            foc_sender_city = np.where(df['Address_Type'] == 'Sender', df['City'].fillna(''), None),
            foc_sender_state = np.where(df['Address_Type'] == 'Sender', df['State'].fillna(''), None),
            foc_sender_pincode = np.where(df['Address_Type'] == 'Sender', df['Pincode'].fillna(''), None)
        )
    
def Transform_Consignment_History(self, df):
        return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
        foc_non_dlv_attempts = lambda df: np.where(df['Status_Code'] == 'NONDLV', 1, 0).astype(int),
        foc_rto_non_dlv_attempts = lambda df: np.where(df['Status_Code'] == 'RTONONDLV', 1, 0).astype(int),
        )


def convert_to_timestamp(value):
    if isinstance(value, (int, float)):  # Check if it's a numeric (epoch timestamp)
        return pd.to_datetime(value, unit='ms', errors='coerce')
    try:
        # If it's already a string or datetime-like object, convert directly
        return pd.to_datetime(value, errors='coerce')
    except (ValueError, TypeError):
        return pd.NaT  # Return NaT if it's neither a valid timestamp nor a valid datetime string
