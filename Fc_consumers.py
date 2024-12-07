import logging
import json
import pandas as pd
import numpy as np
from confluent_kafka import Consumer, KafkaError
from datetime import datetime
import mysql.connector
from contextlib import closing
import Fc_transformations as tc
from contextlib import closing

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class KafkaConsumer:
    def __init__(self, consumer_settings, consumer_topic, dest_settings,
                 Consignment_Final_Schema, Booking_Details_Final_Schema,
                 Payment_Details_Final_Schema, Adress_Details_Final_Schema,
                 Consignment_History_Final_Schema, source_connection, Address_query, Booking_query, Payments_query, History_query):
        self.consumer_settings = consumer_settings
        self.now = datetime.now()
        self.curent_dt = self.now.strftime("%Y-%m-%d %H:%M:%S")
        self.dest_settings = dest_settings
        self.Consignment_Final_Schema = Consignment_Final_Schema
        self.Booking_Details_Final_Schema = Booking_Details_Final_Schema
        self.Payment_Details_Final_Schema = Payment_Details_Final_Schema
        self.Adress_Details_Final_Schema = Adress_Details_Final_Schema
        self.Consignment_History_Final_Schema = Consignment_History_Final_Schema
        self.consumer_topic = consumer_topic
        self.foc_consigments = 'fact_consignment'
        self.consumer_instance = None

        self.transform_consignments = tc.transform_consignments
        self.transform_address_dtails = tc.transform_address_dtails
        self.transform_booking_details = tc.transform_booking_details
        self.transform_payment_details = tc.transform_payment_details
        self.Transform_Consignment_History = tc.Transform_Consignment_History

    def create_consumer(self):
        self.consumer_instance = Consumer(**self.consumer_settings)

    def subscribe(self):
        if self.consumer_instance is None:
            raise Exception("Consumer instance is not created. Call create_consumer() first.")
        self.consumer_instance.subscribe(self.consumer_topic)

    def consume_messages(self):
        if self.consumer_instance is None:
            raise Exception("Consumer instance is not created. Call create_consumer() first.")
        #consumer_id = self.consumer_instance.config['client.id']
        #logging.info(f"Starting consumer with ID: {consumer_id}")
        try:
            while True:
                msg = self.consumer_instance.poll(1.0)  # Adjusted polling time
                if msg is None:
                    continue
                if msg.error():
                    self.handle_error(msg.error())
                    continue  # Continue consuming after handling the error
                message_value = msg.value().decode('utf-8')
                tracking_number = self.extract_tracking_number(message_value)
                table_name = self.extract_table_name(message_value)

                if tracking_number and table_name:
                    transformed_data = self.transform_data(message_value, table_name)
                if transformed_data is not None:
                    self.process_message(table_name, transformed_data, message_value)

        except KeyboardInterrupt:
            logging.info("Consumer interrupted.")
        except Exception as e:
            logging.error(f"An error occurred during message consumption: {e}")
        finally:
            self.close_consumer()

    def handle_error(self, error):
        if error.code() == KafkaError._PARTITION_EOF:
            return  # End of partition, nothing to do
        logging.error(f"Consumer error: {error}")

    def process_message(self, table_name, transformed_data, message_value):
        if table_name == 'CONSIGNMENT':
            queries = self.generate_upsert_query(self.foc_consigments, transformed_data, primary_key='id')
            self.execute_upsert_queries(queries, self.dest_settings)
        else:
            self.execute_update_query(self.foc_consigments, table_name, transformed_data, message_value)

    def close_consumer(self):
        if self.consumer_instance:
            self.consumer_instance.close()
            logging.info("Consumer closed.")

    def extract_tracking_number(self, message_value):
        try:
            data = json.loads(message_value)
            after_data = data.get('after')
            if after_data:
                return after_data.get("Tracking_Number")
            logging.warning("Missing 'after' data in the message.")
            return None
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(f"Failed to extract Tracking_Number: {e}")
        return None 
       

    def extract_table_name(self, message_value):
        try:
            data = json.loads(message_value)
            source_data = data.get('source')
            if source_data:
                return source_data.get("table")
            logging.warning("Missing 'source' data in the message.")
            return None
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(f"Failed to extract table: {e}")
            return None

    def transform_data(self, message_value, topic):
        try:
            data = json.loads(message_value)
            opType = data['op']
            if opType not in ['c', 'u']:
                logging.warning(f"Unsupported operation type: {opType} for topic: {topic}")
                return None

            df = pd.DataFrame([data['after']])
            transformations = {
                'CONSIGNMENT': (self.transform_consignments, self.Consignment_Final_Schema),
                'BOOKING_DETAILS': (self.transform_booking_details, self.Booking_Details_Final_Schema),
                'PAYMENT_DETAILS': (self.transform_payment_details, self.Payment_Details_Final_Schema),
                'ADDRESS_DETAILS': (self.transform_address_dtails, self.Adress_Details_Final_Schema),
                'CONSIGNMENT_HISTORY': (self.Transform_Consignment_History, self.Consignment_History_Final_Schema)
            }

            if topic in transformations:
                transform_func, schema = transformations[topic]
                latest_update_df = transform_func(self,df)
                latest_update_df = latest_update_df[schema]
                logging.info(f"{topic} transformed successfully with operation type: {opType}.")
                return latest_update_df

            logging.warning(f"Unsupported topic: {topic}")
            return None

        except Exception as e:
            logging.error(f"Transformation failed for topic: {topic} with error: {e}")
        return None

    def generate_upsert_query(self, table_name, df, primary_key):
        queries = []
        for _, row in df.iterrows():
            columns = ', '.join(row.index)
            values = ', '.join(f"'{str(value).replace('\'', '\'\'')}'" for value in row.values)
            update_columns = ', '.join(f"{col} = VALUES({col})" for col in row.index if col != primary_key)
            query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({values})
            ON DUPLICATE KEY UPDATE {update_columns};
            """
            queries.append(query.strip())
        return queries

    def execute_upsert_queries(self, queries, db_config):
        try:
            with closing(mysql.connector.connect(**db_config)) as connection:
                with closing(connection.cursor()) as cursor:
                    for query in queries:
                        cursor.execute(query)

                    connection.commit()
                    logging.info("All upsert queries executed successfully.")

        except mysql.connector.Error as err:
            logging.error(f"SQL Error: {err}")

    def generate_insert_queries(self, foc_consigments_tbl, table_name, df, message_value):
        queries = []
        columns = df.columns.tolist()
        for index, row in df.iterrows():
            values = ', '.join(f"'{str(value).replace('\'', '\'\'')}'" if pd.notnull(value) else 'NULL' for value in row)
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values});"
            queries.append(query)
        return queries       

    def execute_update_query(self, foc_consigments_tbl, table_name, df, message_value):
        smg = json.loads(message_value)
        opt = smg['op']
        now = datetime.now()
        ModifiedTs = now.strftime("%Y-%m-%d %H:%M:%S")
        df['ModifiedTs'] = ModifiedTs
        ModifiedBy = 'DeStreamer'
        df['ModifiedBy'] = ModifiedBy
        try:
            with closing(mysql.connector.connect(**self.dest_settings)) as connection:
                with closing(connection.cursor()) as cursor:
                    rows_updated = 0  # Track the number of rows updated
                    if table_name in ['PAYMENT_DETAILS', 'ADDRESS_DETAILS', 'BOOKING_DETAILS']:
                        for _, row in df.iterrows():
                            set_clause = ', '.join(f"{col} = %s" for col in row.index if col != 'foc_cnno')
                            update_query = f"""UPDATE {foc_consigments_tbl} SET {set_clause} WHERE foc_cnno = %s;"""
                            values = (*row.drop('foc_cnno').values, row['foc_cnno'])
                            cursor.execute(update_query, values)
                            rows_updated += cursor.rowcount  # Increment the count of updated rows

                    elif table_name == 'CONSIGNMENT_HISTORY' and opt == 'c':
                        for _, row in df.iterrows():
                            foc_delvstatus_code = row['foc_non_dlv_attempts']
                            foc_ch_dlv_attempts = row['foc_rto_non_dlv_attempts']
                            foc_cnno = row['foc_cnno']
                            print(foc_delvstatus_code, ' :', foc_cnno)
                            sql = f"""UPDATE {foc_consigments_tbl} SET foc_non_dlv_attempts = foc_non_dlv_attempts + %s, foc_rto_non_dlv_attempts = foc_rto_non_dlv_attempts + %s, ModifiedTs = '{ModifiedTs}', ModifiedBy = '{ModifiedBy}'  WHERE foc_cnno = %s;"""
                            values = (foc_delvstatus_code, foc_ch_dlv_attempts, foc_cnno)
                            cursor.execute(sql, values)
                            rows_updated += cursor.rowcount  # Increment the count of updated rows
                    else:
                        pass        

                    if rows_updated == 0:
                        self.log_zero_update(table_name, message_value)

                    connection.commit() 
                    logging.info("All update queries executed successfully.")

        except mysql.connector.Error as err:
            logging.error(f"SQL Error: {err}")

    def log_zero_update(self, table_name, message_value):
        try:
            with closing(mysql.connector.connect(**self.dest_settings)) as connection:
                with closing(connection.cursor()) as cursor:
                    log_query = "INSERT INTO failure_log (table_name, message) VALUES (%s, %s);"
                    cursor.execute(log_query, (table_name, message_value))
                    connection.commit()
                    logging.info(f"Logged zero update for message in {table_name}.")
        except mysql.connector.Error as err:
            logging.error(f"Failed to log zero update: {err}")

    def run(self):
        try:
            self.create_consumer()
            self.subscribe()
            self.consume_messages()
        except Exception as e:
            logging.error(f"An error occurred in run: {e}")
        finally:
            self.close_consumer()
