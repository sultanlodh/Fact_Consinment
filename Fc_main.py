from Fc_consumers import KafkaConsumer  # Ensure this matches the actual class name
import Fc_configs as conf

try:
    # Extract consumer and producer settings
    consumer_settings = conf.config['consumer_settings']
    dest_connection = conf.config['dest_Connections']
    source_connection = conf.config['source_connection']
    Address_query = conf.config['Address_query']
    Booking_query = conf.config['Booking_query']
    Payments_query = conf.config['Payments_query']
    History_query = conf.config['History_query']
    Consignment_Final_Schema = conf.config['Consignment_Final_Schema']
    Booking_Details_Final_Schema = conf.config['Booking_Details_Final_Schema']
    Payment_Details_Final_Schema = conf.config['Payment_Details_Final_Schema']
    Adress_Details_Final_Schema = conf.config['Adress_Details_Final_Schema']
    Consignment_History_Final_Schema = conf.config['Consignment_History_Final_Schema']
    producer_settings = conf.config['producer_settings']  # Assuming this is also defined in configs
    topic = conf.config['topics']  # Assuming you want the first topic

    # Create an instance of KafkaRouter
    kafkaConsumer = KafkaConsumer(consumer_settings, topic, dest_connection, Consignment_Final_Schema, Booking_Details_Final_Schema, Payment_Details_Final_Schema, Adress_Details_Final_Schema, Consignment_History_Final_Schema, source_connection, Address_query, Booking_query, Payments_query, History_query)

    try:
        # Start consuming messages and producing them based on Tracking_Number
        kafkaConsumer.run()
    except Exception as e:
        print(f"Error while processing messages: {e}")

except KeyError as e:
    print(f"Missing configuration key: {e}")
except Exception as e:
    print(f"Error initializing Consumer: {e}")
