import paho.mqtt.client as mqtt
import logging
import logging.config
import yaml
import os
import ssl
from configparser import ConfigParser
from pydal import DAL, Field
import datetime
from queue import Queue
from threading import Thread


def setup_logging(
        default_path='logging.yaml',
        default_level=logging.DEBUG,
        env_key='LOG_CFG'
):
    """Setup logging configuration

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


class Mqtt2Db:

    dbs = []
    tables = []
    subscriptions = []
    brokers = []

    def __init__(self):

        setup_logging()

        self.logger = logging.getLogger('mqtt2db')

        # Load YAML configuration
        with open('config.yaml') as fp:
            self.config = yaml.load(fp)

        self.logger.debug("Loaded config: %s" % self.config)

        # Get the databases
        for db in self.config['dbs']:
            url = 'mysql://%s:%s@%s:%s/%s' % (db['username'],
                                              db['password'],
                                              db['host'],
                                              db['port'],
                                              db['database_name'])
            self.dbs.append(DAL(url, pool_size=0, migrate=False))
            self.logger.debug("Configuration: Loaded DB: %s" % url)

        # Get the tables
        for table in self.config['tables']:

            table_name = table['table_name']
            table_db_name = table['db']
            current_db = self.dbs[0] # TODO find appropriate database

            # Storage for the fields
            table_field_array = []

            # Determine all fields
            for column in table['columns']:
                table_field_array.append(Field(column['column_name'], type=column['type']))

            table_ref = current_db.define_table(table_name, fields=table_field_array)

            table_details = {
                'table_name': table_name,
                'db': table_db_name,
                'ref': table_ref
            }

            self.tables.append(table_details)
            self.logger.debug("Configuration: Loaded Table: %s" % table_name)

    def monitor_updates(self):
        print("Starting monitoring...")

        for broker in self.config['brokers']:
            self.logger.info("Try to connect to %s on %s:%s" % (broker['brokername'],
                                                                broker['host'],
                                                                broker['port']))

            client = mqtt.Client()
            client.on_connect = self.on_connect
            client.on_message = self.on_message
            client.connect(broker['host'],
                           broker['port'],
                           60)

            broker_details = {
                'brokername': broker['brokername'],
                'host': broker['host'],
                'port': broker['port'],
                'ref': client
            }

            self.brokers.append(broker_details)

            self.logger.debug("Start looping")
            client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        self.logger.info("Connected with result code " + str(rc))

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        host = client._host
        broker_name = 'home' # TODO find correct broker name

        self.logger.debug(self.config['subscriptions'])

        for topic in self.config['subscriptions']:
            broker = topic['broker']
            client.subscribe(topic['topic'])
            self.logger.info("Subscribed to %s on %s" % (topic['topic'], topic['broker']))

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        self.logger.debug(msg.topic + " " + str(msg.payload))

        update_time = datetime.datetime.now()

        for topic in self.config['subscriptions']:

            if msg.topic == topic['topic']:

                # TODO: handle same table names on multiple dbs
                table_details = None

                for table in self.tables:
                    if table['table_name'] == topic['table']:
                        table_details = table

                column_name = topic['column']

                # Is there already a row for this dt?
                existing_update = table_details['ref'](dt=update_time)

                column_data = {
                    'dt': update_time,
                    column_name: msg.payload
                }

                self.logger.debug("Update received: %s" % column_data)

                if existing_update is None:

                    print("attempt insert")
                    insert = table_details['ref'].insert(**column_data)
                    print(insert)

                else:

                    existing_update.update_record(**column_data)

                # TODO: handle multiple dbs
                print("attempt commit")
                self.dbs[0].commit()

        # if msg.topic == "wxstation/wind_speed":
        #
        #     update_time = datetime.datetime.now()
        #
        #     # Is there already a row for this dt?
        #     existing_update = self.raw_wind(dt=update_time)
        #
        #     if existing_update is None:
        #         update = self.raw_wind.insert(dt=update_time, wind_speed=msg.payload)
        #         # print("Existing wind update for time %s not found when inserting wind speed" % update_time)
        #     else:
        #         update = existing_update.update_record(wind_speed=msg.payload)
        #         # print("Existing wind update for time %s found when inserting wind speed" % update_time)
        #
        #     self.db.commit()
        #
        # if msg.topic == "wxstation/wind_direction":
        #
        #     update_time = datetime.datetime.now()
        #
        #     # Is there already a row for this dt?
        #     existing_update = self.raw_wind(dt=update_time)
        #
        #     if existing_update is None:
        #         update = self.raw_wind.insert(dt=update_time, wind_direction=msg.payload)
        #         # print("Existing wind update for time %s not found when inserting wind direction" % update_time)
        #     else:
        #         update = existing_update.update_record(wind_direction=msg.payload)
        #         # print("Existing wind update for time %s found when inserting wind direction" % update_time)
        #
        #     self.db.commit()
        #
        # if msg.topic == "wxstation/rain":
        #
        #     if msg.payload != self.last_rain:
        #
        #         self.db.raw_rain.insert(dt=datetime.datetime.now(), rain=msg.payload)
        #         self.db.commit()
        #         self.last_rain = msg.payload
        #
        # if msg.topic == "wxstation/tempC":
        #
        #     update_time = datetime.datetime.now()
        #
        #     # Is there already a row for this dt?
        #     existing_update = self.raw_temp_rh(dt=update_time)
        #
        #     if existing_update is None:
        #         update = self.raw_temp_rh.insert(dt=update_time, temperature=msg.payload)
        #         print("Existing temp/rh update for time %s not found when inserting temperature" % update_time)
        #     else:
        #         update = existing_update.update_record(temperature=msg.payload)
        #         print("Existing temp/rh update for time %s found when inserting inserting" % update_time)
        #
        #     self.db.commit()
        #
        # if msg.topic == "wxstation/humidityPc":
        #
        #     update_time = datetime.datetime.now()
        #
        #     # Is there already a row for this dt?
        #     existing_update = self.raw_temp_rh(dt=update_time)
        #
        #     if existing_update is None:
        #         update = self.raw_temp_rh.insert(dt=update_time, humidity=msg.payload)
        #         # print("Existing temp/rh update for time %s not found when inserting humidity" % update_time)
        #     else:
        #         update = existing_update.update_record(humidity=msg.payload)
        #         # print("Existing temp/rh update for time %s found when inserting inserting humidity" % update_time)
        #
        #     self.db.commit()
        #
        # if msg.topic == "wxstation/pressurePa":
        #
        #     pressure = float(msg.payload) / 100
        #
        #     self.db.raw_air_pressure.insert(dt=datetime.datetime.now(), pressure=pressure)
        #     self.db.commit()


if __name__ == "__main__":
    mqtt2db = Mqtt2Db()
    mqtt2db.monitor_updates()
