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
        default_level=logging.INFO,
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

    def __init__(self):
        self.config = ConfigParser()
        self.config.read(['config.ini'])
        self.username = self.config['db']['username']
        self.password = self.config['db']['password']
        self.server = self.config['db']['server']
        self.port = self.config['db']['port']
        self.database = self.config['db']['database']

        self.url = 'mysql://%s:%s@%s:%s/%s' % (self.username, self.password, self.server, self.port, self.database)

        self.db = DAL(self.url, pool_size=0, migrate=False)
        self.raw_wind = self.db.define_table('raw_wind', Field('wind_direction', type='integer'),
                             Field('wind_speed', type='float'), Field('dt', type='datetime'))
        self.raw_rain = self.db.define_table('raw_rain', Field('rain', type='float'),
                              Field('dt', type='datetime'))

        self.last_rain = None

    def monitor_updates(self):
        print("Starting monitoring...")

        client = mqtt.Client()

        # client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED,
        #                tls_version=ssl.PROTOCOL_TLS, ciphers=None)
        # client.enable_logger(logger=mqtt.MQTT_LOG_DEBUG)
        client.on_connect = self.on_connect
        client.on_message = self.on_message

        print("Try to connect")
        client.connect(self.config['mqtt']['broker'], self.config['mqtt'].getint('port'), 60)

        print("Start looping")
        client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " + str(rc))

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe(self.config['topic'])

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        # print(msg.topic + " " + str(msg.payload))

        if msg.topic == "wxstation/wind_speed":

            update_time = datetime.datetime.now()

            # Is there already a row for this dt?
            existing_update = self.raw_wind(dt=update_time)

            if existing_update is None:
                update = self.raw_wind.insert(dt=update_time, wind_speed=msg.payload)
                print("Existing wind update for time %s not found when inserting wind speed" % update_time)
            else:
                update = existing_update.update_record(wind_speed=msg.payload)
                print("Existing wind update for time %s found when inserting wind speed" % update_time)

            self.db.commit()

        if msg.topic == "wxstation/wind_direction":

            update_time = datetime.datetime.now()

            # Is there already a row for this dt?
            existing_update = self.raw_wind(dt=update_time)

            if existing_update is None:
                update = self.raw_wind.insert(dt=update_time, wind_direction=msg.payload)
                print("Existing wind update for time %s not found when inserting wind direction" % update_time)
            else:
                update = existing_update.update_record(wind_direction=msg.payload)
                print("Existing wind update for time %s found when inserting wind direction" % update_time)

            self.db.commit()

        if msg.topic == "wxstation/rain":

            if msg.payload != self.last_rain:

                self.db.raw_rain.insert(dt=datetime.datetime.now(), rain=msg.payload)
                self.db.commit()
                self.last_rain = msg.payload


if __name__ == "__main__":
    mqtt2db = Mqtt2Db()
    mqtt2db.monitor_updates()
