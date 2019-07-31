import abc
import constants.settings_constants as sc


class ConsumerManager(metaclass=abc.ABCMeta):

    """
        Abstract consumer manager class with abstract methods and common properties.
    """

    def __init__(self, consumer_type, host, port):
        self.consumer_type = consumer_type
        self.host = host
        self.port = port

    """
        @property
        This property "consumer_type" decides what type of consumer is being tried to attached to.
        The supported values for this is under settings_constant.py class SettingsConstants --> valid_counsumer_sources
    """

    def get_consumer_type(self):
        return self._consumer_type

    def set_consumer_type(self, consumer_type):
        if consumer_type not in sc.SettingConstants.valid_consumer_sources:
            raise ValueError("Invalid value for consumer_type. Valid values are %s." %
                             str(sc.SettingConstants.valid_consumer_sources))
        self._consumer_type = consumer_type

    def del_consumer_type(self):
        del self._consumer_type

    """
        @property
        This property defines the port to which to connect for consumer.
    """

    def get_port(self):
        return self._port

    def set_port(self, port):

        if not isinstance(port, int):
            raise ValueError("Value of parameter 'port' should be integer.")

        if port <= 0:
            raise ValueError("Value of port must be greater that 0.")

        self._port = port

    def del_port(self):
        del self._port

    """
        @property
        This property defines the server host name to connect to for consumption.
    """

    def get_host(self):
        return self._host

    def set_host(self, host):

        if not isinstance(host, str):
            raise ValueError("Host should be of type string.")
        self._host = host

    def del_host(self):
        del self._host

    consumer_type = property(get_consumer_type, set_consumer_type, del_consumer_type)
    port = property(get_port, set_port, del_port)
    host = property(get_host, set_host, del_host)


class AMQConsumerManager(ConsumerManager):

    def __init__(self, host, port):
        ConsumerManager.__init__(self, consumer_type='amq', host=host, port=port)