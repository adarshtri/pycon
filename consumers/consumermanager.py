import abc
import constants.settings_constants as sc


class ConsumerManager(metaclass=abc.ABCMeta):

    """
        Abstract consumer manager class with abstract methods and common properties.
    """

    def __init__(self, consumer_type, consumption_config):
        self.consumer_type = consumer_type
        self.consumption_config = consumption_config

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
        This property defines the consumption configuration array. Basically host, port and channel_name.
        The setter method is an abstract method and defined by each of the consumer manager subclass.
    """

    def get_consumption_config(self) -> list:
        return self._consumption_config

    @abc.abstractmethod
    def start_consumers(self):
        pass

    @abc.abstractmethod
    def stop_consumers(self):
        pass

    @abc.abstractmethod
    def _check_consumption_config(self, consumption_config):
        pass

    consumer_type = property(get_consumer_type, set_consumer_type, del_consumer_type)
    consumption_config = property(get_consumption_config)

    @consumption_config.setter
    def consumption_config(self, consumption_config):
        self._check_consumption_config(consumption_config)


class AMQQueueConsumerManager(ConsumerManager):

    def __init__(self, consumption_config):
        ConsumerManager.__init__(self, consumption_config=consumption_config, consumer_type='amqq')

    def start_consumers(self):
        pass

    def stop_consumers(self):
        pass

    def _check_consumption_config(self, consumption_config):

        if not isinstance(consumption_config, list):
            raise ValueError("Consumption config must be of type list.")

        for config in consumption_config:

            if not isinstance(config, dict):
                raise ValueError("Every config in consumption config must be of type dict.")

            if "host" in config and "port" in config and "channel" in config:
                continue
            else:
                raise KeyError("""Missing key "host" or "port" or "channel" in config dictionary.""")

        else:
            self._consumption_config = consumption_config
