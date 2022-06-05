import json
import os
import types
import typing as t


class Partial(dict):
    pass


class Config(dict):
    """Configuration in Swill is stored as a dictionary. Entries are namespaced:
    eg: swill.introspection refers to config['swill']['introspection']

    An environment variable can be used to load a configuration file:
    SWILL_CONFIG_FILE=config.py
    config.load_from_env_file()

    config.py:
        swill = {
            'introspection': True
        }

    Configuration files can also be specified on the Config object:
    config.load_from_file('config.py')


    Environment variables can be used to populate the configuration:
    PREFIX_SWILL_INTROSPECTION=1 --> config['swill']['introspection']
    config.load_from_env('PREFIX')

    And finally can also be populated from an object:
    class Config:
        swill = {
            'introspection': True
        }
    config.load_from_object(Config)

    """

    def __init__(self, root_path: str, defaults: dict = None):
        self.root_path = root_path
        super().__init__(defaults or {})

    def load_from_env(self, prefix='SWILL', loads: t.Callable[[str], t.Any] = json.loads):
        """Load configuration from the environment prefixed by prefix"""
        full_prefix = f'{prefix}_'
        prefix_length = len(full_prefix)
        for key in sorted(filter(lambda v: v.startswith(full_prefix), os.environ)):
            value = loads(os.environ[key])
            key = key[prefix_length:].lower()

            if "__" not in key:
                # A non-nested key, set directly.
                self[key] = value
                continue

            # Traverse nested dictionaries with keys separated by "__".
            current = self
            *parts, tail = key.split("__")

            for part in parts:
                # If an intermediate dict does not exist, create it.
                if part not in current:
                    current[part] = {}

                current = current[part]

            current[tail] = value

    def load_from_env_file(self, env_name='SWILL_CONFIG_FILE'):
        """Load configuration from the file specified by env_name"""
        self.load_from_file(os.getenv(env_name))

    def load_from_file(self, filename: str):
        """Load the configuration from the python file specified by filename"""
        filename = os.path.join(self.root_path, filename)
        d = types.ModuleType("config")
        d.__file__ = filename
        with open(filename, mode="rb") as config_file:
            exec(compile(config_file.read(), filename, "exec"), d.__dict__)
        return True

    def load_from_object(self, obj: object):
        """Load the configuration from the given object"""
        for key in dir(obj):
            if key.startswith('__'):
                continue
            value = getattr(obj, key)
            if isinstance(value, Partial) or obj.__annotations__.get(key) == Partial:
                # Update the configuration rather than replace it
                self[key].update(value)
            else:
                self[key] = value

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __getitem__(self, key: str):
        """Get a configuration key. You can use dot notation to get a specific key
        eg: swill.introspection is equivalent to: return self['swill']['introspection']"""
        if '.' not in key:
            return super().__getitem__(key)

        namespace, others = key.split('.', 1)
        if namespace not in self:
            self[namespace] = {}

        return self[namespace][others]

    def __setitem__(self, key: str, value: t.Any):
        """Set the configuration key"""
        if '.' not in key:
            return super().__setitem__(key, value)

        namespace, others = key.split('.', 1)
        if namespace not in self:
            self[namespace] = {}

        self[namespace][others] = key
