import os
import yaml

class Application:
    '''
    application configuration manager.
    '''
    @staticmethod
    def read(data):
        '''
        read configuration from file or input

        Args:
            data (str): file path or yaml string

        Returns:
            dict: configuration
        '''
        if isinstance(data, str):
            if os.path.exists(data):
                # read yaml from file
                with open(data, "r", encoding="utf-8") as f:
                    # read configuration
                    return yaml.safe_load(f)

            # attempt to read yaml from input
            data = yaml.safe_load(data)
            if not isinstance(data, str):
                return data

            # file not found and input is not yaml, raise error
            raise FileNotFoundError(f"File not found: {data}")
        
        # return unmodified
        return data

    def __init__(self, config=None):
        '''
        initialize application configuration.

        Args:
            config (dict): configuration.
        '''
        self.config = self.read(config or os.environ.get("CONFIG", "config.yml"))

    def __call__(self, data):
        '''

        '''
        pass