import configparser


class ConfigHandler:
    def __init__(self, config_file):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

    def get_value(self, section, key):
        try:
            value = self.config.get(section, key)
            return value
        except (configparser.NoSectionError, configparser.NoOptionError):
            return None

    def set_value(self, section, key, value):
        if not self.config.has_section(section):
            self.config.add_section(section)
        self.config.set(section, key, value)

    def save_config(self, config_file):
        with open(config_file, "w") as file:
            self.config.write(file)
