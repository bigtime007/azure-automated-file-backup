from configparser import ConfigParser

def config(filename="config.ini", section="config") ->dict:
    
    parser = ConfigParser()
    parser.read(filename)
    settings = {}
    
    if parser.has_section(section):
        
        params = parser.items(section)
        for param in params:
            settings[param[0]] = param[1]

    else:
        raise Exception('Section{0} is not found in the {1} file. '.format(section, filename))

    return settings

if __name__ == "__main__":

    print(config())