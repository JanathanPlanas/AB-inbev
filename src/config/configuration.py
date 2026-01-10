import os 
import yaml


#============================================================


class Config:


    _instance = None 

    def __new__(cls, config_file):

        """
        Cria uma nova instância da classe Config, se ainda não existir.
        Args:
            config_file (str): Caminho para o arquivo de configuração YAML.
        Returns:
            Config: Instância única da classe Config.
        """

        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._load_config(cls._instance, config_file)
        return cls._instance
    
    @staticmethod

    def _load_config(instance, config_file):
        """
        Carrega as configurações do arquivo  YAML e inicializa  os atributos da instancia 
        Args:
            instance (Config): instancia da classe Config
            config_file (str) : caminho para o arquivo de configuracao YAML

        """

        with open(config_file, 'r') as file:
                config_data = yaml.safe_load(file)
                instance.api = Config.api(config_data.get("api"))
                instance.params = Config.params(config_data.get("params"))

    class api:
         
         def __init__(self, data):
              
            self.base_url = data.get('base_url')
            
            
    class params:
    
        def __init__(self, data):
            
            self.per_page = data.get('per_page')
            self.max_retries = data.get('max_retries')
            self.backoff_factor = data.get('backoff_factor')
            self.timeout = data.get('timeout')
            



config_path = os.path.join(os.path.dirname(__file__), '..','..', 'config', 'config.yaml')
config = Config(config_path)

