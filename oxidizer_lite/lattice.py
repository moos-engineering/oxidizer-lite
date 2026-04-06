# CONFIG MANAGER 

import yaml 
from oxidizer_lite.crucible import Crucible
from oxidizer_lite.residue import Residue, Ash


class Lattice(Residue):
    def __init__(self, crucible: Crucible):
        """
        Initializes the Lattice configuration manager.
        
        Args:
            crucible (Crucible): The Crucible S3 client used for loading and saving configurations.
        """
        super().__init__(component_name="lattice")
        self.config = None
        self.crucible = crucible

    def load_config(self, bucket_name, object_key):
        """
        Loads a configuration from S3 using Crucible.
        
        Args:
            bucket_name (str): The name of the S3 bucket containing the configuration.
            object_key (str): The S3 object key for the configuration file.
        
        Returns:
            dict: The loaded configuration as a dictionary.
        """
        config_data = self.crucible.get_object(bucket_name, object_key) 
        self.config = yaml.safe_load(config_data)
        self.residue(self.ash.DEBUG, f"LATTICE: Loaded configuration {object_key} from bucket '{bucket_name}' with key '{object_key}'.")
        return self.config

    def save_config(self, bucket_name, object_key):
        """
        Saves the current configuration to S3 using Crucible.
        
        Args:
            bucket_name (str): The name of the S3 bucket to save the configuration to.
            object_key (str): The S3 object key for the configuration file.
        """
        config_data = yaml.dump(self.config)
        self.crucible.put_object(bucket_name, object_key, config_data)
        self.residue(self.ash.DEBUG, f"LATTICE: Saved configuration {object_key} to bucket '{bucket_name}' with key '{object_key}'.")

    def validate_config(self):
        """
        Validates the current configuration against required fields and data types.
        
        Returns:
            None
        """
        # Implement validation logic based on your requirements
        # For example, check for required fields, data types, etc.
        pass