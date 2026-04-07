# CACHE MANAGER

import redis 
from redis.commands.json.path import Path 
from redis.exceptions import ResponseError, ConnectionError as RedisConnectionError
import json

from oxidizer_lite.residue import Residue


class CatalystConnection:
    def __init__(self, user=None, password=None, host='localhost', port=6379, db=0):
        """
        Initializes the Redis connection configuration.
        
        Args:
            user (str | None): The Redis username for authentication.
            password (str | None): The Redis password for authentication.
            host (str): The Redis server hostname.
            port (int): The Redis server port.
            db (int): The Redis database index.
        """
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db = db


# Create a Redis client
class Catalyst(Residue):
    def __init__(self, connection: CatalystConnection):
        """
        Initializes the Catalyst Redis client with the provided connection configuration.
        
        Args:
            connection (CatalystConnection): The Redis connection configuration.
        """
        super().__init__(component_name="catalyst")
        self.client = redis.Redis(
            host=connection.host,
            port=connection.port,
            db=connection.db,
            username=connection.user,
            password=connection.password
        )

    def create_stream(self, stream_name):
        """
        Creates a Redis stream if it doesn't already exist.
        
        Args:
            stream_name (str): The name of the Redis stream to create.
        """
        # Redis streams are created automatically when you add the first entry, so we can just ensure it exists by adding an initial entry and then deleting it.
        if not self.client.exists(stream_name):
            self.client.xadd(stream_name, {"initial": "entry"})
            self.client.xtrim(stream_name, maxlen=0)  # Remove the initial entry immediately after creating the stream
            
            self.residue(self.ash.DEBUG, f"CATALYST: Stream '{stream_name}' created.")
        else:
            self.residue(self.ash.DEBUG, f"CATALYST: Stream '{stream_name}' already exists.")

    def create_consumer_group(self, stream_name, group_name):
        """
        Creates a consumer group for a Redis stream if it doesn't already exist.
        
        Args:
            stream_name (str): The name of the Redis stream.
            group_name (str): The name of the consumer group to create.
        """
        try:
            self.client.xgroup_create(stream_name, group_name, id='0', mkstream=True)
            self.residue(self.ash.DEBUG, f"CATALYST: Consumer group '{group_name}' created for stream '{stream_name}'.")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                self.residue(self.ash.DEBUG, f"CATALYST: Consumer group '{group_name}' already exists for stream '{stream_name}'.")
            else:
                raise e

    # Write to Stream Function
    def write_to_stream(self, stream_name, data):
        """
        Writes a message to the specified Redis stream.
        
        Args:
            stream_name (str): The name of the Redis stream.
            data (dict): The data to write to the stream, as a dictionary of field-value pairs.
        
        Returns:
            str: The ID of the added stream entry.
        """
        # Add the data to the stream and get the entry ID
        data = {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in data.items()} # Ensure all values are strings or JSON-encoded
        entry_id = self.client.xadd(stream_name, data)
        self.residue(self.ash.DEBUG, f"CATALYST: Add to stream '{stream_name}': with Message ID: {entry_id}")
        return entry_id


    # Read from Stream Function
    def read_from_stream(self, stream_name, group_name, consumer_name, count=1, block=1000):
        """
        Reads messages from a Redis stream using a consumer group.
        
        Args:
            stream_name (str): The name of the Redis stream.
            group_name (str): The name of the consumer group.
            consumer_name (str): The name of the consumer within the group.
            count (int): The maximum number of messages to read at once.
            block (int): The time in milliseconds to block if no messages are available.
        
        Returns:
            list: A list of messages read from the stream, each as a tuple of (entry_id, data).
        """
        # Read messages from the stream for the specified consumer group and consumer
        messages = self.client.xreadgroup(group_name, consumer_name, {stream_name: '>'}, count=count, block=block) 
        
        
        # update Messages to use this too: Note this is for a single message
        # msg_id = raw_msg_id.decode('utf-8') if isinstance(raw_msg_id, bytes) else raw_msg_id
        # data = {k.decode('utf-8') if isinstance(k, bytes) else k: json.loads(v) if self.catalyst.is_json(v) else v.decode('utf-8') if isinstance(v, bytes) else v for k, v in raw_invocation.items()}
        # messages = [(message_id, data) for _, msgs in messages for message_id, data in msgs]
        messages = [(msg_id.decode('utf-8') if isinstance(msg_id, bytes) else msg_id, {k.decode('utf-8') if isinstance(k, bytes) else k: json.loads(v) if self.is_json(v) else v.decode('utf-8') if isinstance(v, bytes) else v for k, v in msg_data.items()}) for _, msgs in messages for msg_id, msg_data in msgs]
        
        
        if messages:
            self.residue(self.ash.DEBUG, f"CATALYST: Read from stream '{stream_name}'")
        else:
            self.residue(self.ash.DEBUG, f"CATALYST: No new messages in stream '{stream_name}' for consumer '{consumer_name}'.")
        return messages


    def is_json(self, value):
        """
        Helper method to check if a value is a JSON string.
        Args:
            value: The value to check.
        Returns:
            bool: True if the value is a JSON string, False otherwise.
        """
        try:
            json.loads(value)
            return True
        except (TypeError, json.JSONDecodeError):
            return False

    # Acknowledge Message Function
    def acknowledge_message(self, stream_name, group_name, msg_id):
        """
        Acknowledges a message in a Redis stream, marking it as processed.
        
        Args:
            stream_name (str): The name of the Redis stream.
            group_name (str): The name of the consumer group.
            msg_id (str): The ID of the stream entry to acknowledge.
        """    
        self.client.xack(stream_name, group_name, msg_id)
        self.residue(self.ash.DEBUG, f"CATALYST: Acknowledged message with Message ID '{msg_id}' in stream '{stream_name}' for group '{group_name}'.")


    # Delete Message Function
    def delete_message(self, stream_name, msg_id):
        """
        Deletes a message from a Redis stream.
        
        Args:
            stream_name (str): The name of the Redis stream.
            msg_id (str): The ID of the stream entry to delete.
        """
        self.client.xdel(stream_name, msg_id)
        self.residue(self.ash.DEBUG, f"CATALYST: Deleted message with Message ID '{msg_id}' from stream '{stream_name}'.")

    
    # SET JSON Function
    def set_json(self, key, value):
        """
        Sets a JSON value in Redis.
        
        Args:
            key (str): The key under which to store the JSON value.
            value (dict): The JSON-serializable dictionary to store.
        """
        self.client.json().set(key, Path.root_path(), value)
        self.residue(self.ash.DEBUG, f"CATALYST: Set JSON value for key '{key}'")
        

    # GET JSON Function
    def get_json(self, key, path=Path.root_path()):
        """
        Gets a JSON value from Redis.
        
        Args:
            key (str): The key from which to retrieve the JSON value.
        Returns:
            dict: The JSON value retrieved from Redis, or None if the key does not exist.  
        """
        value = self.client.json().get(key, path)
        if value is not None:
            self.residue(self.ash.DEBUG, f"CATALYST: Got JSON value for key '{key}' at path '{path}'")
            return value
        else:
            self.residue(self.ash.DEBUG, f"CATALYST: No value found for key '{key}' at path '{path}'.")
            return None
        

    def update_json(self, key, path, value):
        """
        Updates a JSON value in Redis at a specific path.
        
        Args:
            key (str): The key under which the JSON value is stored.
            path (str): The JSON path to update (e.g., '.field.subfield').
            value: The new value to set at the specified JSON path.
        """
        self.client.json().set(key, Path(path), value)
        self.residue(self.ash.DEBUG, f"CATALYST: Updated JSON value for key '{key}' at path '{path}'")

    

    # Rename Key Function
    def rename_key(self, old_key, new_key):
        """
        Renames a key in Redis.
        
        Args:
            old_key (str): The current name of the key.
            new_key (str): The new name for the key.
        """
        # Check if the old key exists before renaming
        if not self.client.exists(old_key):
            self.residue(self.ash.WARNING, f"Key '{old_key}' does not exist. Cannot rename.")
            return
        self.client.rename(old_key, new_key)
        self.residue(self.ash.DEBUG, f"CATALYST: Renamed key from '{old_key}' to '{new_key}'.")


    # Set TTL Function
    def set_ttl(self, key, ttl_seconds):
        """
        Sets a time-to-live (TTL) for a key in Redis.
        
        Args:
            key (str): The key for which to set the TTL.
            ttl_seconds (int): The TTL in seconds.
        """
        self.client.expire(key, ttl_seconds)
        self.residue(self.ash.DEBUG, f"CATALYST: Set TTL of {ttl_seconds} seconds for key '{key}'.")

    # Get TTL Function
    def get_ttl(self, key):
        """
        Gets the time-to-live (TTL) for a key in Redis.
        
        Args:
            key (str): The key for which to get the TTL.
        Returns:
            int: The TTL in seconds, or -1 if the key exists but has no associated TTL, or -2 if the key does not exist.
        """
        ttl = self.client.ttl(key)
        if ttl >= 0:
            self.residue(self.ash.DEBUG, f"CATALYST: TTL for key '{key}': {ttl} seconds")
        elif ttl == -1:
            self.residue(self.ash.DEBUG, f"CATALYST: Key '{key}' exists but has no associated TTL.")
        else:
            self.residue(self.ash.DEBUG, f"CATALYST: Key '{key}' does not exist.")
        return ttl
    
    # Scan Keys Function
    def scan_keys(self, pattern):
        """
        Scans for keys in Redis that match a given pattern.
        
        Args:
            pattern (str): The pattern to match keys against (e.g., 'oxidizer:topology:state:*').
        Returns:
            list: A list of keys that match the given pattern.
        """
        keys = []
        cursor = 0
        while True:
            cursor, batch = self.client.scan(cursor=cursor, match=pattern, count=100)
            # convert redis bytes to strings if necessary
            batch = [key.decode('utf-8') if isinstance(key, bytes) else key for key in batch]
            keys.extend(batch)
            if cursor == 0:
                break
        self.residue(self.ash.DEBUG, f"CATALYST: Scanned keys with pattern '{pattern}': {keys}")
        return keys