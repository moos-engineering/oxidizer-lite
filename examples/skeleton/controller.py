from oxidizer_lite.oxidizer import Oxidizer
from oxidizer_lite.catalyst import CatalystConnection
from oxidizer_lite.crucible import CrucibleConnection

# Initialize Catalyst Connection
catalyst_connection = CatalystConnection( 
    host="localhost",
    port=6379,
    db=0
)

# Initialize Crucible Credentials (Minio)
crucible_connection = CrucibleConnection(
    s3_bucket="oxidizer-configs",
    s3_url="http://localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin" 
)


oxidizer = Oxidizer(catalyst_connection, crucible_connection)

oxidizer.oxidize()