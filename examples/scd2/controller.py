from oxidizer_lite.oxidizer import Oxidizer
from oxidizer_lite.catalyst import CatalystConnection
from oxidizer_lite.crucible import CrucibleConnection

# Initialize Catalyst Connection
catalyst_connection = CatalystConnection( 
    host="localhost",
    port=6379,
    db=0
)

# Initialize Crucible Credentials (S3)
crucible_connection = CrucibleConnection(
    s3_bucket="your-config-bucket",
    sso_role_name="your-sso-profile"
)

# Initialize Oxidizer
oxidizer = Oxidizer(catalyst_connection, crucible_connection)
oxidizer.oxidize()