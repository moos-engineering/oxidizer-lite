from oxidizer_lite.microscope import Microscope
from oxidizer_lite.catalyst import Catalyst, CatalystConnection
from oxidizer_lite.crucible import Crucible, CrucibleConnection


creds = CrucibleConnection(
    s3_bucket="oxidizer-configs",
    s3_url="http://localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin"
)
crucible = Crucible(creds)

conn = CatalystConnection(host="localhost", port=6379, db=0)
catalyst = Catalyst(conn)   

microscope = Microscope(crucible=crucible, catalyst=catalyst)

microscope.run(host="0.0.0.0", port=8000)