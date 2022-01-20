import pymongo
import sshtunnel
from pathlib import Path

class Mongo():

    """
    Clase encargada de iniciar, por medio de SSH, la conexion a nuestro servidor remoto y posteriormente retornar la conexion a MongoDB
    """

    def __init__(self):

        base_path = Path(__file__).parent
        file_path = (base_path / 'config/private_key')
        self.path_str = str(file_path) 

    def server(self):

        client = pymongo.MongoClient("localhost", 27017)
        conexion_local = client.workflow
        return conexion_local
    
    def remote_server(self):
        
        sshtunnel.SSH_TIMEOUT = sshtunnel.TUNNEL_TIMEOUT = 10.0
        server = sshtunnel.open_tunnel(
            ('XXX.XXX.X.XX', 22),
            ssh_username='ubuntu',
            ssh_pkey=self.path_str,
            remote_bind_address=('127.0.0.1', 27017)
        )

        server.start()
        remote_client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)
        conexion_remote = remote_client.workflow
        
        return conexion_remote

        


         