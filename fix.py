from conectors.mongodb import Mongo

class Fix:

    """
    Clase encargada de reparar ciertos errores en los datos

    Errores corregidos hasta el momento:
    - Eliminar scores incorrectos dentro de la coleccion "ScoreVotes"
    
    """

    def __init__(self,location):
        
        connection = Mongo()

        if location == 'local':
            self.conexion = connection.server()
        elif location == 'remote':
            self.conexion = connection.remote_server()

    def delete_from_score_votes(self, created_at):

        self.conexion['ScoreVotes'].delete_many({'CreatedAt': created_at})