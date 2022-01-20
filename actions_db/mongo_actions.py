from conectors.mongodb import Mongo

class MongoActions():

    """
    Esta clase cuenta con diversos metodos encargados de actualizar, insertar y eliminar registros de nuestras colecciones de MongoDB.

    Parametros:
        - location: nos determina en que entorno vamos a trabajar (local/server)
    
    """

    def __init__(self,location):
        
        connection = Mongo()

        if location == 'local':
            self.conexion = connection.server()
        elif location == 'remote':
            self.conexion = connection.remote_server()
    
    def _add_data(self, payload, collections):

        """
        Metodo encargado de insertar registros en nuestra coleccion.

        Parametros:
        - payload: datos generados del proceso de scraping
        - collections: storage en Mongo

        """

        if payload:
            self.conexion[collections].insert_many(payload)

    def _platform_find(self,platform, collections):

        """
        Metodo encargado de buscar los registros relacionados a una determinada plataforma en nuestra coleccion.

        Parametros:
        - platform: nombre de la plataforma
        - collections: storage en Mongo

        """

        result = self.conexion[collections].find({'Platform':platform})

        return result
    
    def _find_name_platform(self,collections):

        """
        Metodo encargado de buscar que plataformas unicas tenemos dentro de una determinada coleccion.

        Parametros:
        - collections: storage en Mongo

        """

        result = self.conexion[collections].distinct('Platform')

        return result
    
    def _find_all(self,query, collections):

        """
        Metodo encargado de buscar datos en nuestra coleccion a partir de una query.

        Parametros:
        - query
        - collections: storage en Mongo

        """

        result = self.conexion[collections].find(query)

        return result

    def _insert_bulk(self, insert, collection):

        """
        Metodo encargado de insertar o actualizar registros en nuestra coleccion a partir del matcheo.

        Parametros:
        - insert: registros que buscamos insertar o actualizar
        - collections: storage en Mongo

        """

        result = self.conexion[collection].bulk_write(insert)
        bulk_api_result = result.bulk_api_result
        print(f''
        f'Insert: {bulk_api_result["nUpserted"]} - '
        f'Matched:  {bulk_api_result["nMatched"]} - '
        f'Modified: {bulk_api_result["nModified"]}'
        )
    
    def _update_data(self,collection, data):

        """
        Metodo encargado de actualizar registros en nuestra coleccion.

        Parametros:
        - collections: storage en Mongo
        - data: registros que buscamos actualizar

        """

        for item in data:
            #Matching with Title and Platform
            query = {
                'Title':item['Title'],
                'Platform':item['Platform']
            }
            self.conexion[collection].update_one(query,{u"$set": item})