import hashlib

def generate_hash_unique(title, year):

    """
    Funcion encarga de gener un ID unico por cada videojuego disponible en el sitio web.

    El ID esta conformado por el titulo y el año del videojuego.
    """

    hash_string = f'{title}{year}'

    encoded = hash_string.encode('UTF-8')

    return hashlib.md5(encoded).hexdigest()