import argparse

"""
Script encargado de iniciar comandos Bash en nuestro servidor remoto o a nivel local.
"""

if __name__=='__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-p','--process',help='Proceso', const=True, nargs='?', default=False)
    parser.add_argument('-s','--subprocess',help='Subproceso', const=True, nargs='?', default=False)
    parser.add_argument('-t','--type',help='Type [selenium,requests] ', const=True, nargs='?', default=False)
    parser.add_argument('-l','--location',help='Local or Remote', const=True, nargs='?', default=False)
    parser.add_argument('-d','--date',help='CreatedAt', const=True, nargs='?', default=False)
    args= parser.parse_args()

    type_scrap = args.type
    process = args.process
    subprocess = args.subprocess
    location = args.location
    date = args.date

    if process == 'popularity':
        from handler.popularity import TopPopularity
        TopPopularity(type_scrap, location)

    if process == 'metadata_content':
        from handler.metadata import Games
        Games(location)

    if process == 'fix':
        if subprocess == 'score_votes':
            from fix import Fix
            fix = Fix(location)
            fix.delete_from_score_votes(date)