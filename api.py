#Importar las librerías 
import os
import json

from flask import Flask, request, jsonify
from playwright.sync_api import sync_playwright
from functions import conexion_base, iniciar_base, webscraping




# ----- PARAMETROS
# Importar Parametros
with open(os.path.join('settings.json'), 'r') as file:
    settings = json.load(file)


# Indicadores visuales.
verbose = settings['verbose']['verbose']
show_navigator = settings['verbose']['show_navigator']
url_pagina = settings['links']['url']
pagina= settings['pagina']['pagina']

# iniciar la app y la base de datos
if __name__ == '__main__':
    iniciar_base()

    # Ejecutar el scraping solo en el proceso principal no en recargador con flask
    if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        with sync_playwright() as pw:
            # Scrapear el número de páginas que ponga en settings, adicional si se quiere o no mostrar el navegador
            webscraping(pw, url_pagina, pagina=pagina, show_navigator=show_navigator, verbose=verbose)

    print("Iniciando API con flask")
    app = Flask(__name__)

    @app.route('/quotes', methods=['GET'])
    def get_quotes():
        """
        Filtros: author, tag, search. Pueden combinarse.
        """
        author_filter = request.args.get('author')
        tag_filter = request.args.get('tag')
        search_filter = request.args.get('search')

        conn = conexion_base()
        cursor = conn.cursor()

        query = "SELECT DISTINCT q.id, q.content, q.author FROM citas q"
        unir_clases = []
        valor = []
        parametro = []

        # Unir con tags si se filtra por tag
        if tag_filter:
            unir_clases.append("JOIN quote_tags qt ON q.id = qt.quote_id JOIN tags t ON qt.tag_id = t.id")
            valor.append("t.name LIKE ?")
            parametro.append(f"%{tag_filter}%")

        # Filtrar por autor
        if author_filter:
            valor.append("q.author LIKE ?")
            parametro.append(f"%{author_filter}%")

        # Búsqueda libre por contenido
        if search_filter:
            valor.append("q.content LIKE ?")
            parametro.append(f"%{search_filter}%")

        # Construir la consulta final
        if unir_clases:
            query += " " + " ".join(unir_clases)
        if valor:
            query += " WHERE " + " AND ".join(valor)

        cursor.execute(query, parametro)
        citas_total = cursor.fetchall()

        # Formatear la respuesta
        lista_citas = []
        for i in citas_total:
            citas_diccionario = dict(i)
            # Obtener las etiquetas para cada cita
            cursor.execute('''
                SELECT t.name FROM tags t
                JOIN quote_tags qt ON t.id = qt.tag_id
                WHERE qt.quote_id = ?
            ''', (citas_diccionario['id'],))
            tags_for_quote = [tag_row['name'] for tag_row in cursor.fetchall()]
            citas_diccionario['tags'] = tags_for_quote
            lista_citas.append(citas_diccionario)

        conn.close()
        # Devolver la lista de citas como JSON
        return jsonify(lista_citas)

    # Iniciar el servidor Flask
    app.run(port=8000)

    #usar en el navegador:
# http://127.0.0.1:8000/quotes: devuelve todas las citas.
# http://127.0.0.1:8000/quotes?author=...: filtra por autor.
# http://127.0.0.1:8000/quotes?tag=...: filtra por etiqueta.
# http://127.0.0.1:8000/quotes?search=...: búsqueda libre por contenido de la cita.
# http://127.0.0.1:8000/quotes?author=Steve%20Martin&tag=humor ejemplo de combinación.