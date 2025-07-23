import sqlite3
import time

from bs4 import BeautifulSoup
from playwright.sync_api import Playwright, Page



def conexion_base():
    """crea conexión a la base de datos y ordena la información por nombre de columna."""
    conn = sqlite3.connect('base.db')
    conn.row_factory = sqlite3.Row  # para acceder a los datos solo por nombre de columna
    return conn

def iniciar_base():
    """iniciar bases de datos"""
    conn = conexion_base()
    cursor = conn.cursor()

    # Tabla de citas
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS citas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            author TEXT NOT NULL
        )
    ''')

    # Tabla de etiquetas
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
    ''')

    # Tabla de unión para la relación N:N entre citas y etiquetas
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS quote_tags (
            quote_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            PRIMARY KEY (quote_id, tag_id),
            FOREIGN KEY (quote_id) REFERENCES citas(id) ON DELETE CASCADE,
            FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
        )
    ''')
    conn.commit()
    conn.close()

def webscraping(pw: Playwright, url: str, pagina: int, show_navigator: bool = False, verbose: bool = False, verbose_extend: bool = False):
    """
    Realiza conexión con la url proporcionada y extrae citas, almacenándolas
    en la base de datos con relaciones N:N para las etiquetas.

    Parameters
    ----------
    pw : Playwright - Instancia de Playwright para manejar el navegador.
    url : str - URL de la página a scrapear.
    pagina: int - Número de páginas a scrapear.
    show_navigator : bool - Si es True, muestra el navegador durante el scraping.
    verbose : bool - Si es True, imprime mensajes de estado.
    verbose_extend : bool - Si es True, imprime mensajes extendidos de estado.
    """
    conn = conexion_base()
    cursor = conn.cursor()

    # Iniciar el navegador
    browser = pw.chromium.launch(headless=not show_navigator, slow_mo=50)
    # Abrir la pagina
    page = browser.new_page()

    print("Iniciando web scraping...")
    for j in range(1, pagina + 1):
        page.goto(url.format(j), timeout=27000)
        time.sleep(5) 
        print(f'Página {j} cargada correctamente.')

        # Obtener el HTML de la página
        html = page.content()
        soup = BeautifulSoup(html, 'html.parser')
        citas_pagina = soup.select('.quote')

        for i, quote_element in enumerate(citas_pagina):
            content = quote_element.select_one('.text').get_text(strip=True)
            author = quote_element.select_one('.author').get_text(strip=True)
            tags = [tag.get_text() for tag in quote_element.select('.tags .tag')]

            # Verificar si la cita ya existe por su contenido
            cursor.execute("SELECT id FROM citas WHERE content = ?", (content,))
            existing_quote = cursor.fetchone()

            quote_id = None
            if existing_quote:
                quote_id = existing_quote['id']
                if verbose_extend:
                    print(f"Cita existente encontrada: '{content[:50]}...'")
            else:
                # Insertar la cita en la tabla 'citas'
                cursor.execute('''
                    INSERT INTO citas (content, author) VALUES (?, ?)
                ''', (content, author))
                quote_id = cursor.lastrowid # Obtener el ID de la cita recién insertada
                if verbose:
                    print(f"Cita nueva insertada: '{content[:50]}...'")

            if quote_id:
                # Procesar las etiquetas
                for tag_name in tags:
                    # Verificar si la etiqueta ya existe
                    cursor.execute("SELECT id FROM tags WHERE name = ?", (tag_name,))
                    existing_tag = cursor.fetchone()

                    tag_id = None
                    if existing_tag:
                        tag_id = existing_tag['id']
                    else:
                        # Insertar la etiqueta si no existe
                        cursor.execute("INSERT INTO tags (name) VALUES (?)", (tag_name,))
                        tag_id = cursor.lastrowid

                    # Insertar la relación en la tabla 'quote_tags'
                    try:
                        cursor.execute('''
                            INSERT INTO quote_tags (quote_id, tag_id) VALUES (?, ?)
                        ''', (quote_id, tag_id))
                    except sqlite3.IntegrityError:
                        # Ya existe la relación, no hacer nada
                        if verbose_extend:
                            print(f"Relación quote_id={quote_id}, tag_id={tag_id} ya existe.")
                        pass # La relación ya existe, no es un error

        conn.commit() # Guardar los cambios después de cada página
        print(f"Datos de la página {j} guardados en la base de datos.")

    browser.close()
    conn.close()
    print("Web scraping completado y datos guardados en la base de datos.")
