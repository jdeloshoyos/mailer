#! /usr/bin/python3
# Encoding: UTF-8
# Script para envío masivo de correos a una lista.
# Maneja un cuerpo predefinido de correo con "placeholders", una lista de
# direcciones y archivos a adjuntar, y un intervalo de tiempo en segundos
# a esperar entre envíos de correo (para no alertar a sistemas anti-spam).
# 2012 por Jaime de los Hoyos M.
#
# v1.00 (Agosto 2012): Versión inicial
# v1.01 (9 enero 2013): Muestra para cada correo si ha sido correctamente enviado
# v1.02 (11 enero 2013): Es capaz de continuar, aún si uno de los envíos en la lista falla.
# v1.03 (6 agosto 2018): Limpieza menor de código para publicar en GitHub
# v1.04 (24 abril 2019): Permite enviar correo a varios destinatarios en una sola línea del listado, separándolos por comas.
# v1.05 (6 junio 2019): Parámetros son leídos desde archivo .JSON
# v1.06 (18 julio 2019): Incluimos Content-ID para los adjuntos, necesario para referenciar imágenes
# v1.07 (2 agosto 2019): Separadores del CSV (de columnas, y de subcampos en las columnas de destinatarios y adjuntos) se leen desde JSON; upgrade a Python 3
# v1.08 (8 agosto 2019): Parámetro "timeout" en config.json
# v1.09 (23 marzo 2020): Muestra resultados finales al terminar proceso
# v1.10 (17 abril 2020): Capacidad de seleccionar motor de envío (SMTP / API SendGrid v3, con uso de librería)

# MIT License
# 
# Copyright (c) 2019 jdeloshoyos
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import sys
import os
import time
import smtplib
import mimetypes
import json
import re
from optparse import OptionParser
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import *

parser = OptionParser(usage="""\
Envia un correo tipo a una lista de direcciones, opcionalmente con archivos adjuntos.

Uso: %prog [options]

""")
parser.add_option('-l', '--lista',
    type='string', action='store', metavar='lista_csv',
    help="""(REQUERIDO) Especifica el archivo CSV que contiene la lista de distribucion. Para mas detalles, ver el archivo 'lista.csv' de ejemplo.""")
parser.add_option('-t', '--texto',
    type='string', action='store', metavar='texto_txt',
    help="""(REQUERIDO) Especifica el archivo con el texto tipo que se usara en el contenido del correo. La primera linea se usara como Asunto del correo redactado. Es posible usar 'placeholders' determinados dentro de este texto, para ser reemplazados por textos que se definan en el archivo .csv de la lista. Ver ejemplos y documentacion.""")
parser.add_option('-d', '--delay',
    type='int', action='store', metavar='segundos', default=0,
    help="""Cantidad de segundos a esperar entre envios de correo, para no activar sistemas anti-spam. Si no se especifica, no habra demora entre correos.""")
parser.add_option('-c', '--config',
    type='string', action='store', metavar='config', default='config.json',
    help="""Especifica el archivo JSON con la configuración a cargar. Si no se especifica, por defecto busca y carga config.json.""")
    
opts, args = parser.parse_args()
if not opts.lista or not opts.texto:
    # Falta algún parámetro obligatorio
    parser.print_help()
    sys.exit(1)

if not os.path.isfile(opts.lista):
    print("Error: No se encuentra el archivo "+opts.lista)
    sys.exit(1)
    
if not os.path.isfile(opts.texto):
    print("Error: No se encuentra el archivo "+opts.texto)
    sys.exit(1)

# Carga de la configuración
# La configuración se carga desde un archivo JSON, por defecto config.json pero es posible especificarlo con la opción -c
# en la línea de comando. Este JSON debe tener la siguiente estructura:
#{
#    "servidor_smtp":"servidor:puerto",
#    "username":"username",
#    "password":"password",
#    "from_email":"Dirección de correo <correo@dominio.com>",
#    "separador_csv": ";",
#    "separador_subcampos": ",",
#    "timeout": timeout_en_segundos,
#    "engine": "SMTP" o "SENDGRID"
#}
try:
    with open(opts.config, 'r', encoding='utf-8') as json_file:
        config=json.load(json_file)
except:
    print("Error: No se pudo abrir el archivo de configuracion "+opts.config)
    sys.exit(1)

# Recuperamos el asunto y el texto del correo
f=open(opts.texto, 'r', encoding='utf-8')
cuerpo=''
asunto=None
for linea in f:
    if asunto is None:
        asunto=linea.strip()    # Quitamos el CRLF del final
    else:
        cuerpo=cuerpo+linea
f.close()

# Ahora parseamos el CSV, cargando cada línea en una lista.
f=open(opts.lista, 'r', encoding='utf-8')
columnas=[]
placeholders={}
elems_lista=[]
for linea in f:
    if len(columnas)==0:
        columnas=linea.rstrip().split(config['separador_csv'])
        # Determinamos cuáles de las columnas son placeholders, viendo si empiezan y terminan con "||"
        for indice, valor in enumerate(columnas):
            if valor.startswith('||') and valor.endswith('||'):
                placeholders[valor]=indice
    else:
        elems_lista.append(linea.rstrip().split(config['separador_csv']))
f.close()

# Iniciamos el proceso!
total_elems=len(elems_lista)
elem_actual=0

if config['engine'].upper() == "SENDGRID":
    # Envío usando la API de Sendgrid

    sendgrid_client = SendGridAPIClient(config['password'])

    # Sendgrid tiene varias restricciones. Permite hasta 1000 "personalizaciones" por envío,
    # hasta 1000 direcciones de correo individuales por envío,
    # y el payload del JSON para un envío no puede superar 30 MB.
    # Entonces, por seguridad, restringimos los bloques de envío a una cantidad de "líneas" por vez.
    # Sendgrid incorpora la capacidad de hacer sustituciones directamente con su API.
    bloque_envios = 200
    while elem_actual < total_elems:
        print("Enviando correos", elem_actual + 1, "al", min(elem_actual + bloque_envios, total_elems), "de", total_elems, "...") 
        mensaje = Mail()
        pers_id = 0
        dict_adjuntos = {}

        # Antes de iniciar, vamos a determinar si hay adjuntos que estén presentes en TODAS las personalizaciones,
        # para en ese caso agregarlas una única vez al final. Esto lo hacemos contando las ocurrencias de cada
        # nombre de adjunto en la lista; si el conteo de un adjunto = total_elems, está en todos los envíos.
        for i in range(elem_actual, min(elem_actual + bloque_envios, total_elems)):
            adjuntos_linea = elems_lista[i][1].split(config['separador_subcampos'])

            # Eliminamos posibles duplicados en la lista de adjuntos para esta línea
            adjuntos_linea = list(dict.fromkeys(adjuntos_linea))

            # Incrementamos el valor del diccionario para cada nombre de adjunto
            for j in adjuntos_linea:
                dict_adjuntos[j] = dict_adjuntos.get(j, 0) + 1

        # Ahora sí, procesamos todos los elementos del bloque
        for i in range(elem_actual, min(elem_actual + bloque_envios, total_elems)):
            # Envío del bloque

            # Destinatarios
            to = []
            for j in elems_lista[i][0].split(config['separador_subcampos']):
                to.append(To(j, p=pers_id))
            mensaje.to = to

            # Cc
            if elems_lista[i][2] != '':
                cc = []
                for j in elems_lista[i][2].split(config['separador_subcampos']):
                    cc.append(Cc(j, p=pers_id))
                mensaje.cc = cc

            # Bcc
            if elems_lista[i][3] != '':
                bcc = []
                for j in elems_lista[i][3].split(config['separador_subcampos']):
                    bcc.append(Bcc(j, p=pers_id))
                mensaje.bcc = bcc

            # Sustituciones
            if len(placeholders) > 0:
                sustituciones = []
                for k, v in placeholders.items():
                    sustituciones.append(Substitution(k, elems_lista[i][v], p=pers_id))
                mensaje.substitution = sustituciones

            # Adjuntos
            adjuntos_linea = elems_lista[i][1].split(config['separador_subcampos'])

            # Eliminamos posibles duplicados en la lista de adjuntos para esta línea
            adjuntos_linea = list(dict.fromkeys(adjuntos_linea))

            for j in adjuntos_linea:
                if dict_adjuntos.get(j, 0) < bloque_envios:
                    # No es un adjunto común a TODOS los elementos del bloque, así que lo procesamos aquí
                    # OJO: Parece que SendGrid NO permite enviar adjuntos diferenciados en cada personalización...
            pers_id += 1

        # Elementos comunes a todo el mensaje
        m=re.search(r'^([^<]+)<([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})>$', config['from_email'])
        mensaje.from_email = From(m.group(2), m.group(1).strip())
        mensaje.subject = Subject(asunto)
        mensaje.content = Content(MimeType.html, cuerpo)
        # Adjuntos comunes a todas las personalizaciones del bloque
        for k for k, v in dict_adjuntos.items() if float(v) = bloque_envios:
            # Procesamos cada adjunto común aquí

        # Bloque compilado. Hacemos el envío!
        try:
            print("[OK] Bloque enviado OK. Respuesta del servidor:")
            response = sendgrid_client.send(mensaje)
            print(response.status_code)
            print(response.body)
            print(response.headers)
        except Exception as e:
            print("[ERROR] Envío de bloque falló. Respuesta del servidor:")
            print(e)

        elem_actual = elem_actual + bloque_envios

elif config['engine'].upper() == "SMTP":
    # Envío por medio de SMTP regular

    envios_ok=0
    envios_error=0

    for i in elems_lista:
        elem_actual+=1
        elem_asunto=asunto
        elem_cuerpo=cuerpo
        for k, v in placeholders.items():
            # Hacemos las sustituciones de todos los placeholders
            elem_asunto=elem_asunto.replace(k, i[v])
            elem_cuerpo=elem_cuerpo.replace(k, i[v])
        print('['+str(elem_actual)+'/'+str(total_elems)+'] Enviando: '+i[0], end=' ')
        
        try:
            # Create the enclosing (outer) message
            outer = MIMEMultipart()
            outer['Subject'] = elem_asunto
            outer['To'] = i[0]
            rcpt=i[0].split(config['separador_subcampos'])
            if i[2]!='':
                outer['Cc'] = i[2]
                rcpt=rcpt+i[2].split(config['separador_subcampos'])
            if i[3]!='':
                outer['Bcc'] = i[3]
                rcpt=rcpt+i[3].split(config['separador_subcampos'])
            outer['From'] = config['from_email']
            #outer.preamble = 'You will not see this in a MIME-aware mail reader.\n'
            outer.attach(MIMEText(elem_cuerpo, 'html', 'UTF-8'))	# Sustituir plain por html y listo
            # Adjuntar una versión HTML del cuerpo es trivial:
            # http://stackoverflow.com/questions/882712/sending-html-email-in-python
            
            # El siguiente código analiza la lista de adjuntos, la codifica según corresponda tratando de asignarle el 
            # MIME Type correcto, y lo agrega al mensaje.
            for filename in i[1].split(config['separador_subcampos']):
                path = filename
                if not os.path.isfile(path):
                    continue
                # Guess the content type based on the file's extension.  Encoding
                # will be ignored, although we should check for simple things like
                # gzip'd or compressed files.
                ctype, encoding = mimetypes.guess_type(path)
                if ctype is None or encoding is not None:
                    # No guess could be made, or the file is encoded (compressed), so
                    # use a generic bag-of-bits type.
                    ctype = 'application/octet-stream'
                maintype, subtype = ctype.split('/', 1)
                if maintype == 'text':
                    fp = open(path)
                    # Note: we should handle calculating the charset
                    msg = MIMEText(fp.read(), _subtype=subtype)
                    fp.close()
                elif maintype == 'image':
                    fp = open(path, 'rb')
                    msg = MIMEImage(fp.read(), _subtype=subtype)
                    fp.close()
                elif maintype == 'audio':
                    fp = open(path, 'rb')
                    msg = MIMEAudio(fp.read(), _subtype=subtype)
                    fp.close()
                else:
                    fp = open(path, 'rb')
                    msg = MIMEBase(maintype, subtype)
                    msg.set_payload(fp.read())
                    fp.close()
                    # Encode the payload using Base64
                    encoders.encode_base64(msg)
                # Set the filename parameter
                msg.add_header('Content-Disposition', 'attachment', filename=filename)  # attachment para adjuntos regulares, inline para imágenes en el cuerpo del correo
                msg.add_header('Content-ID', '<'+filename+'>')  # Necesario para referenciar imágenes desde el cuerpo del correo
                outer.attach(msg)
            # Now send or store the message
            composed = outer.as_string()
            
            # Listo. Composed contiene el mensaje armado completo, como un string, listo para ser enviado.
            server = smtplib.SMTP(config['servidor_smtp'], timeout=config['timeout'])
            server.starttls()
            server.login(config['username'], config['password'])
            
            server.sendmail(config['from_email'], rcpt, composed)  # Esto considera múltiples recipientes
            server.quit()
            
            print('[OK]')    # Sólo se mostrará si el envío fue exitoso
            envios_ok=envios_ok+1

        except:
            print('[ERROR]', end=' ')
            print(sys.exc_info()[1])
            envios_error=envios_error+1

        finally:
            time.sleep(opts.delay)

    print("\nProceso completo.", envios_ok, "correos enviados OK,", envios_error, "correos con error.")
