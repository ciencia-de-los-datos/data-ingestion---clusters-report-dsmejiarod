"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    #
    # Inserte su código aquí
    #
    import re
    import pandas as pd
    def corregir_datos(datos):
      datos = [re.sub(' {3,7}', '#', row) for row in datos]
      datos = [row.split('#') for row in datos]
      datos = [value for row in datos for value in row if len(value) >= 1]
      return datos
    def volver_estructura(datos):
      texto  = datos[3:]
      texto = [t.replace('\n','')  for t in texto]
      texto = " ".join(texto)
      texto = texto.split(',')
      texto = [row.strip()  for row in texto ]
      texto = [re.sub(' {1,7}', ' ', row)  for row in texto ]
      texto = [row.replace('.','')  for row in texto ]
      texto = ", ".join(texto)
      datos[2] = datos[2].replace(',','.')
    
      datos = [int(datos[0].strip())] + [int(datos[1].strip())] + [float(datos[2].replace('%','').strip())] + [texto]
      return datos


    l = []
    with open('clusters_report.txt', mode='r') as text_file:
        for line in text_file:
            l.append(line)

    encabezados = l[0:3]
    encabezados = [row.replace('  ','#') for row in encabezados ]
    encabezados = [row.replace('\n','#') for row in encabezados ]
    encabezados = [row.split('#') for row in encabezados ]
    encabezados.pop(2)
    encabezados[0][1] = encabezados[0][1] + encabezados[1][4]
    encabezados[0][3] = encabezados[0][3] + ' ' + encabezados[1][5]
    encabezados.pop(1)
    encabezados = [row.strip()  for row in encabezados[0] if len(row) > 1]
    encabezados = [row.replace(' ','_')  for row in encabezados]
    encabezados = [row.lower()  for row in encabezados]
    datos = l[4:]
    espacios_entre_datos = []
    for n in range(len(datos)):
      if datos[n].strip() == '':
        espacios_entre_datos.append(n)
    espacios_entre_datos.append(70)
    cont = 0
    for i in espacios_entre_datos:
      if cont == 0:
        globals()[ "datos"+"_"+str(cont+1)] = datos [0:i]
        cont +=1
      else: 
        globals()[ "datos"+"_"+str(cont+1)] = datos [espacios_entre_datos[cont-1]+1:i]
        cont +=1
        #print(cont)
    datos_completo = []
    for i in range (1,cont):
      y = corregir_datos(globals()[f"datos_{i}"])
      x = volver_estructura(y)
      datos_completo.append(x)
    datos_fin = pd.DataFrame(datos_completo, columns = encabezados)
    datos_fin
    return datos_fin
