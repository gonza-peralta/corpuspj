import pandas as pd
from nltk import word_tokenize
import re
import pymysql

#cargo sentencias del corpus
corpus = pd.read_csv("output.csv", delimiter=',', skip_blank_lines=True, encoding='utf-8')
corpus = corpus.drop_duplicates()
sentencias = corpus["sentencia"]
	
#función de devuelve si palabra no es alfanumérica
def is_non_alphanumeric(w):
    return re.match(r'^\W+$', w) != None

#conexión a la base de datos
conn = pymysql.connect(host='localhost', user='root', passwd='root', db='corpus_jurisprudencia', charset='utf8')
insert_sent = conn.cursor()
select = conn.cursor()
insert_fs = conn.cursor()
insert_frec = conn.cursor()

#inicializo numeradores
i_sent = 1
i_frec = 1
sent_words = []

#itero en las sentencias del corpus
for s in sentencias:
	#inserto la sentencia en la tabla de sentencias
    s = s.replace("'",'"')
    query = "INSERT INTO sentencias VALUES (" + str(i_sent) + ", '" + s + "' );"
    insert_sent.execute(query)
	#tokenizo la sentencia y me quedo con las palabras que son alfanuméricas
    sent_words = word_tokenize(s)
    sent_words2 = [w for w in sent_words if not is_non_alphanumeric(w)]
	#obtengo las frecuencias de las palabras (pasándolas a minúscula antes)
    freq_sentencias = nltk.FreqDist(w.lower() for w in sent_words2)
	#para cada palabra y su frecuencia
    for f_s in freq_sentencias.most_common():
		#ejecuto sentencia para ver si la palabra ya existe en la tabla de frecuencias
        query = 'select id, palabra, frecuencia from frecuencias where palabra = "' + f_s[0].replace("'",'"') + '"'
        select.execute(query)
        entro = False
        id_frec = 0
        for (id_f,pal,f) in select:
			#si la palabra existe en la tabla de frecuencias, sumo a la frecuencia de esa palabra la corresondiente a la sentencia actual
            entro = True
            nueva_frec = f + f_s[1]			
            query = 'update frecuencias set frecuencia = ' + str(nueva_frec) + ' where id = ' + str(id_f)
            insert_frec.execute(query)
            id_frec = id_f
        if not entro:
			#si la palabra no existe en la tabla de frecuencias la inserto
            query = "INSERT INTO frecuencias VALUES (" + str(i_frec) + ", '" + f_s[0].replace("'",'"') + "', " +   str(f_s[1]) + ")"
            insert_frec.execute(query)
            id_frec = i_frec
            i_frec = i_frec + 1
		#creo registro en la tabla frec_sentencias, con el identificador de la sentencia y la frecuencia de la palabra en la sentencia
        query = "INSERT INTO frec_sentencias VALUES (" + str(i_sent) + "," + str(id_frec) + "," +   str(f_s[1]) + ")"
        insert_fs.execute(query)
    i_sent = i_sent + 1
    conn.commit()
insert_sent.close()
select.close()
insert_fs.close()
insert_frec.close()
conn.close()
