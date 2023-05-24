from time import time
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Creamos el directorio Tarea_1/data/shakespeare
data_dir = Path("data") / "shakespeare"
data_dir.mkdir(parents=True, exist_ok=True)


def load_table(table_name, engine):
    """
    Leer la tabla con SQL y guardarla como CSV,
    o cargarla desde el CSV si ya existe
    """
    path_table = data_dir / f"{table_name}.csv"
    if not path_table.exists(): 
        print(f"Consultando tabla con SQL: {table_name}")
        t0 = time()
        df_table = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        t1 = time()
        print(f"Tiempo: {t1 - t0:.1f} segundos")

        print(f"Guardando: {path_table}\n")
        df_table.to_csv(path_table)
    else:
        print(f"Cargando tabla desde CSV: {path_table}")
        df_table = pd.read_csv(path_table, index_col=[0])
    return df_table


print("Conectando a la base...")
conn_str = "mysql+pymysql://guest:relational@relational.fit.cvut.cz:3306/Shakespeare"
engine = create_engine(conn_str)

# DataFrame con todas las obras:
df_works = load_table("works", engine)

# # # Todos los párrafos de todas las obras
df_paragraphs = load_table("paragraphs", engine)
# print(df_paragraphs)

# # # Todos los capitulos de todas las obras
df_chapters = load_table("chapters", engine)

# # # Todos los personajes de todas las obras
df_characters = load_table("characters", engine)

def clean_text(df, column_name):
    # Convertir todo a minúsculas
    result = df[column_name].str.lower()

    # Quitar signos de puntuación y cambiarlos por espacios (" ")
    # TODO: completar signos de puntuación faltantes
    for punc in ["[", "\n", ",", ".", ";", "]", "'", "?", "!", '"', ":", "-", "(", ")"]:
        result = result.str.replace(punc, " ")
        # result = replace_substring(result, punc, " ")
    
    #result = replace_substring(result, "'s", " is")
    #result = replace_substring(result, "'ll", " will")
    #result = replace_substring(result, "'re", " are")

    return result

#def replace_substring(string, old_value, new_value):
    #return string.str.replace(old_value, new_value)

# Creamos una nueva columna CleanText a partir de PlainText
df_paragraphs["CleanText"] = clean_text(df_paragraphs, "PlainText")

# Veamos la diferencia
df_paragraphs[["PlainText", "CleanText"]]

# Convierte párrafos en listas "palabra1 palabra2 palabra3" -> ["palabra1", "palabra2", "palabra3"]
df_paragraphs["WordList"] = df_paragraphs["CleanText"].str.split()

# Veamos la nueva columna creada
# Notar que a la derecha tenemos una lista: [palabra1, palabra2, palabra3]
df_paragraphs[["CleanText", "WordList"]]

# Nuevo dataframe: cada fila ya no es un párrafo, sino una sóla palabra
df_words = df_paragraphs.explode("WordList")

# Quitamos estas columnas redundantes
df_words.drop(columns=["CleanText", "PlainText"], inplace=True)

# Renombramos la columna WordList -> word
df_words.rename(columns={"WordList": "word"}, inplace=True)

# Verificar que el número de filas es mucho mayor
# print(df_words.to_string())

# Agregamos el nombre de los personajes
# TODO: des-comentar luego de cargar df_characters
df_words = pd.merge(df_words, df_characters[["id", "CharName"]], left_on="character_id", right_on="id")

# TODO:
# - des-comentar luego de hacer el merge
# - Encuentra algún problema en los resultados?

words_per_character = df_words.groupby("CharName")["word"].count().sort_values(ascending=False)
char_show = words_per_character[:10]
print(char_show)
    
df_words_filtered = df_words.drop(df_words[df_words["CharName"].isin(['Poet', '(stage directions)'])].index)
words_per_character = df_words_filtered.groupby("CharName")["word"].count().sort_values(ascending=False)
char_show = words_per_character[:10]
print(char_show)



# print(char_show)
fig = plt.figure(figsize = (18, 13))
plt.bar(char_show.index, char_show.values, color = 'skyblue')
_ = plt.xticks(rotation=90)
plt.savefig('words_per_character.png')
# plt.show()
plt.clf()


words_per_gen = df_works.groupby("GenreType")["Title"].count().sort_values(ascending=False)
gen_show = words_per_gen
# print(char_show)
fig = plt.figure(figsize = (8, 8))
plt.bar(gen_show.index, gen_show.values, color = 'skyblue')
plt.savefig('works_per_gen.png')
plt.clf()


works_per_year = df_works.groupby("Date")["Title"].count()
works_per_year

# Obras a lo largo de los años
work_show = works_per_year
fig = plt.figure(figsize = (8, 8))
plt.bar(work_show.index, work_show.values, color = 'skyblue')
plt.savefig('works_per_year.png')
plt.clf()

# years = []

# for index, row in df_words.iterrows():
#     if row['GenreType'] not in genres:
#         genres.append(row['GenreType'])



words_count = df_words.groupby("word")["word"].count().sort_values(ascending=False)
word_show = words_count[:20]
print(word_show)

df_words_filtered_conector = df_words.drop(df_words[df_words["word"].isin(['the', 'and', 'i', 'to', 'of', 'a', 'you', 'my', 'that', 'in', 'is', 'not', 'with', 'for', 'me', 'it', 's', 'his', 'be', 'he', 'this', 'your', 'but', 'd', 'have', 'as', 'thou', 'him', 'so', 'will', 'what', 'her', 'thy', 'all', 'by', 'do', 'no', 'we', 'shall', 'if', 'are', 'thee', 'on', 'o', 'our', 'she', 'from', 'they', 'at', 'or', 'which', 'll', 'here', 'would', 'was', 'then', 'there', 'their', 'how', 'am', 'when', 'them', 'than', 'an', 'one', 'did', 'may'])].index)
words_count = df_words_filtered_conector.groupby("word")["word"].count().sort_values(ascending=False)
word_show = words_count[:20]
print(word_show)
fig = plt.figure(figsize = (15,8))
plt.bar(word_show.index, word_show.values, color = 'skyblue')
plt.savefig('words_count.png')
plt.clf()

#archivo-salida.py
f = open ('resultado.txt','w')
f.write(words_per_character.to_string())
f.close()