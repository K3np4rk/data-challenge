import pandas as pd
import mysql.connector as mysql 
from mysql.connector import Error

conn = None

try:
    
    print('criando conexão') #LOG

    conn = mysql.connect(host='localhost', user='admin', password='123@Mudar')
    
    print('conexao ok') #LOG

    cursor = conn.cursor()

    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute('CREATE DATABASE IF NOT EXISTS DATA_CHALLENGE')
        cursor.execute('USE DATA_CHALLENGE;')

    print('db criado') #LOG

    print('criando tabela') #LOG

    with open('/home/higor/Documentos/src/create_table.SQL', 'r') as file:
        CreateTable = file.read()

    if conn.is_connected():
        cursor.execute('DROP TABLE IF EXISTS MICRODADOS_ENEM;')
        cursor.execute(CreateTable)

    print('Tabela criada') #LOG

    with open('/home/higor/Documentos/data-challenge/src/insert_table.sql', 'r') as file:
        sql = file.read()
     
    print('inserindo dados') #LOG   

    for bloco in pd.read_csv('/home/higor/Documentos/dados/DADOS/MICRODADOS_ENEM_2020.csv', sep=';', encoding='ISO-8859-1', chunksize = 10000):
        
        bloco = bloco.fillna(0)    
    
        for i,row in bloco.iterrows():
            cursor.execute(sql, tuple(row))
            
            # if i % 10000 == 0:
            #     conn.commit()

        conn.commit()

    with open('/home/higor/Documentos/data-challenge/src/media_nota_estado.sql', 'r') as file:
         media_nota = file.read()

    cursor.execute('USE DATA_CHALLENGE;')
    cursor.execute(media_nota)
    media_nota = cursor.fetchall()
    print(media_nota)


    with open('/home/higor/Documentos/data-challenge/src/precentual_acerto_habilidade.sql', 'r') as file:
         percent_aceto = file.read()

    cursor.execute('USE DATA_CHALLENGE;')
    cursor.execute(percent_aceto)
    percent_aceto = cursor.fetchall()
    print(percent_aceto)



except Error as e:
    print("Erro de conexão", e)

finally:
    if conn:
        conn.close()

    print('conexão fechada')
    


