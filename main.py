import psycopg2
import random
import datetime

dni_prefix = "200"

def completar_ceros(n):
    num = str(n)
    len_num = len(num)

    for x in range(1,5-len_num+1):
        num = "0" + num

    return num

def estudiaEn(desde, hasta):
    cod_uni = "" #codigo universidad

    conn = None
    try:
        conn = psycopg2.connect(database='ubicate', user='postgres', password='sapguinario2019')
        cur = conn.cursor()

        for i in range(desde, hasta + 1):
            dni = dni_prefix + completar_ceros(i)
            cod_uni = str(random.randint(1, 134)) #universidad desde 1-134
            cur.callproc('test.SP_TEST_ESTUDIA_EN', (dni, cod_uni))

        cur.close()
        conn.commit()
        print("Datos insertados en tabla estudia_en")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally :
        if conn is not None :
            conn.close()

def estudianteDiario():
    next_date = datetime.datetime.today() + datetime.timedelta(days=1)
    formated_date = next_date.strftime('%d-%m-%Y')
    print("next date  " +  str(formated_date))

if __name__ == "__main__":
    estudiaEn(5001, 11458)