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

def estudiante(desde, hasta):
    conn = None
    pais = "PE"
    region = "15" #Lima
    ciudad = "01" #Lima

    try:
        conn = psycopg2.connect(database='ubicate', user='postgres', password='sapguinario2019')
        cur = conn.cursor()

        for i in range(desde, hasta + 1):
            dni = dni_prefix + completar_ceros(i)
            edad = str(random.randint(17, 26)) #edad entre 1-26
            distrito = str(random.randint(17, 26)) #distrito entre 1-43

            cur.callproc('test.sp_estudiante_ins', (dni, dni, edad, "nombre " + str(i), "apellido " + str(i),
                                                    "direccion " + str(i), "F", pais, region, ciudad,
                                                    distrito))

        cur.close()
        conn.commit()
        print("Datos insertados en tabla estudiante")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None :
            conn.close()

def estudianteDiario(desde, hasta):
    conn = None
    lleva_food = True

    try:
        conn = psycopg2.connect(database='ubicate', user='postgres', password='sapguinario2019')
        cur = conn.cursor()

        for i in range(desde, hasta + 1):
            next_date = datetime.datetime.today() - datetime.timedelta(days=90)
            dni = dni_prefix + completar_ceros(i)

            for j in range(1, 60):
                next_date = next_date + datetime.timedelta(days=1)
                formated_date = next_date.strftime('%m-%d-%Y')
                #print(str(formated_date))
                dinero = str(random.randint(10, 70))  # dinero entre 10 y 70

                if random.randint(0, 2) == 0:
                    lleva_food = False
                else:
                    lleva_food = True

                cur.callproc('test.sp_test_estudi_diario_ins', (dni, str(formated_date), lleva_food, dinero))

        cur.close()
        conn.commit()
        print("Datos insertados en tabla estudiante_diario")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None :
            conn.close()

if __name__ == "__main__":
    #estudiaEn(1, 100000)
    #estudiante(69267, 100000)
    estudianteDiario(1001, 20000)
