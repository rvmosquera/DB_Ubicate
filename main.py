import psycopg2
import random
import datetime

dni_prefix = "200"
dataBase = "UbicateV2"

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
        conn = psycopg2.connect(database=dataBase, user='postgres', password='sapguinario2019')
        cur = conn.cursor()

        for i in range(desde, hasta + 1):
            dni = dni_prefix + completar_ceros(i)
            cod_uni = str(random.randint(1, 134)) #universidad desde 1-134
            cur.callproc('without_index.sp_estudia_en1m_ins', (dni, cod_uni))

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
        conn = psycopg2.connect(database=dataBase, user='postgres', password='sapguinario2019')
        cur = conn.cursor()

        for i in range(desde, hasta + 1):
            dni = dni_prefix + completar_ceros(i)
            edad = str(random.randint(17, 26)) #edad entre 1-26
            distrito = str(random.randint(17, 26)) #distrito entre 1-43

            cur.callproc('without_index.sp_estudiante1m_ins', (dni, dni, edad, "nombre " + str(i), "apellido " + str(i),
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
        conn = psycopg2.connect(database=dataBase, user='postgres', password='sapguinario2019')
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

                cur.callproc('without_index.sp_estudi_diario1m_ins', (dni, str(formated_date), lleva_food, dinero))

        cur.close()
        conn.commit()
        print("Datos insertados en tabla estudiante_diario")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None :
            conn.close()

def restaurant_ofrece(idRestdesde, idResthasta, idPlatodesde, idPlatoHasta):
    conn = None
    idRest=""

    try:
        conn = psycopg2.connect(database=dataBase, user='postgres', password='sapguinario2019')
        cur = conn.cursor()

        for i in range(idRestdesde, idResthasta + 1):
            idRest = str(i)

            for j in range(idPlatodesde, idPlatoHasta + 1):
                idPlato = str(j)

                cur.callproc('without_index.sp_ofrece1m_ins', (idRest, idPlato))

        cur.close()
        conn.commit()
        print("Datos insertados en tabla ofrece")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None :
            conn.close()

def pedido_restaurante(desde, hasta):
    conn = None
    idRest=""

    try:
        conn = psycopg2.connect(database=dataBase, user='postgres', password='sapguinario2019')
        cur = conn.cursor()
    #solo un pedido x restaurante
        for i in range(desde, hasta + 1):
            next_date = datetime.datetime.today() - datetime.timedelta(days=90)
            idRest = str(i)

            for j in range(1, 5):
                next_date = next_date + datetime.timedelta(days=1)
                formated_date = next_date.strftime('%m-%d-%Y')

                cur.callproc('without_index.pedido_rest1m', (idRest, str(j), str(formated_date), "14:00:00"))

        cur.close()
        conn.commit()
        print("Datos insertados en tabla pedido_rest")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None :
            conn.close()

def venta_restaurante(estuDesde, estuHasta, restDesde, restHasta):
    conn = None
    idRest=""

    try:
        conn = psycopg2.connect(database=dataBase, user='postgres', password='sapguinario2019')
        cur = conn.cursor()

        for i in range(estuDesde, estuHasta + 1): #estudiante diario
            next_date = datetime.datetime.today() - datetime.timedelta(days=90)
            dni = dni_prefix + completar_ceros(i)
            for k in range(restDesde, restHasta + 1):
                idRest = str(k)
                next_date = next_date + datetime.timedelta(days=1)

                for j in range(1, 3): #2 platos
                    id_plato = str(random.randint(1, 1000))  # entre 1 y 1k
                    # medio_pago = str(random.randint(1, 8))  # entre 1 y 8
                    medio_pago = str(j)

                    formated_date = next_date.strftime('%m-%d-%Y')

                    cur.callproc('without_index.sp_venta_rest1m_ins', (
                        dni, str(formated_date), idRest, '1', id_plato, medio_pago, 5.00))

        cur.close()
        conn.commit()
        print("Datos insertados en tabla venta_rest")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None :
            conn.close()

def venta(idTiendaDesde, idTiendaHasta, nroPedidos):
    conn = None

    try:
        conn = psycopg2.connect(database=dataBase, user='postgres', password='sapguinario2019')
        cur = conn.cursor()

        for i in range(idTiendaDesde, idTiendaHasta + 1): #Tiendas
            id_tienda = str(i)
            next_date = datetime.datetime.today() - datetime.timedelta(days=90)

            for k in range(1, nroPedidos + 1 ): #nro pedidos
                id_pedido = str(k)
                next_date = next_date + datetime.timedelta(days=1)

                formated_date = next_date.strftime('%m-%d-%Y')

                cur.callproc('without_index.sp_venta1m_ins', (
                    id_tienda, id_pedido, str(formated_date), "13:00:00" ))

        cur.close()
        conn.commit()
        print("Datos insertados en tabla venta")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None :
            conn.close()

def venta_a_realizar(idTiendaDesde, idTiendaHasta, nroPedidos, nroProductos):
    conn = None

    try:
        conn = psycopg2.connect(database=dataBase, user='postgres', password='sapguinario2019')
        cur = conn.cursor()

        for i in range(idTiendaDesde, idTiendaHasta + 1): #Tiendas
            id_tienda = str(i)
            next_date = datetime.datetime.today() - datetime.timedelta(days=90)

            dni = dni_prefix + completar_ceros(i+999)

            for k in range(1, nroPedidos + 1 ): #nro pedidos
                id_pedido = str(k)
                next_date = next_date + datetime.timedelta(days=1)

                for m in range(1, nroProductos + 1): #cantidad de productos
                    formated_date = next_date.strftime('%m-%d-%Y')
                    idProducto = str(random.randint(1, 10001))  # entre 1 y 10k1
                    cantidad = random.randint(1, 7)  # entre 1 y 7
                    medio_pago = random.randint(1, 3)  # entre 1 y 3
                    porcentaje = random.randint(1, 7)  # entre 1 y 7
                    cur.callproc('without_index.sp_venta_realizar1m_ins', (
                        id_tienda, id_pedido, idProducto, dni, str(formated_date), str(medio_pago),
                        porcentaje, cantidad))

        cur.close()
        conn.commit()
        print("Datos insertados en tabla venta a realizar")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None :
            conn.close()

def viajes(estuDesde, estuHasta, idTransDesde, idTransHasta, medioPago):
    conn = None
    idTransPrefix="T"

    try:
        conn = psycopg2.connect(database=dataBase, user='postgres', password='sapguinario2019')
        cur = conn.cursor()

        for i in range(estuDesde, estuHasta + 1): #estudiante diario
            next_date = datetime.datetime.today() - datetime.timedelta(days=90)
            dni = dni_prefix + completar_ceros(i)

            for k in range(1, 45): #dias
                next_date = next_date + datetime.timedelta(days=1)

                for j in range(1, 3): #2 viajes ida y vuelta
                    idTransp = idTransPrefix + str(random.randint(idTransDesde, idTransHasta))

                    formated_date = next_date.strftime('%m-%d-%Y')

                    duracion = 15 * random.randint(1, 8)
                    if j == 1:
                        hora = "08:00:00"
                    else:
                        hora = "19:00:00"

                    cur.callproc('without_index.sp_viaje1m_ins', (
                        idTransp, dni, str(formated_date), str(medioPago), str(j), str(formated_date),
                        hora, duracion))

        cur.close()
        conn.commit()
        print("Datos insertados en tabla viaje")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None :
            conn.close()


if __name__ == "__main__":
    #estudiante(69267, 100000)
    #estudiaEn(1, 100000)
    #estudianteDiario(20001, 30000)
    #pedido_restaurante(23, 24023)
    #venta_restaurante(17001, 20000, 9000, 9041)
    #viajes(11001, 13000, 107, 108, 2)
    #restaurant_ofrece(23, 200, 1, 49996)
    #venta(1, 3160, 50)
    venta_a_realizar(1001, 3160, 50, 10)
