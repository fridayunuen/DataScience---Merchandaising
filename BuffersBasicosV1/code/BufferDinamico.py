import math
#Funciones
def condiciones(INV_BUFFER):
    cond_superior = 66
    cond_inferior = 33
    if INV_BUFFER > cond_superior:
        return 'BUFFER ALTO'
    elif INV_BUFFER < cond_inferior:
        return 'BUFFER BAJO'
    else:
        return 'BUFFER OK'

def sigmoid(INV_BUFFER):    
    rango = 1.3
    aceleracion = 4.9
    cruce = 0.6

    return rango*(1-1/(1+math.exp(-aceleracion*((INV_BUFFER/100)-cruce))))-rango/2

def redondear(numero, multiplo):
    return round(numero / multiplo) * multiplo

def buffer_dinamico(Inventario_Actual, Buffer, Ventas):
    #print(Inventario_Actual, Ventas)

    INV_BUFFER = round(Inventario_Actual / Buffer*100,1)
    
    try:
        WOS = round(Inventario_Actual/Ventas,1)
    except:
        WOS = 0
            
    Nota_Buffer = condiciones(INV_BUFFER)
    Incremento_buffer = sigmoid(INV_BUFFER)
    DB_Profundidad = Buffer*(1+Incremento_buffer)
    DB_Profundidad = round(DB_Profundidad, 0)
    INV_BUFFER = str(INV_BUFFER) + '%'
    #Incremento_buffer = Incremento_buffer.astype(str) + '%'
    return INV_BUFFER, Nota_Buffer, Incremento_buffer, DB_Profundidad
