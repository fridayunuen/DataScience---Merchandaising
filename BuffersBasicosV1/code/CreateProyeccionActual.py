import datetime
import numpy as np
import pandas as pd
import numpy as np
import send_email as se

# INPUTS ------------------------------------------------
BU=input('''Selecciona la BU: BEAUTY JEW ACC CALZADO ROPA ''') 

#def CreateProyeccionActual(BU):

div =   {'BEAUTY': "[Division Code] = 'BEAUTY'", 
            'JEW': "[Division Code] IN ('W-JEW', 'M-JEW')",
            'ACC': "[Division Code] IN ('W-ACC', 'M-ACC')",
        'CALZADO':"[Division Code] in ('W-SHO','M-SHO')",
           'ROPA':"[Division Code] in ('W-CLO','M-CLO')"} 


today = datetime.date.today()
#today = datetime.date(2023, 2, 10)
end_date = str(today)
fecha_analizada = datetime.datetime.strptime(end_date, '%Y-%m-%d')

print('Buffers Basicos')
print("Fecha: " +end_date +"    Semana: "+fecha_analizada.strftime("%V"))
print("En: "+str(div[BU]))

week_actual = str(int(str(fecha_analizada.strftime("%V"))))

carpeta_input = R'S:\BI\3. MERCHANDISING\FRAMEWORK\FRAMEWORK\ZIMA\BuffersBasicosV1\input'
carpeta_output = R'S:\BI\3. MERCHANDISING\FRAMEWORK\FRAMEWORK\ZIMA\BuffersBasicosV1\output'

# Determinando que clase de proyeccion se va a utilizar -------------------------------------------------
def sumar_semanas(week, semanas):
    if (week + semanas) > 52:        
        return np.concatenate((np.array(range(week, 52)),np.array(range(1, (week + 8) - 52))))
        
    elif (week + semanas) < 1:
        return np.concatenate((np.array(range(52 + semanas, 52+1)),np.array(range(1, week)))).astype(int)
    else:
        return np.array(range(week, week + semanas))

# 4 semanas anteriores --- hoy --- proyeccion 7- 8 semanas        
rango_calculo = np.concatenate((sumar_semanas(int(week_actual),-4),sumar_semanas(int(week_actual),8)))

tipo_proyeccion = ''
for i in rango_calculo:
    if i in [48, 49, 50, 51, 52, 1, 2, 53]:    
        #print('Proyeccion Actual realizada con base en el año anterior')
        tipo_proyeccion = 'Year_Anterior'
        break

if tipo_proyeccion == 'Year_Anterior':
    import TemporadaAlta as TA
    df_producto_tienda = TA.crea_proyeccion_actual(BU, today, div, carpeta_input, carpeta_output)

if tipo_proyeccion == '':
    import TemporadaNormal as TN
    print(tipo_proyeccion)
    df_producto_tienda = TN.crea_proyeccion_actual(BU, today, div, carpeta_input, carpeta_output)

    
# Confirmación enviada por correo -----------------------
fecha = datetime.datetime.now().strftime("%Y-%m-%d")
hora = datetime.datetime.now().strftime("%H:%M:%S")

mensaje = fecha + ' '+ hora + ' '+ BU
se.envia_correo('Proyeccion actual generada con exito', mensaje, 'CredencialesCorreo.json')

print('Proceso concluido con exito')