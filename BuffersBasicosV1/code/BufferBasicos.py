BU=input('''Selecciona la BU: BEAUTY JEW ACC CALZADO ROPA ''') 

#def CreateProyeccionActual(BU):

div =   {'BEAUTY': "[Division Code] = 'BEAUTY'", 
            'JEW': "[Division Code] IN ('W-JEW', 'M-JEW')",
            'ACC': "[Division Code] IN ('W-ACC', 'M-ACC')",
        'CALZADO':"[Division Code] in ('W-SHO','M-SHO')",
           'ROPA':"[Division Code] in ('W-CLO','M-CLO')"} 

import pandas as pd
import os
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import numpy as np
import datetime
import json

# Credenciales de SQL Server
with open('Credenciales.json') as f:
   credenciales = json.load(f)

today = datetime.date.today()
#today = datetime.date(2023, 2, 10)
end_date = str(today)
fecha_analizada = datetime.datetime.strptime(end_date, '%Y-%m-%d')

print('Buffers Basicos')
print("Fecha: " +end_date +"    Semana: "+fecha_analizada.strftime("%V"))
print("En: "+str(div[BU]))

week_actual = str(int(str(fecha_analizada.strftime("%V")))-1)
WK2 = "'"+ week_actual +"'"
YEAR_ACTUAL = str(fecha_analizada.year)

carpeta_input = R'S:\BI\3. MERCHANDISING\FRAMEWORK\FRAMEWORK\ZIMA\BuffersBasicosV1\input'
carpeta_output = R'S:\BI\3. MERCHANDISING\FRAMEWORK\FRAMEWORK\ZIMA\BuffersBasicosV1\output'

# Consultando predicción actual (método con el que se calcula actuaqlmente ), esta se realiza cada semana-----------------
df_producto_tienda = os.path.join(carpeta_output, 'df_producto_tienda_'+BU+'.csv')
df_producto_tienda = pd.read_csv(df_producto_tienda)
df_producto_tienda['SKU'] = df_producto_tienda['SKU'].astype(str)

# Conexion SQL Server--------------------------------------------------------------------------------------
connection_string = 'DRIVER={SQL Server};SERVER=Shjet-prod;DATABASE=Allocations;UID='+ credenciales['usuario'] +';PWD='+credenciales['password']
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine = create_engine(connection_url)
conn = engine.connect()

# Agregando el inventario actual -------------------------------------------
print('Consultando inventario actual... ')

query_inventarioActual = '''
		SELECT CONCAT(   SUBSTRING(CONCAT([Item No_], [VariantColor]),1,10) , [Location Code]) AS [ID]
			,CAST(SUM([Inventario]) AS INT) AS [Inventario_Actual]
		
		FROM PadresClones_Basicos AS pcb
		JOIN  [Flash_BC].[dbo].[InvHistBC] AS inv
		ON SUBSTRING(CONCAT(inv.[Item No_], inv.[VariantColor]),1,10) = pcb.VarianteColor


		WHERE '''+ str(div[BU]) +'''
			
			AND [WK] = '''+str(WK2)+'''
			
			AND [year] = '''+str(YEAR_ACTUAL)+'''
			
			-- Seleccionando solo Tiendas y E002
			AND (SUBSTRING([Location Code], 1, 1) = 'E' 
						OR	
				SUBSTRING([Location Code], 1, 1) = 'T')

		GROUP BY CONCAT(SUBSTRING(CONCAT([Item No_], [VariantColor]),1,10), [Location Code])
	'''

df_inventarioActual = pd.read_sql(query_inventarioActual, con=conn)

df_producto_tienda = pd.merge(df_producto_tienda, df_inventarioActual, on='ID', how='left')
df_producto_tienda['Inventario_Actual'] = df_producto_tienda['Inventario_Actual'].fillna(0)
if len(df_producto_tienda[df_producto_tienda['Proyeccion_Actual'].isna()]) >0:
    print('Faltan datos para realizar la proyeccion actual, son sustituidos por 0')
    df_producto_tienda['Proyeccion_Actual'] = df_producto_tienda['Proyeccion_Actual'].fillna(0)

# Este es un input
Minimos_Maximos = pd.read_excel(os.path.join(carpeta_input, "Minimos_Maximos.xlsx"), dtype=str)
Minimos_Maximos["SKU"] = Minimos_Maximos["Original_Vendor_Item_No"].astype(str) + Minimos_Maximos["Code_Color"].astype(str)
Minimos_Maximos = Minimos_Maximos[["SKU", "Multiplo_Distribucion","Minimo_Tienda", "Maximo_Tienda"]]
Minimos_Maximos[['Multiplo_Distribucion', 'Minimo_Tienda', 'Maximo_Tienda']] = Minimos_Maximos[['Multiplo_Distribucion', 'Minimo_Tienda', 'Maximo_Tienda']].astype(int)

df_producto_tienda = pd.merge(df_producto_tienda, Minimos_Maximos, on='SKU', how='left')

# Calculo de buffer
# Redondear a multiplo de distribución deja el multiplo mas cercano 
def roundBy(x, base, iferror):
    try:
        rnd = base*round(x/base)
        return rnd
    except:

        return iferror
# Regla creada por el equipo de merch 
def crear_nuevo_buffer(cobertura_ventas, minimo_tienda, maximo_tienda):
    
    if cobertura_ventas <= minimo_tienda:
        cobertura_ventas = minimo_tienda

    if cobertura_ventas > maximo_tienda:
        cobertura_ventas = maximo_tienda 
    
    return cobertura_ventas

# Calculando buffer con el método actual
df_producto_tienda["Cobertura_ventas"] = df_producto_tienda.apply(lambda x: roundBy(x["Proyeccion_Actual"],x["Multiplo_Distribucion"], x["Multiplo_Distribucion"]), axis=1)
df_producto_tienda["Nuevo_Buffer"] = df_producto_tienda.apply(lambda x: crear_nuevo_buffer(x["Cobertura_ventas"], x["Minimo_Tienda"], x["Maximo_Tienda"]), axis=1)
df_producto_tienda['Ventas_Suma'] = df_producto_tienda['Ventas_Suma'].astype(int)
df_producto_tienda['Proyeccion_Actual'] = df_producto_tienda['Proyeccion_Actual'].astype(int)
df_producto_tienda['Inventario_Actual'] = df_producto_tienda['Inventario_Actual'].astype(int)

if df_producto_tienda['Multiplo_Distribucion'].isna().sum()>0: 
    print('ERROR: Existen productos sin información de Multiplo de distribución Minimos o Máximos')
    print(df_producto_tienda[df_producto_tienda['Multiplo_Distribucion'].isna()]['SKU'].unique())
    exit()

df_producto_tienda['Nuevo_Buffer'] = df_producto_tienda['Nuevo_Buffer'].astype(int)

#df_producto_tienda['Proyeccion_RNN'] = df_producto_tienda['Proyeccion_RNN'].astype(int)
#df_producto_tienda['Nuevo_Buffer_RNN'] = df_producto_tienda['Nuevo_Buffer_RNN'].astype(int)
#df_producto_tienda["Cobertura_ventas_RNN"] = df_producto_tienda.apply(lambda x: roundBy(x["Proyeccion_RNN"]*x["Cobertura"],x["Multiplo_Distribucion"], x["Multiplo_Distribucion"]), axis=1)
#df_producto_tienda["Cobertura_ventas_RNN"] = df_producto_tienda.apply(lambda x: roundBy(x["Proyeccion_RNN"],x["Multiplo_Distribucion"], x["Multiplo_Distribucion"]), axis=1)
#df_producto_tienda["Nuevo_Buffer_RNN"] = df_producto_tienda.apply(lambda x: crear_nuevo_buffer(x["Cobertura_ventas_RNN"], x["Minimo_Tienda"], x["Maximo_Tienda"]), axis=1)




data_tiendas = pd.read_excel(os.path.join(carpeta_input, "DATOS DE TIENDAS PBI 221206.xlsx"))
# Agregando todos las caracterísiticas de la tienda ------------------------
data_tiendas= data_tiendas[['NO', 'TIENDA', 'CLIMA', 'SH MAN', 'CANDY']]
data_tiendas.columns = ['Location Code', 'Tienda', 'Clima', 'ShMan', 'Candy']
df_producto_tienda = pd.merge(df_producto_tienda, data_tiendas, on='Location Code', how='left')
# Agregando caracteristicas producto ---------------------------------------
print("Consultando clones... ")
query_clones = '''
SELECT 
	SUBSTRING(CONCAT([Original Vendor Item No_], [Variant Code]), 1, 10) AS [SKU],
	IIF([No_] = '',
				SUBSTRING(CONCAT([Original Vendor Item No_], [Variant Code]), 1, 10),
				SUBSTRING(CONCAT([No_], [Variant Code]), 1, 10)
				) AS [No_], 
				
						pcb.[Division Code],
						[Description], 
						[Product Group Code], 
						[Item Category Code],
						[Weather]
				
	FROM   PadresClones_Basicos AS pcb
	JOIN [Allocations].[dbo].[Item_BC] 
	ON pcb.VarianteColor = SUBSTRING(CONCAT([No_], [Variant Code]), 1, 10)

	WHERE pcb.'''+ str(div[BU]) +'''

	ORDER BY [SKU]
	'''
	
df_clones = pd.read_sql_query(query_clones, conn)
df_clones.drop_duplicates(subset ="SKU", keep = 'first', inplace = True)
df_producto_tienda = pd.merge(df_producto_tienda, df_clones, on='SKU', how='left')

# ON ORDER ----------------------------------------------------------------------------------
end_date = str(today )
end_date = "'"+end_date+"'"

# llegará mercancia durante el tiempo de cobertura? 

cobertura_onorder = str(today +  pd.DateOffset(days=8*7))
cobertura_onorder = cobertura_onorder.replace(" 00:00:00","")
cobertura_onorder = "'"+cobertura_onorder+"'"

print('Consultando On Order...')
query_onOrder = '''
    SELECT [VarianteColor] AS [No_], SUM([Qtty Pen]) AS [Qtty Pen]
    FROM [Allocations].[dbo].[PadresClones_Basicos] AS pcb
    JOIN SH_REPORTS.dbo.SH_OnOrderBC_vw as OnOrder
    ON pcb.VarianteColor = SUBSTRING( CONCAT([Item], [Variant Code]),1,10)
    WHERE  pcb.'''+ str(div[BU]) +'''
    AND [Expected Receipt Date] BETWEEN '''+end_date+''' AND '''+cobertura_onorder+'''
    AND [Qtty Pen] IS NOT NULL
    GROUP BY [VarianteColor]

'''
df_onorder = pd.read_sql(query_onOrder, con=conn)

df_onorder = pd.merge(df_clones, df_onorder,on='No_', how='left')
df_onorder['Qtty Pen'] = df_onorder['Qtty Pen'].fillna(0)
df_onorder = df_onorder[['SKU', 'Qtty Pen']]
df_onorder = df_onorder.groupby('SKU').sum().reset_index()
df_producto_tienda = pd.merge(df_producto_tienda, df_onorder, on='SKU', how='left')

# MATRIX paa poder jalar REST LEAN ----------------------------------------------------------------------------------
print('Consultando matrices y res lean...')
query_matrix = '''
SELECT 
       [Product Group Code] COLLATE SQL_Latin1_General_CP1_CI_AS AS [Product Group Code],
       [Matriz] COLLATE SQL_Latin1_General_CP1_CI_AS AS [Matriz]   
      
  FROM [Allocations].[dbo].[Matrix]
  '''

df_matrix = pd.read_sql(query_matrix, con=conn)
matrix = pd.read_excel(r"S:\\BI\\3. MERCHANDISING\\FRAMEWORK\\FRAMEWORK\\ZIMA\\Tabla Matrices.xlsx",skiprows = 1)
matrix = matrix[matrix["TIPO DE RESURTIDO"] == "RES LEAN"]
matrix = matrix[["MATRIZ", "MAX"]]
matrix.columns = ["Matriz", "RES_LEAN"]

df_matrix = pd.merge(df_matrix, matrix, on='Matriz', how='left')
df_producto_tienda = pd.merge(df_producto_tienda, df_matrix, on='Product Group Code', how='left')

# Obteniendo el buffer actual ----------------------------------------------------------------------------------
def get_bufferanterior(file_excel):
        
    col_names =  []
    for i in range(len(file_excel.iloc[:,0].values)):
        #if file_excel.iloc[i,0]=="No.":
        if file_excel.iloc[i,0]=="Location Code":
            index = i
            break
    col_names = file_excel.iloc[index].values
    col_names = [x.replace('B-','') for x in col_names]
    file_excel = file_excel.iloc[index+1:]
    file_excel.columns = col_names
    file_excel = file_excel.reset_index(drop=True)

    df = pd.DataFrame(columns=["Location Code", "SKU", "Buffer Actual"])

    # Estoy segura de que hay una forma más elegante de hacer esto, pero no la encontré se puede mejorar, mientras tanto, usamos un ciclo for
    items = file_excel.loc[:,"-":].columns
    items = items[1:]
    items = [x for x in items if str(x) != '']
    # delete everything not numeric value 
    items = [x for x in items if str(x).isnumeric()]
    # drop duplicates
    items = list(dict.fromkeys(items))

    # Si hay columnas duplicadas se eliminan directamente, deberiamos validarlo? 
    file_excel = file_excel.loc[:,~file_excel.columns.duplicated()]

    for item in items:

        SKU = [item] * len(file_excel["Location Code"])
        Loc = file_excel["Location Code"].values
        df1 = pd.DataFrame({"Location Code": Loc, "SKU": SKU})
        df1["Buffer Actual"] = file_excel[item].values
        df = pd.concat([df, df1])
        df = df.reset_index(drop=True)

    df["ID"] = df["SKU"] + df["Location Code"] 
    df = df[["ID", "Buffer Actual"]]
    df["Buffer Actual"] = df["Buffer Actual"].fillna(0)
    df["Buffer Actual"] = df["Buffer Actual"].apply(lambda x: int(x))
    df["Buffer Actual"] = df["Buffer Actual"].apply(lambda x: round(x))

    return df

print('Obteniendo el buffer actual.... ')
if BU == 'BEAUTY' or BU == 'JEW' or BU == 'ACC':
    acces = pd.read_excel("S:/BI/3. MERCHANDISING/FRAMEWORK/FRAMEWORK/ZIMA/Framework de Tiendas 2.0.xlsx", sheet_name='BASICOS ACCESORIOS')
    df_buffers = get_bufferanterior(acces)
   
if BU == "ROPA":
    dama = pd.read_excel("S:/BI/3. MERCHANDISING/FRAMEWORK/FRAMEWORK/ZIMA/Framework de Tiendas 2.0.xlsx", sheet_name='BASICOS DAMA')
    man = pd.read_excel("S:/BI/3. MERCHANDISING/FRAMEWORK/FRAMEWORK/ZIMA/Framework de Tiendas 2.0.xlsx", sheet_name='BASICOS MAN')
    df_buffers = pd.concat([get_bufferanterior(dama), get_bufferanterior(man)]).reset_index(drop=True)

if BU == "CALZADO":
    calzado = pd.read_excel("S:/BI/3. MERCHANDISING/FRAMEWORK/FRAMEWORK/ZIMA/Framework de Tiendas 2.0.xlsx", sheet_name='BASICOS CALZADO')
    df_buffers = get_bufferanterior(calzado)    

df_producto_tienda = pd.merge(df_producto_tienda, df_buffers, on='ID', how='left')

# KPI que utiliza merch --------------------------------------------------------------------------------------------------------------
df_producto_tienda["Rotacion_Proyectada"] = df_producto_tienda["Nuevo_Buffer"]/ df_producto_tienda["Ventas_Promedio"]
df_producto_tienda["Rotacion_Proyectada"].replace([np.inf, -np.inf], np.nan, inplace=True)
df_producto_tienda["WOS"] = df_producto_tienda["Inventario_Actual"] / df_producto_tienda["Ventas_Promedio"]


df_producto_tienda[["Multiplo_Distribucion", "Ventas_Promedio", "Cobertura", "Minimo_Tienda", "Maximo_Tienda"]] = df_producto_tienda[["Multiplo_Distribucion", "Ventas_Promedio", "Cobertura", "Minimo_Tienda", "Maximo_Tienda"]].astype(float)
df_producto_tienda["Diferencia_Buffer"] = df_producto_tienda["Nuevo_Buffer"] - df_producto_tienda["Buffer Actual"] 
df_producto_tienda["Diferencia_Buffer_Caja"]  =  df_producto_tienda["Diferencia_Buffer"] /df_producto_tienda["Multiplo_Distribucion"]


# Lo dejo por separado para consultar si merch propone una diferencia diferente a 10 cajas por BU
if div[BU] == "[Division Code] in ('W-CLO','M-CLO')": 
    df_producto_tienda["Flag_CambioBuffer_Caja"] = np.where(abs(df_producto_tienda["Diferencia_Buffer_Caja"]) >= 10, "Revisar", "Ok")
if div[BU] == "[Division Code] = 'BEAUTY'":
    df_producto_tienda["Flag_CambioBuffer_Caja"] = np.where(abs(df_producto_tienda["Diferencia_Buffer_Caja"]) >= 10, "Revisar", "Ok")
if div[BU] == "[Division Code] IN ('W-JEW', 'M-JEW')":
    df_producto_tienda["Flag_CambioBuffer_Caja"] = np.where(abs(df_producto_tienda["Diferencia_Buffer_Caja"]) >= 10, "Revisar", "Ok")
if div[BU] == "[Division Code] in ('W-SHO','M-SHO')":
    df_producto_tienda["Flag_CambioBuffer_Caja"] = np.where(abs(df_producto_tienda["Diferencia_Buffer_Caja"]) >= 10, "Revisar", "Ok")
if div[BU] == "[Division Code] IN ('W-ACC', 'M-ACC')":
    df_producto_tienda["Flag_CambioBuffer_Caja"] = np.where(abs(df_producto_tienda["Diferencia_Buffer_Caja"]) >= 10, "Revisar", "Ok")





# El resuemn incluye los productos agrupados por SKU, ya no hay desglose por tienda
#resumen= df_producto_tienda[['SKU', 'Ventas_Suma','Proyeccion_RNN','Proyeccion_Actual', 'Inventario_Actual', "Nuevo_Buffer", "Nuevo_Buffer_RNN"]]
resumen= df_producto_tienda[['SKU', 'Ventas_Suma','Proyeccion_Actual', 'Inventario_Actual', "Nuevo_Buffer"]]
resumen = resumen.groupby('SKU').sum().reset_index()

resumen = pd.merge(resumen, df_clones, on='SKU', how='left')
resumen = pd.merge(resumen, df_onorder, on='SKU', how='left')
resumen = pd.merge(resumen, df_matrix, on='Product Group Code', how='left')

df_buffers['SKU'] = df_buffers['ID'].str[0:10]
df_buffers_resumen = df_buffers.groupby('SKU')['Buffer Actual'].sum().reset_index()
resumen = pd.merge(resumen, df_buffers_resumen, on='SKU', how='left')



'''BUFFER DINAMICO''' #RECORDAR MANTENER ESTE BLOQUE EN FUTURAS VERSIONES
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

def sigmoid(df):
    rango = 1.3
    aceleracion = 4.9
    cruce = 0.6
    return rango*(1-1/(1+math.exp(-aceleracion*((df['INV_BUFFER']/100)-cruce))))-rango/2

def redondear(numero, multiplo):
    return round(numero / multiplo) * multiplo

#apply conditions #adaptar al nuevo input
df_producto_tienda['INV_BUFFER'] = round(df_producto_tienda['Inventario_Actual'] / df_producto_tienda['Buffer Actual']*100,1)
df_producto_tienda['WOS'] = round(df_producto_tienda['Inventario_Actual']/df_producto_tienda['Ventas_Suma'],1)
df_producto_tienda['Nota_Buffer'] = df_producto_tienda['INV_BUFFER'].apply(condiciones)
df_producto_tienda['Incremento_buffer'] = df_producto_tienda.apply(sigmoid, axis=1)
df_producto_tienda['DB_Profundidad'] = df_producto_tienda['Buffer Actual']*(1+df_producto_tienda['Incremento_buffer'])
df_producto_tienda['DB_Profundidad'] = df_producto_tienda['DB_Profundidad'].round(0)

#FORMATO
#CHANGE DE FORMAT OF COLUMNS 'INV_BUFFER' AND 'INCREMENTO_BUFFER' TO PERCENTAGE
df_producto_tienda['INV_BUFFER'] = df_producto_tienda['INV_BUFFER'].astype(str) + '%'
df_producto_tienda['Incremento_buffer'] = df_producto_tienda['Incremento_buffer'].astype(str) + '%'

#round by with the function rounby the values in the column 'DB_Profundidad'
df_producto_tienda["DB_Profundidad"] = df_producto_tienda.apply(lambda x: roundBy(x["DB_Profundidad"],x["Multiplo_Distribucion"], x["Multiplo_Distribucion"]), axis=1)



df_producto_tienda.to_excel(os.path.join(carpeta_output, 'BufferBasico_'+BU+'.xlsx'), index=False)

buffer_dinamico = df_producto_tienda[['SKU','DB_Profundidad']].groupby('SKU').sum().reset_index()
resumen = pd.merge(resumen, buffer_dinamico, on='SKU', how='left')
resumen.to_excel(os.path.join(carpeta_output, 'Resumen_'+BU+'.xlsx'), index=False)


print('Archivo de buffers por tiendas y resumen generados con exito')