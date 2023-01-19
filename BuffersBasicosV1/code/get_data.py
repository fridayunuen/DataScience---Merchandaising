def get_inv_venta_hist(BU, div, start_date, end_date, carpeta_input, carpeta_output, minimo_semanas):
    
    import datetime
    import pandas as pd
    from sqlalchemy.engine import URL
    from sqlalchemy import create_engine
    import os
    import json


    fecha_inicio = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    YEAR = str(fecha_inicio.year)
    fecha_analizada = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    start_date = "'"+start_date+"'"
    end_date = "'"+end_date+"'"

    # Credenciales de SQL Server
    with open('Credenciales.json') as f:
        credenciales = json.load(f)

    connection_string = 'DRIVER={SQL Server};SERVER=Shjet-prod;DATABASE=Allocations;UID='+ credenciales['usuario'] +';PWD='+credenciales['password']
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)
    conn = engine.connect()

    # Todo esto se puede hacer desde sql, esta separado para visualizar de una mejor manera de donde viene la info
    # Pero se puede hacer todo en una sola query

    # No se filtra las ultimas 4 semanas por que necesitamos que si exista inventario para realizar los calculos
    # por ejemplo si las ultimas semanas de venta no hubo inventario entonces no son representativas. 

    from datetime import datetime
    # Inventarios ------------------------------------------------
    print("Consultando inventarios... ")

    query_inventarios = '''

    SELECT CONCAT([year], RIGHT(CONCAT('0',[wk]),2), [Location Code], SUBSTRING(CONCAT([Item No_], [VariantColor]),1,10)) AS [key]
            ,CAST(SUM([Inventario]) AS INT) AS [Inventario], [year], RIGHT(CONCAT('0',CAST([wk] AS INT)+ 1),2) AS SemanaMasUno, IIF(CAST([wk] AS INT)+1 = 53, 'yes', 'no') AS Flag
        
    FROM PadresClones_Basicos AS pcb
    JOIN  [Flash_BC].[dbo].[InvHistBC] AS inv
    ON SUBSTRING(CONCAT(inv.[Item No_], inv.[VariantColor]),1,10) = pcb.VarianteColor


    WHERE '''+ str(div[BU]) +'''
        AND  [Fecha] BETWEEN CAST('''+start_date+''' AS datetime) AND CAST('''+ end_date +''' AS datetime)  
        
        -- Seleccionando solo Tiendas y E002
        AND (SUBSTRING([Location Code], 1, 1) = 'E' 
                    OR	
            SUBSTRING([Location Code], 1, 1) = 'T')

    GROUP BY CONCAT([year],RIGHT(CONCAT('0',[wk]),2), [Location Code], SUBSTRING(CONCAT([Item No_], [VariantColor]),1,10)), 
            RIGHT(CONCAT('0',CAST([wk] AS INT)+ 1),2) , 
            IIF(CAST([wk] AS INT)+1 =53, 'yes', 'no'), [year] 

    '''

    df_inv = pd.read_sql_query(query_inventarios, conn)

    # Se recorren los inventarios  --- esto se puede hacer en sql ? Mejor no, le cargamos el trbajo a python
    df_inv.loc[df_inv["Flag"] == 'yes', "year"] = df_inv[df_inv["Flag"] == 'yes']["year"] + 1
    df_inv.loc[df_inv["Flag"] == 'yes', "SemanaMasUno"] = '01'
    df_inv['year'] = df_inv['year'].astype(str)

    df_inv['key'] = df_inv['year'] + df_inv["SemanaMasUno"] + df_inv["key"].str[6:] 
    df_inv = df_inv[['key', 'Inventario']]


    # Ventas ------------------------------------------------
    print("Consultando ventas... ")

    query_ventas = '''

    SELECT CONCAT([year], RIGHT(CONCAT('0',[wk]),2) ,[Tienda] ,CONCAT([Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS, [VariantColor] COLLATE SQL_Latin1_General_CP1_CI_AS )) AS [key],
        SUM(CAST([PiezasVenta] AS INT)) AS Ventas, [wk], [year]


    FROM PadresClones_Basicos AS pcb
    JOIN [Flash_BC].[dbo].[VentaVinTBC] AS vnta
    ON pcb.[VarianteColor] = CONCAT([Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS, [VariantColor] COLLATE SQL_Latin1_General_CP1_CI_AS )


    WHERE '''+ str(div[BU]) +'''
        AND [year] >= '''+YEAR+'''
        AND wk >= '01'

    GROUP BY CONCAT([year], RIGHT(CONCAT('0',[wk]),2),[Tienda] ,CONCAT([Item No_] COLLATE SQL_Latin1_General_CP1_CI_AS, [VariantColor] COLLATE SQL_Latin1_General_CP1_CI_AS )), [wk], [year]

    '''
    df_ventas = pd.read_sql_query(query_ventas, conn)

    df_ventas["Date"] =  df_ventas["year"].astype(str) + ' ' + df_ventas["wk"].astype(str)
    df_ventas["Date"] = df_ventas["Date"].apply(lambda x: datetime.strptime(x + ' 0', "%Y %W %w"))
    df_ventas = df_ventas[df_ventas['Date'] < fecha_analizada]
    df_ventas = df_ventas[['key', 'Ventas']]

    # Cruce de inventarios y ventas --------------------------------
    df_inv_ventas_hist = pd.merge(df_inv, df_ventas, on='key', how='outer')
    df_inv_ventas_hist["year"] = df_inv_ventas_hist["key"].str[0:4]
    df_inv_ventas_hist["wk"]= df_inv_ventas_hist["key"].str[4:6]
    df_inv_ventas_hist["Location Code"] = df_inv_ventas_hist["key"].str[6:10]
    df_inv_ventas_hist["Item No_"] = df_inv_ventas_hist["key"].str[10:]
    df_inv_ventas_hist["Ventas"] = df_inv_ventas_hist["Ventas"].fillna(0) 
    df_inv_ventas_hist["Inventario"] = df_inv_ventas_hist["Inventario"].fillna(0)


    # Clones ------------------------------------------------
    # Se hace esta consulta para poder agrupar los clones y sumar sus ventas e inventarios
    print("Consultando clones... ")

    query_clones = '''
    SELECT 
        SUBSTRING(CONCAT([Original Vendor Item No_], [Variant Code]), 1, 10) AS [Padre],
        IIF([No_] = '',
                    SUBSTRING(CONCAT([Original Vendor Item No_], [Variant Code]), 1, 10),
                    SUBSTRING(CONCAT([No_], [Variant Code]), 1, 10)
                    ) AS [Item No_], 
                    
                            pcb.[Division Code],
                            [Description], 
                            [Product Group Code], 
                            [Item Category Code],
                            [Weather]
                    
        FROM   PadresClones_Basicos AS pcb
        JOIN [Allocations].[dbo].[Item_BC] 
        ON pcb.VarianteColor = SUBSTRING(CONCAT([No_], [Variant Code]), 1, 10)

        WHERE pcb.'''+ str(div[BU]) +'''

        ORDER BY [Padre]
        '''
        
    df_clones = pd.read_sql_query(query_clones, conn)
    df_clones.drop_duplicates(subset ="Item No_", keep = 'first', inplace = True)

    # Agrupando clones -------------------------------------------------------------------------------------
    df_inv_ventas_hist = pd.merge(df_inv_ventas_hist, df_clones, on='Item No_', how='left')

    df_inv_ventas_hist["ID"] = df_inv_ventas_hist["year"] + df_inv_ventas_hist["wk"]+df_inv_ventas_hist["Location Code"]+df_inv_ventas_hist["Padre"]
    df_inv_ventas_hist = df_inv_ventas_hist[["ID", "Inventario", "Ventas"]] #26041
    df_inv_ventas_hist = df_inv_ventas_hist.groupby(["ID"]).sum()

    # Preparando la data  ---------------------------------------------------------------------
    print("Preparando data... ")
    df_inv_ventas_hist = df_inv_ventas_hist.reset_index()

    df_inv_ventas_hist["year"] = df_inv_ventas_hist["ID"].str[0:4]
    df_inv_ventas_hist["wk"]= df_inv_ventas_hist["ID"].str[4:6]
    df_inv_ventas_hist["Location Code"] = df_inv_ventas_hist["ID"].str[6:10]
    df_inv_ventas_hist["SKU"] = df_inv_ventas_hist["ID"].str[10:]


    # Se coloca el domingo como día de corte
    df_inv_ventas_hist["Date"] =  df_inv_ventas_hist["year"].astype(str) + ' ' + df_inv_ventas_hist["wk"].astype(str)
    df_inv_ventas_hist["Date"] = df_inv_ventas_hist["Date"].apply(lambda x: datetime.strptime(x + ' 0', "%Y %W %w"))
    df_inv_ventas_hist = df_inv_ventas_hist.sort_values(by=['Date'], ascending=[True])

    # Se crea una tabla marcando los inventarios negativos con ventas diferentes de cero para que se tomen medidas despues lol
    inv_negativos = df_inv_ventas_hist[(df_inv_ventas_hist["Inventario"] <=0) & (df_inv_ventas_hist["Ventas"] != 0) ]     
    inv_negativos.to_excel(os.path.join(carpeta_output, "inv_negativos_ventas_positivas_"+BU+".xlsx"), index=False)

    # Ahora, en el modelo de merch no se toman en cuenta las semanas que no tengan ventas y que el inventario sea negativo o cero 
    df_inv_ventas_hist = df_inv_ventas_hist[~((df_inv_ventas_hist["Inventario"] <=0) & (df_inv_ventas_hist["Ventas"] == 0)) ]
    df_inv_ventas_hist = df_inv_ventas_hist.reset_index(drop=True)

    # Ordenado 
    df_inv_ventas_hist = df_inv_ventas_hist[['ID', 'SKU', 'Location Code', 'Date', 'Ventas', 'Inventario']]
    df_inv_ventas_hist = df_inv_ventas_hist.sort_values(by=['SKU', 'Location Code', 'Date']).reset_index(drop=True)

    # Se elimina T000 porque no es una tienda solo es una prueba
    df_inv_ventas_hist = df_inv_ventas_hist[df_inv_ventas_hist['Location Code'] != 'T000']
    # Se elimina T085 por que es outlet
    df_inv_ventas_hist = df_inv_ventas_hist[df_inv_ventas_hist['Location Code'] != 'T085']

    # A partir de aquí se detecta si hay datos suficientes para hacer un buen análisis ---------------------------------------------
    # semanas suficientes
    semanas = pd.DataFrame(columns=['SKU', 'Location Code','Semanas', 'Ultima_Semana'])

    SKUS = df_inv_ventas_hist['SKU'].unique()
    for sku in SKUS: 
        
        df = df_inv_ventas_hist[df_inv_ventas_hist['SKU'] == sku] # for
        tiendas = df['Location Code'].unique()
        for tienda in tiendas:
        
            df_tienda = df[df['Location Code'] == tienda]

            first_week = df_tienda["Date"].min().strftime("%Y-%m-%d")
            last_week = df_tienda["Date"].max().strftime("%Y-%m-%d")

            # Se crea un rango de fechas con el fin de identificar si hay datos faltantes
            weeks = pd.date_range(start=first_week, end=last_week, freq='W')
            semanas = pd.concat([semanas, pd.DataFrame({'SKU': sku, 'Location Code': tienda, 'Semanas': len(weeks), 'Ultima_Semana': last_week}, index=[0])])

    # Pocas Semanas ---------------------------------------------------------------------------------------------------------------------------------------------
    # Si contamos con menos de 4 semanas no se podrá realizar el promedio de venta y proyectarlo a futuro
    # por lo tanto se detecan los items con poca historia 
    pocas_semanas = semanas[semanas['Semanas']< minimo_semanas].sort_values(by=['Ultima_Semana'], ascending=False).reset_index(drop=True)

    pocas_semanas['Ultima_Semana'] = pd.to_datetime(pocas_semanas['Ultima_Semana'])
    this_week = fecha_analizada + pd.DateOffset(days=fecha_analizada.weekday()) + pd.DateOffset(days=2)

    # se crea un rango de semanas que contienenen todo el mes anterior a hoy
    rango_semanas = pd.date_range(start=this_week+ pd.DateOffset(days=7) - pd.DateOffset(months=1) , periods=4, freq='W')

    # notamos si pocas semnas esta dentro del rango de semanas
    pocas_semanas['No_Ultima_Semana'] = pocas_semanas['Ultima_Semana'].apply(lambda x: x in rango_semanas)

    # Eliminando pocas semanas from df_inv_ventas_hist
    # piensa solo eliminarse No_Ultima_Semana == False ya que lo mas posible es que sea  un basico no vigente pediente por eliminar, esperando confirmacion de Analí
    # Para No_Ultima_Semana == True se busca  una tienda con carácteristicas similares para hacer la proyección, esta tienda tiene que ser designada por Merch
    # Podría realizarse el desarrollo pero tabla de información de tiendas debe estar completa y actualizada

    df_inv_ventas_hist = df_inv_ventas_hist[~df_inv_ventas_hist['ID'].str[6:].isin(pocas_semanas['Location Code'] + pocas_semanas['SKU'])].reset_index(drop=True)
    # Agregando Datos tiendas ----------------------------------------------------------------------------------------------------------------------------------
    #Desde aquí se agregan los datos de tiendas para poder la tabla masiva, no tomar en cuenta las tiendas que esta cerradas o articulos que no corresponden a la tienda
    # por ejemplo shman o candy
    print('Identificando características de tiendas y productos....')
    data_tiendas = pd.read_excel(os.path.join(carpeta_input, "DATOS DE TIENDAS PBI 221206.xlsx"))
    #data_tiendas = data_tiendas[data_tiendas['Comps/Total'] != 'CERRADA']
    #data_tiendas= data_tiendas[['NO', 'TIENDA', 'Comps/Total', 'ESTADO', 'ClaveEstado', 'CLIMA', 'REGION', 'DISTRITO', 'MTS2 ARQUITECTURA', 'Bloques Mts2', 'Longitude', 'Latitude', 'Max Roll Transacciones', 'Ciudad', 'SH MAN', 'CANDY',  'Tipo']]

    status = data_tiendas[['NO', 'Comps/Total']]
    df_inv_ventas_hist = pd.merge(df_inv_ventas_hist, status, left_on='Location Code', right_on='NO', how='left')
    df_inv_ventas_hist = df_inv_ventas_hist[df_inv_ventas_hist['Comps/Total'] !='CERRADA']
    
    df_clones.columns = ['SKU', 'Item No_', 'Division Code', 'Description', 'Product Group Code', 'Item Category Code', 'Weather']
    df_inv_ventas_hist = pd.merge(df_inv_ventas_hist, df_clones, on='SKU', how='left')

    especificaciones = data_tiendas[['NO', 'SH MAN', 'CANDY']]
    especificaciones.columns = ['Location Code','ShMan', 'Candy']

    df_inv_ventas_hist = pd.merge(df_inv_ventas_hist, especificaciones, on='Location Code', how='left')
    if 'Candy' in df_inv_ventas_hist['Item Category Code']:
        no_candy = df_inv_ventas_hist[(df_inv_ventas_hist['Candy'] != 'CAN') & (df_inv_ventas_hist['Item Category Code'] == 'CANDY')]
        if len(no_candy) > 0:
            
            no_candy[no_candy['Ventas']!=0].to_excel(os.path.join(carpeta_output, 'no_candy_ventas'+ BU +'.xlsx'), index=False)
            df_inv_ventas_hist = df_inv_ventas_hist[~df_inv_ventas_hist['ID'].isin(no_candy['ID'])].reset_index(drop=True)
            print('Existen ventas de candy en tiendas q no tienen candy')

    if 'M-' in df_inv_ventas_hist['Division Code'].str[0]:
        no_man = df_inv_ventas_hist[(df_inv_ventas_hist['ShMan'] == 'NO') & (df_inv_ventas_hist['Division Code'].str[0] == 'M-')]
        no_man = no_man[no_man['Item Category Code'] != 'BOTTLES M']

        if len(no_man) > 0:
            
            no_man[no_man['Ventas']!=0].to_excel(os.path.join(carpeta_output, 'no_man'+BU+'.xlsx'), index=False)
            df_inv_ventas_hist = df_inv_ventas_hist[~df_inv_ventas_hist['ID'].isin(no_man['ID'])].reset_index(drop=True)
            print('Existen ventas de man en tiendas que no tienen man')

    df_inv_ventas_hist['Year'] = df_inv_ventas_hist['ID'].str[:4]
    df_inv_ventas_hist['Week'] = df_inv_ventas_hist['ID'].str[4:6]


    return df_inv_ventas_hist
