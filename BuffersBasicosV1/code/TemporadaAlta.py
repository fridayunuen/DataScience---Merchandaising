def crea_proyeccion_actual(BU, today, div, carpeta_input, carpeta_output):
    import pandas as pd
    import os
    import numpy as np
    
    end_date = str(today)
   
    import get_data as gd
    # vale, la proyeccion es con base en el año anterior entonces, se necesita un query grande, en este caso se toma desde la primera semana del año anterior                                    
    df_inv_ventas_hist = gd.get_inv_venta_hist(BU, div, '2021-01-10', end_date, carpeta_input, carpeta_output, 4)
    
    # Lead Time, selecciona el lead time para calcular la cobertura ----------------------------------
    LeadTime = pd.read_excel(os.path.join(carpeta_input, "LeadTime.xlsx"))
    LeadTime = LeadTime[["Destino", "LTDN"]]
    # 6 semnas de cobertura y 1 de transporte
    LeadTime["Semanas_Transporte"] = LeadTime["LTDN"].apply(lambda x: 1 if x>7 else 0)
    LeadTime["Cobertura"] = LeadTime["Semanas_Transporte"]+7

    LeadTime = LeadTime.rename(columns={"Destino": "Tienda"})
    LeadTime = LeadTime[['Tienda', 'Cobertura']]
    LeadTime.columns = ['Location Code', 'Cobertura']

    df_inv_ventas_hist = pd.merge(df_inv_ventas_hist, LeadTime, on='Location Code', how='left')
    df_inv_ventas_hist['Year'] = df_inv_ventas_hist['Date'].apply(lambda x: x.year)
    df_inv_ventas_hist['Week'] = df_inv_ventas_hist['Date'].apply(lambda x: x.week)
    
    print('Analizando ventas del año anterior para realizar la proyeccion actual')
    print("Detectando semanas faltantes... ")

    df_tienda_bimestre = pd.DataFrame(columns=['ID', 'Date', 'Ventas', 'Inventario'])
    df_producto_tienda = pd.DataFrame(columns=['ID', 'Ventas_Suma', 'Inventario_Suma', 'Ventas_Promedio', 'Inventario_Promedio', 'Inicio', 'Fin'])
    datos_faltantes = pd.DataFrame(columns=df_inv_ventas_hist.columns)
    numero_datos_faltantes = pd.DataFrame(columns=['Location Code', 'SKU', 'Cantidad'])


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

            # Se crea un dataframe con el rango de fechas
            df_weeks = pd.DataFrame(weeks, columns=['Date'])

            df_tienda = pd.merge(df_weeks, df_tienda, how='left', on='Date')

            # Se rellenan los datos faltantes con nan
            df_tienda = df_tienda.fillna(np.nan)

            # Se rellenan los vacios con interpolacion lineal * deje ID con nans para identificarlos
            df_tienda['Inventario'] = df_tienda['Inventario'].interpolate()
            df_tienda['Ventas'] = df_tienda['Ventas'].interpolate()

            df_tienda['Location Code'].fillna(tienda, inplace=True)
            df_tienda['SKU'].fillna(sku, inplace=True)

            #print(df_tienda['ID'].isna().sum())
            Cantidad = df_tienda['ID'].isna().sum()
            faltantes = pd.DataFrame({'Location Code': tienda, 'SKU': sku, 'Cantidad': Cantidad}, index=[0])
            numero_datos_faltantes = pd.concat([numero_datos_faltantes, faltantes])

            # Se agrega en el dataframe de datos faltantes
            datos_faltantes = pd.concat([datos_faltantes, df_tienda[df_tienda['ID'].isna()]])

            # tiempo interpolacion 1 min 40 seg
            # 54 seg
            # 48 seg
            
    df_inv_ventas_hist['Date'] = pd.to_datetime(df_inv_ventas_hist['Date'])
    df_inv_ventas_hist = pd.concat([df_inv_ventas_hist, datos_faltantes]).reset_index(drop=True)
    df_inv_ventas_hist = df_inv_ventas_hist.sort_values(by=['SKU', 'Location Code', 'Date']).reset_index(drop=True) ##  

    #producto = '1215560051'
    #tienda = 'T009'
    for producto in df_inv_ventas_hist['SKU'].unique():
        df = df_inv_ventas_hist[df_inv_ventas_hist['SKU']==producto]
        for tienda in df['Location Code'].unique():        
            df  = df_inv_ventas_hist[(df_inv_ventas_hist['SKU']==producto) & (df_inv_ventas_hist['Location Code']==tienda)]
            cobertura = df['Cobertura'].unique()[0]

            import datetime

            year_ago = today - datetime.timedelta(days=365)
            # year ago + 7-8 weeks
            year_ago_proyeccion = year_ago + datetime.timedelta(weeks=int(cobertura)) 
            # creando un rango de fechas para el año anterior
            rango_proyeccion = pd.date_range(start=year_ago, end=year_ago_proyeccion, freq='W')

            df = df[df['Date'].isin(rango_proyeccion)]

            incio =  df['Date'].min()
            fin = df['Date'].max()

            max_ventas_hist = df['Ventas'].max()


            df = df[['Date', 'Ventas', 'Inventario']]
            df['ID'] = producto + tienda


            df_tienda_bimestre = pd.concat([df_tienda_bimestre, df]).reset_index(drop=True)

            df = df[['Ventas', 'Inventario']]
            resumen = pd.DataFrame(df.sum()).T

            resumen.columns = ['Ventas_Suma', 'Inventario_Suma']
            resumen['ID'] = producto + tienda
            resumen['Inicio'] = incio
            resumen['Fin'] = fin
            resumen['Max_Ventas_Hist'] = max_ventas_hist

            # Vamos a omitir estos valores para no confundir el asunto
            resumen['Ventas_Promedio'] = df['Inventario'].mean()
            #resumen['Inventario_Promedio'] = df['Inventario'].mean()

            df_producto_tienda = pd.concat([df_producto_tienda, resumen]).reset_index(drop=True)
            # 26 MIN
            
    # Cuando se tenga el cluster de tienda y producto, se puede hacer un filtro para obtener los datos faltantes
    no_data = df_producto_tienda[df_producto_tienda['Inicio'].isna()]
    
    no_data.to_excel(os.path.join(carpeta_output, 'no_data_'+ BU +'.xlsx'), index=False)
    print('No data: ', len(no_data))

    df_producto_tienda['Location Code'] = df_producto_tienda['ID'].str[10:]
    df_producto_tienda['SKU'] = df_producto_tienda['ID'].str[:10]
    df_producto_tienda = pd.merge(df_producto_tienda, LeadTime, on='Location Code', how='left')

    df_producto_tienda['Proyeccion_Actual'] =  df_producto_tienda['Ventas_Suma']
    df_producto_tienda.to_csv(os.path.join(carpeta_output, 'df_producto_tienda_'+ BU +'.csv'), index=False)
    print('Proyeccion Actual guardada...')

    return df_producto_tienda
