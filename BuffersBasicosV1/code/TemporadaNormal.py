def crea_proyeccion_actual(BU, today, div, carpeta_input, carpeta_output):    
    import pandas as pd
    import os
    import numpy as np

    end_date = str(today)
    import get_data as gd
    # vale, la proyeccion es con base en el año anterior entonces, se necesita un query grande, en este caso se toma desde la primera semana del año anterior                                    
    df_inv_ventas_hist = gd.get_inv_venta_hist(BU, div, '2022-01-09', end_date, carpeta_input, carpeta_output, 4)
    columns_df_inv_ventas_hist = df_inv_ventas_hist.columns
    # Lead Time, selecciona el lead time para calcular la cobertura ----------------------------------
    LeadTime = pd.read_excel(os.path.join(carpeta_input, "LeadTime.xlsx"))
    LeadTime = LeadTime[["Destino", "LTDN"]]
    # 6 SEMANAS DE COBERTURA Y 1 DE TRANSPORTE
    LeadTime["Semanas_Transporte"] = LeadTime["LTDN"].apply(lambda x: 1 if x>7 else 0)
    LeadTime["Cobertura"] = LeadTime["Semanas_Transporte"]+7

    LeadTime = LeadTime.rename(columns={"Destino": "Tienda"})
    LeadTime = LeadTime[['Tienda', 'Cobertura']]
    LeadTime.columns = ['Location Code', 'Cobertura']

    # Ahora lo que vamos a hacer en las siguientes es una adicion que solicitó merch, 
    # no se toman en cuenta las semanas que tienene algun tipo de adicion en mostrador por que incrementa sus ventas 

    df_inv_ventas_hist['Year-Week'] = df_inv_ventas_hist['Year'] + 'W' + df_inv_ventas_hist['Week']
    # order by year, week
    df_inv_ventas_hist = df_inv_ventas_hist.sort_values(by=['Year-Week'], ascending=False)
    #am: adicion al mostrador
    path_am = r'C:\Users\fcolin\OneDrive - SERVICIOS SHASA S DE RL DE CV\DataScience-Merchandaising\AdicionMostrador.xlsx'
    am = pd.read_excel(path_am)
    # Seleccionando las columnas que contienen un valor
    am = am[am.columns[~am.isna().all()]]
    am['SKU'] = am['SKU'].astype(str)
    # Determinamos las semanas que se van a utilizar
    am.columns = am.columns.str.replace('W', '')

    am_weeks = list(am.columns[1:])
    am_weeks = [x.replace('2023W', '') for x in am_weeks]

    for column in am_weeks:
        am[column] = am[column].apply(lambda x: column if x == 1 else np.nan)
    # Se van a eliminar las semanas que se marcan en am
    def eliminar_semanas_ofertas(df_inv_ventas_hist, am):
        df_inv_ventas_hist['ProductoSemana'] = df_inv_ventas_hist['ID'].str[:6] + df_inv_ventas_hist['ID'].str[-10:]

        am_producto_semana = pd.DataFrame(columns=['ProductoSemana'])
        for column in am_weeks:
            df = am[column] + am['SKU'].values
            df = df.dropna()
            df = df.to_frame()
            df.columns = ['ProductoSemana']

        am_producto_semana = pd.concat([am_producto_semana, df], axis=0)

        am_producto_semana = am_producto_semana.drop_duplicates()
        am_producto_semana.reset_index(drop=True, inplace=True)

        am_producto_semana['NoConsiderar'] = 1
        df_inv_ventas_hist = pd.merge(df_inv_ventas_hist, am_producto_semana, how='left', on='ProductoSemana')

        #df = df_inv_ventas_hist[df_inv_ventas_hist['NoConsiderar'] == 1]
        df_inv_ventas_hist = df_inv_ventas_hist[df_inv_ventas_hist['NoConsiderar'] != 1]

        df_inv_ventas_hist = df_inv_ventas_hist[columns_df_inv_ventas_hist]
        return df_inv_ventas_hist 

    # detectar si am[Item Code] esta en df_inv_ventas_hist[Item Code]
    if df_inv_ventas_hist['SKU'].isin(am['SKU']).sum()>0:
        print('Se detectaron productos con adicion al mostrador...')
        df_inv_ventas_hist = eliminar_semanas_ofertas(df_inv_ventas_hist, am)
    # Eliminamos temporada alta de la base de datos para opmitr valores altos 
    semanas_TA = ['48', '49', '50', '51', '52', '01', '02', '53']
    # si df_inv_ventas_hist['Week' esta en semanas_TA drop it
    df_inv_ventas_hist = df_inv_ventas_hist[~df_inv_ventas_hist['Week'].isin(semanas_TA)]
    print('Seleccionando las ultimas semanas de venta....')

    df_tienda_bimestre = pd.DataFrame(columns=['ID', 'Date', 'Ventas', 'Inventario'])
    df_producto_tienda = pd.DataFrame(columns=['ID', 'Ventas_Suma', 'Inventario_Suma', 'Ventas_Promedio', 'Inventario_Promedio', 'Inicio', 'Fin'])

    for producto in df_inv_ventas_hist['SKU'].unique():
        df = df_inv_ventas_hist[df_inv_ventas_hist['SKU'] == producto]
        for tienda in df['Location Code'].unique():
            # print(producto, tienda)
            mes = 4 # ultimo mes de ventas 
            bimestre = 8 # ultimo bimestre de ventas

            df_tienda = df_inv_ventas_hist[(df_inv_ventas_hist['SKU'] == producto) & (df_inv_ventas_hist['Location Code'] == tienda)]
            max_ventas_hist = df_tienda['Ventas'].max()

            df_tienda = df_tienda.sort_values(by=['Date']).reset_index(drop=True)

            df_tienda['ID'] = df_tienda['SKU'] + df_tienda['Location Code']

            df_tienda = df_tienda.tail(bimestre).reset_index(drop=True)

            df_tienda = df_tienda[['ID', 'Date', 'Ventas', 'Inventario']]

            df_tienda_bimestre = pd.concat([df_tienda_bimestre, df_tienda])

            # Las medidas centrales son calculadas con el ultimo mes de ventas, por indicacion de merch
            df_tienda = df_tienda.tail(mes)

            inicio = df_tienda['Date'].min()
            fin = df_tienda['Date'].max()

            df_tienda = df_tienda[['ID', 'Ventas', 'Inventario']]        
            agrupado = df_tienda.groupby('ID')

            suma = agrupado.sum() #
            suma.columns = ['Ventas_Suma', 'Inventario_Suma']
            promedio = agrupado.mean()#
            promedio.columns = ['Ventas_Promedio', 'Inventario_Promedio']
            max = agrupado.max()
            max.columns = ['Ventas_Max', 'Inventario_Max']

            resumen = pd.merge(suma, promedio, on='ID', how='left').reset_index()
            resumen = pd.merge(resumen, max, on='ID', how='left').reset_index()
            
            resumen['Inicio'] = inicio
            resumen['Fin'] = fin
            resumen['Max_Ventas_Hist'] = max_ventas_hist

            df_producto_tienda = pd.concat([df_producto_tienda, resumen])

    # Proyeccion de ventas    
    df_producto_tienda['Location Code'] = df_producto_tienda['ID'].str[10:]
    df_producto_tienda['SKU'] = df_producto_tienda['ID'].str[:10]
    df_producto_tienda = pd.merge(df_producto_tienda, LeadTime, on='Location Code', how='left')
    df_producto_tienda['Proyeccion_Actual'] = df_producto_tienda['Cobertura'] * df_producto_tienda['Ventas_Promedio']

    df_producto_tienda.to_csv(os.path.join(carpeta_output, 'df_producto_tienda_'+ BU +'.csv'), index=False)
    print('Proyeccion Actual guardada...')
    # Documento para que equipo merch vea los datos en los que se basaron los calculos, en temporada normal son 8 semanas de venta, temporada alta son 
    # las mismas semanas que el año anterior 
    df_tienda_bimestre.to_excel(os.path.join(carpeta_output, 'df_producto_tienda_'+ BU +'.xlsx'), index=False)  
    print('Datos en tienda guardados bimestre anterior guardaos...') 