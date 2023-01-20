BU=input('''Selecciona la BU: BEAUTY JEW ACC CALZADO ROPA ''') 

#def CreateProyeccionActual(BU):

div =   {'BEAUTY': "[Division Code] = 'BEAUTY'", 
            'JEW': "[Division Code] IN ('W-JEW', 'M-JEW')",
            'ACC': "[Division Code] IN ('W-ACC', 'M-ACC')",
        'CALZADO':"[Division Code] in ('W-SHO','M-SHO')",
           'ROPA':"[Division Code] in ('W-CLO','M-CLO')"} 



import datetime
import pandas as pd
import numpy as np
import tensorflow as tf
import numpy as np
#import matplotlib.pyplot as plt
import os 
import get_data as gd

today = datetime.date.today()
carpeta_input = R'S:\BI\3. MERCHANDISING\FRAMEWORK\FRAMEWORK\ZIMA\BuffersBasicosV1\input'
carpeta_output = R'S:\BI\3. MERCHANDISING\FRAMEWORK\FRAMEWORK\ZIMA\BuffersBasicosV1\output'           

end_date = str(today)

# vale, la proyeccion es con base en el año anterior entonces, se necesita un query grande, en este caso se toma desde la primera semana del año anterior                                    
df_inv_ventas_hist = gd.get_inv_venta_hist(BU, div, '2022-01-09', end_date, carpeta_input, carpeta_output, 33)

#T117 2112513083
print("Detectando semanas faltantes... ") #-------------------------------------------------------------------------------------------------
# Aquí se crea un rango de fechas para cada SKU y tienda

# Creando un dataframe con los datos faltantes
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

print('Porcentaje de datos interpolados:', str((df_inv_ventas_hist['ID'].isnull().sum() / len(df_inv_ventas_hist))*100) + '%')

def windowed_dataset(series, window_size, batch_size, shuffle_buffer):
    
    dataset = tf.data.Dataset.from_tensor_slices(series)

    dataset = dataset.window(window_size + 1, shift=1, drop_remainder=True)
    # Single batch
    dataset = dataset.flat_map(lambda window: window.batch(window_size + 1))

    dataset = dataset.map(lambda window: (window[:-1], window[-1]))

    dataset = dataset.shuffle(shuffle_buffer)
    
    dataset = dataset.batch(batch_size).prefetch(1)
    
    return dataset

from datetime import datetime


def proyeccion_RNN(producto, tienda):
    print(producto, tienda)
        
    df = df_inv_ventas_hist[(df_inv_ventas_hist['SKU'] == producto) & (df_inv_ventas_hist['Location Code'] == tienda)].reset_index(drop=True)
    df = df[['Date', 'Ventas']]
    df['Ventas']= df['Ventas'].astype(int)

    # ordenando por fecha
    df = df.sort_values(by=['Date']).reset_index(drop=True)
    df['time'] = range(len(df))

    time = df['time'].values
    series = df['Ventas'].values

    # Define the split time
    split_time = int(round(len(df)*0.80,0))

    # Get the train set 
    time_train = time[:split_time]
    x_train = series[:split_time]

    # Get the validation set
    time_valid = time[split_time:]
    x_valid = series[split_time:]    

    window_size = 4
    batch_size = 30
    shuffle_buffer_size = 1000

    dataset = windowed_dataset(x_train, window_size, batch_size, shuffle_buffer_size)    

    l0 = tf.keras.layers.Dense(1, input_shape=[window_size])

    model = tf.keras.models.Sequential([
    tf.keras.layers.Conv1D(filters=64, kernel_size=3,
                        strides=1, padding="causal",
                        activation="relu",
                        input_shape=[window_size, 1]),
    tf.keras.layers.LSTM(64, return_sequences=True),
    tf.keras.layers.LSTM(64),
    tf.keras.layers.Dense(1),
    tf.keras.layers.Lambda(lambda x: x * 400)
    ])

    #model.compile(loss="mse", optimizer=tf.keras.optimizers.SGD(learning_rate=1e-6, momentum=0.9), metrics=["accuracy"])
    model.compile(loss=tf.keras.losses.Huber(), optimizer=tf.keras.optimizers.SGD(learning_rate=1e-6, momentum=0.9), metrics=["mae"])

    history = model.fit(dataset,epochs=100)

    # Todo esto es para hacer el forecast en el tiempo de validacion
    '''forecast = []
    for time in range(len(series) - window_size):
        forecast.append(model.predict(series[time:time + window_size][np.newaxis]))

    forecast = forecast[split_time - window_size:]
    results = np.array(forecast).squeeze()

    #Convert to a numpy array and drop single dimensional axes
    results = np.array(forecast).squeeze()
    results = np.round(abs(results), 0)

    # Overlay the results with the validation set
    time = df['time'].values
    series = df['Ventas'].values
    forecast = forecast[split_time - window_size:]
    # plot_series(time_valid, (x_valid, abs(results)))

    #print("Real: ", sum(x_valid[-7:]))
    #print("Proyectada: ", sum(abs(results[-7:])))

    #print(tf.keras.metrics.mean_squared_error(x_valid, results).numpy())
    #print(tf.keras.metrics.mean_absolute_error(x_valid, results).numpy())'''

    ultima_fecha = str(df.loc[(len(df)-1),'Date'])[:10]
    ultima_fecha = datetime.strptime(ultima_fecha, '%Y-%m-%d')
    from datetime import timedelta

    size_proyeccion = 8
    fechas_proyeccion = []
    forecast = []

    #series = series[:split_time]

    for i in range(size_proyeccion):
        forecast.append(model.predict(series[-window_size:][np.newaxis]))
        series = np.append(series, abs(int(forecast[-1].squeeze())))
        ultima_fecha = ultima_fecha + timedelta(days=7)
        fechas_proyeccion.append(ultima_fecha)
        i = i + 1
        
    #print('Real: ', sum(series2[split_time:split_time+8]))
    #print('Proyectada: ', sum(series[-8:]))
    #print("Actual: ", (series2[split_time:split_time+8]).mean()*8)
    #plot_series(time_valid[:8], (series2[split_time:split_time+8], series[-8:]))
    
    proyeccion = series[-size_proyeccion:]
    b = pd.DataFrame({'Fecha':fechas_proyeccion, 'Proyeccion':proyeccion})
    b['SKU'] = producto
    b['Tienda'] = tienda
    
    mae=history.history['mae'][-1]
    loss=history.history['loss'][-1]

    tf.keras.backend.clear_session()

    return b, loss, mae

#producto = '2112513083'
#tienda = 'T117'    
#bd, loss, mae = proyeccion_RNN(producto, tienda)

Proyecciones = pd.DataFrame(columns=['SKU', 'Tienda', 'Fecha', 'Proyeccion'])
Status = pd.DataFrame(columns=['SKU', 'Tienda', 'loss', 'mae'])

for producto in df_inv_ventas_hist['SKU'].unique():
    df = df_inv_ventas_hist[df_inv_ventas_hist['SKU'] == producto]
    for tienda in df['Location Code'].unique():
        bd, loss, mae = proyeccion_RNN(producto, tienda)
        Proyecciones = pd.concat([Proyecciones, bd], axis=0)
        Status = Status.append({'SKU':producto, 'Tienda':tienda, 'loss':loss, 'mae':mae}, ignore_index=True)



Status.to_csv('Status_'+BU+'.csv', index=False)
Proyecciones.to_csv('Proyecciones_'+BU+'.csv', index=False)  

Status.to_csv(os.path.join(carpeta_output, 'Status_'+BU+'.csv'), index=False)
Proyecciones.to_csv(os.path.join(carpeta_output, 'Proyecciones_'+BU+'.csv'), index=False)        



# Confirmación enviada por correo -----------------------
fecha = datetime.datetime.now().strftime("%Y-%m-%d")
hora = datetime.datetime.now().strftime("%H:%M:%S")

mensaje = fecha + ' '+ hora + ' '+ BU
se.envia_correo('Proyeccion RNN generada con exito', mensaje, 'CredencialesCorreo.json')

print('Proceso concluido con exito')