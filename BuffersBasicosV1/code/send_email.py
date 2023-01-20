def envia_correo(asunto, mensaje, json_credenciales):
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    import smtplib
    import json

    with open(json_credenciales) as f:
     credenciales = json.load(f)

    message = MIMEMultipart('alternative')
    message['Subject'] = asunto
    message['From'] =  credenciales['usuario']
    message['To'] = credenciales['destinatarios']

    message.attach(MIMEText(mensaje, 'plain'))
    #message.attach(MIMEText('<h1 style="color: blue">A Heading</a><p>Something else in the body</p>', 'html'))

    server = smtplib.SMTP('smtp-mail.outlook.com', 587)
    server.starttls()
    server.login( credenciales['usuario'], credenciales['password'])
    server.sendmail( credenciales['usuario'], credenciales['destinatarios'], message.as_string())
    server.quit()

    print('Correo enviado con exito...')