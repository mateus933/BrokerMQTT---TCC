#Biblioteca Paho para criação do cliente
import paho.mqtt.client as mqtt

#Blibiotecas para o tempo do sistema
import time
import datetime

#Blibiotecas para o sensor e LED RGB
import Adafruit_DHT
import RPi.GPIO as GPIO



#Tópico e Servidor Broker MQTT
MQTT_SERVER = "localhost" #Servidor
MQTT_PORT = 1883 #Porta do servidor
MQTT_KEEPALIVE_INTERVAL = 60 # Tempo maximo de espera da mensagem
MQTT_TOPIC = "dht22" # Nome do sensor sera o Tópico


#O callback quando o cliente recebe uma resposta valida do servidor
def on_connect(cliente, userdata, flags, rc):
    
    if rc == 0:
        
        print("Conectado ao MQTT Broker ...")
    else:
        print("Falha na conexao, retorna codigo de erro", rc)

#PUBLISH é recebida do servidor.
def on_publish(cliente, userdata, mid):
    print("Mensagem Publicada: " +str(mid))



#Inicializa o cliente do MQTT
cliente = mqtt.Client()
#Registra a publicação na funcao de publicação
cliente.on_publish = on_publish
cliente.on_connect = on_connect
#Usuario e Senha do usuario do Broker Mosquitto
#cliente.username_pw_set("mateus", "Viana301.")
#Usuario e Senha do usuario do Broker RabbitMQ
#cliente.username_pw_set("mateus", "Mendes301.")
#Conexao com o MQTT Broker
cliente.connect(MQTT_SERVER, MQTT_PORT, MQTT_KEEPALIVE_INTERVAL)


#Sensor DHT22
Nome_Sensor = Adafruit_DHT.DHT22 #Nome do sensor utilizando a bibilioteca Adafruit 
Pino_Sensor = 4 #Pino de dados do Sensor


#Sinalização RGB
GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)
AZUL = 17
VERDE = 27
VERMELHO = 22

#Inicializando RGB como zero.
GPIO.setup(AZUL, GPIO.OUT)
GPIO.output(AZUL,0)
GPIO.setup(VERDE, GPIO.OUT)
GPIO.output(VERDE,0)
GPIO.setup(VERMELHO, GPIO.OUT)
GPIO.output(VERMELHO,0)


# Tempo de 1 hora enviando mensagens
tempo_de_conexao = time.time() + 3600

#Numero de Mensagens
numero_de_mensagens = 1000
i = 0

#Sensor Modelo
sensor_id = "DHT22_AM2302"

#Iniciando o Loop
cliente.loop_start()

try:
    while i < numero_de_mensagens:
    #while time.time() < tempo de conexao:  # Tempo de 1 hora enviando mensagens
        umidade, temperatura = Adafruit_DHT.read(Nome_Sensor, Pino_Sensor) #Realiza a leitura da umidade e da temperutara
        horario_rasp = datetime.datetime.now() #Pega o horário do Raspberry Pi
        if temperatura is not None and umidade is not None: #Testa se a temperatura e umidade não são vazias para publicar as mensagens.
            if(temperatura < 20): #Temparatura Fria se menor que 20 C
                GPIO.output(AZUL,1) #Ascede LED Azul
                print("\n ------- Dados Obtidos -------")
                print("Horario: {} \n".format(horario_rasp)) #Imprime no Shell o horario da leitura
                print("Temperatura Fria: {} C Umidade relativa do ar: {} Horario: {}".format(temperatura, umidade, horario_rasp)) #Imprime os dados coletados pelo sensor e o horario
                MSG = ' "Horario_rasp": "{}","Sensor": "{:s}", "Temperatura Fria": "{}", Umidade relativa do ar": "{}" '.format(horario_rasp, sensor_id, temperatura, umidade)#Mensagem que sera enviada
                MSG = '{'+MSG+'}'
                print(MSG) #Imprime a mensagem do servidor
                cliente.publish(MQTT_TOPIC,MSG, qos=2, retain=False) #topico, mensagem, nivel de QOS = {0,1,2}, não retem a ultima leitura realizada
                print(" ------- Fim de Dados -------\n")
                i = i + 1 #1000 Mensagens
      
            elif(temperatura >= 21 and temperatura <= 25): #Temparatura Normal se maior ou igual a 20 C ou menor igual a 25 C
                  GPIO.output(VERDE, 1) #Ascede LED Verde
                  print("\n ------- Dados Obtidos -------")
                  print("Horario: {} \n".format(horario_rasp)) #Imprime no Shell o horario da leitura
                  print('Temperatura Normal: {} C Umidade relativa do ar: {} Horario: {}'.format(temperatura, umidade, horario_rasp)) #Imprime os dados coletados pelo sensor e o horario
                  MSG = ' "Horario_rasp": "{}","Sensor": "{:s}", "Temperatura Normal": "{}", "Umidade relativa do ar": "{}" '.format(horario_rasp, sensor_id, temperatura, umidade)#Mensagem que sera enviada
                  MSG = '{'+MSG+'}'
                  print(MSG) #Imprime a mensagem do servidor
                  cliente.publish(MQTT_TOPIC,MSG, qos=2, retain=False) #topico, mensagem, nivel de QOS = {0,1,2}, não retem a ultima leitura realizada
                  print(" ------- Fim de Dados -------\n")
                  i = i + 1 #1000 Mensagens       
           
            elif(temperatura > 25): #Temparatura Normal se maior ou igual a 20 C ou menor igual a 25 C
                  GPIO.output(VERMELHO,1) #Ascede LED Vermelho
                  print("\n ------- Dados Obtidos -------")
                  print("Horario: {} \n".format(horario_rasp)) #Imprime no Shell o horario da leitura
                  print("Temperatura Normal: {} C Umidade relativa do ar: {} Horario: {}".format(temperatura, umidade, horario_rasp)) #Imprime os dados coletados pelo sensor e o horario
                  MSG = ' "Horario_rasp": "{}","Sensor":"{:s}", "Temperatura Quente": "{}", "Umidade relativa do ar": "{}" '.format(horario_rasp, sensor_id, temperatura, umidade)#Mensagem que sera enviada
                  MSG = '{'+MSG+'}'
                  print(MSG) #Imprime a mensagem do servidor
                  cliente.publish(MQTT_TOPIC,MSG, qos=2, retain=False) #topico, mensagem, nivel de QOS = {0,1,2}, não retem a ultima leitura realizada
                  print(" ------- Fim de Dados -------\n")
                  i = i + 1 #1000 Mensagens
                   
        else:
                        pass
                        
except KeyboardInterrupt:
                        pass

finally:
                        print("Saindo do Teste...")
                        cliente.disconnect() #Desconecta do Broker
                        GPIO.cleanup() #Apaga Led
                        print("Desconectado do Broker MQTT...")
        






             
