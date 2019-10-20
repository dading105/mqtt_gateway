# -*- coding: utf-8 -*-import json
import paho.mqtt.client as paho
import json
import time
import sqlite3
import threading
import socket
 
import serial  
import modbus_tk  
import modbus_tk.defines as cst  
from modbus_tk import modbus_rtu  

import binascii
import Queue
import datetime
import pytz

tz = pytz.timezone('Asia/Shanghai') #东八区
q = Queue.Queue()
qmqtt = Queue.Queue()
qmqttset = Queue.Queue()

bVolStat=[0,0,0]
bCurStat=[0,0,0]
bFactorStat=0
wPCStat=0
times={}

dicPlc={}
dicForEventsV={}
dicForEventsI={}
dicForEventsF={}
dicForEventsP={}
dicForEventsVDelay={}
dicForEventsIDelay={}
dicForEventsFDelay={}
dicForEventsPDelay={}

V=[0.0,0.0,0.0]
I=[0.0,0.0,0.0]
P=[0.0,0.0,0.0,0.0]
Pr=[0.0,0.0,0.0,0.0]
PF=[0.0,0.0,0.0,0.0]


Cmd645 = [0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x33,0x33,0x33,0x33,0xad,0x16]
Cmd64597 = [0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x01,0x02,0x43,0xc3,0xd5,0x16]
Cmd645td = [[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x33,0x32,0x34,0x35,0xaf,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x33,0x32,0x35,0x35,0xb0,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x33,0x32,0x36,0x35,0xb1,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x33,0x32,0x37,0x35,0xb2,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x33,0x32,0x38,0x35,0xb3,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x33,0x32,0x39,0x35,0xb4,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x33,0x32,0x3a,0x35,0xb5,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x32,0x34,0x3d,0x35,0xb9,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x32,0x35,0x3d,0x35,0xba,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x32,0x36,0x3d,0x35,0xbb,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x32,0x34,0x3e,0x35,0xba,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x32,0x35,0x3e,0x35,0xbb,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x32,0x36,0x3e,0x35,0xbc,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x32,0x33,0xb3,0x35,0x2e,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x33,0x33,0x34,0x33,0xae,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x33,0x33,0x35,0x33,0xaf,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x33,0x33,0x36,0x33,0xb0,0x16],\
[0xfe,0xfe,0x68,0xaa,0xaa,0xaa,0xaa,0xaa,0xaa,0x68,0x11,0x04,0x33,0x33,0x37,0x33,0xb1,0x16]]



sPort="/dev/ttyAMA3"
hostname = '183.230.40.39'  # OneNET服务器地址
port = 6002  # OneNET服务器端口地址
clientid = '11111111'  # 创建设备时得到的设备ID，为数字字串
username = '111111'  # 注册产品时，平台分配的产品ID，为数字字串
password = '111123456'  # 为设备的鉴权信息（即唯一设备编号，SN），或者为apiKey，为字符串
DATABASE = '/home/pi/db/mqttsqljson2.db' 

#isfinished=threading.Event()

cx = sqlite3.connect(DATABASE)
#cu = cx.cursor() 
cu = cx.execute('select DeviceID, ProductID, IP, PORT, KEY, Alivetime from devices order by id desc limit 0,1')  
devices = [dict(DeviceID=row[0], ProductID=row[1], IP=row[2], PORT=row[3], KEY=row[4], Alivetime=row[5]) for row in cu.fetchall()] 
for device in devices:
    clientid=device['DeviceID']
    username=device['ProductID']
    password=device['KEY']
    hostname=device['IP']
    port=int(device['PORT'])

#cu = cx.cursor()
cu = cx.execute('select Pub, Startaddr, Endaddr, Qos, Uppub, Interval, checked, Json, DeviceType, ServiceId, mspId from modbus order by id desc limit 0,50') 
modbus = [dict(Pub =row[0], Startaddr=row[1], Endaddr=row[2], Qos=row[3], Uppub=row[4], Interval=row[5], checked=row[6], Json=row[7], DeviceType=row[8], ServiceId=row[9], mspId=row[10]) for row in cu.fetchall()]

client = paho.Client(client_id=clientid, clean_session=True, userdata=None, protocol=paho.MQTTv311, transport="tcp")
#print(clientid)
# 配置用户名和密码
client.username_pw_set(username, password)
# 请根据实际情况改变MQTT代理服务器的IP地址
#print(username+password)

def getdata1(Maddr,Mfunction,Mdate,Minterval):  
    #logger = modbus_tk.utils.create_logger("console")  
    tru = 1
    while tru>0:
        print('...')
        try:  
            #Connect to the slave  
            master = modbus_rtu.RtuMaster(  
                serial.Serial(port=sPort, baudrate=9600, bytesize=8, parity='N', stopbits=1, xonxoff=0)  
            )  
            master.set_timeout(Minterval*0.1)  
            master.set_verbose(True)  
            #logger.info("connected")  
            
            #received=master.execute(Maddr, cst.READ_HOLDING_REGISTERS, Mfunction, Mdate)
            received=master.execute(Maddr, cst.READ_DISCRETE_INPUTS, Mfunction, Mdate)
            
            q.put(received)
            #logger.info(received)  
        
            #send some queries  
            #logger.info(master.execute(1, cst.READ_COILS, 0, 10))  
            #logger.info(master.execute(1, cst.READ_DISCRETE_INPUTS, 0, 8))  
            #logger.info(master.execute(1, cst.READ_INPUT_REGISTERS, 100, 3))  
            #logger.info(master.execute(1, cst.READ_HOLDING_REGISTERS, 100, 12))  
            #logger.info(master.execute(1, cst.WRITE_SINGLE_COIL, 7, output_value=1))  
            #logger.info(master.execute(1, cst.WRITE_SINGLE_REGISTER, 100, output_value=54))  
            #logger.info(master.execute(1, cst.WRITE_MULTIPLE_COILS, 0, output_value=[1, 1, 0, 1, 1, 0, 1, 1]))  
            #logger.info(master.execute(1, cst.WRITE_MULTIPLE_REGISTERS, 100, output_value=xrange(12)))  
        
        #except modbus_tk.modbus.ModbusError as exc:  
            #logger.error("%s- Code=%d", exc, exc.get_exception_code())  
            #isfinished.set()
        except Exception,err:
            #logger.error(err)  
            #isfinished.set()
            time.sleep(0.2)
            tru+=1
            if tru > 5:
                tru=0
            print(err) 
        else:
            #time.sleep(0.2)
            tru = 0
    #logger.info('finished')
    #isfinished.set()
    
def getdata4(Maddr,Mfunction,Mdate,Minterval):  
    #logger = modbus_tk.utils.create_logger("console")  
    tru = 1
    while tru>0:
        print('...')
        try:  
            #Connect to the slave  
            master = modbus_rtu.RtuMaster(  
                serial.Serial(port=sPort, baudrate=9600, bytesize=8, parity='N', stopbits=1, xonxoff=0)  
            )  
            master.set_timeout(Minterval*0.1)  
            master.set_verbose(True)  
            #logger.info("connected")  
            
            #received=master.execute(Maddr, cst.READ_HOLDING_REGISTERS, Mfunction, Mdate)
            #received=master.execute(Maddr, cst.READ_DISCRETE_INPUTS, Mfunction, Mdate)
            received=master.execute(Maddr, cst.READ_COILS, Mfunction, Mdate)
            
            q.put(received)
            #logger.info(received)  
        
            #send some queries  
            #logger.info(master.execute(1, cst.READ_COILS, 0, 10))  
            #logger.info(master.execute(1, cst.READ_DISCRETE_INPUTS, 0, 8))  
            #logger.info(master.execute(1, cst.READ_INPUT_REGISTERS, 100, 3))  
            #logger.info(master.execute(1, cst.READ_HOLDING_REGISTERS, 100, 12))  
            #logger.info(master.execute(1, cst.WRITE_SINGLE_COIL, 7, output_value=1))  
            #logger.info(master.execute(1, cst.WRITE_SINGLE_REGISTER, 100, output_value=54))  
            #logger.info(master.execute(1, cst.WRITE_MULTIPLE_COILS, 0, output_value=[1, 1, 0, 1, 1, 0, 1, 1]))  
            #logger.info(master.execute(1, cst.WRITE_MULTIPLE_REGISTERS, 100, output_value=xrange(12)))  
        
        #except modbus_tk.modbus.ModbusError as exc:  
            #logger.error("%s- Code=%d", exc, exc.get_exception_code())  
            #isfinished.set()
        except Exception,err:
            #logger.error(err)  
            #isfinished.set()
            time.sleep(0.2)
            tru+=1
            if tru > 5:
                tru=0
            print(err) 
        else:
            #time.sleep(0.2)
            tru = 0
    #logger.info('finished')
    #isfinished.set()

def getdata(Maddr,Mfunction,Mdate,Minterval):  
    #logger = modbus_tk.utils.create_logger("console")  
    tru = 1
    while tru>0:
        print(',,,')
        try:  
            #Connect to the slave  
            ser = serial.Serial(port=sPort, baudrate=2400, bytesize=8, parity='E', stopbits=1, timeout=(Minterval*0.1))  
            #clean buffer
            #ser.readall()
            if len(Maddr)>=6:
                for index in range(6):
                    Cmd645[3+index]=Maddr[6-1-index]
            else:
                for index in range(len(Maddr)):
                    Cmd645[3+index]=Maddr[len(Maddr)-1-index]
                for index in range(6-len(Maddr)):
                    Cmd645[3+len(Maddr)+index]=0
            
            Cmd645[15] = (((Mfunction>>24)+0x33)&0xff)
            Cmd645[14] = (((Mfunction>>16)+0x33)&0xff)
            Cmd645[13] = (((Mfunction>>8)+0x33)&0xff)  
            Cmd645[12] = ((Mfunction+0x33)&0xff)	
            
            Cmd645[len(Cmd645)-2]=0
            for index in range(2,len(Cmd645)-2):
                Cmd645[len(Cmd645)-2] +=Cmd645[index]
            Cmd645[len(Cmd645)-2] = Cmd645[len(Cmd645)-2]&0xff;
            
#            y = str(bytearray(Cmd645))  
#            z = binascii.b2a_hex(y)
#            print(Cmd645)
            ser.write(Cmd645)
            time.sleep(0.2)
            response = ser.readall()
#            print(response)
            ser.close()
            if response:
                response = [ord(c) for c in response]
#                print(response)
                responsedate=[]
                if (len(response)-12)>0:
                    for index in range(len(response)-12):
                        if response[index]==104:
                            if response[index+7]==104:
                                k=response[index+9]
                                for index2 in range(k-4):
                                    responsedate.append((response[index+14+index2]-0x33)&0xff)
                                #print(responsedate)
                                break;
                    else:
                        tru+=1
                else:
                    tru+=1
                if responsedate:
                    tru=0
                    y = str(bytearray(responsedate))  
                    z = binascii.b2a_hex(y)
                    q.put(z)
            else:
                tru+=1
        except Exception,err:
            tru+=1
            print(err)  
            #isfinished.set()
        #logger.info('finished')
        #isfinished.set()
        if tru>5:
            tru=0

def getdata97(Maddr,Mfunction,Mdate,Minterval):  
    #logger = modbus_tk.utils.create_logger("console")  
    tru = 1
    while tru>0:
        print(',,,')
        try:  
            #Connect to the slave  
            ser = serial.Serial(port=sPort, baudrate=2400, bytesize=8, parity='E', stopbits=1, timeout=(Minterval*0.1))  
            #clean buffer
            #ser.readall()
            if len(Maddr)>=6:
                for index in range(6):
                    Cmd64597[3+index]=Maddr[6-1-index]
            else:
                for index in range(len(Maddr)):
                    Cmd64597[3+index]=Maddr[len(Maddr)-1-index]
                for index in range(6-len(Maddr)):
                    Cmd64597[3+len(Maddr)+index]=0
            

            Cmd64597[13] = (((Mfunction>>8)+0x33)&0xff)  
            Cmd64597[12] = ((Mfunction+0x33)&0xff)	
            
            Cmd64597[len(Cmd64597)-2]=0
            for index in range(2,len(Cmd64597)-2):
                Cmd64597[len(Cmd64597)-2] +=Cmd64597[index]
            Cmd64597[len(Cmd64597)-2] = Cmd64597[len(Cmd64597)-2]&0xff;
            
#            y = str(bytearray(Cmd64597))  
#            z = binascii.b2a_hex(y)
#            print(Cmd64597)
            ser.write(Cmd64597)
            time.sleep(0.2)
            response = ser.readall()
#            print(response)
            ser.close()
            if response:
                response = [ord(c) for c in response]
#                print(response)
                responsedate=[]
                if (len(response)-12)>0:
                    for index in range(len(response)-12):
                        if response[index]==104:
                            if response[index+7]==104:
                                k=response[index+9]
                                for index2 in range(k-2):
                                    responsedate.append((response[index+12+index2]-0x33)&0xff)
                                #print(responsedate)
                                break;
                    else:
                        tru+=1
                else:
                    tru+=1
                if responsedate:
                    tru=0
                    y = str(bytearray(responsedate))  
                    z = binascii.b2a_hex(y)
                    q.put(z)
            else:
                tru+=1
        except Exception,err:
            tru+=1
            print(err)  
            #isfinished.set()
        #logger.info('finished')
        #isfinished.set()
        if tru>5:
            tru=0
            
def getdata2(Maddr,Mfunction,Mdate,Minterval):  
    #logger = modbus_tk.utils.create_logger("console")  
    tru = 1
    while tru>0:
        print('***')
        try:  
            #Connect to the slave  
            #ser = serial.Serial(port=sPort, baudrate=2400, bytesize=8, parity='E', stopbits=1, timeout=(Minterval*0.1))  
            #clean buffer
            #ser.readall()
            ipadr=Maddr.split(':')
            print(ipadr)
            sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
            sock.settimeout(Minterval*0.01)
            sock.connect((ipadr[0], int(ipadr[1])))
            
            responsedates=[]
            for index in range(len(Cmd645td)):
                truu=1
                while truu:
                    senddata = str(bytearray(Cmd645td[index]))  
                    sock.send(senddata)
                    #print(Cmd645td[index])
                    #print(senddata)
                    try:
                        response = sock.recv(1024)
                    #except sock.timeout:
                    except Exception,err:
                        print(err)
                        truu+=1
                        if truu>5:
                            tru+=1
                            truu=0
                    else:
                        break
                #print(response)
                #print(index)
                if response:
                    response = [ord(c) for c in response]
                    #print(response)
                    #time.sleep(10)
                    if (len(response)-12)>0:
                        for index in range(len(response)-12):
                            if response[index]==104:
                                if response[index+7]==104:
                                    k=response[index+9]
                                    responsedate=[]
                                    for index2 in range(k-4):
                                        responsedate.append((response[index+14+index2]-0x33)&0xff)
                                    responsedates.append(responsedate)
                                    #print(responsedate)
                                    
                                    break;
                        else:
                            tru+=1
                    else:
                        tru+=1
                else:
                    tru+=1
            else:
                q.put(responsedates)
                #print('get udp data')
                #print(responsedates)
                #time.sleep(10)
            tru=0
            sock.close()
        except Exception,err:
            tru+=1
            print(err)
            sock.close()
            #isfinished.set()
        #logger.info('finished')
        #isfinished.set()
        if tru>5:
            tru=0
            
def getdata22(Maddr,Mfunction,Mdate,Minterval):  
    #logger = modbus_tk.utils.create_logger("console")  
    tru = 1
    while tru>0:
        print('******')
        try:  
            ipadr=Maddr.split(':')
            print(ipadr)
            sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
            sock.settimeout(Minterval*0.01)
            sock.connect((ipadr[0], int(ipadr[1])))
            
            Cmd645[15] = (((Mfunction>>24)+0x33)&0xff)
            Cmd645[14] = (((Mfunction>>16)+0x33)&0xff)
            Cmd645[13] = (((Mfunction>>8)+0x33)&0xff)  
            Cmd645[12] = ((Mfunction+0x33)&0xff)	
            
            Cmd645[len(Cmd645)-2]=0
            for index in range(2,len(Cmd645)-2):
                Cmd645[len(Cmd645)-2] +=Cmd645[index]
            Cmd645[len(Cmd645)-2] = Cmd645[len(Cmd645)-2]&0xff;
            
            truu=1
            while truu:
                senddata = str(bytearray(Cmd645))  
                sock.send(senddata)
                print(Cmd645)
                print(senddata)
                try:
                    response = sock.recv(1024)
                except Exception,err:
                    truu+=1
                    if truu>5:
                        tru+=1
                        truu=0
                else:
                    break
            #print(response)
            if response:
                response = [ord(c) for c in response]
                print(response)
                responsedate=[]
                if (len(response)-12)>0:
                    for index in range(len(response)-12):
                        if response[index]==104:
                            if response[index+7]==104:
                                k=response[index+9]
                                for index2 in range(k-4):
                                    responsedate.append((response[index+14+index2]-0x33)&0xff)
                                #print(responsedate)
                                break;
                    else:
                        tru+=1
                else:
                    tru+=1
                if responsedate:
                    tru=0
                    y = str(bytearray(responsedate))  
                    z = binascii.b2a_hex(y)
                    q.put(z)
            else:
                tru+=1
            sock.close()
        except Exception,err:
            tru+=1
            print(err)
            sock.close()
            #isfinished.set()
        #logger.info('finished')
        #isfinished.set()
        if tru>5:
            tru=0
            
def getdata2297(Maddr,Mfunction,Mdate,Minterval):  
    #logger = modbus_tk.utils.create_logger("console")  
    tru = 1
    while tru>0:
        print('******')
        try:  
            ipadr=Maddr.split(':')
            print(ipadr)
            sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
            sock.settimeout(Minterval*0.01)
            sock.connect((ipadr[0], int(ipadr[1])))
            
            Cmd64597[13] = (((Mfunction>>8)+0x33)&0xff)  
            Cmd64597[12] = ((Mfunction+0x33)&0xff)	
            
            Cmd64597[len(Cmd64597)-2]=0
            for index in range(2,len(Cmd64597)-2):
                Cmd64597[len(Cmd64597)-2] +=Cmd64597[index]
            Cmd64597[len(Cmd64597)-2] = Cmd64597[len(Cmd64597)-2]&0xff;
            
            truu=1
            while truu:
                senddata = str(bytearray(Cmd64597))  
                sock.send(senddata)
                #print(Cmd64597)
                #print(senddata)
                print(Minterval*0.01)
                time.sleep(Minterval*0.01)
                try:
                    response = sock.recv(1024)
                except Exception,err:
                    truu+=1
                    if truu>5:
                        tru+=1
                        truu=0
                else:
                    break
            #print(response)
            if response:
                response = [ord(c) for c in response]
                print(response)
                responsedate=[]
                if (len(response)-12)>0:
                    for index in range(len(response)-12):
                        if response[index]==104:
                            if response[index+7]==104:
                                k=response[index+9]
                                for index2 in range(k-2):
                                    responsedate.append((response[index+12+index2]-0x33)&0xff)
                                #print(responsedate)
                                break;
                    else:
                        tru+=1
                else:
                    tru+=1
                if responsedate:
                    tru=0
                    y = str(bytearray(responsedate))  
                    z = binascii.b2a_hex(y)
                    q.put(z)
            else:
                tru+=1
            sock.close()
        except Exception,err:
            tru+=1
            print(err)
            sock.close()
            #isfinished.set()
        #logger.info('finished')
        #isfinished.set()
        if tru>5:
            tru=0
            
def getdata3(Maddr,Mfunction,Mdate,Minterval):  
    #logger = modbus_tk.utils.create_logger("console")  
    tru = 1
    while tru>0:
        print('...')
        try:  
            #Connect to the slave  
            master = modbus_rtu.RtuMaster(  
                serial.Serial(port=sPort, baudrate=4800, bytesize=8, parity='N', stopbits=1, xonxoff=0)  
            )  
            master.set_timeout(Minterval*0.1)  
            master.set_verbose(True)  
            #logger.info("connected")  
            #print(Mfunction)
            received=master.execute(Maddr, cst.READ_HOLDING_REGISTERS, Mfunction, Mdate)
            #received=master.execute(Maddr, cst.READ_DISCRETE_INPUTS, Mfunction, Mdate)
            
            q.put(received)
            #logger.info(received)  
        
            #send some queries  
            #logger.info(master.execute(1, cst.READ_COILS, 0, 10))  
            #logger.info(master.execute(1, cst.READ_DISCRETE_INPUTS, 0, 8))  
            #logger.info(master.execute(1, cst.READ_INPUT_REGISTERS, 100, 3))  
            #logger.info(master.execute(1, cst.READ_HOLDING_REGISTERS, 100, 12))  
            #logger.info(master.execute(1, cst.WRITE_SINGLE_COIL, 7, output_value=1))  
            #logger.info(master.execute(1, cst.WRITE_SINGLE_REGISTER, 100, output_value=54))  
            #logger.info(master.execute(1, cst.WRITE_MULTIPLE_COILS, 0, output_value=[1, 1, 0, 1, 1, 0, 1, 1]))  
            #logger.info(master.execute(1, cst.WRITE_MULTIPLE_REGISTERS, 100, output_value=xrange(12)))  
        
        #except modbus_tk.modbus.ModbusError as exc:  
            #logger.error("%s- Code=%d", exc, exc.get_exception_code())  
            #isfinished.set()
        except Exception,err:
            #logger.error(err)  
            #isfinished.set()
            time.sleep(0.2)
            tru+=1
            if tru > 5:
                tru=0
            print(err) 
        else:
            #time.sleep(0.2)
            tru = 0
    #logger.info('finished')
    #isfinished.set()
            
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe('/mqtt/recall/'+puball+'/+')
    print('/mqtt/recall/'+puball+'/+')
    client.subscribe('/mqtt/config/'+puball)
    print('/mqtt/config/'+puball)
    
def on_message(client, userdata, msg):
    #try:
        print(msg.topic + " " + ":" + str(msg.payload))
        if '/mqtt/recall/' in msg.topic:
            #dic_re=json.loads(msg.payload)
            qmqtt.put(msg.payload)
        elif '/mqtt/config/' in msg.topic:
            #dic_re=json.loads(msg.payload)
            qmqttset.put(msg.payload)
            
            
        #cu = cx.execute('select Pub, mspId, Uppub, ServiceId, strJson, time_stamp from history where Pub=? and mspId=? and time_stamp between ? and ?',[dic_re['clientid'],dic_re['mspid'],dic_re['starttime'],dic_re['endtime']]) 
        #history = [dict(Pub =row[0], mspId=row[1], Uppub=row[2], ServiceId=row[3], strJson=row[4], time_stamp=row[5]) for row in cu.fetchall()]
        #print(history)
        #for his in history:
        #    client.publish('/mqtt/event/'+his['Pub']+'/'+his['mspId'],his['strJson'],0) 
        #    print(his['strJson'])
        #    print('...')
    #except Exception,err:
    #    print(err)
while True:
    try:
        print('connect')
        '''
        print(modbus[49]['ServiceId'])
        print(modbus[48]['ServiceId'])
        print(modbus[47]['ServiceId'])
        print(modbus[46]['ServiceId'])
        print(modbus[45]['ServiceId'])
        print(modbus[44]['ServiceId'])
        m_wVolGuoya=(int(modbus[49]['Uppub']))*(modbus[49]['Startaddr'])/100.0
        m_wVolGuoyaRe=(int(modbus[49]['Uppub']))*(modbus[49]['Endaddr'])/100.0
        m_wVolDiya=(int(modbus[48]['Uppub']))*(modbus[48]['Startaddr'])/100.0
        m_wVolDiyaRe=(int(modbus[48]['Uppub']))*(modbus[48]['Endaddr'])/100.0
        m_wVolShiya=(int(modbus[47]['Uppub']))*(modbus[47]['Startaddr'])/100.0
        m_wVolShiyaRe=(int(modbus[47]['Uppub']))*(modbus[47]['Endaddr'])/100.0
        m_wCurGuoliu=(int(modbus[46]['Uppub']))*(modbus[46]['Startaddr'])/100.0
        m_wCurGuoliuRe=(int(modbus[46]['Uppub']))*(modbus[46]['Endaddr'])/100.0
        m_wFactorGuogao=(modbus[45]['Startaddr'])/100.0
        m_wFactorGuogaoRe=(modbus[45]['Endaddr'])/100.0
        m_wFactorGuodi=(modbus[44]['Startaddr'])/100.0
        m_wFactorGuodiRe=(modbus[44]['Endaddr'])/100.0
        print(m_wVolGuoya)
        print(m_wVolGuoyaRe)
        print(m_wVolDiya)
        print(m_wVolDiyaRe)
        print(m_wVolShiya)
        print(m_wVolShiyaRe)
        print(m_wCurGuoliu)
        print(m_wCurGuoliuRe)
        print(m_wFactorGuogao)
        print(m_wFactorGuogaoRe)
        print(m_wFactorGuodi)
        print(m_wFactorGuodiRe)
        '''
        
        serwdg = serial.Serial(port="/dev/ttyAMA4", baudrate=1200, bytesize=8, parity='N', stopbits=1)  
        print('uart4 write 0')
        serwdg.write('\0')
        serwdg.close()

        client.on_connect = on_connect
        client.on_message = on_message

        global puball
        puball=modbus[0]['Pub']
        try:
            client.connect(hostname, port, 60)
        except Exception,err:
            print(err)
        print('start loop')
        
        #client.subscribe('/mqtt/recall/'+puball+'/+')
        #print('/mqtt/recall/'+puball+'/+')
        client.loop_start()
        
        #time.sleep(2)
        
        
        while True:
            serwdg = serial.Serial(port="/dev/ttyAMA4", baudrate=1200, bytesize=8, parity='N', stopbits=1)  
            print('uart4 write 0')
            serwdg.write('\0')
            serwdg.close()
            
            for modbuslist in modbus:
                pub=modbuslist['Pub']
                startaddr=modbuslist['Startaddr']
                endaddr=modbuslist['Endaddr']
                qos=modbuslist['Qos']
                uppub=modbuslist['Uppub']
                interval=modbuslist['Interval']
                check=modbuslist['checked']
                jsondate=modbuslist['Json']
                devicetype=modbuslist['DeviceType']
                serviceid=modbuslist['ServiceId']
                mspid=modbuslist['mspId']
                
                if pub and check==1:
                    if devicetype==2:
                        if uppub:
                            uppubinit=int(uppub)
                        else:
                            uppubinit=1

                        #isfinished.clear()
                        getdata1(uppubinit,startaddr,endaddr,interval)
                        #print('测量中m...')
                        #isfinished.wait()
                        while not q.empty():
                            #ts=time.strftime("%Y-%m-%d %H:%M:%S")
                            ts = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
                            print(ts)
                            mbdates=q.get()
                            #print(len(mbdates))
                            #print(type(mbdates))
                            jsondates=jsondate.split(',')
                            #print(len(jsondates))
                            #print(jsondates)
                            serviceids=serviceid.split(',')
                            dic={}
                            dic['data_value']=[]
                            for index in range(len(jsondates)):
                                dic['data_value'].append({jsondates[index]:mbdates[index]})
                                #dic[jsondates[index]]=mbdates[index]
                            dic['clientid']=pub
                            dic['mspid']=mspid#uppub
                            dic['time_stamp']=ts
                            dic['actioncode']=serviceids[0]
                            payload=json.dumps(dic)
                            #payload=repr(q.get())
                            client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                            #print(payload)
                            
                            if dicPlc.has_key(mspid):
                                tmpmbdates=dicPlc[mspid]
                            else:
                                tmpmbdates=mbdates
                            if cmp(mbdates,tmpmbdates)==0:
                                print('same')
                            else:
                                print('difference')
                                print(tmpmbdates)
                                print(mbdates)
                                
                                #write mqtt event
                                dicplc={}  #plc
                                dicplc['clientid']=pub
                                dicplc['mspid']=mspid#uppub
                                dicplc['time_stamp']=ts
                                dicplc['actioncode']='SwitchActionRecord'
                                dicplc['data_value']=[]
                                dicplc['data_value'].append({'SwitchType':'4'})
                                dicplc['data_value'].append({'OriginalStatus':tmpmbdates})
                                dicplc['data_value'].append({'NewStatus':mbdates})
                                payload=json.dumps(dicplc)
                                print(payload)
                                client.publish('/mqtt/event/'+pub+'/'+mspid,payload,2,retain=True) 
                                #wrtie sql
                                cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dicplc['clientid'], dicplc['mspid'], uppub, dicplc['actioncode'], payload])
                                cx.commit()  
                                
                            dicPlc[mspid]=mbdates
                    
                    elif devicetype==1:
                        if uppub:
                            y=bytearray.fromhex(uppub)
                            z = list(y)
                        else:
                            y=bytearray.fromhex('111111111111')
                            z = list(y)
                    
                        #isfinished.clear()
                        getdata(z,startaddr,endaddr,interval)
                        print(uppub)
                        #print(z)
                        #print('测量中...')
                        #isfinished.wait()
                        while not q.empty():
                            #ts=time.strftime("%Y-%m-%d %H:%M:%S")
                            ts = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
                            print(ts)
                            mbdates=q.get()
                            #print(len(mbdates))
                            #print(mbdates)
                            jsondates=jsondate.split(',')
#                            print(len(jsondates))
#                            print(jsondates)
                            if startaddr == 50331392:
                                dic={}
                                y = bytearray.fromhex(mbdates)  
                                z = list(y)
                                #print z
                                dic['ts']=ts
                                if(z[2]&0x80):
                                    x=(z[0]-(z[0]>>4)*6)+(z[1]-(z[1]>>4)*6)*100+((z[2]&0x7f)-((z[2]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[0]-(z[0]>>4)*6)+(z[1]-(z[1]>>4)*6)*100+((z[2]&0x7f)-((z[2]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["总有功功率"]=x
                                P[0]=x
                                if(z[5]&0x80):
                                    x=(z[3]-(z[3]>>4)*6)+(z[4]-(z[4]>>4)*6)*100+((z[5]&0x7f)-((z[5]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[3]-(z[3]>>4)*6)+(z[4]-(z[4]>>4)*6)*100+((z[5]&0x7f)-((z[5]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["A有功功率"]=x
                                P[1]=x
                                if(z[8]&0x80):
                                    x=(z[6]-(z[6]>>4)*6)+(z[7]-(z[7]>>4)*6)*100+((z[8]&0x7f)-((z[8]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[6]-(z[6]>>4)*6)+(z[7]-(z[7]>>4)*6)*100+((z[8]&0x7f)-((z[8]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["B有功功率"]=x
                                P[2]=x
                                if(z[11]&0x80):
                                    x=(z[9]-(z[9]>>4)*6)+(z[10]-(z[10]>>4)*6)*100+((z[11]&0x7f)-((z[11]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[9]-(z[9]>>4)*6)+(z[10]-(z[10]>>4)*6)*100+((z[11]&0x7f)-((z[11]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["C有功功率"]=x
                                P[3]=x
                                if(z[14]&0x80):
                                    x=(z[12]-(z[12]>>4)*6)+(z[13]-(z[13]>>4)*6)*100+((z[14]&0x7f)-((z[14]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[12]-(z[12]>>4)*6)+(z[13]-(z[13]>>4)*6)*100+((z[14]&0x7f)-((z[14]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["总无功功率"]=x
                                Pr[0]=x
                                if(z[17]&0x80):
                                    x=(z[15]-(z[15]>>4)*6)+(z[16]-(z[16]>>4)*6)*100+((z[17]&0x7f)-((z[17]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[15]-(z[15]>>4)*6)+(z[16]-(z[16]>>4)*6)*100+((z[17]&0x7f)-((z[17]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["A无功功率"]=x
                                Pr[1]=x
                                if(z[20]&0x80):
                                    x=(z[18]-(z[18]>>4)*6)+(z[19]-(z[19]>>4)*6)*100+((z[20]&0x7f)-((z[20]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[18]-(z[18]>>4)*6)+(z[19]-(z[19]>>4)*6)*100+((z[20]&0x7f)-((z[20]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["B无功功率"]=x
                                Pr[2]=x
                                if(z[23]&0x80):
                                    x=(z[21]-(z[21]>>4)*6)+(z[22]-(z[22]>>4)*6)*100+((z[23]&0x7f)-((z[23]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[21]-(z[21]>>4)*6)+(z[22]-(z[22]>>4)*6)*100+((z[23]&0x7f)-((z[23]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["C无功功率"]=x
                                Pr[3]=x
                                x=(z[24]-(z[24]>>4)*6)+(z[25]-(z[25]>>4)*6)*100
                                x=float(x)/1000
                                dic["总功率因素"]=x
                                PF[0]=x
                                x=(z[26]-(z[26]>>4)*6)+(z[27]-(z[27]>>4)*6)*100
                                x=float(x)/1000
                                dic["A功率因素"]=x
                                PF[1]=x
                                x=(z[28]-(z[28]>>4)*6)+(z[29]-(z[29]>>4)*6)*100
                                x=float(x)/1000
                                dic["B功率因素"]=x
                                PF[2]=x
                                x=(z[30]-(z[30]>>4)*6)+(z[31]-(z[31]>>4)*6)*100
                                x=float(x)/1000
                                dic["C功率因素"]=x
                                PF[3]=x
                                x=(z[32]-(z[32]>>4)*6)+(z[33]-(z[33]>>4)*6)*100
                                x=float(x)/10
#                                print z[32]
#                                print z[33]
#                                print x
                                dic["A相电压"]=x
                                V[0]=x
                                x=(z[34]-(z[34]>>4)*6)+(z[35]-(z[35]>>4)*6)*100
                                
                                x=float(x)/10
                                dic["B相电压"]=x
                                V[1]=x
                                x=(z[36]-(z[36]>>4)*6)+(z[37]-(z[37]>>4)*6)*100
                                x=float(x)/10
                                dic["C相电压"]=x
                                V[2]=x
                                if(z[40]&0x80):
                                    x=(z[38]-(z[38]>>4)*6)+(z[39]-(z[39]>>4)*6)*100+((z[40]&0x7f)-((z[40]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/1000)
                                else:
                                    x=(z[38]-(z[38]>>4)*6)+(z[39]-(z[39]>>4)*6)*100+((z[40]&0x7f)-((z[40]&0x7f)>>4)*6)*10000
                                    x=float(x)/1000
                                dic["A相电流"]=x
                                I[0]=x
                                if(z[43]&0x80):
                                    x=(z[41]-(z[41]>>4)*6)+(z[42]-(z[42]>>4)*6)*100+((z[43]&0x7f)-((z[43]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/1000)
                                else:
                                    x=(z[41]-(z[41]>>4)*6)+(z[42]-(z[42]>>4)*6)*100+((z[43]&0x7f)-((z[43]&0x7f)>>4)*6)*10000
                                    x=float(x)/1000
                                dic["B相电流"]=x
                                I[1]=x
                                if(z[46]&0x80):
                                    x=(z[44]-(z[44]>>4)*6)+(z[45]-(z[45]>>4)*6)*100+((z[46]&0x7f)-((z[46]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/1000)
                                else:
                                    x=(z[44]-(z[44]>>4)*6)+(z[45]-(z[45]>>4)*6)*100+((z[46]&0x7f)-((z[46]&0x7f)>>4)*6)*10000
                                    x=float(x)/1000
                                dic["C相电流"]=x
                                I[2]=x
                                x=(z[47]-(z[47]>>4)*6)+(z[48]-(z[48]>>4)*6)*100+(z[49]-(z[49]>>4)*6)*10000+(z[50]-(z[50]>>4)*6)*1000000
                                x=float(x)/100
                                dic["总有功电能"]=x
                                x=(z[51]-(z[51]>>4)*6)+(z[52]-(z[52]>>4)*6)*100+(z[53]-(z[53]>>4)*6)*10000+(z[54]-(z[54]>>4)*6)*1000000
                                x=float(x)/100
                                dic["总无功电能"]=x
                                x=(z[55]-(z[55]>>4)*6)+(z[56]-(z[56]>>4)*6)*100+(z[57]-(z[57]>>4)*6)*10000+(z[58]-(z[58]>>4)*6)*1000000
                                x=float(x)/100
                                dic["总有反电能"]=x
                                x=(z[59]-(z[59]>>4)*6)+(z[60]-(z[60]>>4)*6)*100+(z[61]-(z[61]>>4)*6)*10000+(z[62]-(z[62]>>4)*6)*1000000
                                x=float(x)/100
                                dic["总无反电能"]=x
#                                print(dic)
#                                payload=json.dumps(dic)
#                                client.publish(pub,payload,qos) 
                                serviceids=serviceid.split(',')
                                dic1={}  #v
                                dic1['clientid']=pub
                                dic1['mspid']=mspid#uppub
                                dic1['time_stamp']=ts
                                dic1['actioncode']=serviceids[0]
                                dic1['data_value']=[]
                                dic1['data_value'].append({'UA':V[0]})
                                dic1['data_value'].append({'UB':V[1]})
                                dic1['data_value'].append({'UC':V[2]})
                                payload=json.dumps(dic1)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic2={}  #i
                                dic2['clientid']=pub
                                dic2['mspid']=mspid#uppub
                                dic2['time_stamp']=ts
                                dic2['actioncode']=serviceids[1]
                                dic2['data_value']=[]
                                dic2['data_value'].append({'IA':I[0]})
                                dic2['data_value'].append({'IB':I[1]})
                                dic2['data_value'].append({'IC':I[2]})
                                payload=json.dumps(dic2)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic3={}  #pf
                                dic3['clientid']=pub
                                dic3['mspid']=mspid#uppub
                                dic3['time_stamp']=ts
                                dic3['actioncode']=serviceids[2]
                                dic3['data_value']=[]
                                dic3['data_value'].append({'PF':PF[0]})
                                payload=json.dumps(dic3)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic4={}  #energy
                                dic4['clientid']=pub
                                dic4['mspid']=mspid#uppub
                                dic4['time_stamp']=ts
                                dic4['actioncode']='APValueRecord'
                                dic4['data_value']=[]
                                dic4['data_value'].append({'PAPValue':dic["总有功电能"]})
                                dic4['data_value'].append({'RAPValue':dic["总有反电能"]})
                                payload=json.dumps(dic4)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic5={}  #energy
                                dic5['clientid']=pub
                                dic5['mspid']=mspid#uppub
                                dic5['time_stamp']=ts
                                dic5['actioncode']='RPValueRecord'
                                dic5['data_value']=[]
                                dic5['data_value'].append({'PRPValue':dic["总无功电能"]})
                                dic5['data_value'].append({'RRPValue':dic["总无反电能"]})
                                payload=json.dumps(dic5)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                m_wVolGuoya=(int(modbus[49]['Uppub']))*(modbus[49]['Startaddr'])/100.0
                                m_wVolGuoyaRe=(int(modbus[49]['Uppub']))*(modbus[49]['Endaddr'])/100.0
                                m_wVolDiya=(int(modbus[48]['Uppub']))*(modbus[48]['Startaddr'])/100.0
                                m_wVolDiyaRe=(int(modbus[48]['Uppub']))*(modbus[48]['Endaddr'])/100.0
                                m_wVolShiya=(int(modbus[47]['Uppub']))*(modbus[47]['Startaddr'])/100.0
                                m_wVolShiyaRe=(int(modbus[47]['Uppub']))*(modbus[47]['Endaddr'])/100.0
                                m_wCurGuoliu=(int(modbus[46]['Uppub']))*(modbus[46]['Startaddr'])/100.0
                                m_wCurGuoliuRe=(int(modbus[46]['Uppub']))*(modbus[46]['Endaddr'])/100.0
                                m_wFactorGuogao=(modbus[45]['Startaddr'])/100.0
                                m_wFactorGuogaoRe=(modbus[45]['Endaddr'])/100.0
                                m_wFactorGuodi=(modbus[44]['Startaddr'])/100.0
                                m_wFactorGuodiRe=(modbus[44]['Endaddr'])/100.0
                                
                                if dicForEventsV.has_key(uppub):
                                    m_bVolStat=dicForEventsV[uppub]
                                    m_bCurStat=dicForEventsI[uppub]
                                    m_bFactorStat=dicForEventsF[uppub]
                                    m_wPCStat=dicForEventsP[uppub]
                                    m_dwVolDelay=dicForEventsVDelay[uppub]
                                    m_dwCurDelay=dicForEventsIDelay[uppub]
                                    m_dwFDelay=dicForEventsFDelay[uppub]
                                    m_dwPDelay=dicForEventsPDelay[uppub]
                                else:
                                    m_bVolStat=[0,0,0]
                                    m_bCurStat=[0,0,0]
                                    m_bFactorStat=0
                                    m_wPCStat=0
                                    m_dwVolDelay=[[0 for i in range(3)] for i in range(3)]
                                    m_dwCurDelay=[[0 for i in range(3)] for i in range(1)]
                                    m_dwFDelay=[0 for i in range(2)]
                                    m_dwPDelay=0
                                
                                #bVolStat=[0,0,0]
                                #bCurStat=[0,0,0]
                                bVolStat[0]=0
                                bVolStat[1]=0
                                bVolStat[2]=0
                                bCurStat[0]=0
                                bCurStat[1]=0
                                bCurStat[2]=0
                                bFactorStat=0
                                wPCStat=0
                                
                                for index in range(3):
                                    if V[index]>m_wVolGuoya:
                                        bVolStat[index] |=0x01
                                    elif V[index]<m_wVolGuoyaRe:
                                        bVolStat[index] |=0x02
                                    
                                    if V[index]<m_wVolDiya:
                                        bVolStat[index] |=0x04
                                    elif V[index]>m_wVolDiyaRe:
                                        bVolStat[index] |=0x08
                                        
                                    if V[index]<m_wVolShiya:
                                        bVolStat[index] |=0x10
                                    elif V[index]>m_wVolShiyaRe:
                                        bVolStat[index] |=0x20
                                        
                                    if abs(I[index])>m_wCurGuoliu:
                                        bCurStat[index] |=0x01
                                    elif I[index]<m_wCurGuoliuRe:
                                        bCurStat[index] |=0x02
                                #print(bCurStat)
                                #print(bVolStat)
                                
                                if P[0]<0:
                                    wPCStat |=0x01
                                else:
                                    wPCStat |=0x02
                                
                                if PF[0]>m_wFactorGuogao:
                                    bFactorStat |=0x01
                                elif PF[0]<m_wFactorGuogaoRe:
                                    bFactorStat |=0x02
                                
                                if PF[0]<m_wFactorGuodi:
                                    bFactorStat |=0x04
                                elif PF[0]>m_wFactorGuodiRe:
                                    bFactorStat |=0x08

                                for lindex in range(3):
                                    s=lindex*2
                                    for index in range(3):
                                        bCur=(bVolStat[index]>>s)&0x03
                                        bLast=(m_bVolStat[index]>>s)&0x03
                                        if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                            if bLast==0x00:
                                                m_dwVolDelay[lindex][index]=0
                                                m_bVolStat[index] |=(0x01<<s)
                                                bLast |=0x01
                                            if bLast!=0x00:#else:
                                                if m_dwVolDelay[lindex][index]<modbus[49-lindex]['Interval']:
                                                    m_dwVolDelay[lindex][index] +=1
                                                else:
                                                    if bLast==0x01:
                                                        m_bVolStat[index] |=(0x02<<s)
                                                        print(m_bVolStat)
                                                        
                                                        #write mqtt event
                                                        dic={}  #v
                                                        dic['clientid']=pub
                                                        dic['mspid']=mspid#uppub
                                                        dic['time_stamp']=ts
                                                        dic['actioncode']=modbus[49-lindex]['ServiceId']
                                                        dic['data_value']=[]
                                                        dic['data_value'].append({'UA':V[0]})
                                                        dic['data_value'].append({'UB':V[1]})
                                                        dic['data_value'].append({'UC':V[2]})
                                                        payload=json.dumps(dic)
                                                        print(payload)
                                                        client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[49-lindex]['Qos'],retain=True) 
                                                        #wrtie sql
                                                        cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                        cx.commit()  
                                                        
                                                        #cu = cx.execute('select Pub, mspId, Uppub, ServiceId, strJson, time_stamp from history where mspId="D01" and time_stamp between "2018-10-19 10:00:00" and "2018-10-19 12:00:00"')
                                                        #history = [dict(Pub =row[0], mspId=row[1], Uppub=row[2], ServiceId=row[3], strJson=row[4], time_stamp=row[5]) for row in cu.fetchall()]
                                                        #print(history)
                                                    #else:
                                                        #total time add
                                                        
                                        elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                            if bLast==0x03:
                                                m_bVolStat[index] &=~(0x03<<s)
                                                print(m_bVolStat)
                                                #write mqtt eventover
                                                #write sql
                                            m_bVolStat[index] &=~(0x03<<s)
                                        
                                for lindex in range(1):
                                    s=lindex*2
                                    for index in range(3):
                                        bCur=(bCurStat[index]>>s)&0x03
                                        bLast=(m_bCurStat[index]>>s)&0x03
                                        if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                            if bLast==0x00:
                                                m_dwCurDelay[lindex][index]=0
                                                m_bCurStat[index] |=(0x01<<s)
                                                bLast |=0x01
                                            if bLast!=0x00:#else:
                                                if m_dwCurDelay[lindex][index]<modbus[46-lindex]['Interval']:
                                                    m_dwCurDelay[lindex][index] +=1
                                                else:
                                                    if bLast==0x01:
                                                        m_bCurStat[index] |=(0x02<<s)
                                                        print(m_bCurStat)
                                                        #write mqtt event
                                                        dic={}  #v
                                                        dic['clientid']=pub
                                                        dic['mspid']=mspid#uppub
                                                        dic['time_stamp']=ts
                                                        dic['actioncode']=modbus[46-lindex]['ServiceId']
                                                        dic['data_value']=[]
                                                        dic['data_value'].append({'IA':I[0]})
                                                        dic['data_value'].append({'IB':I[1]})
                                                        dic['data_value'].append({'IC':I[2]})
                                                        payload=json.dumps(dic)
                                                        print(payload)
                                                        client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[46-lindex]['Qos'],retain=True) 
                                                        #wrtie sql
                                                        cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                        cx.commit()  
                                                    #else:
                                                        #total time add
                                                        
                                        elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                            if bLast==0x03:
                                                m_bCurStat[index] &=~(0x03<<s)
                                                print(m_bCurStat)
                                                #write mqtt eventover
                                                #write sql
                                            m_bCurStat[index] &=~(0x03<<s)
                                
                                for lindex in range(2):
                                    s=lindex*2
                                    bCur=(bFactorStat>>s)&0x03
                                    bLast=(m_bFactorStat>>s)&0x03
                                    if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                        if bLast==0x00:
                                            m_dwFDelay[lindex]=0
                                            m_bFactorStat |=(0x01<<s)
                                            bLast |=0x01
                                        if bLast!=0x00:#else:
                                            if m_dwFDelay[lindex]<modbus[45-lindex]['Interval']:
                                                m_dwFDelay[lindex] +=1
                                            else:
                                                if bLast==0x01:
                                                    m_bFactorStat |=(0x02<<s)
                                                    print(m_bFactorStat)
                                                    
                                                    #write mqtt event
                                                    dic={}  #f
                                                    dic['clientid']=pub
                                                    dic['mspid']=mspid#uppub
                                                    dic['time_stamp']=ts
                                                    dic['actioncode']=modbus[45-lindex]['ServiceId']
                                                    dic['data_value']=[]
                                                    dic['data_value'].append({'PF':PF[0]})
                                                    payload=json.dumps(dic)
                                                    print(payload)
                                                    client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[45-lindex]['Qos'],retain=True) 
                                                    #wrtie sql
                                                    cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                    cx.commit()
                                                #else:
                                                    #total time add
                                                    
                                    elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                        if bLast==0x03:
                                            m_bFactorStat &=~(0x03<<s)
                                            print(m_bFactorStat)
                                            #write mqtt eventover
                                            #write sql
                                        m_bFactorStat &=~(0x03<<s)
                                
                                s=0
                                bCur=(wPCStat>>s)&0x03
                                bLast=(m_wPCStat>>s)&0x03
                                if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                    if bLast==0x00:
                                        m_dwPDelay=0
                                        m_wPCStat |=(0x01<<s)
                                        bLast |=0x01
                                    if bLast!=0x00:#else:
                                        if m_dwPDelay<modbus[43]['Interval']:
                                            m_dwPDelay +=1
                                        else:
                                            if bLast==0x01:
                                                m_wPCStat |=(0x02<<s)
                                                print(m_wPCStat)
                                                
                                                #write mqtt event
                                                dic={}  #-p
                                                dic['clientid']=pub
                                                dic['mspid']=mspid#uppub
                                                dic['time_stamp']=ts
                                                dic['actioncode']=modbus[43]['ServiceId']
                                                dic['data_value']=[]
                                                dic['data_value'].append({'TotalAP':P[0]})
                                                payload=json.dumps(dic)
                                                print(payload)
                                                client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[43-lindex]['Qos'],retain=True) 
                                                #wrtie sql
                                                cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                cx.commit()  
                                            #else:
                                                #total time add
                                                
                                elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                    if bLast==0x03:
                                        m_wPCStat &=~(0x03<<s)
                                        print(m_wPCStat)
                                        #write mqtt eventover
                                        #write sql
                                    m_wPCStat &=~(0x03<<s)
                                
                                dicForEventsV[uppub]=m_bVolStat
                                dicForEventsI[uppub]=m_bCurStat
                                dicForEventsF[uppub]=m_bFactorStat
                                dicForEventsP[uppub]=m_wPCStat
                                dicForEventsVDelay[uppub]=m_dwVolDelay
                                dicForEventsIDelay[uppub]=m_dwCurDelay
                                dicForEventsFDelay[uppub]=m_dwFDelay
                                dicForEventsPDelay[uppub]=m_dwPDelay
                                '''
                                print(dicForEventsV)
                                print(dicForEventsI)
                                print(dicForEventsF)
                                print(dicForEventsP)
                                print(dicForEventsVDelay)
                                print(dicForEventsIDelay)
                                print(dicForEventsFDelay)
                                print(dicForEventsPDelay)
                                '''
                                
#                                m_dwCurReverse
#                                m_dwCurReverseRe
                                
#                                print(payload)
                            else:
                                dic={}
                                for index in range(len(jsondates)):
                                    dic[jsondates[index]]=mbdates
                                dic['ts']=ts
                                payload=json.dumps(dic)
                                #payload=repr(q.get())
                                client.publish(pub,payload,qos) 
                                print(payload)

                            times[mspid]=0
                        else:
                            if mspid in times:
                                #print(mspid)
                                times[mspid]=times[mspid]+1
                                #print(times[mspid])
                                if times[mspid]==40:
                                    #up deviceevent
                                    ts = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
                                    print(ts)
                                    dic={}  #-p
                                    dic['clientid']=pub
                                    dic['mspid']=mspid#uppub
                                    dic['time_stamp']=ts
                                    dic['actioncode']='DeviceStatus'
                                    dic['data_value']=[]
                                    dic['data_value'].append({'SensorStatusCode':'001'})
                                    payload=json.dumps(dic)
                                    print(payload)
                                    client.publish('/mqtt/deviceevent/'+pub+'/'+mspid,payload,2,retain=True) 
                                    
                            else:
                                times[mspid]=0
                                #print(mspid)
                                #print(times[mspid])
                    elif devicetype==3:
                        if startaddr == 50331392:
                            print('start evp')
                            getdata2(uppub,startaddr,endaddr,interval)
                            while not q.empty():
                                #ts=time.strftime("%Y-%m-%d %H:%M:%S")
                                ts = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
                                print(ts)
                                mbdates=q.get()
                                #print(len(mbdates))
                                #print(mbdates)
                                jsondates=jsondate.split(',')
#                                print(len(jsondates))
#                                print(jsondates)
                                dic={}
                                z=mbdates
                                print(z)
                                #time.sleep(10)
                                
                                dic['ts']=ts
                                if(z[2][2]&0x80):
                                    x=(z[2][0]-(z[2][0]>>4)*6)+(z[2][1]-(z[2][1]>>4)*6)*100+((z[2][2]&0x7f)-((z[2][2]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[2][0]-(z[2][0]>>4)*6)+(z[2][1]-(z[2][1]>>4)*6)*100+((z[2][2]&0x7f)-((z[2][2]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["总有功功率"]=x
                                P[0]=x
                                if(z[2][5]&0x80):
                                    x=(z[2][3]-(z[2][3]>>4)*6)+(z[2][4]-(z[2][4]>>4)*6)*100+((z[2][5]&0x7f)-((z[2][5]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[2][3]-(z[2][3]>>4)*6)+(z[2][4]-(z[2][4]>>4)*6)*100+((z[2][5]&0x7f)-((z[2][5]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["A有功功率"]=x
                                P[1]=x
                                if(z[2][8]&0x80):
                                    x=(z[2][6]-(z[2][6]>>4)*6)+(z[2][7]-(z[2][7]>>4)*6)*100+((z[2][8]&0x7f)-((z[2][8]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[2][6]-(z[2][6]>>4)*6)+(z[2][7]-(z[2][7]>>4)*6)*100+((z[2][8]&0x7f)-((z[2][8]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["B有功功率"]=x
                                P[2]=x
                                if(z[2][11]&0x80):
                                    x=(z[2][9]-(z[2][9]>>4)*6)+(z[2][10]-(z[2][10]>>4)*6)*100+((z[2][11]&0x7f)-((z[2][11]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[2][9]-(z[2][9]>>4)*6)+(z[2][10]-(z[2][10]>>4)*6)*100+((z[2][11]&0x7f)-((z[2][11]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["C有功功率"]=x
                                P[3]=x
                                if(z[3][2]&0x80):
                                    x=(z[3][0]-(z[3][0]>>4)*6)+(z[3][1]-(z[3][1]>>4)*6)*100+((z[3][2]&0x7f)-((z[3][2]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[3][0]-(z[3][0]>>4)*6)+(z[3][1]-(z[3][1]>>4)*6)*100+((z[3][2]&0x7f)-((z[3][2]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["总无功功率"]=x
                                Pr[0]=x
                                if(z[3][5]&0x80):
                                    x=(z[3][3]-(z[3][3]>>4)*6)+(z[3][4]-(z[3][4]>>4)*6)*100+((z[3][5]&0x7f)-((z[3][5]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[3][3]-(z[3][3]>>4)*6)+(z[3][4]-(z[3][4]>>4)*6)*100+((z[3][5]&0x7f)-((z[3][5]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["A无功功率"]=x
                                Pr[1]=x
                                if(z[3][8]&0x80):
                                    x=(z[3][6]-(z[3][6]>>4)*6)+(z[3][7]-(z[3][7]>>4)*6)*100+((z[3][8]&0x7f)-((z[3][8]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[3][6]-(z[3][6]>>4)*6)+(z[3][7]-(z[3][7]>>4)*6)*100+((z[3][8]&0x7f)-((z[3][8]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["B无功功率"]=x
                                Pr[2]=x
                                if(z[3][11]&0x80):
                                    x=(z[3][9]-(z[3][9]>>4)*6)+(z[3][10]-(z[3][10]>>4)*6)*100+((z[3][11]&0x7f)-((z[3][11]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[3][9]-(z[3][9]>>4)*6)+(z[3][10]-(z[3][10]>>4)*6)*100+((z[3][11]&0x7f)-((z[3][11]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["C无功功率"]=x
                                Pr[3]=x
                                if(z[5][1]&0x80):
                                    x=(z[5][0]-(z[5][0]>>4)*6)+((z[5][1]&0x7f)-((z[5][1]&0x7f)>>4)*6)*100
                                    x=-(float(x)/1000)
                                else:
                                    x=(z[5][0]-(z[5][0]>>4)*6)+((z[5][1]&0x7f)-((z[5][1]&0x7f)>>4)*6)*100
                                    x=float(x)/1000
                                dic["总功率因素"]=x
                                PF[0]=x
                                x=(z[5][2]-(z[5][2]>>4)*6)+(z[5][3]-(z[5][3]>>4)*6)*100
                                x=float(x)/1000
                                dic["A功率因素"]=x
                                PF[1]=x
                                x=(z[5][4]-(z[5][4]>>4)*6)+(z[5][5]-(z[5][5]>>4)*6)*100
                                x=float(x)/1000
                                dic["B功率因素"]=x
                                PF[2]=x
                                x=(z[5][6]-(z[5][6]>>4)*6)+(z[5][7]-(z[5][7]>>4)*6)*100
                                x=float(x)/1000
                                dic["C功率因素"]=x
                                PF[3]=x
                                x=(z[0][0]-(z[0][0]>>4)*6)+(z[0][1]-(z[0][1]>>4)*6)*100
                                x=float(x)/10
#                                print z[32]
#                                print z[33]
#                                print x
                                dic["A相电压"]=x
                                V[0]=x
                                x=(z[0][2]-(z[0][2]>>4)*6)+(z[0][3]-(z[0][3]>>4)*6)*100
                                
                                x=float(x)/10
                                dic["B相电压"]=x
                                V[1]=x
                                x=(z[0][4]-(z[0][4]>>4)*6)+(z[0][5]-(z[0][5]>>4)*6)*100
                                x=float(x)/10
                                dic["C相电压"]=x
                                V[2]=x
                                if(z[1][2]&0x80):
                                    x=(z[1][0]-(z[1][0]>>4)*6)+(z[1][1]-(z[1][1]>>4)*6)*100+((z[1][2]&0x7f)-((z[1][2]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/1000)
                                else:
                                    x=(z[1][0]-(z[1][0]>>4)*6)+(z[1][1]-(z[1][1]>>4)*6)*100+((z[1][2]&0x7f)-((z[1][2]&0x7f)>>4)*6)*10000
                                    x=float(x)/1000
                                dic["A相电流"]=x
                                I[0]=x
                                if(z[1][5]&0x80):
                                    x=(z[1][3]-(z[1][3]>>4)*6)+(z[1][4]-(z[1][4]>>4)*6)*100+((z[1][5]&0x7f)-((z[1][5]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/1000)
                                else:
                                    x=(z[1][3]-(z[1][3]>>4)*6)+(z[1][4]-(z[1][4]>>4)*6)*100+((z[1][5]&0x7f)-((z[1][5]&0x7f)>>4)*6)*10000
                                    x=float(x)/1000
                                dic["B相电流"]=x
                                I[1]=x
                                if(z[1][8]&0x80):
                                    x=(z[1][6]-(z[1][6]>>4)*6)+(z[1][7]-(z[1][7]>>4)*6)*100+((z[1][8]&0x7f)-((z[1][8]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/1000)
                                else:
                                    x=(z[1][6]-(z[1][6]>>4)*6)+(z[1][7]-(z[1][7]>>4)*6)*100+((z[1][8]&0x7f)-((z[1][8]&0x7f)>>4)*6)*10000
                                    x=float(x)/1000
                                dic["C相电流"]=x
                                I[2]=x
                                x=(z[14][0]-(z[14][0]>>4)*6)+(z[14][1]-(z[14][1]>>4)*6)*100+(z[14][2]-(z[14][2]>>4)*6)*10000+(z[14][3]-(z[14][3]>>4)*6)*1000000
                                x=float(x)/100
                                dic["总有功电能"]=x
                                x=(z[16][0]-(z[16][0]>>4)*6)+(z[16][1]-(z[16][1]>>4)*6)*100+(z[16][2]-(z[16][2]>>4)*6)*10000+(z[16][3]-(z[16][3]>>4)*6)*1000000
                                x=float(x)/100
                                dic["总无功电能"]=x
                                x=(z[15][0]-(z[15][0]>>4)*6)+(z[15][1]-(z[15][1]>>4)*6)*100+(z[15][2]-(z[15][2]>>4)*6)*10000+(z[15][3]-(z[15][3]>>4)*6)*1000000
                                x=float(x)/100
                                dic["总有反电能"]=x
                                x=(z[17][0]-(z[17][0]>>4)*6)+(z[17][1]-(z[17][1]>>4)*6)*100+(z[17][2]-(z[17][2]>>4)*6)*10000+(z[17][3]-(z[17][3]>>4)*6)*1000000
                                x=float(x)/100
                                dic["总无反电能"]=x
                                #print(dic)
                                #payload=json.dumps(dic)
                                #client.publish(pub,payload,qos) 
                                #print(payload)
                                
                                serviceids=serviceid.split(',')
                                dic1={}  #v
                                dic1['clientid']=pub
                                dic1['mspid']=mspid#uppub
                                dic1['time_stamp']=ts
                                dic1['actioncode']=serviceids[0]
                                dic1['data_value']=[]
                                dic1['data_value'].append({'UA':V[0]})
                                dic1['data_value'].append({'UB':V[1]})
                                dic1['data_value'].append({'UC':V[2]})
                                payload=json.dumps(dic1)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic2={}  #i
                                dic2['clientid']=pub
                                dic2['mspid']=mspid#uppub
                                dic2['time_stamp']=ts
                                dic2['actioncode']=serviceids[1]
                                dic2['data_value']=[]
                                dic2['data_value'].append({'IA':I[0]})
                                dic2['data_value'].append({'IB':I[1]})
                                dic2['data_value'].append({'IC':I[2]})
                                payload=json.dumps(dic2)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic3={}  #pf
                                dic3['clientid']=pub
                                dic3['mspid']=mspid#uppub
                                dic3['time_stamp']=ts
                                dic3['actioncode']=serviceids[2]
                                dic3['data_value']=[]
                                dic3['data_value'].append({'PF':PF[0]})
                                payload=json.dumps(dic3)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic4={}  #energy
                                dic4['clientid']=pub
                                dic4['mspid']=mspid#uppub
                                dic4['time_stamp']=ts
                                dic4['actioncode']='APValueRecord'
                                dic4['data_value']=[]
                                dic4['data_value'].append({'PAPValue':dic["总有功电能"]})
                                dic4['data_value'].append({'RAPValue':dic["总有反电能"]})
                                payload=json.dumps(dic4)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic5={}  #energy
                                dic5['clientid']=pub
                                dic5['mspid']=mspid#uppub
                                dic5['time_stamp']=ts
                                dic5['actioncode']='RPValueRecord'
                                dic5['data_value']=[]
                                dic5['data_value'].append({'PRPValue':dic["总无功电能"]})
                                dic5['data_value'].append({'RRPValue':dic["总无反电能"]})
                                payload=json.dumps(dic5)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                m_wVolGuoya=(int(modbus[49]['Uppub']))*(modbus[49]['Startaddr'])/100.0
                                m_wVolGuoyaRe=(int(modbus[49]['Uppub']))*(modbus[49]['Endaddr'])/100.0
                                m_wVolDiya=(int(modbus[48]['Uppub']))*(modbus[48]['Startaddr'])/100.0
                                m_wVolDiyaRe=(int(modbus[48]['Uppub']))*(modbus[48]['Endaddr'])/100.0
                                m_wVolShiya=(int(modbus[47]['Uppub']))*(modbus[47]['Startaddr'])/100.0
                                m_wVolShiyaRe=(int(modbus[47]['Uppub']))*(modbus[47]['Endaddr'])/100.0
                                m_wCurGuoliu=(int(modbus[46]['Uppub']))*(modbus[46]['Startaddr'])/100.0
                                m_wCurGuoliuRe=(int(modbus[46]['Uppub']))*(modbus[46]['Endaddr'])/100.0
                                m_wFactorGuogao=(modbus[45]['Startaddr'])/100.0
                                m_wFactorGuogaoRe=(modbus[45]['Endaddr'])/100.0
                                m_wFactorGuodi=(modbus[44]['Startaddr'])/100.0
                                m_wFactorGuodiRe=(modbus[44]['Endaddr'])/100.0
                                
                                if dicForEventsV.has_key(uppub):
                                    m_bVolStat=dicForEventsV[uppub]
                                    m_bCurStat=dicForEventsI[uppub]
                                    m_bFactorStat=dicForEventsF[uppub]
                                    m_wPCStat=dicForEventsP[uppub]
                                    m_dwVolDelay=dicForEventsVDelay[uppub]
                                    m_dwCurDelay=dicForEventsIDelay[uppub]
                                    m_dwFDelay=dicForEventsFDelay[uppub]
                                    m_dwPDelay=dicForEventsPDelay[uppub]
                                else:
                                    m_bVolStat=[0,0,0]
                                    m_bCurStat=[0,0,0]
                                    m_bFactorStat=0
                                    m_wPCStat=0
                                    m_dwVolDelay=[[0 for i in range(3)] for i in range(3)]
                                    m_dwCurDelay=[[0 for i in range(3)] for i in range(1)]
                                    m_dwFDelay=[0 for i in range(2)]
                                    m_dwPDelay=0
                                
                                #bVolStat=[0,0,0]
                                #bCurStat=[0,0,0]
                                bVolStat[0]=0
                                bVolStat[1]=0
                                bVolStat[2]=0
                                bCurStat[0]=0
                                bCurStat[1]=0
                                bCurStat[2]=0
                                bFactorStat=0
                                wPCStat=0
                                
                                for index in range(3):
                                    if V[index]>m_wVolGuoya:
                                        bVolStat[index] |=0x01
                                    elif V[index]<m_wVolGuoyaRe:
                                        bVolStat[index] |=0x02
                                    
                                    if V[index]<m_wVolDiya:
                                        bVolStat[index] |=0x04
                                    elif V[index]>m_wVolDiyaRe:
                                        bVolStat[index] |=0x08
                                        
                                    if V[index]<m_wVolShiya:
                                        bVolStat[index] |=0x10
                                    elif V[index]>m_wVolShiyaRe:
                                        bVolStat[index] |=0x20
                                        
                                    if abs(I[index])>m_wCurGuoliu:
                                        bCurStat[index] |=0x01
                                    elif I[index]<m_wCurGuoliuRe:
                                        bCurStat[index] |=0x02
                                #print(bCurStat)
                                #print(bVolStat)
                                
                                if P[0]<0:
                                    wPCStat |=0x01
                                else:
                                    wPCStat |=0x02
                                
                                if PF[0]>m_wFactorGuogao:
                                    bFactorStat |=0x01
                                elif PF[0]<m_wFactorGuogaoRe:
                                    bFactorStat |=0x02
                                
                                if PF[0]<m_wFactorGuodi:
                                    bFactorStat |=0x04
                                elif PF[0]>m_wFactorGuodiRe:
                                    bFactorStat |=0x08

                                for lindex in range(3):
                                    s=lindex*2
                                    for index in range(3):
                                        bCur=(bVolStat[index]>>s)&0x03
                                        bLast=(m_bVolStat[index]>>s)&0x03
                                        if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                            if bLast==0x00:
                                                m_dwVolDelay[lindex][index]=0
                                                m_bVolStat[index] |=(0x01<<s)
                                                bLast |=0x01
                                            if bLast!=0x00:#else:
                                                if m_dwVolDelay[lindex][index]<modbus[49-lindex]['Interval']:
                                                    m_dwVolDelay[lindex][index] +=1
                                                else:
                                                    if bLast==0x01:
                                                        m_bVolStat[index] |=(0x02<<s)
                                                        print(m_bVolStat)
                                                        
                                                        #write mqtt event
                                                        dic={}  #v
                                                        dic['clientid']=pub
                                                        dic['mspid']=mspid#uppub
                                                        dic['time_stamp']=ts
                                                        dic['actioncode']=modbus[49-lindex]['ServiceId']
                                                        dic['data_value']=[]
                                                        dic['data_value'].append({'UA':V[0]})
                                                        dic['data_value'].append({'UB':V[1]})
                                                        dic['data_value'].append({'UC':V[2]})
                                                        payload=json.dumps(dic)
                                                        print(payload)
                                                        client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[49-lindex]['Qos'],retain=True) 
                                                        #wrtie sql
                                                        cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                        cx.commit()  
                                                        
                                                        #cu = cx.execute('select Pub, mspId, Uppub, ServiceId, strJson, time_stamp from history where mspId="D01" and time_stamp between "2018-10-19 10:00:00" and "2018-10-19 12:00:00"')
                                                        #history = [dict(Pub =row[0], mspId=row[1], Uppub=row[2], ServiceId=row[3], strJson=row[4], time_stamp=row[5]) for row in cu.fetchall()]
                                                        #print(history)
                                                    #else:
                                                        #total time add
                                                        
                                        elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                            if bLast==0x03:
                                                m_bVolStat[index] &=~(0x03<<s)
                                                print(m_bVolStat)
                                                #write mqtt eventover
                                                #write sql
                                            m_bVolStat[index] &=~(0x03<<s)
                                        
                                for lindex in range(1):
                                    s=lindex*2
                                    for index in range(3):
                                        bCur=(bCurStat[index]>>s)&0x03
                                        bLast=(m_bCurStat[index]>>s)&0x03
                                        if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                            if bLast==0x00:
                                                m_dwCurDelay[lindex][index]=0
                                                m_bCurStat[index] |=(0x01<<s)
                                                bLast |=0x01
                                            if bLast!=0x00:#else:
                                                if m_dwCurDelay[lindex][index]<modbus[46-lindex]['Interval']:
                                                    m_dwCurDelay[lindex][index] +=1
                                                else:
                                                    if bLast==0x01:
                                                        m_bCurStat[index] |=(0x02<<s)
                                                        print(m_bCurStat)
                                                        #write mqtt event
                                                        dic={}  #v
                                                        dic['clientid']=pub
                                                        dic['mspid']=mspid#uppub
                                                        dic['time_stamp']=ts
                                                        dic['actioncode']=modbus[46-lindex]['ServiceId']
                                                        dic['data_value']=[]
                                                        dic['data_value'].append({'IA':I[0]})
                                                        dic['data_value'].append({'IB':I[1]})
                                                        dic['data_value'].append({'IC':I[2]})
                                                        payload=json.dumps(dic)
                                                        print(payload)
                                                        client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[46-lindex]['Qos'],retain=True) 
                                                        #wrtie sql
                                                        cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                        cx.commit()  
                                                    #else:
                                                        #total time add
                                                        
                                        elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                            if bLast==0x03:
                                                m_bCurStat[index] &=~(0x03<<s)
                                                print(m_bCurStat)
                                                #write mqtt eventover
                                                #write sql
                                            m_bCurStat[index] &=~(0x03<<s)
                                
                                for lindex in range(2):
                                    s=lindex*2
                                    bCur=(bFactorStat>>s)&0x03
                                    bLast=(m_bFactorStat>>s)&0x03
                                    if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                        if bLast==0x00:
                                            m_dwFDelay[lindex]=0
                                            m_bFactorStat |=(0x01<<s)
                                            bLast |=0x01
                                        if bLast!=0x00:#else:
                                            if m_dwFDelay[lindex]<modbus[45-lindex]['Interval']:
                                                m_dwFDelay[lindex] +=1
                                            else:
                                                if bLast==0x01:
                                                    m_bFactorStat |=(0x02<<s)
                                                    print(m_bFactorStat)
                                                    
                                                    #write mqtt event
                                                    dic={}  #f
                                                    dic['clientid']=pub
                                                    dic['mspid']=mspid#uppub
                                                    dic['time_stamp']=ts
                                                    dic['actioncode']=modbus[45-lindex]['ServiceId']
                                                    dic['data_value']=[]
                                                    dic['data_value'].append({'PF':PF[0]})
                                                    payload=json.dumps(dic)
                                                    print(payload)
                                                    client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[45-lindex]['Qos'],retain=True) 
                                                    #wrtie sql
                                                    cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                    cx.commit()
                                                #else:
                                                    #total time add
                                                    
                                    elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                        if bLast==0x03:
                                            m_bFactorStat &=~(0x03<<s)
                                            print(m_bFactorStat)
                                            #write mqtt eventover
                                            #write sql
                                        m_bFactorStat &=~(0x03<<s)
                                
                                s=0
                                bCur=(wPCStat>>s)&0x03
                                bLast=(m_wPCStat>>s)&0x03
                                if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                    if bLast==0x00:
                                        m_dwPDelay=0
                                        m_wPCStat |=(0x01<<s)
                                        bLast |=0x01
                                    if bLast!=0x00:#else:
                                        if m_dwPDelay<modbus[43]['Interval']:
                                            m_dwPDelay +=1
                                        else:
                                            if bLast==0x01:
                                                m_wPCStat |=(0x02<<s)
                                                print(m_wPCStat)
                                                
                                                #write mqtt event
                                                dic={}  #-p
                                                dic['clientid']=pub
                                                dic['mspid']=mspid#uppub
                                                dic['time_stamp']=ts
                                                dic['actioncode']=modbus[43]['ServiceId']
                                                dic['data_value']=[]
                                                dic['data_value'].append({'TotalAP':P[0]})
                                                payload=json.dumps(dic)
                                                print(payload)
                                                client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[43-lindex]['Qos'],retain=True) 
                                                #wrtie sql
                                                cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                cx.commit()  
                                            #else:
                                                #total time add
                                                
                                elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                    if bLast==0x03:
                                        m_wPCStat &=~(0x03<<s)
                                        print(m_wPCStat)
                                        #write mqtt eventover
                                        #write sql
                                    m_wPCStat &=~(0x03<<s)
                                
                                dicForEventsV[uppub]=m_bVolStat
                                dicForEventsI[uppub]=m_bCurStat
                                dicForEventsF[uppub]=m_bFactorStat
                                dicForEventsP[uppub]=m_wPCStat
                                dicForEventsVDelay[uppub]=m_dwVolDelay
                                dicForEventsIDelay[uppub]=m_dwCurDelay
                                dicForEventsFDelay[uppub]=m_dwFDelay
                                dicForEventsPDelay[uppub]=m_dwPDelay
                                
                                time.sleep(interval*0.1)
                        else:
                            print('start evp')
                            getdata22(uppub,startaddr,endaddr,interval)
                            while not q.empty():
                                ts = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
                                print(ts)
                                mbdates=q.get()
                                #print(len(mbdates))
                                print(mbdates)
                                jsondates=jsondate.split(',')
                                dic={}
                                for index in range(len(jsondates)):
                                    dic[jsondates[index]]=mbdates
                                dic['ts']=ts
                                payload=json.dumps(dic)
                                #payload=repr(q.get())
                                client.publish(pub,payload,qos) 
                                print(payload)
                                time.sleep(interval*0.1)
                    elif devicetype==4:
                        if uppub:
                            uppubinit=int(uppub)
                        else:
                            uppubinit=1

                        #isfinished.clear()
                        getdata3(uppubinit,startaddr,endaddr,interval)
                        #print('测量中m...')
                        #isfinished.wait()
                        while not q.empty():
                            #ts=time.strftime("%Y-%m-%d %H:%M:%S")
                            ts = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
                            print(ts)
                            mbdates=q.get()
                            #print(len(mbdates))
                            #print(mbdates)
                            jsondates=jsondate.split(',')
                            #print(len(jsondates))
                            #print(jsondates)

                            if startaddr == 259:
                                dic={}
                                z = mbdates
                                #print z
                                dic['ts']=ts
                                if z[9]>32767:
                                    x=float(z[9]-65536)/1000
                                else:
                                    x=float(z[9])/1000
                                dic["总有功功率"]=x
                                P[0]=x
                                if z[6]>32767:
                                    x=float(z[6]-65536)/1000
                                else:
                                    x=float(z[6])/1000
                                dic["A有功功率"]=x
                                P[1]=x
                                if z[7]>32767:
                                    x=float(z[7]-65536)/1000
                                else:
                                    x=float(z[7])/1000
                                dic["B有功功率"]=x
                                P[2]=x
                                if z[8]>32767:
                                    x=float(z[8]-65536)/1000
                                else:
                                    x=float(z[8])/1000
                                dic["C有功功率"]=x
                                P[3]=x
                                if z[13]>32767:
                                    x=float(z[13]-65536)/1000
                                else:
                                    x=float(z[13])/1000
                                dic["总无功功率"]=x
                                Pr[0]=x
                                P[3]=x
                                if z[10]>32767:
                                    x=float(z[10]-65536)/1000
                                else:
                                    x=float(z[10])/1000
                                dic["A无功功率"]=x
                                Pr[1]=x
                                if z[11]>32767:
                                    x=float(z[11]-65536)/1000
                                else:
                                    x=float(z[11])/1000
                                dic["B无功功率"]=x
                                Pr[2]=x
                                if z[12]>32767:
                                    x=float(z[12]-65536)/1000
                                else:
                                    x=float(z[12])/1000
                                dic["C无功功率"]=x
                                Pr[3]=x
                                
                                if z[21]>32767:
                                    x=float(z[21]-65536)/100
                                else:
                                    x=float(z[21])/100
                                dic["总功率因素"]=x
                                PF[0]=x
                                
                                if z[18]>32767:
                                    x=float(z[18]-65536)/100
                                else:
                                    x=float(z[18])/100
                                dic["A功率因素"]=x
                                PF[1]=x
                                
                                if z[19]>32767:
                                    x=float(z[19]-65536)/100
                                else:
                                    x=float(z[19])/100
                                dic["B功率因素"]=x
                                PF[2]=x
                                
                                if z[20]>32767:
                                    x=float(z[20]-65536)/100
                                else:
                                    x=float(z[20])/100
                                dic["C功率因素"]=x
                                PF[3]=x

                                x=float(z[0])/10
                                dic["A相电压"]=x
                                V[0]=x
                                x=float(z[1])/10
                                dic["B相电压"]=x
                                V[1]=x
                                x=float(z[2])/10
                                dic["C相电压"]=x
                                V[2]=x
                                x=float(z[3])/100
                                dic["A相电流"]=x
                                I[0]=x
                                x=float(z[4])/100
                                I[1]=x
                                x=float(z[5])/100
                                dic["C相电流"]=x
                                I[2]=x
                                x=float(z[22])
                                dic["总有功电能"]=x
                                x=float(z[23])
                                dic["总无功电能"]=x
                                x=0
                                dic["总有反电能"]=x
                                x=0
                                dic["总无反电能"]=x
                                #print(dic)
#                                payload=json.dumps(dic)
#                                client.publish(pub,payload,qos) 
                                serviceids=serviceid.split(',')
                                dic1={}  #v
                                dic1['clientid']=pub
                                dic1['mspid']=mspid#uppub
                                dic1['time_stamp']=ts
                                dic1['actioncode']=serviceids[0]
                                dic1['data_value']=[]
                                dic1['data_value'].append({'UA':V[0]})
                                dic1['data_value'].append({'UB':V[1]})
                                dic1['data_value'].append({'UC':V[2]})
                                payload=json.dumps(dic1)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic2={}  #i
                                dic2['clientid']=pub
                                dic2['mspid']=mspid#uppub
                                dic2['time_stamp']=ts
                                dic2['actioncode']=serviceids[1]
                                dic2['data_value']=[]
                                dic2['data_value'].append({'IA':I[0]})
                                dic2['data_value'].append({'IB':I[1]})
                                dic2['data_value'].append({'IC':I[2]})
                                payload=json.dumps(dic2)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic3={}  #pf
                                dic3['clientid']=pub
                                dic3['mspid']=mspid#uppub
                                dic3['time_stamp']=ts
                                dic3['actioncode']=serviceids[2]
                                dic3['data_value']=[]
                                dic3['data_value'].append({'PF':PF[0]})
                                payload=json.dumps(dic3)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic4={}  #energy
                                dic4['clientid']=pub
                                dic4['mspid']=mspid#uppub
                                dic4['time_stamp']=ts
                                dic4['actioncode']='APValueRecord'
                                dic4['data_value']=[]
                                dic4['data_value'].append({'PAPValue':dic["总有功电能"]})
                                dic4['data_value'].append({'RAPValue':dic["总有反电能"]})
                                payload=json.dumps(dic4)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic5={}  #energy
                                dic5['clientid']=pub
                                dic5['mspid']=mspid#uppub
                                dic5['time_stamp']=ts
                                dic5['actioncode']='RPValueRecord'
                                dic5['data_value']=[]
                                dic5['data_value'].append({'PRPValue':dic["总无功电能"]})
                                dic5['data_value'].append({'RRPValue':dic["总无反电能"]})
                                payload=json.dumps(dic5)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                m_wVolGuoya=(int(modbus[49]['Uppub']))*(modbus[49]['Startaddr'])/100.0
                                m_wVolGuoyaRe=(int(modbus[49]['Uppub']))*(modbus[49]['Endaddr'])/100.0
                                m_wVolDiya=(int(modbus[48]['Uppub']))*(modbus[48]['Startaddr'])/100.0
                                m_wVolDiyaRe=(int(modbus[48]['Uppub']))*(modbus[48]['Endaddr'])/100.0
                                m_wVolShiya=(int(modbus[47]['Uppub']))*(modbus[47]['Startaddr'])/100.0
                                m_wVolShiyaRe=(int(modbus[47]['Uppub']))*(modbus[47]['Endaddr'])/100.0
                                m_wCurGuoliu=(int(modbus[46]['Uppub']))*(modbus[46]['Startaddr'])/100.0
                                m_wCurGuoliuRe=(int(modbus[46]['Uppub']))*(modbus[46]['Endaddr'])/100.0
                                m_wFactorGuogao=(modbus[45]['Startaddr'])/100.0
                                m_wFactorGuogaoRe=(modbus[45]['Endaddr'])/100.0
                                m_wFactorGuodi=(modbus[44]['Startaddr'])/100.0
                                m_wFactorGuodiRe=(modbus[44]['Endaddr'])/100.0
                                
                                if dicForEventsV.has_key(uppub):
                                    m_bVolStat=dicForEventsV[uppub]
                                    m_bCurStat=dicForEventsI[uppub]
                                    m_bFactorStat=dicForEventsF[uppub]
                                    m_wPCStat=dicForEventsP[uppub]
                                    m_dwVolDelay=dicForEventsVDelay[uppub]
                                    m_dwCurDelay=dicForEventsIDelay[uppub]
                                    m_dwFDelay=dicForEventsFDelay[uppub]
                                    m_dwPDelay=dicForEventsPDelay[uppub]
                                else:
                                    m_bVolStat=[0,0,0]
                                    m_bCurStat=[0,0,0]
                                    m_bFactorStat=0
                                    m_wPCStat=0
                                    m_dwVolDelay=[[0 for i in range(3)] for i in range(3)]
                                    m_dwCurDelay=[[0 for i in range(3)] for i in range(1)]
                                    m_dwFDelay=[0 for i in range(2)]
                                    m_dwPDelay=0
                                
                                #bVolStat=[0,0,0]
                                #bCurStat=[0,0,0]
                                bVolStat[0]=0
                                bVolStat[1]=0
                                bVolStat[2]=0
                                bCurStat[0]=0
                                bCurStat[1]=0
                                bCurStat[2]=0
                                bFactorStat=0
                                wPCStat=0
                                
                                for index in range(3):
                                    if V[index]>m_wVolGuoya:
                                        bVolStat[index] |=0x01
                                    elif V[index]<m_wVolGuoyaRe:
                                        bVolStat[index] |=0x02
                                    
                                    if V[index]<m_wVolDiya:
                                        bVolStat[index] |=0x04
                                    elif V[index]>m_wVolDiyaRe:
                                        bVolStat[index] |=0x08
                                        
                                    if V[index]<m_wVolShiya:
                                        bVolStat[index] |=0x10
                                    elif V[index]>m_wVolShiyaRe:
                                        bVolStat[index] |=0x20
                                        
                                    if abs(I[index])>m_wCurGuoliu:
                                        bCurStat[index] |=0x01
                                    elif I[index]<m_wCurGuoliuRe:
                                        bCurStat[index] |=0x02
                                #print(bCurStat)
                                #print(bVolStat)
                                
                                if P[0]<0:
                                    wPCStat |=0x01
                                else:
                                    wPCStat |=0x02
                                
                                if PF[0]>m_wFactorGuogao:
                                    bFactorStat |=0x01
                                elif PF[0]<m_wFactorGuogaoRe:
                                    bFactorStat |=0x02
                                
                                if PF[0]<m_wFactorGuodi:
                                    bFactorStat |=0x04
                                elif PF[0]>m_wFactorGuodiRe:
                                    bFactorStat |=0x08

                                for lindex in range(3):
                                    s=lindex*2
                                    for index in range(3):
                                        bCur=(bVolStat[index]>>s)&0x03
                                        bLast=(m_bVolStat[index]>>s)&0x03
                                        if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                            if bLast==0x00:
                                                m_dwVolDelay[lindex][index]=0
                                                m_bVolStat[index] |=(0x01<<s)
                                                bLast |=0x01
                                            if bLast!=0x00:#else:
                                                if m_dwVolDelay[lindex][index]<modbus[49-lindex]['Interval']:
                                                    m_dwVolDelay[lindex][index] +=1
                                                else:
                                                    if bLast==0x01:
                                                        m_bVolStat[index] |=(0x02<<s)
                                                        print(m_bVolStat)
                                                        
                                                        #write mqtt event
                                                        dic={}  #v
                                                        dic['clientid']=pub
                                                        dic['mspid']=mspid#uppub
                                                        dic['time_stamp']=ts
                                                        dic['actioncode']=modbus[49-lindex]['ServiceId']
                                                        dic['data_value']=[]
                                                        dic['data_value'].append({'UA':V[0]})
                                                        dic['data_value'].append({'UB':V[1]})
                                                        dic['data_value'].append({'UC':V[2]})
                                                        payload=json.dumps(dic)
                                                        print(payload)
                                                        client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[49-lindex]['Qos'],retain=True) 
                                                        #wrtie sql
                                                        cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                        cx.commit()  
                                                        
                                                        #cu = cx.execute('select Pub, mspId, Uppub, ServiceId, strJson, time_stamp from history where mspId="D01" and time_stamp between "2018-10-19 10:00:00" and "2018-10-19 12:00:00"')
                                                        #history = [dict(Pub =row[0], mspId=row[1], Uppub=row[2], ServiceId=row[3], strJson=row[4], time_stamp=row[5]) for row in cu.fetchall()]
                                                        #print(history)
                                                    #else:
                                                        #total time add
                                                        
                                        elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                            if bLast==0x03:
                                                m_bVolStat[index] &=~(0x03<<s)
                                                print(m_bVolStat)
                                                #write mqtt eventover
                                                #write sql
                                            m_bVolStat[index] &=~(0x03<<s)
                                        
                                for lindex in range(1):
                                    s=lindex*2
                                    for index in range(3):
                                        bCur=(bCurStat[index]>>s)&0x03
                                        bLast=(m_bCurStat[index]>>s)&0x03
                                        if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                            if bLast==0x00:
                                                m_dwCurDelay[lindex][index]=0
                                                m_bCurStat[index] |=(0x01<<s)
                                                bLast |=0x01
                                            if bLast!=0x00:#else:
                                                if m_dwCurDelay[lindex][index]<modbus[46-lindex]['Interval']:
                                                    m_dwCurDelay[lindex][index] +=1
                                                else:
                                                    if bLast==0x01:
                                                        m_bCurStat[index] |=(0x02<<s)
                                                        print(m_bCurStat)
                                                        #write mqtt event
                                                        dic={}  #v
                                                        dic['clientid']=pub
                                                        dic['mspid']=mspid#uppub
                                                        dic['time_stamp']=ts
                                                        dic['actioncode']=modbus[46-lindex]['ServiceId']
                                                        dic['data_value']=[]
                                                        dic['data_value'].append({'IA':I[0]})
                                                        dic['data_value'].append({'IB':I[1]})
                                                        dic['data_value'].append({'IC':I[2]})
                                                        payload=json.dumps(dic)
                                                        print(payload)
                                                        client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[46-lindex]['Qos'],retain=True) 
                                                        #wrtie sql
                                                        cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                        cx.commit()  
                                                    #else:
                                                        #total time add
                                                        
                                        elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                            if bLast==0x03:
                                                m_bCurStat[index] &=~(0x03<<s)
                                                print(m_bCurStat)
                                                #write mqtt eventover
                                                #write sql
                                            m_bCurStat[index] &=~(0x03<<s)
                                
                                for lindex in range(2):
                                    s=lindex*2
                                    bCur=(bFactorStat>>s)&0x03
                                    bLast=(m_bFactorStat>>s)&0x03
                                    if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                        if bLast==0x00:
                                            m_dwFDelay[lindex]=0
                                            m_bFactorStat |=(0x01<<s)
                                            bLast |=0x01
                                        if bLast!=0x00:#else:
                                            if m_dwFDelay[lindex]<modbus[45-lindex]['Interval']:
                                                m_dwFDelay[lindex] +=1
                                            else:
                                                if bLast==0x01:
                                                    m_bFactorStat |=(0x02<<s)
                                                    print(m_bFactorStat)
                                                    
                                                    #write mqtt event
                                                    dic={}  #f
                                                    dic['clientid']=pub
                                                    dic['mspid']=mspid#uppub
                                                    dic['time_stamp']=ts
                                                    dic['actioncode']=modbus[45-lindex]['ServiceId']
                                                    dic['data_value']=[]
                                                    dic['data_value'].append({'PF':PF[0]})
                                                    payload=json.dumps(dic)
                                                    print(payload)
                                                    client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[45-lindex]['Qos'],retain=True) 
                                                    #wrtie sql
                                                    cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                    cx.commit()
                                                #else:
                                                    #total time add
                                                    
                                    elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                        if bLast==0x03:
                                            m_bFactorStat &=~(0x03<<s)
                                            print(m_bFactorStat)
                                            #write mqtt eventover
                                            #write sql
                                        m_bFactorStat &=~(0x03<<s)
                                
                                s=0
                                bCur=(wPCStat>>s)&0x03
                                bLast=(m_wPCStat>>s)&0x03
                                if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                    if bLast==0x00:
                                        m_dwPDelay=0
                                        m_wPCStat |=(0x01<<s)
                                        bLast |=0x01
                                    if bLast!=0x00:#else:
                                        if m_dwPDelay<modbus[43]['Interval']:
                                            m_dwPDelay +=1
                                        else:
                                            if bLast==0x01:
                                                m_wPCStat |=(0x02<<s)
                                                print(m_wPCStat)
                                                
                                                #write mqtt event
                                                dic={}  #-p
                                                dic['clientid']=pub
                                                dic['mspid']=mspid#uppub
                                                dic['time_stamp']=ts
                                                dic['actioncode']=modbus[43]['ServiceId']
                                                dic['data_value']=[]
                                                dic['data_value'].append({'TotalAP':P[0]})
                                                payload=json.dumps(dic)
                                                print(payload)
                                                client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[43-lindex]['Qos'],retain=True) 
                                                #wrtie sql
                                                cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                cx.commit()  
                                            #else:
                                                #total time add
                                                
                                elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                    if bLast==0x03:
                                        m_wPCStat &=~(0x03<<s)
                                        print(m_wPCStat)
                                        #write mqtt eventover
                                        #write sql
                                    m_wPCStat &=~(0x03<<s)
                                
                                dicForEventsV[uppub]=m_bVolStat
                                dicForEventsI[uppub]=m_bCurStat
                                dicForEventsF[uppub]=m_bFactorStat
                                dicForEventsP[uppub]=m_wPCStat
                                dicForEventsVDelay[uppub]=m_dwVolDelay
                                dicForEventsIDelay[uppub]=m_dwCurDelay
                                dicForEventsFDelay[uppub]=m_dwFDelay
                                dicForEventsPDelay[uppub]=m_dwPDelay
                                
                                time.sleep(interval*0.1)
                            
                            else:
                                serviceids=serviceid.split(',')
                                dic={}
                                dic['data_value']=[]
                                for index in range(len(jsondates)):
                                    dic['data_value'].append({jsondates[index]:mbdates[index]})
                                    #dic[jsondates[index]]=mbdates[index]
                                dic['clientid']=pub
                                dic['mspid']=mspid#uppub
                                dic['time_stamp']=ts
                                dic['actioncode']=serviceids[0]
                                payload=json.dumps(dic)
                                #payload=repr(q.get())
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                print(payload)
                                
                                time.sleep(interval*0.1)

                    elif devicetype==5:
                        if uppub:
                            uppubinit=int(uppub)
                        else:
                            uppubinit=1

                        #isfinished.clear()
                        getdata4(uppubinit,startaddr,endaddr,interval)
                        #print('测量中m...')
                        #isfinished.wait()
                        while not q.empty():
                            #ts=time.strftime("%Y-%m-%d %H:%M:%S")
                            ts = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
                            print(ts)
                            mbdates=q.get()
                            #print(len(mbdates))
                            #print(mbdates)
                            jsondates=jsondate.split(',')
                            #print(len(jsondates))
                            #print(jsondates)
                            serviceids=serviceid.split(',')
                            dic={}
                            dic['data_value']=[]
                            for index in range(len(jsondates)):
                                dic['data_value'].append({jsondates[index]:mbdates[index]})
                                #dic[jsondates[index]]=mbdates[index]
                            dic['clientid']=pub
                            dic['mspid']=mspid#uppub
                            dic['time_stamp']=ts
                            dic['actioncode']=serviceids[0]
                            payload=json.dumps(dic)
                            #payload=repr(q.get())
                            client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                            print(payload)
                            
                            if dicPlc.has_key(mspid):
                                tmpmbdates=dicPlc[mspid]
                            else:
                                tmpmbdates=mbdates
                            if cmp(mbdates,tmpmbdates)==0:
                                print('same')
                            else:
                                print('difference')
                                print(tmpmbdates)
                                print(mbdates)
                                
                                #write mqtt event
                                dicplc={}  #plc
                                dicplc['clientid']=pub
                                dicplc['mspid']=mspid#uppub
                                dicplc['time_stamp']=ts
                                dicplc['actioncode']='SwitchActionRecord'
                                dicplc['data_value']=[]
                                dicplc['data_value'].append({'SwitchType':'4'})
                                dicplc['data_value'].append({'OriginalStatus':tmpmbdates})
                                dicplc['data_value'].append({'NewStatus':mbdates})
                                payload=json.dumps(dicplc)
                                print(payload)
                                client.publish('/mqtt/event/'+pub+'/'+mspid,payload,2,retain=True) 
                                #wrtie sql
                                cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dicplc['clientid'], dicplc['mspid'], uppub, dicplc['actioncode'], payload])
                                cx.commit()  
                                
                            dicPlc[mspid]=mbdates
                    elif devicetype==6:
                        if uppub:
                            y=bytearray.fromhex(uppub)
                            z = list(y)
                        else:
                            y=bytearray.fromhex('111111111111')
                            z = list(y)
                    
                        #isfinished.clear()
                        getdata97(z,startaddr,endaddr,interval)
                        print(uppub)
                        #print(z)
                        #print('测量中...')
                        #isfinished.wait()
                        while not q.empty():
                            #ts=time.strftime("%Y-%m-%d %H:%M:%S")
                            ts = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
                            print(ts)
                            mbdates=q.get()
                            #print(len(mbdates))
                            print(mbdates)
                            jsondates=jsondate.split(',')
#                            print(len(jsondates))
#                            print(jsondates)
                            if startaddr == 50331392:
                                dic={}
                                y = bytearray.fromhex(mbdates)  
                                z = list(y)
                                #print z
                                dic['ts']=ts
                                if(z[2]&0x80):
                                    x=(z[0]-(z[0]>>4)*6)+(z[1]-(z[1]>>4)*6)*100+((z[2]&0x7f)-((z[2]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[0]-(z[0]>>4)*6)+(z[1]-(z[1]>>4)*6)*100+((z[2]&0x7f)-((z[2]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["总有功功率"]=x
                                P[0]=x
                                if(z[5]&0x80):
                                    x=(z[3]-(z[3]>>4)*6)+(z[4]-(z[4]>>4)*6)*100+((z[5]&0x7f)-((z[5]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[3]-(z[3]>>4)*6)+(z[4]-(z[4]>>4)*6)*100+((z[5]&0x7f)-((z[5]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["A有功功率"]=x
                                P[1]=x
                                if(z[8]&0x80):
                                    x=(z[6]-(z[6]>>4)*6)+(z[7]-(z[7]>>4)*6)*100+((z[8]&0x7f)-((z[8]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[6]-(z[6]>>4)*6)+(z[7]-(z[7]>>4)*6)*100+((z[8]&0x7f)-((z[8]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["B有功功率"]=x
                                P[2]=x
                                if(z[11]&0x80):
                                    x=(z[9]-(z[9]>>4)*6)+(z[10]-(z[10]>>4)*6)*100+((z[11]&0x7f)-((z[11]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[9]-(z[9]>>4)*6)+(z[10]-(z[10]>>4)*6)*100+((z[11]&0x7f)-((z[11]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["C有功功率"]=x
                                P[3]=x
                                if(z[14]&0x80):
                                    x=(z[12]-(z[12]>>4)*6)+(z[13]-(z[13]>>4)*6)*100+((z[14]&0x7f)-((z[14]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[12]-(z[12]>>4)*6)+(z[13]-(z[13]>>4)*6)*100+((z[14]&0x7f)-((z[14]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["总无功功率"]=x
                                Pr[0]=x
                                if(z[17]&0x80):
                                    x=(z[15]-(z[15]>>4)*6)+(z[16]-(z[16]>>4)*6)*100+((z[17]&0x7f)-((z[17]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[15]-(z[15]>>4)*6)+(z[16]-(z[16]>>4)*6)*100+((z[17]&0x7f)-((z[17]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["A无功功率"]=x
                                Pr[1]=x
                                if(z[20]&0x80):
                                    x=(z[18]-(z[18]>>4)*6)+(z[19]-(z[19]>>4)*6)*100+((z[20]&0x7f)-((z[20]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[18]-(z[18]>>4)*6)+(z[19]-(z[19]>>4)*6)*100+((z[20]&0x7f)-((z[20]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["B无功功率"]=x
                                Pr[2]=x
                                if(z[23]&0x80):
                                    x=(z[21]-(z[21]>>4)*6)+(z[22]-(z[22]>>4)*6)*100+((z[23]&0x7f)-((z[23]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/10000)
                                else:
                                    x=(z[21]-(z[21]>>4)*6)+(z[22]-(z[22]>>4)*6)*100+((z[23]&0x7f)-((z[23]&0x7f)>>4)*6)*10000
                                    x=float(x)/10000
                                dic["C无功功率"]=x
                                Pr[3]=x
                                x=(z[24]-(z[24]>>4)*6)+(z[25]-(z[25]>>4)*6)*100
                                x=float(x)/1000
                                dic["总功率因素"]=x
                                PF[0]=x
                                x=(z[26]-(z[26]>>4)*6)+(z[27]-(z[27]>>4)*6)*100
                                x=float(x)/1000
                                dic["A功率因素"]=x
                                PF[1]=x
                                x=(z[28]-(z[28]>>4)*6)+(z[29]-(z[29]>>4)*6)*100
                                x=float(x)/1000
                                dic["B功率因素"]=x
                                PF[2]=x
                                x=(z[30]-(z[30]>>4)*6)+(z[31]-(z[31]>>4)*6)*100
                                x=float(x)/1000
                                dic["C功率因素"]=x
                                PF[3]=x
                                x=(z[32]-(z[32]>>4)*6)+(z[33]-(z[33]>>4)*6)*100
                                x=float(x)/10
#                                print z[32]
#                                print z[33]
#                                print x
                                dic["A相电压"]=x
                                V[0]=x
                                x=(z[34]-(z[34]>>4)*6)+(z[35]-(z[35]>>4)*6)*100
                                
                                x=float(x)/10
                                dic["B相电压"]=x
                                V[1]=x
                                x=(z[36]-(z[36]>>4)*6)+(z[37]-(z[37]>>4)*6)*100
                                x=float(x)/10
                                dic["C相电压"]=x
                                V[2]=x
                                if(z[40]&0x80):
                                    x=(z[38]-(z[38]>>4)*6)+(z[39]-(z[39]>>4)*6)*100+((z[40]&0x7f)-((z[40]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/1000)
                                else:
                                    x=(z[38]-(z[38]>>4)*6)+(z[39]-(z[39]>>4)*6)*100+((z[40]&0x7f)-((z[40]&0x7f)>>4)*6)*10000
                                    x=float(x)/1000
                                dic["A相电流"]=x
                                I[0]=x
                                if(z[43]&0x80):
                                    x=(z[41]-(z[41]>>4)*6)+(z[42]-(z[42]>>4)*6)*100+((z[43]&0x7f)-((z[43]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/1000)
                                else:
                                    x=(z[41]-(z[41]>>4)*6)+(z[42]-(z[42]>>4)*6)*100+((z[43]&0x7f)-((z[43]&0x7f)>>4)*6)*10000
                                    x=float(x)/1000
                                dic["B相电流"]=x
                                I[1]=x
                                if(z[46]&0x80):
                                    x=(z[44]-(z[44]>>4)*6)+(z[45]-(z[45]>>4)*6)*100+((z[46]&0x7f)-((z[46]&0x7f)>>4)*6)*10000
                                    x=-(float(x)/1000)
                                else:
                                    x=(z[44]-(z[44]>>4)*6)+(z[45]-(z[45]>>4)*6)*100+((z[46]&0x7f)-((z[46]&0x7f)>>4)*6)*10000
                                    x=float(x)/1000
                                dic["C相电流"]=x
                                I[2]=x
                                x=(z[47]-(z[47]>>4)*6)+(z[48]-(z[48]>>4)*6)*100+(z[49]-(z[49]>>4)*6)*10000+(z[50]-(z[50]>>4)*6)*1000000
                                x=float(x)/100
                                dic["总有功电能"]=x
                                x=(z[51]-(z[51]>>4)*6)+(z[52]-(z[52]>>4)*6)*100+(z[53]-(z[53]>>4)*6)*10000+(z[54]-(z[54]>>4)*6)*1000000
                                x=float(x)/100
                                dic["总无功电能"]=x
                                x=(z[55]-(z[55]>>4)*6)+(z[56]-(z[56]>>4)*6)*100+(z[57]-(z[57]>>4)*6)*10000+(z[58]-(z[58]>>4)*6)*1000000
                                x=float(x)/100
                                dic["总有反电能"]=x
                                x=(z[59]-(z[59]>>4)*6)+(z[60]-(z[60]>>4)*6)*100+(z[61]-(z[61]>>4)*6)*10000+(z[62]-(z[62]>>4)*6)*1000000
                                x=float(x)/100
                                dic["总无反电能"]=x
#                                print(dic)
#                                payload=json.dumps(dic)
#                                client.publish(pub,payload,qos) 
                                serviceids=serviceid.split(',')
                                dic1={}  #v
                                dic1['clientid']=pub
                                dic1['mspid']=mspid#uppub
                                dic1['time_stamp']=ts
                                dic1['actioncode']=serviceids[0]
                                dic1['data_value']=[]
                                dic1['data_value'].append({'UA':V[0]})
                                dic1['data_value'].append({'UB':V[1]})
                                dic1['data_value'].append({'UC':V[2]})
                                payload=json.dumps(dic1)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic2={}  #i
                                dic2['clientid']=pub
                                dic2['mspid']=mspid#uppub
                                dic2['time_stamp']=ts
                                dic2['actioncode']=serviceids[1]
                                dic2['data_value']=[]
                                dic2['data_value'].append({'IA':I[0]})
                                dic2['data_value'].append({'IB':I[1]})
                                dic2['data_value'].append({'IC':I[2]})
                                payload=json.dumps(dic2)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic3={}  #pf
                                dic3['clientid']=pub
                                dic3['mspid']=mspid#uppub
                                dic3['time_stamp']=ts
                                dic3['actioncode']=serviceids[2]
                                dic3['data_value']=[]
                                dic3['data_value'].append({'PF':PF[0]})
                                payload=json.dumps(dic3)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic4={}  #energy
                                dic4['clientid']=pub
                                dic4['mspid']=mspid#uppub
                                dic4['time_stamp']=ts
                                dic4['actioncode']='APValueRecord'
                                dic4['data_value']=[]
                                dic4['data_value'].append({'PAPValue':dic["总有功电能"]})
                                dic4['data_value'].append({'RAPValue':dic["总有反电能"]})
                                payload=json.dumps(dic4)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                dic5={}  #energy
                                dic5['clientid']=pub
                                dic5['mspid']=mspid#uppub
                                dic5['time_stamp']=ts
                                dic5['actioncode']='RPValueRecord'
                                dic5['data_value']=[]
                                dic5['data_value'].append({'PRPValue':dic["总无功电能"]})
                                dic5['data_value'].append({'RRPValue':dic["总无反电能"]})
                                payload=json.dumps(dic5)
                                #print(payload)
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                
                                m_wVolGuoya=(int(modbus[49]['Uppub']))*(modbus[49]['Startaddr'])/100.0
                                m_wVolGuoyaRe=(int(modbus[49]['Uppub']))*(modbus[49]['Endaddr'])/100.0
                                m_wVolDiya=(int(modbus[48]['Uppub']))*(modbus[48]['Startaddr'])/100.0
                                m_wVolDiyaRe=(int(modbus[48]['Uppub']))*(modbus[48]['Endaddr'])/100.0
                                m_wVolShiya=(int(modbus[47]['Uppub']))*(modbus[47]['Startaddr'])/100.0
                                m_wVolShiyaRe=(int(modbus[47]['Uppub']))*(modbus[47]['Endaddr'])/100.0
                                m_wCurGuoliu=(int(modbus[46]['Uppub']))*(modbus[46]['Startaddr'])/100.0
                                m_wCurGuoliuRe=(int(modbus[46]['Uppub']))*(modbus[46]['Endaddr'])/100.0
                                m_wFactorGuogao=(modbus[45]['Startaddr'])/100.0
                                m_wFactorGuogaoRe=(modbus[45]['Endaddr'])/100.0
                                m_wFactorGuodi=(modbus[44]['Startaddr'])/100.0
                                m_wFactorGuodiRe=(modbus[44]['Endaddr'])/100.0
                                
                                if dicForEventsV.has_key(uppub):
                                    m_bVolStat=dicForEventsV[uppub]
                                    m_bCurStat=dicForEventsI[uppub]
                                    m_bFactorStat=dicForEventsF[uppub]
                                    m_wPCStat=dicForEventsP[uppub]
                                    m_dwVolDelay=dicForEventsVDelay[uppub]
                                    m_dwCurDelay=dicForEventsIDelay[uppub]
                                    m_dwFDelay=dicForEventsFDelay[uppub]
                                    m_dwPDelay=dicForEventsPDelay[uppub]
                                else:
                                    m_bVolStat=[0,0,0]
                                    m_bCurStat=[0,0,0]
                                    m_bFactorStat=0
                                    m_wPCStat=0
                                    m_dwVolDelay=[[0 for i in range(3)] for i in range(3)]
                                    m_dwCurDelay=[[0 for i in range(3)] for i in range(1)]
                                    m_dwFDelay=[0 for i in range(2)]
                                    m_dwPDelay=0
                                
                                #bVolStat=[0,0,0]
                                #bCurStat=[0,0,0]
                                bVolStat[0]=0
                                bVolStat[1]=0
                                bVolStat[2]=0
                                bCurStat[0]=0
                                bCurStat[1]=0
                                bCurStat[2]=0
                                bFactorStat=0
                                wPCStat=0
                                
                                for index in range(3):
                                    if V[index]>m_wVolGuoya:
                                        bVolStat[index] |=0x01
                                    elif V[index]<m_wVolGuoyaRe:
                                        bVolStat[index] |=0x02
                                    
                                    if V[index]<m_wVolDiya:
                                        bVolStat[index] |=0x04
                                    elif V[index]>m_wVolDiyaRe:
                                        bVolStat[index] |=0x08
                                        
                                    if V[index]<m_wVolShiya:
                                        bVolStat[index] |=0x10
                                    elif V[index]>m_wVolShiyaRe:
                                        bVolStat[index] |=0x20
                                        
                                    if abs(I[index])>m_wCurGuoliu:
                                        bCurStat[index] |=0x01
                                    elif I[index]<m_wCurGuoliuRe:
                                        bCurStat[index] |=0x02
                                #print(bCurStat)
                                #print(bVolStat)
                                
                                if P[0]<0:
                                    wPCStat |=0x01
                                else:
                                    wPCStat |=0x02
                                
                                if PF[0]>m_wFactorGuogao:
                                    bFactorStat |=0x01
                                elif PF[0]<m_wFactorGuogaoRe:
                                    bFactorStat |=0x02
                                
                                if PF[0]<m_wFactorGuodi:
                                    bFactorStat |=0x04
                                elif PF[0]>m_wFactorGuodiRe:
                                    bFactorStat |=0x08

                                for lindex in range(3):
                                    s=lindex*2
                                    for index in range(3):
                                        bCur=(bVolStat[index]>>s)&0x03
                                        bLast=(m_bVolStat[index]>>s)&0x03
                                        if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                            if bLast==0x00:
                                                m_dwVolDelay[lindex][index]=0
                                                m_bVolStat[index] |=(0x01<<s)
                                                bLast |=0x01
                                            if bLast!=0x00:#else:
                                                if m_dwVolDelay[lindex][index]<modbus[49-lindex]['Interval']:
                                                    m_dwVolDelay[lindex][index] +=1
                                                else:
                                                    if bLast==0x01:
                                                        m_bVolStat[index] |=(0x02<<s)
                                                        print(m_bVolStat)
                                                        
                                                        #write mqtt event
                                                        dic={}  #v
                                                        dic['clientid']=pub
                                                        dic['mspid']=mspid#uppub
                                                        dic['time_stamp']=ts
                                                        dic['actioncode']=modbus[49-lindex]['ServiceId']
                                                        dic['data_value']=[]
                                                        dic['data_value'].append({'UA':V[0]})
                                                        dic['data_value'].append({'UB':V[1]})
                                                        dic['data_value'].append({'UC':V[2]})
                                                        payload=json.dumps(dic)
                                                        print(payload)
                                                        client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[49-lindex]['Qos'],retain=True) 
                                                        #wrtie sql
                                                        cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                        cx.commit()  
                                                        
                                                        #cu = cx.execute('select Pub, mspId, Uppub, ServiceId, strJson, time_stamp from history where mspId="D01" and time_stamp between "2018-10-19 10:00:00" and "2018-10-19 12:00:00"')
                                                        #history = [dict(Pub =row[0], mspId=row[1], Uppub=row[2], ServiceId=row[3], strJson=row[4], time_stamp=row[5]) for row in cu.fetchall()]
                                                        #print(history)
                                                    #else:
                                                        #total time add
                                                        
                                        elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                            if bLast==0x03:
                                                m_bVolStat[index] &=~(0x03<<s)
                                                print(m_bVolStat)
                                                #write mqtt eventover
                                                #write sql
                                            m_bVolStat[index] &=~(0x03<<s)
                                        
                                for lindex in range(1):
                                    s=lindex*2
                                    for index in range(3):
                                        bCur=(bCurStat[index]>>s)&0x03
                                        bLast=(m_bCurStat[index]>>s)&0x03
                                        if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                            if bLast==0x00:
                                                m_dwCurDelay[lindex][index]=0
                                                m_bCurStat[index] |=(0x01<<s)
                                                bLast |=0x01
                                            if bLast!=0x00:#else:
                                                if m_dwCurDelay[lindex][index]<modbus[46-lindex]['Interval']:
                                                    m_dwCurDelay[lindex][index] +=1
                                                else:
                                                    if bLast==0x01:
                                                        m_bCurStat[index] |=(0x02<<s)
                                                        print(m_bCurStat)
                                                        #write mqtt event
                                                        dic={}  #v
                                                        dic['clientid']=pub
                                                        dic['mspid']=mspid#uppub
                                                        dic['time_stamp']=ts
                                                        dic['actioncode']=modbus[46-lindex]['ServiceId']
                                                        dic['data_value']=[]
                                                        dic['data_value'].append({'IA':I[0]})
                                                        dic['data_value'].append({'IB':I[1]})
                                                        dic['data_value'].append({'IC':I[2]})
                                                        payload=json.dumps(dic)
                                                        print(payload)
                                                        client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[46-lindex]['Qos'],retain=True) 
                                                        #wrtie sql
                                                        cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                        cx.commit()  
                                                    #else:
                                                        #total time add
                                                        
                                        elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                            if bLast==0x03:
                                                m_bCurStat[index] &=~(0x03<<s)
                                                print(m_bCurStat)
                                                #write mqtt eventover
                                                #write sql
                                            m_bCurStat[index] &=~(0x03<<s)
                                
                                for lindex in range(2):
                                    s=lindex*2
                                    bCur=(bFactorStat>>s)&0x03
                                    bLast=(m_bFactorStat>>s)&0x03
                                    if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                        if bLast==0x00:
                                            m_dwFDelay[lindex]=0
                                            m_bFactorStat |=(0x01<<s)
                                            bLast |=0x01
                                        if bLast!=0x00:#else:
                                            if m_dwFDelay[lindex]<modbus[45-lindex]['Interval']:
                                                m_dwFDelay[lindex] +=1
                                            else:
                                                if bLast==0x01:
                                                    m_bFactorStat |=(0x02<<s)
                                                    print(m_bFactorStat)
                                                    
                                                    #write mqtt event
                                                    dic={}  #f
                                                    dic['clientid']=pub
                                                    dic['mspid']=mspid#uppub
                                                    dic['time_stamp']=ts
                                                    dic['actioncode']=modbus[45-lindex]['ServiceId']
                                                    dic['data_value']=[]
                                                    dic['data_value'].append({'PF':PF[0]})
                                                    payload=json.dumps(dic)
                                                    print(payload)
                                                    client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[45-lindex]['Qos'],retain=True) 
                                                    #wrtie sql
                                                    cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                    cx.commit()
                                                #else:
                                                    #total time add
                                                    
                                    elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                        if bLast==0x03:
                                            m_bFactorStat &=~(0x03<<s)
                                            print(m_bFactorStat)
                                            #write mqtt eventover
                                            #write sql
                                        m_bFactorStat &=~(0x03<<s)
                                
                                s=0
                                bCur=(wPCStat>>s)&0x03
                                bLast=(m_wPCStat>>s)&0x03
                                if bCur==0x01 or (bLast==0x03 and bCur==0x00):
                                    if bLast==0x00:
                                        m_dwPDelay=0
                                        m_wPCStat |=(0x01<<s)
                                        bLast |=0x01
                                    if bLast!=0x00:#else:
                                        if m_dwPDelay<modbus[43]['Interval']:
                                            m_dwPDelay +=1
                                        else:
                                            if bLast==0x01:
                                                m_wPCStat |=(0x02<<s)
                                                print(m_wPCStat)
                                                
                                                #write mqtt event
                                                dic={}  #-p
                                                dic['clientid']=pub
                                                dic['mspid']=mspid#uppub
                                                dic['time_stamp']=ts
                                                dic['actioncode']=modbus[43]['ServiceId']
                                                dic['data_value']=[]
                                                dic['data_value'].append({'TotalAP':P[0]})
                                                payload=json.dumps(dic)
                                                print(payload)
                                                client.publish('/mqtt/event/'+pub+'/'+mspid,payload,modbus[43-lindex]['Qos'],retain=True) 
                                                #wrtie sql
                                                cx.execute('insert into history (pub, mspId, Uppub, ServiceId, strJson) values (?, ?, ?, ?, ?)', [dic['clientid'], dic['mspid'], uppub, dic['actioncode'], payload])
                                                cx.commit()  
                                            #else:
                                                #total time add
                                                
                                elif bCur==0x02 or (bLast<0x03 and bCur==0x00):
                                    if bLast==0x03:
                                        m_wPCStat &=~(0x03<<s)
                                        print(m_wPCStat)
                                        #write mqtt eventover
                                        #write sql
                                    m_wPCStat &=~(0x03<<s)
                                
                                dicForEventsV[uppub]=m_bVolStat
                                dicForEventsI[uppub]=m_bCurStat
                                dicForEventsF[uppub]=m_bFactorStat
                                dicForEventsP[uppub]=m_wPCStat
                                dicForEventsVDelay[uppub]=m_dwVolDelay
                                dicForEventsIDelay[uppub]=m_dwCurDelay
                                dicForEventsFDelay[uppub]=m_dwFDelay
                                dicForEventsPDelay[uppub]=m_dwPDelay
                                '''
                                print(dicForEventsV)
                                print(dicForEventsI)
                                print(dicForEventsF)
                                print(dicForEventsP)
                                print(dicForEventsVDelay)
                                print(dicForEventsIDelay)
                                print(dicForEventsFDelay)
                                print(dicForEventsPDelay)
                                '''
                                
#                                m_dwCurReverse
#                                m_dwCurReverseRe
                                
#                                print(payload)
                            else:
                                serviceids=serviceid.split(',')
                                dic={}
                                dic['data_value']=[]
                                dic['data_value'].append({jsondates[0]:mbdates})
                                dic['clientid']=pub
                                dic['mspid']=mspid#uppub
                                dic['time_stamp']=ts
                                dic['actioncode']=serviceids[0]
                                payload=json.dumps(dic)
                                #payload=repr(q.get())
                                client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                                '''
                                dic={}
                                for index in range(len(jsondates)):
                                    dic[jsondates[index]]=mbdates
                                dic['ts']=ts
                                payload=json.dumps(dic)
                                #payload=repr(q.get())
                                client.publish(pub,payload,qos) 
                                '''
                                print(payload)

                            times[mspid]=0
                        else:
                            if mspid in times:
                                #print(mspid)
                                times[mspid]=times[mspid]+1
                                #print(times[mspid])
                                if times[mspid]==40:
                                    #up deviceevent
                                    ts = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
                                    print(ts)
                                    dic={}  #-p
                                    dic['clientid']=pub
                                    dic['mspid']=mspid#uppub
                                    dic['time_stamp']=ts
                                    dic['actioncode']='DeviceStatus'
                                    dic['data_value']=[]
                                    dic['data_value'].append({'SensorStatusCode':'001'})
                                    payload=json.dumps(dic)
                                    print(payload)
                                    client.publish('/mqtt/deviceevent/'+pub+'/'+mspid,payload,2,retain=True) 
                                    
                            else:
                                times[mspid]=0
                                #print(mspid)
                                #print(times[mspid])            
                    else:
                        print('start evp 97')
                        getdata2297(uppub,startaddr,endaddr,interval)
                        while not q.empty():
                            ts = datetime.datetime.fromtimestamp(int(time.time()), pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')
                            print(ts)
                            mbdates=q.get()
                            #print(len(mbdates))
                            print(mbdates)
                            jsondates=jsondate.split(',')
                            serviceids=serviceid.split(',')
                            dic={}
                            dic['data_value']=[]
                            dic['data_value'].append({jsondates[0]:mbdates})
                            dic['clientid']=pub
                            dic['mspid']=mspid#uppub
                            dic['time_stamp']=ts
                            dic['actioncode']=serviceids[0]
                            payload=json.dumps(dic)
                            #payload=repr(q.get())
                            client.publish('/mqtt/continued/'+pub+'/'+mspid,payload,qos) 
                            '''
                            dic={}
                            for index in range(len(jsondates)):
                                dic[jsondates[index]]=mbdates
                            dic['ts']=ts
                            payload=json.dumps(dic)
                            #payload=repr(q.get())
                            client.publish(pub,payload,qos) 
                            '''
                            print(payload)
                            #time.sleep(interval*0.1)
            time.sleep(0.1)
            while not qmqtt.empty():
                try:
                    dic_re=qmqtt.get()
                    dic_re=json.loads(dic_re)
                    print(dic_re)
                
                    cu = cx.execute('select Pub, mspId, Uppub, ServiceId, strJson, time_stamp from history where Pub=? and mspId=? and time_stamp between ? and ?',[dic_re['clientid'],dic_re['mspid'],dic_re['starttime'],dic_re['endtime']]) 
                    history = [dict(Pub =row[0], mspId=row[1], Uppub=row[2], ServiceId=row[3], strJson=row[4], time_stamp=row[5]) for row in cu.fetchall()]
                    #print(history)
                    for his in history:
                        client.publish('/mqtt/event/'+his['Pub']+'/'+his['mspId'],his['strJson'],0) 
                        print(his['strJson'])
                        print('...')
                except Exception,err:
                    print('string err')
                    print(err)

            while not qmqttset.empty():
                try:
                    dic_re=qmqttset.get()
                    dic_re=json.loads(dic_re)
                    print(dic_re)
                
                    if dic_re['actioncode']=='OverVoltage':
                        print(modbus[49])
                        modbus[49]['Startaddr']=int(dic_re['threshold'])
                        modbus[49]['Endaddr']=int(dic_re['recover_threshold'])
                        print(modbus[49])
                    elif dic_re['actioncode']=='LowVoltage':
                        print(modbus[48])
                        modbus[48]['Startaddr']=int(dic_re['threshold'])
                        modbus[48]['Endaddr']=int(dic_re['recover_threshold'])
                        print(modbus[48])
                    elif dic_re['actioncode']=='LossVoltage':
                        print(modbus[47])
                        modbus[47]['Startaddr']=int(dic_re['threshold'])
                        modbus[47]['Endaddr']=int(dic_re['recover_threshold'])
                        print(modbus[47])
                    elif dic_re['actioncode']=='Overcurrent':
                        print(modbus[46])
                        modbus[46]['Startaddr']=int(dic_re['threshold'])
                        modbus[46]['Endaddr']=int(dic_re['recover_threshold'])
                        print(modbus[46])
                    elif dic_re['actioncode']=='OverPowerFactorAlarm':
                        print(modbus[45])
                        modbus[45]['Startaddr']=int(dic_re['threshold'])
                        modbus[45]['Endaddr']=int(dic_re['recover_threshold'])
                        print(modbus[45])
                    elif dic_re['actioncode']=='LowPowerFactorAlarm':
                        print(modbus[44])
                        modbus[44]['Startaddr']=int(dic_re['threshold'])
                        modbus[44]['Endaddr']=int(dic_re['recover_threshold'])
                        print(modbus[44])
                        
                    else :
                        raise Exception("not actioncode")
                    
                    dic_re['status']='ok'
                    payload=json.dumps(dic_re)
                    print(payload)
                    client.publish('/mqtt/configok/'+modbus[0]['Pub'],payload,0) 
                    
                    for m in range(50-1,-1,-1):
                        cx.execute('insert into modbus (Pub, Startaddr, Endaddr, Qos, Uppub, Interval, checked, Json, DeviceType, ServiceId, mspId) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', [modbus[m]['Pub'], modbus[m]['Startaddr'], modbus[m]['Endaddr'], modbus[m]['Qos'], modbus[m]['Uppub'], modbus[m]['Interval'], modbus[m]['checked'], modbus[m]['Json'], modbus[m]['DeviceType'], modbus[m]['ServiceId'], modbus[m]['mspId']])
                    cx.commit()  
                    
                except Exception,err:
                    print('string err')
                    print(err)

            #print('reconnect')
            #client.reconnect()
            #client.on_message=on_message
            #client.subscribe('/mqtt/recall/'+puball+'/+')
            #print('/mqtt/recall/'+puball+'/+')
            #time.sleep(5)
        #client.disconnect()
    except KeyboardInterrupt:
            print("bye!")
            client.disconnect()
    except Exception,err:
            print(err)
            client.disconnect()
    print('reconnect 60s')
    time.sleep(60)
