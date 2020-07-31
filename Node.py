import socket
import random
import threading
import time
import math
import json
import random


class Node(object):

    def __init__(self,nodeId,baseTime,totalTime = 300,N=3):
        self.id = nodeId
        self.maxNeighborsCount = N
        self.biNeighbors = []
        self.attemptNeighbors = set()
        self.uniNeighbors = set()
        self.socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        self.socket.bind(('127.0.0.1',0))
        self.recvTimes = dict()
        self.sendTimes = dict()
        self.baseTime = baseTime
        self.recvTimers = dict()
        self.sendTimers = dict()
        self.uniNeighborsRecvTimers = dict()
        self.attemptNeighborsSendTimers = dict()
        self.totalTime = totalTime
        self.state = 'Active'
        self.sendHistory = dict()
        self.recvHistory = dict()
        self.neighborsOfNeighbors = dict()
        self.neighborsAvailability = dict()
        self.neighborsEntranceTime = dict()
        
    def start(self,addresses):
        addresses = addresses[:]
        addresses.remove(self.socket.getsockname())
        self.addresses = addresses
        self.recvThread = threading.Thread(target=self.__recvMessage__,daemon=True)
        self.selectThread = threading.Thread(target=self.__selectNewNeighbor__,daemon=True)
        self.recvThread.start()
        self.selectThread.start()
    
    def deActive(self,time=20):
        self.state = 'deActive'
        self.attemptNeighbors.clear()
        self.uniNeighbors.clear()
        for timer in self.recvTimers.values():
            timer.cancel()
        for timer in self.sendTimers.values():
            timer.cancel()
        for timer in self.attemptNeighborsSendTimers.values():
            timer.cancel()
        for timer in self.uniNeighborsRecvTimers.values():
            timer.cancel()
        for neighbor in self.biNeighbors:
            self.__removeFromNeighbors__(neighbor)
        timer = threading.Timer(time,self.__active__)
        timer.start()

    def report(self,address="./json_output/"):
        with open(address+'report_node_'+str(self.id)+".json",'w') as file:
            vertex = set(self.biNeighbors+[self.socket.getsockname()]+list(self.attemptNeighbors)+list(self.uniNeighbors))
            for neighbor,biNeighbors in self.neighborsOfNeighbors.items():
                if neighbor in self.biNeighbors:
                    vertex = vertex.union(set(biNeighbors))
            edges = set()
            for neighbor in self.biNeighbors:
                edges.add((self.socket.getsockname(),neighbor))
                edges.add((neighbor,self.socket.getsockname()))
            for neighbor in self.attemptNeighbors:
                edges.add((self.socket.getsockname(),neighbor))
            for neighbor in self.uniNeighbors:
                edges.add((neighbor,self.socket.getsockname()))
            for addr,biNeighbors in self.neighborsOfNeighbors.items():
                for neighbor in biNeighbors:
                    if neighbor in vertex and addr in self.biNeighbors:
                        edges.add((addr,neighbor))
                        edges.add((neighbor,addr))
            nodeReport = {
                'state' : self.state,
                'address' : self.socket.getsockname(),
                'all_neighbors' : [
                    {
                        'ip' : addr[0],
                        'port' : addr[1],
                        'send_to_count' : self.sendHistory.get(addr,0),
                        'recv_from_count' : self.recvHistory.get(addr,0),
                        'availability' : availability/(time.time()-self.baseTime)
                    } for addr,availability in self.neighborsAvailability.items()
                ],
                'current_neighbors' : list(set(self.biNeighbors)),
                'topology' : {
                    'vertex' : list(vertex) if self.state == 'Active' else [self.socket.getsockname()],
                    'edges' : list(edges) if self.state == 'Active' else []
                }
            }
            json.dump(nodeReport,file)

    def __active__(self):
        self.state = 'Active'
        

    def __selectNewNeighbor__(self):
        while time.time()-self.baseTime < self.totalTime:
            while (self.state == 'deActive' or len(self.biNeighbors) == self.maxNeighborsCount) and time.time() - self.baseTime < self.totalTime:
                continue
            if not (time.time()-self.baseTime < self.totalTime):
                break
            addr = random.choice(self.addresses)
            if(not addr in self.biNeighbors) and (not addr in self.attemptNeighbors) and (len(self.biNeighbors) < self.maxNeighborsCount):
                self.attemptNeighbors.add(addr)
                self.__sendHelloMessagePeriodicallyToAttemptNeighbors__(addr)

    def __helloMessage__(self,addr):
        hello = {
            'id' : self.id,
            'ip' : self.socket.getsockname()[0],
            'port' : self.socket.getsockname()[1],
            'type' : 'HELLO',
            'neighbors' : self.biNeighbors,
            'last_send_time' : self.sendTimes.get(addr,math.nan),
            'last_recv_time' : self.recvTimes.get(addr,math.nan)
        }
        return json.dumps(hello).encode()

    def __sendHelloMessagePeriodicallyToBiNeighbors__(self,addr):
        if self.state == 'deActive':
            return
        self.sendTimes[addr] = time.time() - self.baseTime
        self.sendHistory[addr] = self.sendHistory.get(addr,0) + 1
        self.socket.sendto(self.__helloMessage__(addr),addr)
        self.sendTimers[addr] = threading.Timer(2,self.__sendHelloMessagePeriodicallyToBiNeighbors__,(addr,))
        self.sendTimers[addr].start()

    def __sendHelloMessagePeriodicallyToAttemptNeighbors__(self,addr):
        if self.state == 'deActive':
            return
        self.sendTimes[addr] = time.time() - self.baseTime
        self.socket.sendto(self.__helloMessage__(addr),addr)
        self.attemptNeighborsSendTimers[addr] = threading.Timer(2,self.__sendHelloMessagePeriodicallyToAttemptNeighbors__,(addr,))
        self.attemptNeighborsSendTimers[addr].start()


    def __removeFromNeighbors__(self,addr):
        sendTimer = self.sendTimers.get(addr,None)
        if not (sendTimer is None):
            sendTimer.cancel()
        if addr in self.biNeighbors:
            self.biNeighbors.remove(addr)
        end = time.time()
        self.neighborsAvailability[addr] = self.neighborsAvailability.get(addr,0) + (end - self.neighborsEntranceTime.get(addr,end-self.baseTime) -  self.baseTime)
        if self.neighborsEntranceTime.get(addr,None):
            del self.neighborsEntranceTime[addr]

    def __removeFromInputUniNeighbors__(self,addr):
        if addr in self.uniNeighbors:
            self.uniNeighbors.remove(addr)

    def __processMsg__(self,msg,addr):
        neighbors = [tuple(x) for x in msg['neighbors']]
        if self.socket.getsockname() in neighbors:
            self.recvHistory[addr] = self.recvHistory.get(addr,0) + 1
        timer = self.recvTimers.get(addr,None)
        if not (timer is None):
            timer.cancel()
        self.recvTimes[addr] = time.time() - self.baseTime
        self.recvTimers[addr] = threading.Timer(8,self.__removeFromNeighbors__,(addr,))
        self.recvTimers[addr].start()
        self.neighborsOfNeighbors[addr] = [tuple(x) for x in msg['neighbors']]


    def __addToBiNeighborsWhenIsInAttemptNeighbors__(self,addr):
        self.neighborsAvailability[addr] = self.neighborsAvailability.get(addr,0)
        self.biNeighbors.append(addr)
        self.neighborsEntranceTime[addr] = time.time() - self.baseTime
        self.attemptNeighbors.remove(addr)
        if addr in self.attemptNeighborsSendTimers.keys():
            self.attemptNeighborsSendTimers[addr].cancel()
        self.__sendHelloMessagePeriodicallyToBiNeighbors__(addr)
        
    
    def __addToBiNeighborsWhenIsNotInAttemptNeighbors__(self,addr):
        self.biNeighbors.append(addr)
        self.neighborsEntranceTime[addr] = time.time() - self.baseTime
        self.__sendHelloMessagePeriodicallyToBiNeighbors__(addr)

    
    def __addToUniNeighbors__(self,addr):
        self.uniNeighbors.add(addr)
        timer = self.uniNeighborsRecvTimers.get(addr,None)
        if timer:
            timer.cancel()
        self.uniNeighborsRecvTimers[addr] = threading.Timer(8,self.__removeFromInputUniNeighbors__,args=(addr,))
        self.uniNeighborsRecvTimers[addr].start()

    def __terminate__(self):
        end = time.time()
        for addr in self.biNeighbors:
            self.neighborsAvailability[addr] = self.neighborsAvailability.get(addr,0)  + (end - self.neighborsEntranceTime.get(addr,end-self.baseTime) - self.baseTime)
            if self.neighborsEntranceTime.get(addr,None):
                del self.neighborsEntranceTime[addr]
        for timer in self.recvTimers.values():
            timer.cancel()
        for timer in self.sendTimers.values():
            timer.cancel()
        for timer in self.uniNeighborsRecvTimers.values():
            timer.cancel()
        for timer in self.attemptNeighborsSendTimers.values():
            timer.cancel()

    def __recvMessage__(self):
        while time.time() - self.baseTime < self.totalTime:
            try:
                self.socket.settimeout(1)
                msg , addr = self.socket.recvfrom(65535)
                msg = json.loads(msg.decode())
                recvNeighbors = [tuple(x) for x in msg['neighbors']]
                shouldMissMessage = random.choices([True,False],[0.05,0.95],k=1)
                if shouldMissMessage[0]:
                    continue
                if self.state == 'deActive':
                    continue
                myAddr = self.socket.getsockname()
                if (addr in self.attemptNeighbors) and (myAddr in recvNeighbors) and (len(self.biNeighbors) < self.maxNeighborsCount):
                    self.__addToBiNeighborsWhenIsInAttemptNeighbors__(addr)
                    self.__processMsg__(msg,addr)
                elif (addr in self.biNeighbors) and (myAddr in recvNeighbors):
                    self.__processMsg__(msg,addr)
                elif (addr not in self.attemptNeighbors) and (addr not in self.biNeighbors) and (len(self.biNeighbors)<self.maxNeighborsCount):
                    self.__addToBiNeighborsWhenIsNotInAttemptNeighbors__(addr)
                    self.__processMsg__(msg,addr)
                elif (addr not in self.biNeighbors) and len(self.biNeighbors) == self.maxNeighborsCount:
                    self.__addToUniNeighbors__(addr)
                if len(self.biNeighbors) == self.maxNeighborsCount:
                    self.attemptNeighbors.clear()
                    for t in self.attemptNeighborsSendTimers.values():
                        t.cancel()
            except:
                continue
        self.__terminate__()


    

