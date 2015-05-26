import itertools, socket, collections, random
from zerocon.orders.getdefinition import GetDefinition
from zerocon.orders.getheadtimestamp import GetHeadTimestamp
from zerocon.orders.updatedefinition import UpdateDefinition
from zerocon.orders.writeseries import WriteSeries
from zerocon.orders.read import Read
from zerocon.orders.readhead import ReadHead

class ZeroInterface(object):
    def __init__(self, addresses, autoexecute=True, loadbalance=False):
        """
        Initializes a zero interface
        
        @param addresses: a sequence of addresses of Zero endpoints
            an address is either a TCP socket of form: tuple(address, port)
            or a string, describing UNIX domain socket
            
        @param autoexecute: if execute() needs to be called to process queries
        @param loadbalance: if addresses are randomized prior to cycling
        """
        
        if loadbalance:
            self.addresses = random.shuffle(addresses)
        self.addresses = itertools.cycle(addresses)
        self.sock = None
        self.autoexecute = autoexecute
        self.orders = collections.deque()
        self.buffer = bytearray()       # buffer for received data
        
    def close(self):
        """Call when everything is read!"""
        try:
            self.sock.close()
        except:
            pass
        
    def __on_dc(self):
        """I was just disconnected!!!"""
        try:
            self.sock.close()
        except:
            pass
            
        self.sock = None
        if len(self.orders) == 0:       # so what? we don't need to reconnect right now..
            return
        
        while True:            
            self.__reconnect()
            
            newords = collections.deque()
            for order in self.orders:
                newords.append(order.copy())
            self.orders = newords

            try:
                for ord in self.orders:
                    self.sock.send(str(ord))
            except IOError as e:
                continue
            else:
                break
        
    def __reconnect(self):
        while True:
            self.buffer = bytearray()
            try:
                self.sock.close()
            except:
                pass
            
            nextaddr = self.addresses.next()
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.settimeout(10)
                sock.connect(nextaddr)
                sock.send('\x03')
            except IOError:
                self.sock = None
                continue
            else:
                self.sock = sock
                break

    def get(self):
        """Block until leftmost order is complete, returning it's result
        or throwing a suitable exception"""
        
        if len(self.orders) == 0:
            raise IndexError('Cannot get() if there are no orders pending')
        
        while True:
            otx = self.orders[0]
                    
            try:
                data = self.sock.recv(1024)
                if len(data) == 0:
                    self.__on_dc()
                    continue
            except IOError:
                self.__on_dc()
                continue
            
            self.buffer.extend(data)

            otx.on_data(self.buffer)

            if otx.is_completed:
                self.orders.popleft()
                return otx.get_result()
            # and if not completed, while will make another pass...                
                
            
    def _addOrder(self, orderObject):
        if self.sock == None:
            self.__reconnect()

        self.orders.append(orderObject)

        try:
            self.sock.send(str(orderObject))
        except socket.error:
            self.__on_dc()
        
        if self.autoexecute:
            return self.get()                
        
    # Helper operations
    
    def writeSeries(self, sd, prev_timestamp, cur_timestamp, data):
        return self._addOrder(WriteSeries(sd, prev_timestamp, cur_timestamp, data))
              
    def read(self, sd, from_, to):
        return self._addOrder(Read(sd, from_, to))
        
    def readHead(self, sd):
        return self._addOrder(ReadHead(sd))
        
    def getDefinition(self, name):
        return self._addOrder(GetDefinition(name))
        
    def getHeadTimestamp(self, sd):
        return self._addOrder(GetHeadTimestamp(sd))
        
    def updateDefinition(self, sd):
        return self._addOrder(UpdateDefinition(sd))