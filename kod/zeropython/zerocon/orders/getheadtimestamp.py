from zerocon.orders import BaseOrder
from zerocon.exceptions import IOException, SeriesNotFoundException, DefinitionMismatchException
import struct

class GetHeadTimestamp(BaseOrder):
    def __init__(self, sd):
        BaseOrder.__init__(self)
        self.sd = sd

    def __str__(self):
        return '\x02' + self.sd.toINTP()

    def on_data(self, buffer):
        if len(buffer) == 0: return        
        if buffer[0] == 0:
            # Done OK!!
            if len(buffer) < 9:
                return
            
            self.result, = struct.unpack('>q', buffer[1:9])
            del buffer[:9]
        elif buffer[0] == 1:
            self.result = IOException()
            del buffer[0]
        elif buffer[0] == 2:
            self.result = SeriesNotFoundException()            
            del buffer[0]
        elif buffer[0] == 3:
            self.result = DefinitionMismatchException()
            del buffer[0]

        self.is_completed = True

    def copy(self):
        return GetHeadTimestamp(self.sd)
