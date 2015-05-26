from zerocon.orders import BaseOrder
from zerocon.exceptions import IOException, SeriesNotFoundException, DefinitionMismatchException, IllegalArgumentException
import struct

class WriteSeries(BaseOrder):
    def __init__(self, sd, prev_timestamp, cur_timestamp, data):
        BaseOrder.__init__(self)
        self.sd = sd
        self.prev_timestamp = prev_timestamp
        self.cur_timestamp = cur_timestamp
        self.data = data

    def __str__(self):
        return '\x03' + self.sd.toINTP() + struct.pack('>qqi', self.prev_timestamp, self.cur_timestamp, len(self.data)) + self.data
        
    def on_data(self, buffer):
        if len(buffer) == 0: return
        if buffer[0] == 1:
            self.result = IOException()
        elif buffer[0] == 2:
            self.result = SeriesNotFoundException()            
        elif buffer[0] == 3:
            self.result = DefinitionMismatchException()
        elif buffer[0] == 4:
            self.result = IllegalArgumentException()

        self.is_completed = True
        del buffer[0]

    def copy(self):
        return WriteSeries(self.sd, self.prev_timestamp, self.cur_timestamp, self.data)     
