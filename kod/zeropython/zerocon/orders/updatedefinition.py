from zerocon.orders import BaseOrder
from zerocon.exceptions import IOException

class UpdateDefinition(BaseOrder):
    def __init__(self, sd):
        BaseOrder.__init__(self)
        self.sd = sd

    def __str__(self):
        return '\x01' + self.sd.toINTP()
        
    def on_data(self, buffer):
        if len(buffer) == 0: return        
        self.is_completed = True
        if buffer[0] == 0:
            # Done OK!!
            self.result = True
        elif buffer[0] == 1:
            # Failed
            self.result = IOException()
        del buffer[0]
        
    def copy(self):
        return UpdateDefinition(self.sd)        
