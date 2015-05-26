
class BaseOrder(object):
    """
    Base class for Zero interface orders
    """
    
    def __init__(self):
        self.is_completed = False
        self.result = None
    
    def __str__(self):
        """
        Return a string or bytearray
        representing on-wire format
        """
        
    def on_data(self, buffer):
        """
        Means data was received (at least one byte is in buffer). Parts of buffer can be read and extracted.
        Modifications on this buffer will survive state
        """
        
    def get_result(self):
        """
        If is_completed is set to True then this call, called only once, will return
        the result of the order.
        
        May throw
        """
        if isinstance(self.result, Exception):
            raise self.result
        else:
            return self.result
        
    def copy(self):
        """Perform a copy for rescheduling purposes"""        
        