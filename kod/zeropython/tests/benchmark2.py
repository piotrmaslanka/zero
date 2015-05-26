from zerocon import IOException
from benchutils import getInterface, getSeries
from threading import Thread
import time, Queue


class Emitter(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        
    def run(self):
        while True:
            time.sleep(1)
            self.queue.put(time.time())

class TestThread(Thread):
    def __init__(self, x):
        Thread.__init__(self)
        self.x = x
        self.ifc = getInterface()
        self.sd, self.head = getSeries(self.ifc, 'x'+str(x))
        self.collector = Queue.Queue()

    def run(self):
        Emitter(self.collector).start()
        head = self.head
        while True:
            issued_on = self.collector.get(True)
            started_on = time.time()
            while True:
                try:
                    self.ifc.writeSeries(self.sd, head, head+1, '    ')
                except IOException:
                    self.ifc.close()
                    self.ifc = getInterface()
                else:
                    break
                
            stopped_on = time.time()
            with open('/%s' % (self.x, ), 'a') as f:
                f.write('%s %s %s\n' % (issued_on, started_on, stopped_on))
                
            head += 1
            
        self.ifc.close()
            
threads = [TestThread(x) for x in xrange(0, 10)]
[thread.start() for thread in threads]
raw_input()

    