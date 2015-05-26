from zerocon import IOException
from benchutils import getInterface, getSeries
from threading import Thread
import time

class TestThread(Thread):
    def __init__(self, x):
        Thread.__init__(self)
        self.x = x
        self.ifc = getInterface()
        self.sd, self.head = getSeries(self.ifc, 'test'+str(x))

    def run(self):
        head = self.head
        for x in xrange(0, 1000):
            while True:
                try:
                    self.ifc.writeSeries(self.sd, head, head+1, '    ')
                except IOException:
                    self.ifc.close()
                    self.ifc = getInterface()
                else:
                    break
                
            head += 1
            
        self.ifc.close()
            
for z in (10, 10, ):#reversed(range(100, 150, 10) + [150]):            

    threads = [TestThread(x) for x in xrange(0, z)]

    now = time.time()
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    now2 = time.time()

    print 'For %s: Took %s seconds' % (z, now2-now, )
    