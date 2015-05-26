from zerocon import IOException
from benchutils import getInterface, getSeries
from threading import Thread
import time, sys

class PleaseMakeItWork(Thread):
    def __init__(self, x, a):
        Thread.__init__(self)
        self.x = x
        self.a = a        

    def dowrites(self):
        """Just do the writes. This should take below a minute, but this procedure doesn't verify that"""
        a = []
        for sd, head in self.a:
            while True:
                try:
                    self.ifc.writeSeries(sd, head, head+1, '    ')
                except IOException:
                    self.ifc.close()
                    self.ifc = getInterface(loadbalance=True)
                else:
                    break
            a.append((sd, head+1))
        self.a = a

    def pdowrites(self):
        """dowrites, with checking that it takes below a minute.
        Return how much it took"""
        a = time.time()
        self.dowrites()
        b = time.time() - a
        if b > 60:
            # This should take below a minute!!!
            print 'Took more than a minute, overshoot by %s' % (b - 60)
        return b    

    def run(self):
        self.ifc = getInterface()

        k = []

        for x in xrange(0, 20):
            b = self.pdowrites()

            if b < 60:
                time.sleep(60-b)
              
            k.append(b)  
            print 'Minute pass'
            
        self.result = k        

        self.ifc.close()


class LolWtfCollector(Thread):
    def __init__(self, x, y):
        self.x = x
        self.y = y
        Thread.__init__(self)
        self.result = None
        
    def run(self):
        ifc = getInterface()
        a = []
        for x in xrange(self.x, self.y):
            a.append([getSeries(ifc, 'y.%s.%s' % (x, y)) for y in xrange(0, 56)])
            print x
            
        self.result = a
        ifc.close()
        
# Seriously, this IS going to take a moment.   
# Going to spawn this shit

a = []
for k, t in zip(xrange(0, 300, 50), xrange(50, 350, 50)):
    a.append(LolWtfCollector(k, t))

[b.start() for b in a]
c = []
for b in a:
    b.join()
    c.extend(b.result) 
    
print len(c)

threads = []
ifc = getInterface()
for x in xrange(0, 300):        
    threads.append(PleaseMakeItWork(x, c[x]))

ifc.close()

print 'Actually, starting'
[thread.start() for thread in threads]

[thread.join() for thread in threads]
