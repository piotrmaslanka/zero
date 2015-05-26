from zerocon import ZeroInterface, ZeroException, \
                    SeriesNotFoundException, \
                    DefinitionMismatchException

zero = ZeroInterface([('192.168.224.100', 8886), 
                      ('192.168.224.101', 8886),
                      ('192.168.224.102', 8886)])    

try:
    sd = zero.getDefinition('test.series')
    head = zero.getHeadTimestamp(sd)

    zero.write(sd, head, head+1, '0000')
    zero.close()
except SeriesNotDefinedException:
    print 'Nie zdefiniowano takiego ciagu'
except ZeroException:
    print 'Wystapil inny problem'

