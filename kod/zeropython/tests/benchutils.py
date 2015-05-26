from zerocon import ZeroInterface, ZeroException, SeriesNotFoundException, SeriesDefinition, IOException

# Create interface. Specify servers
# and backup servers.

def getInterface(autoexecute=True, loadbalance=False):
    return ZeroInterface([('192.168.224.100', 8886), 
                          ('192.168.224.101', 8886),
                          ('192.168.224.102', 8886)],
                         autoexecute=autoexecute,
                         loadbalance=loadbalance)
    
def getSeries(zero, name):
    # Define if not exists, return sd, head timestamp
    try:
        sd = SeriesDefinition(name, 2, 0, 0, 4, '', 0)     
        zero.updateDefinition(sd)
    except IOException:
       return getSeries(zero, name)

    return sd, zero.getHeadTimestamp(sd)
