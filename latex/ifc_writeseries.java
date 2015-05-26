public void writeSeries(SeriesDefinition serdef, long prev_timestamp,
    long cur_timestamp, byte[] data) throws LinkBrokenException,
    IllegalArgumentException, IOException, SeriesNotFoundException,
    DefinitionMismatchException {
        
    NodeDB.NodeInfo[] nodes = NodeDB.getInstance()
            .getResponsibleNodesWithReorder(serdef.seriesName, 
                                            serdef.replicaCount);

    int successes = 0;

    for (NodeDB.NodeInfo ni : nodes) {
        if (!ni.alive) continue;

        for (int i=0; i<2; i++) {// for retries in case of failure
            SystemInterface sin = null;
            try {
                sin = InterfaceFactory.getInterface(ni);
            } catch (IOException e) {
                FailureDetector.getInstance().onFailure(ni.nodehash);				
                continue;
            }
            try {
                sin.writeSeries(serdef, prev_timestamp, 
                                cur_timestamp, data);
                successes++;
                break;
            } catch (LinkBrokenException | IOException e) {
                FailureDetector.getInstance().onFailure(ni.nodehash);
            } catch (DefinitionMismatchException |
                     SeriesNotFoundException |
                     IllegalArgumentException e) {
                throw e;
            } finally {
                sin.close();
            }
        }
    }
    if (successes == 0) throw new LinkBrokenException();
}