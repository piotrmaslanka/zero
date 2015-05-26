/**
 * Adds a record
 * @param previousTimestamp timestamp of previous write
 * @param currentTimestamp timestamp of this write
 * @param value value to write to DB
 * @throws IllegalArgumentException value passsed was malformed
 */
public synchronized void write(long previousTimestamp, 
                               long currentTimestamp,
                               byte[] value)
    throws IllegalArgumentException, IOException {

    long rootserTimestamp = this.primary_storage
                                .getHeadTimestamp();
    if (currentTimestamp <= rootserTimestamp) 
        return;	// just ignore this write

    if (previousTimestamp == rootserTimestamp) {
        // this is a standard LFD-serializable
        this.primary_storage.write(currentTimestamp,
                                   value);
        this.wacon.signalWrite(currentTimestamp);

        if (this.series.autoTrim > 0)
            this.primary_storage.trim(
                currentTimestamp - this.series.autoTrim);
    } else { 			
        this.wacon.write(previousTimestamp, currentTimestamp,
                                            value);
        // Now we need to signal that this series needs a repair...
        ReparatorySupervisorThread.getInstance().postRequest(
            new RepairRequest(this.series, 
                              this.primary_storage
                                  .getHeadTimestamp(), 
                              previousTimestamp));
    }
}