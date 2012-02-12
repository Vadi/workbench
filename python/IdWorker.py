#! /usr/bin/python
# Copyrights licensed to Vadivel Kumar

class IdWorker (object):
    def __init__(self, datacenterId, WorkerId):
        self.datacenterId = datacenterId
        self.workerId = WorkerId
        self.twepoch = 1288834974657L
        self.sequence = 0L
        self.workerIdBits = 5L
        self.datacenterIdBits = 5L
        self.maxWorkerId = -1L ^ (-1L << self.workerIdBits)
        self.maxDatacenterId = -1L ^ (-1L << self.datacenterIdBits)
        self.sequenceBits = 12L
        self.workerIdShift = self.sequenceBits
        self.datacenterIdShift = self.sequenceBits + self.workerIdBits
        self.timestampLeftShift = self.sequenceBits + self.workerIdBits + self.datacenterIdBits
        self.sequenceMask = -1L ^ (-1L << self.sequenceBits)

        self.lastTimestamp = -1L

        self.__validate()

    def __validate(self):
        if ((self.workerId > self.maxWorkerId) or (self.workerId < 0)):
            raise Exception (format('worker Id can''t be greater than %d or less than 0' % self.workerId))
        if ((self.datacenterId > self.maxDatacenterId) or (self.datacenterId < 0)):
            raise Exception (format('datacenter id can''t be greater than %d or less than 0' % self.datacenterId))
        
    def __currentTimeInMillis(self):
        """ Returns times in milliseconds """
        import time
        return int(time.time() * 1000)

    def __untilNextMillis(self, timestmp):
        t = self.__currentTimeInMillis()
        while (t <= self.lastTimestamp):
            t = self.__currentTimeInMillis()
        return t

    def NextId(self):
        timestamp = self.__currentTimeInMillis()

        if (self.lastTimestamp == timestamp):
            self.sequence = (self.sequence + 1) & self.sequenceMask
            if (self.sequence == 0):
                timestamp = self.__untilNextMillis(self.lastTimestamp)
        else:
            self.sequence=0

        
        if (timestamp < self.lastTimestamp):
            raise ("Clock  moved backwards. Refusing to generate id")
        
        self.lastTimestamp = timestamp

        first = ( timestamp - self.twepoch ) << self.timestampLeftShift
        second = self.datacenterId << self.datacenterIdShift
        third = self.workerId << self.workerIdShift

        return (first | second | third | self.sequence)

if __name__ == '__main__':
    idworker0 = IdWorker(1,2)
    idworker1 = IdWorker(1,3)

    for a in range(1,5):
        print format("idworker0= %u" % idworker0.NextId())
    print
    for b in range(1,5):
        print format("idworker1= %u" % idworker1.NextId())
