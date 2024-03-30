import errno
import math
import os
import sys
import time

def main(argv):
    global deviceFDs
    global lastDeviceData

    runID = int(argv[0])
    interval = float(argv[1])
    startTime = time.time()
    nextDue = startTime + interval

    sysInfo = ['run', 'elapsed', ]
    sysInfo += initSystemUsage()
    print(",".join([str(x) for x in sysInfo]))

    devices = []
    deviceFDs = {}
    lastDeviceData = {}

    for dev in argv[2:]:
        if dev.startswith('blk_'):
            devices.append(dev)
        elif dev.startswith('net_'):
            devices.append(dev)
        else:
            raise Exception("unknown device type '" + dev + "'")

    for dev in devices:
        if dev.startswith('blk_'):
            devInfo = ['run', 'elapsed', 'device', ]
            devInfo += initBlockDevice(dev)
            print(",".join([str(x) for x in devInfo]))
        elif dev.startswith('net_'):
            devInfo = ['run', 'elapsed', 'device', ]
            devInfo += initNetDevice(dev)
            print(",".join([str(x) for x in devInfo]))

    sys.stdout.flush()

    try:
        while True:
            now = time.time()
            if nextDue > now:
                time.sleep(nextDue - now)

            elapsed = int((nextDue - startTime) * 1000.0)
            sysInfo = [runID, elapsed, ]
            sysInfo += getSystemUsage()
            print(",".join([str(x) for x in sysInfo]))

            for dev in devices:
                if dev.startswith('blk_'):
                    devInfo = [runID, elapsed, dev, ]
                    devInfo += getBlockUsage(dev, interval)
                    print(",".join([str(x) for x in devInfo]))
                elif dev.startswith('net_'):
                    devInfo = [runID, elapsed, dev, ]
                    devInfo += getNetUsage(dev, interval)
                    print(",".join([str(x) for x in devInfo]))

            nextDue += interval
            sys.stdout.flush()
    except KeyboardInterrupt:
        print("")
        return 0
    except IOError as e:
        if e.errno == errno.EPIPE:
            return 0
        else:
            raise e

def initSystemUsage():
    global procStatFD
    global procVMStatFD
    global lastStatData
    global lastVMStatData

    procStatFD = open("/proc/stat", "rb")
    for line in procStatFD:
        line = line.decode().split()
        if line[0] == "cpu":
            lastStatData = [int(x) for x in line[1:]]
            break
    if len(lastStatData) != 10:
        raise Exception("cpu line in /proc/stat too short")

    procVMStatFD = open("/proc/vmstat", "rb")
    lastVMStatData = {}
    for line in procVMStatFD:
        line = line.decode().split()
        if line[0] in ['nr_dirty', ]:
            lastVMStatData['vm_' + line[0]] = int(line[1])
    if len(lastVMStatData.keys()) != 1:
        raise Exception("not all elements found in /proc/vmstat")

    return [
        'cpu_user', 'cpu_nice', 'cpu_system',
        'cpu_idle', 'cpu_iowait', 'cpu_irq',
        'cpu_softirq', 'cpu_steal',
        'cpu_guest', 'cpu_guest_nice',
        'vm_nr_dirty',
    ]

def getSystemUsage():
    global procStatFD
    global procVMStatFD
    global lastStatData
    global lastVMStatData

    procStatFD.seek(0, 0)
    for line in procStatFD:
        line = line.decode().split()
        if line[0] != "cpu":
            continue
        statData = [int(x) for x in line[1:]]
        deltaTotal = float(sum(statData) - sum(lastStatData))
        if deltaTotal == 0:
            result = [0.0 for x in statData]
        else:
            result = []
            for old, new in zip(lastStatData, statData):
                result.append(float(new - old) / deltaTotal)
        lastStatData = statData
        break

    procVMStatFD.seek(0, 0)
    newVMStatData = {}
    for line in procVMStatFD:
        line = line.decode().split()
        if line[0] in ['nr_dirty', ]:
            newVMStatData['vm_' + line[0]] = int(line[1])

    for key in ['vm_nr_dirty', ]:
        result.append(newVMStatData[key])

    return result

def initBlockDevice(dev):
    global deviceFDs
    global lastDeviceData

    devPath = os.path.join("/sys/block", dev[4:], "stat")
    deviceFDs[dev] = open(devPath, "rb")
    line = deviceFDs[dev].readline().decode().split()
    newData = []
    for idx, mult in [
        (0, 1.0), (1, 1.0), (2, 0.5),
        (4, 1.0), (5, 1.0), (6, 0.5),
    ]:
        newData.append(int(line[idx]))
    lastDeviceData[dev] = newData
    return ['rdiops', 'rdmerges', 'rdkbps', 'wriops', 'wrmerges', 'wrkbps', ]

def getBlockUsage(dev, interval):
    global deviceFDs
    global lastDeviceData

    deviceFDs[dev].seek(0, 0)
    line = deviceFDs[dev].readline().decode().split()
    oldData = lastDeviceData[dev]
    newData = []
    result = []
    ridx = 0
    for idx, mult in [
        (0, 1.0), (1, 1.0), (2, 0.5),
        (4, 1.0), (5, 1.0), (6, 0.5),
    ]:
        newData.append(int(line[idx]))
        result.append(float(newData[ridx] - oldData[ridx]) * mult / interval)
        ridx += 1
    lastDeviceData[dev] = newData
    return result

def initNetDevice(dev):
    global deviceFDs
    global lastDeviceData

    devPath = os.path.join("/sys/class/net", dev[4:], "statistics")
    deviceData = []
    for fname in ['rx_packets', 'rx_bytes', 'tx_packets', 'tx_bytes', ]:
        key = dev + "." + fname
        deviceFDs[key] = open(os.path.join(devPath, fname), "rb")
        deviceData.append(int(deviceFDs[key].read()))
    lastDeviceData[dev] = deviceData
    return ['rxpktsps', 'rxkbps', 'txpktsps', 'txkbps', ]

def getNetUsage(dev, interval):
    global deviceFDs
    global lastDeviceData

    oldData = lastDeviceData[dev]
    newData = []
    for fname in ['rx_packets', 'rx_bytes', 'tx_packets', 'tx_bytes', ]:
        key = dev + "." + fname
        deviceFDs[key].seek(0, 0)
        newData.append(int(deviceFDs[key].read()))
    result = [
        float(newData[0] - oldData[0]) / interval,
        float(newData[1] - oldData[1]) / interval / 1024.0,
        float(newData[2] - oldData[2]) / interval,
        float(newData[3] - oldData[3]) / interval / 1024.0,
    ]
    lastDeviceData[dev] = newData
    return result

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
