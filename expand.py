#!/usr/bin/env python

from collections import deque
import datetime as DT
from datetime import datetime as DTD
from datetime import tzinfo as TZ
import time
import pytz
from itertools import imap

''' Decode solar array sequence of diff data into sequence of data.

Values tracked are : time, Currently, Today, Past Week, Since Installation

Problems:
(a) Incomplete data: sometimes data recording went down for a while.
(b) Non-reporting microinverters: microinverters keep track, but don't report
(c) resolution loss from sigfig limits

Rules:
* Identify problem (a) by scanning ahead:
    Perhaps future histories of "Past Week" and "Since Installation" can illuminate current production which was not recorded
* Identify non-reporting microinverters by scanning ahead in "Today" to see if average production in a particular slot seems greater than 4.1 kW (max)
* Always zero "Today" at midnight.
* Since Installation must be monotonic
* Today must be monotonic
* Past Week must never be less than observed sum over last 7*24 hours
* Add datum "Daymax"
'''


EST = pytz.timezone('EST')
def est(_epoch):
    return DTD.fromtimestamp(_epoch, EST)

ET = pytz.timezone('US/Eastern')
def et(_epoch):
    return DTD.fromtimestamp(_epoch, ET)

def epoch(dt): return time.mktime(dt.timetuple())

firsttime = epoch(DTD.combine(DT.date(year=2015, month=1, day=1),
                              DT.time.min.replace(tzinfo=EST)))


def backfill(iterSolarData):
    '''Corrected solar data, input iterator over raw, output iterator over corrected.'''
    buf = deque()
    popped = {'time':firsttime, 'Currently':None, 'Today':0, 'Past Week':0, 'Since Installation':0}
    weekly_seconds = 7 * 24 * 60 * 60
    
    for datum in iterSolarData:
        
        # push datum into the queue
        buf.append(dict(datum))
            
        # Yield data which do not need or contribute to corrections
        while (buf and
               (None in buf[0].values() or None in popped.values() or
                ( buf[0]['Today'] - popped['Today'] < 0.3 and
                  abs(buf[0]['Past Week'] - popped['Past Week']) < 2 and
                  buf[0]['Since Installation'] - popped['Since Installation'] < 20))):
            if buf[0]['readerror']:
                buf.popleft()
            else:
                if popped['time'] > buf[0]['time']:
                    buf[0]['time'] = popped['time']
                popped = buf.popleft()
                yield popped
            
        if True:
        # Add any missing points implied by "Past Week"
            if buf and buf[-1]['time']-weekly_seconds > buf[0]['time']:
                buf.extendleft([{'time': b['time']-weekly_seconds,
                                 'Since Installation':max(b['Since Installation']-b['Past Week'], popped['Since Installation']),
                                 'Past Week':None,
                                 'Currently':0 if et(b['time']).timetz() < DT.time(4,0,0,0) else None,
                                 'Today':0 if et(b['time']).timetz() < DT.time(4,0,0,0) else None,
                                 'readerror':0}
                                for b in reversed(buf)
                                if b['Past Week'] and
                                buf[0]['time'] > b['time']-weekly_seconds > popped['time']])
                #if not all(popped['time'] <= b['time'] for b in buf):
                #    raise Exception(str(popped), str(b))
                #d = et(popped['time']).date()
                #dsi = popped['Since Installation'] - popped['Today']
                #for b in buf:
                #    if d != et(b['time']).date():
                #        d = et(b['time']).date()
                #        dsi = b['Since Installation'] - b['Today']
                
                popped = buf.popleft()
                yield popped
        
        # Up-correct "Current" when calculation is higher than reported average
            
    for point in buf:
        yield point
    

def integrated(it):
    '''Calculate energy Today from integrated Currently.'''
    v0 = next(it)
    energy = v0['Today']
    v0.update({'Integrated Day':energy})
    yield v0
    
    for v in it:
        if v['Today'] == 0:
            energy = 0
        else:
            energy += 0.5 * (v0['Currently'] + v['Currently']) * (v['time'] - v0['time']) / 60 / 60
        v.update({'Integrated Day':energy})
        yield v
            
def midnights(it):
    '''Bracket midnight with points to  zero "Today".'''
    v0 = dict(next(it))
    yield v0
    for v in it:
        if et(v['time']).date() != et(v0['time']).date():
            pm = ET.localize(DTD.combine(et(v0['time']), DT.time.max))
            am = ET.localize(DTD.combine(et(v0['time']) + DT.timedelta(days=1), DT.time.min))
            am2 = ET.localize(DTD.combine(et(v['time']), DT.time.min))
            AM2, AM, PM = dict(v), dict(v0), dict(v0)
            AM2.update({'time': epoch(am2), 'Today':0.0, 'Currently':0, 'Since Installation':v['Since Installation']-v['Today']})
            AM.update({'time': epoch(am), 'Today':0.0, 'Currently':0})
            PM.update({'time': epoch(pm), 'Currently':0})
            if (PM['Since Installation'] < AM2['Since Installation'] <= v['Since Installation'] and
                am.date() == am2.date()):
                PM.update({"Since Installation": AM2['Since Installation']})
                AM.update({"Since Installation": AM2['Since Installation']})
            yield PM
            yield AM
            if AM2['time'] != AM['time']: yield AM2
        yield v
        v0.update(v)
        
def scrubber(it):
    '''Filter out trivially incorrect data points.'''
    v0 = dict(next(it))
    yield v0
    for v in it:
        if (v0['time'] < v['time']
            and v0['Since Installation'] <= v['Since Installation']
            and v['readerror']!=1
        ):
            yield v
            v0.update(v)

def undiff(iterDiff):
    '''Convert stream of diffs into stream of values.'''
    v = dict(next(iterDiff))
    yield v
    for d in iterDiff:
        v.update(d)
        yield v

if __name__ == "__main__":
    with open('logenvoy.log') as f:
        for v in integrated(midnights(scrubber(undiff(imap(eval, f.readlines()))))):
            print (v['time'] - firsttime)/(24*60*60), v['Currently'], v["Today"], v["Past Week"], v["Since Installation"], v['Integrated Day']

