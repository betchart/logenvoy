#!/usr/bin/env python

import requests
import time
import sys
import os
from daemon import runner

class defaults(object):
    def __init__(self):
        cwd = os.getcwd()
        self.stdin_path = '/dev/null'
        self.stdout_path = '%s/logenvoy.stdout' %cwd
        self.stderr_path = '%s/logenvoy.stderr' %cwd
        self.pidfile_path =  '/tmp/logenvoy.pid'
        self.pidfile_timeout = 5
        checks_per_minute = 1
        self.interval = 60 / checks_per_minute
        self.logfile = '%s/logenvoy.log' %cwd
        ip = 'envoy'
        self.url = 'http://%s/production'%ip
        
try:
    import config
except:
    print "No 'config.py' found! Using defaults!"
    config = defaults()

def query_envoy():
    def fillvalues():
        r = requests.get(config.url, timeout=10)
        class functor(object):
            on = False
            def __init__(self, start, end):
                for item in ['start','end']: setattr(self, item, eval(item))
            def __call__(self,line):
                if self.start in line:
                    self.on = True
                    return False
                elif self.end in line:
                    self.on = False
                return self.on
        f = functor('START MAIN PAGE CONTENT','END MAIN PAGE CONTENT')
        payload = [ l for l in r.text.split('\n') if f(l)]
        payload2 = filter(lambda s: 'tr' not in s, payload[4].replace('><','>\n<').replace('<',' <').replace('>','> ').split('\n'))
        values = dict([tuple([item.strip() for item in filter(lambda s: s.strip(), item.strip().replace('/','').replace('<','').split('td>'))]) for item in payload2])
        for key,val in values.items():
            n,u = val.split()
            values[key] = float(n) * (1e3 if 'M' in u else 1 if 'k' in u else 1e-3)
        return values

    values = {'readerror':0}
    try: values.update(fillvalues())
    except: values.update({'readerror':1})

    values['time'] = int(time.time())
    return values


class logenvoy(object):
    '''A record of state changes'''

    def __init__(self):
        for item in ['interval','logfile','stdin_path','stdout_path','stderr_path','pidfile_path','pidfile_timeout']:
            setattr(self,item,getattr(config,item))

    def run(self):
        self.initial_state = query_envoy()
        self.current_state = self.initial_state
        
        with open(self.logfile, 'a') as f:
            print>>f, self.initial_state
            self.tell = f.tell()
            print>>f, self.current_state

        while True:
            time.sleep(self.interval)
            self.log_changes()
    
    @classmethod
    def diff_state(cls,current,state):
        diffs = {}
        for key in sorted(state):
            if key not in current or type(current[key])!=type(state[key]) or current[key]!=state[key]:
                diffs[key] = state[key]
            elif type(current[key]) is dict:
                subdiffs = cls.diff_state(current[key],state[key])
                if subdiffs:
                    diffs[key] = subdiffs
        return diffs

    def log_changes(self):
        state = query_envoy()
        diff = self.diff_state(self.current_state,state)
        self.current_state.update(state)
        with open(self.logfile, 'a') as f:
            f.truncate(self.tell)
            f.seek(self.tell)
            if len(diff)>1:
                print>>f, diff
                self.tell = f.tell()
            print>>f,self.current_state


if __name__=="__main__":
    app = logenvoy()
    
    daemon_runner = runner.DaemonRunner(app)
    daemon_runner.do_action()
