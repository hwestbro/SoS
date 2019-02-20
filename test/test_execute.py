#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import glob
import os
import sys
import shutil
import subprocess
import unittest

from sos._version import __version__
from sos.parser import SoS_Script
from sos.targets import file_target
from sos.utils import env
# if the test is imported under sos/test, test interacive executor
from sos.workflow_executor import Base_Executor


def multi_attempts(fn):
    def wrapper(*args, **kwargs):
        for n in range(4):
            try:
                fn(*args, **kwargs)
                break
            except Exception:
                if n > 1:
                    raise
    return wrapper


class TestExecute(unittest.TestCase):
    def setUp(self):
        env.reset()
        subprocess.call('sos remove -s', shell=True)
        # self.resetDir('~/.sos')
        self.temp_files = []

    def tearDown(self):
        for f in self.temp_files:
            if file_target(f).exists():
                file_target(f).unlink()

    def touch(self, files):
        '''create temporary files'''
        if isinstance(files, str):
            files = [files]
        #
        for f in files:
            with open(f, 'w') as tmp:
                tmp.write('test')
        #
        self.temp_files.extend(files)

    def resetDir(self, dirname):
        if os.path.isdir(os.path.expanduser(dirname)):
            shutil.rmtree(os.path.expanduser(dirname))
        os.mkdir(os.path.expanduser(dirname))


    def testKillWorker(self):
        '''Test if the workflow can error out after a worker is killed'''
        import psutil
        import time
        with open('testKill.sos', 'w') as tk:
            tk.write('''
import time

[1]
time.sleep(4)

[2]
time.sleep(4)
''')
        ret = subprocess.Popen(['sos', 'run', 'testKill'])
        proc = psutil.Process(ret.pid)
        while True:
            children = proc.children(recursive=True)
            if len(children) == 1:
                children[0].terminate()
                break
            time.sleep(0.1)
        ret.wait()
        self.assertNotEqual(ret.returncode, 0)
        #
        ret = subprocess.Popen(['sos', 'run', 'testKill'])
        proc = psutil.Process(ret.pid)
        while True:
            children = proc.children(recursive=True)
            if len(children) == 1:
                children[0].kill()
                break
            time.sleep(0.1)
        ret.wait()
        self.assertNotEqual(ret.returncode, 0)

    def testKillSubstepWorker(self):
        '''Test if the workflow can error out after a worker is killed'''
        import psutil
        import time
        with open('testKillSubstep.sos', 'w') as tk:
            tk.write('''
import time

[1]
input: for_each=dict(i=range(4))
time.sleep(2)
''')
        ret = subprocess.Popen(['sos', 'run', 'testKillSubstep', '-j3'])
        proc = psutil.Process(ret.pid)
        while True:
            children = proc.children(recursive=True)
            if len(children) == 3:
                children[-1].terminate()
                break
            time.sleep(0.1)
        ret.wait()
        self.assertNotEqual(ret.returncode, 0)
        #
        ret = subprocess.Popen(['sos', 'run', 'testKillSubstep', '-j3'])
        proc = psutil.Process(ret.pid)
        while True:
            children = proc.children(recursive=True)
            if len(children) == 3:
                children[-1].kill()
                break
            time.sleep(0.1)
        ret.wait()
        self.assertNotEqual(ret.returncode, 0)


    def testKillTask(self):
        '''Test if the workflow can error out after a worker is killed'''
        import psutil
        import time
        with open('testKillTask.sos', 'w') as tk:
            tk.write('''

[1]
task:
import time
time.sleep(10)
''')
        ret = subprocess.Popen(['sos', 'run', 'testKillTask', '-s', 'force'])
        proc = psutil.Process(ret.pid)
        while True:
            children = proc.children(recursive=True)
            execute = [x for x in children if 'execute' in x.cmdline()]
            if len(execute) >= 1:
                # a bug: if the process is killed too quickly (the signal
                # function is not called), this will fail.
                time.sleep(1)
                execute[0].terminate()
                break
            time.sleep(0.1)
        ret.wait()
        self.assertNotEqual(ret.returncode, 0)
        #
        ret = subprocess.Popen(['sos', 'run', 'testKillTask'])
        proc = psutil.Process(ret.pid)
        while True:
            children = proc.children(recursive=True)
            execute = [x for x in children if 'execute' in x.cmdline()]
            if len(execute) >= 1:
                time.sleep(1)
                execute[0].kill()
                break
            time.sleep(0.1)
        ret.wait()
        self.assertNotEqual(ret.returncode, 0)

    def testConcurrentRunningTasks(self):
        '''Test two sos instances running the same task'''
        import psutil
        import time
        with open('testKillTask.sos', 'w') as tk:
            tk.write('''

[1]
task:
import time
time.sleep(5)
''')
        ret1 = subprocess.Popen(['sos', 'run', 'testKillTask', '-s', 'force'])
        ret2 = subprocess.Popen(['sos', 'run', 'testKillTask', '-s', 'force'])
        ret1.wait()
        ret2.wait()
        self.assertEqual(ret1.returncode, 0)
        self.assertEqual(ret2.returncode, 0)

    def testRestartOrphanedTasks(self):
        '''Test restarting orphaned tasks which displays as running at first.'''
        import psutil
        import time
        subprocess.call(['sos', 'purge', '--all'])

        with open('testOrphan.sos', 'w') as tk:
            tk.write('''

[1]
task:
import time
time.sleep(12)
''')
        ret = subprocess.Popen(['sos', 'run', 'testOrphan', '-s', 'force'])
        proc = psutil.Process(ret.pid)
        while True:
            children = proc.children(recursive=True)
            execute = [x for x in children if 'execute' in x.cmdline()]
            if len(execute) >= 1:
                # a bug: if the process is killed too quickly (the signal
                # function is not called), this will fail.
                time.sleep(1)
                execute[0].kill()
                break
            time.sleep(0.1)
        proc.kill()
        #
        ret = subprocess.Popen(['sos', 'run', 'testOrphan'])
        ret.wait()
        self.assertEqual(ret.returncode, 0)

if __name__ == '__main__':
    unittest.main()
