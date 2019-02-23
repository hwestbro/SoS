#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import multiprocessing as mp
import os
import signal
import subprocess
import sys
from typing import Any, Dict, Optional

import zmq

from ._version import __version__
from .controller import (close_socket, connect_controllers, create_socket,
                         disconnect_controllers)
from .eval import SoS_exec
from .executor_utils import kill_all_subprocesses
from .targets import sos_targets
from .utils import (WorkflowDict, env, get_traceback, load_config_files,
                    short_repr, ProcessKilled)

def signal_handler(*args, **kwargs):
    raise ProcessKilled()

class SoS_Worker(mp.Process):
    '''
    Worker process to process SoS step or workflow in separate process.
    '''

    def __init__(self, config: Optional[Dict[str, Any]] = None, args: Optional[Any] = None,
            **kwargs) -> None:
        '''

        config:
            values for command line options

            config_file: -c
            output_dag: -d

        args:
            command line argument passed to workflow. However, if a dictionary is passed,
            then it is assumed to be a nested workflow where parameters are made
            immediately available.
        '''
        # the worker process knows configuration file, command line argument etc
        super(SoS_Worker, self).__init__(**kwargs)
        #
        self.config = config

        self.args = [] if args is None else args

        # there can be multiple jobs for this worker, each using their own port and socket
        self._master_sockets = []
        self._master_ports = []
        self._stack_idx = 0

    def reset_dict(self):
        env.sos_dict = WorkflowDict()
        env.parameter_vars.clear()

        env.sos_dict.set('__args__', self.args)
        # initial values
        env.sos_dict.set('SOS_VERSION', __version__)
        env.sos_dict.set('__step_output__', sos_targets())

        # load configuration files
        load_config_files(env.config['config_file'])

        SoS_exec('import os, sys, glob', None)
        SoS_exec('from sos.runtime import *', None)

        if isinstance(self.args, dict):
            for key, value in self.args.items():
                if not key.startswith('__'):
                    env.sos_dict.set(key, value)

    def run(self):
        # env.logger.warning(f'Worker created {os.getpid()}')
        env.config.update(self.config)
        env.zmq_context = connect_controllers()

        # create controller socket
        env.ctrl_socket = create_socket(env.zmq_context, zmq.REQ, 'worker backend')
        env.ctrl_socket.connect(f'tcp://127.0.0.1:{self.config["sockets"]["worker_backend"]}')

        signal.signal(signal.SIGTERM, signal_handler)

        # create at last one master socket
        env.master_socket = create_socket(env.zmq_context, zmq.PAIR)
        port = env.master_socket.bind_to_random_port('tcp://127.0.0.1')
        self._master_sockets.append(env.master_socket)
        self._master_ports.append(port)

        # result socket used by substeps
        env.result_socket = None
        env.result_socket_port = None

        # wait to handle jobs
        while True:
            try:
                if not self._process_job():
                    break
            except ProcessKilled:
                # in theory, this will not be executed because the exception
                # will be caught by the step executor, and then sent to the master
                # process, which will then trigger terminate() and send a None here.
                break
            except KeyboardInterrupt:
                break
        # Finished
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        kill_all_subprocesses(os.getpid())

        close_socket(env.result_socket, 'substep result', now=True)

        for socket in self._master_sockets:
            close_socket(socket, 'worker master', now=True)
        close_socket(env.ctrl_socket, now=True)
        disconnect_controllers(env.zmq_context)

    def push_env(self):
        self._stack_idx += 1
        env.switch(self._stack_idx)
        if len(self._master_sockets) > self._stack_idx:
            # if current stack is ok
            env.master_socket = self._master_sockets[self._stack_idx]
        else:
            # a new socket is needed
            env.master_socket = create_socket(env.zmq_context, zmq.PAIR)
            port = env.master_socket.bind_to_random_port('tcp://127.0.0.1')
            self._master_sockets.append(env.master_socket)
            self._master_ports.append(port)

    def pop_env(self):
        self._stack_idx -= 1
        env.switch(self._stack_idx)
        env.master_socket = self._master_sockets[self._stack_idx]

    def _process_job(self):
        # send the current socket number as a way to notify the availability of worker
        env.ctrl_socket.send_pyobj(self._master_ports[self._stack_idx])
        work = env.ctrl_socket.recv_pyobj()
        env.logger.trace(
            f'Worker {self.name} receives request {short_repr(work)} with master port {self._master_ports[self._stack_idx]}')

        if work is None:
            return False
        elif not work: # an empty task {}
            return True

        if isinstance(work, dict):
            self.run_substep(work)
        elif work[0] == 'step':
            # this is a step ...
            runner = self.run_step(*work[1:])
            try:
                poller = next(runner)
                while True:
                    # if request is None, it is a normal "break" and
                    # we do not need to jump off
                    if poller is None:
                        poller = runner.send(None)
                        continue

                    while True:
                        if poller.poll(200):
                            poller = runner.send(None)
                            break
                        # now let us ask if the master has something else for us
                        self.push_env()
                        self._process_job()
                        self.pop_env()
            except StopIteration as e:
                pass
        else:
            self.run_workflow(*work[1:])
        env.logger.debug(
            f'Worker {self.name} completes request {short_repr(work)}')
        return True


    def run_workflow(self, workflow_id, wf, targets, args, shared, config):
        #
        #
        # get workflow, args, shared, and config
        from .workflow_executor import Base_Executor

        self.args = args
        env.config.update(config)
        self.reset_dict()
        # we are in a separate process and need to set verbosity from workflow config
        # but some tests do not provide verbosity
        env.verbosity = config.get('verbosity', 2)
        env.logger.debug(
            f'Worker {self.name} working on a workflow {workflow_id} with args {args}')
        executer = Base_Executor(wf, args=args, shared=shared, config=config)
        # we send the socket to subworkflow, which would send
        # everything directly to the master process, so we do not
        # have to collect result here
        try:
            executer.run_as_nested(targets=targets, parent_socket=env.master_socket,
                         my_workflow_id=workflow_id)
        except Exception as e:
            env.master_socket.send_pyobj(e)

    def run_step(self, section, context, shared, args, config, verbosity):
        from .step_executor import Step_Executor

        env.logger.debug(
            f'Worker {self.name} working on {section.step_name()} with args {args}')
        env.config.update(config)
        env.verbosity = verbosity
        #
        self.args = args
        self.reset_dict()

        # Execute global namespace. The reason why this is executed outside of
        # step is that the content of the dictioary might be overridden by context
        # variables.
        try:
            SoS_exec(section.global_def)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(e.stderr)
        except RuntimeError:
            if env.verbosity > 2:
                sys.stderr.write(get_traceback())
            raise

        # clear existing keys, otherwise the results from some random result
        # might mess with the execution of another step that does not define input
        for k in ['__step_input__', '__default_output__', '__step_output__']:
            if k in env.sos_dict:
                env.sos_dict.pop(k)
        # if the step has its own context
        env.sos_dict.quick_update(shared)
        # context should be updated after shared because context would contain the
        # correct __step_output__ of the step, whereas shared might contain
        # __step_output__ from auxiliary steps. #526
        env.sos_dict.quick_update(context)

        executor = Step_Executor(
            section, env.master_socket, mode=env.config['run_mode'])

        runner = executor.run()
        try:
            yreq = next(runner)
            while True:
                yres = yield yreq
                yreq = runner.send(yres)
        except StopIteration:
            pass

    def run_substep(self, work):
        from .substep_executor import execute_substep
        execute_substep(**work)

