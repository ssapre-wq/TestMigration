from datetime import datetime

from TM1py.Exceptions import TM1pyException
from TM1py.Services import TM1Service


class ProcessService:

    def __init__(self, source: TM1Service, target: TM1Service, server: TM1Service):
        self.source = source
        self.target = target
        self.server = server

    def copy_process(self, process: str, item: str, deployment: str):
        start_time = datetime.now()
        try:
            if self.source.processes.exists(name=process):
                proc = self.source.processes.get(name_process=process)
                if self.target.processes.exists(name=process):
                    self.target.processes.update(process=proc)
                    message = f"Target Process updated"
		    error_counter=0
                else:
                    self.target.processes.create(process=proc)
                    message = "Target Process created"
                    error_counter=0
             
            else:
                message = "Source process does not exist"
                error_counter=1
 
            end_time = datetime.now()
            duration = end_time - start_time
            cellset = dict()
            cellset[(deployment, item, "Deployment Status")] = message
	    cellset[(deployment, item, "Deployment Error Counter")] = error_counter
            cellset[(deployment, item, "Deployment Start")] = datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S')
            cellset[(deployment, item, "Deployment End")] = datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S')
            cellset[(deployment, item, "Deployment Duration")] = str(duration)
            self.server.cubes.cells.write_values('System - Deployments', cellset)
        except TM1pyException as t:
            print(t)

    def copy_chore(self, chore: str, item: str, deployment: str):
        start_time = datetime.now()
        try:
            if self.source.chores.exists(chore_name=chore):
                ch = self.source.chores.get(chore_name=chore)
                if self.target.chores.exists(chore_name=chore):
                    self.target.chores.update(chore=ch)
                    message = "Target chore updated"
                    error_counter=0
                else:
                    self.target.chores.create(chore=ch)
                    message = "Target chore created"
                    error_counter=0
            else:
                message = "Source chore does not exist"
                error_counter=1
            end_time = datetime.now()
            duration = end_time - start_time
            cellset = dict()
            cellset[(deployment, item, "Deployment Status")] = message
            cellset[(deployment, item, "Deployment Error Counter")] = error_counter
            cellset[(deployment, item, "Deployment Start")] = datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S')
            cellset[(deployment, item, "Deployment End")] = datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S')
            cellset[(deployment, item, "Deployment Duration")] = str(duration)
            self.server.cubes.cells.write_values('System - Deployments', cellset)
        except TM1pyException as t:
            print(t)

    def run_process(self, process: str, params: dict, item: str, deployment: str):
        start_time = datetime.now()
        try:
            if self.target.processes.exists(name=process):
                self.target.processes.execute(process_name=process, **params)
                message = "Process executed"
                error_counter=0
            else:
                message = "Target process does not exist"
                error_counter=1
            end_time = datetime.now()
            duration = end_time - start_time
            cellset = dict()
            cellset[(deployment, item, "Deployment Status")] = message
cellset[(deployment, item, "Deployment Error Counter")] = error_counter
            cellset[(deployment, item, "Deployment Start")] = datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S')
            cellset[(deployment, item, "Deployment End")] = datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S')
            cellset[(deployment, item, "Deployment Duration")] = str(duration)
            self.server.cubes.cells.write_values('System - Deployments', cellset)
        except TM1pyException as t:
            print(t)