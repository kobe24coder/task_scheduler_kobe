
from win32com.taskscheduler import taskscheduler
import win32com.client
from croniter import croniter
from datetime import datetime
import pandas as pd 
import json, os
from typing import Tuple, Iterable



TASK_TRIGGER_TIME = 1
TASK_ACTION_EXEC = 0
TASK_LOGON_INTERACTIVE_TOKEN = 1

TASK_CREATE_OR_UPDATE = 6


class TaskSchedulerFolderMgr(object):
    TASK_ENUM_HIDDEN = 1
    
    def __init__(self, root):
        self.root = root 
        scheduler = win32com.client.Dispatch('Schedule.Service')
        scheduler.Connect()
        
        self.scheduler = scheduler
        self.path_list = None
         
    def get_all_folders(self):
        
        folders = [self.scheduler.GetFolder(self.root)]
        path_list = []
        
        while folders:
            folder = folders.pop(0)
            folders += list(folder.GetFolders(0))
            tasks = list(folder.GetTasks(TASK_ENUM_HIDDEN))
            for task in tasks:
                task.Enabled = False # so that previous tasks don't run # ? though if these are 1 time tasks, then we wouldn't have to worry about it
            path_list.append(folder)

        self.path_list = path_list
        
    
    def smart_create_folders(self, folder_iterable):
        existing_folders = self.path_list
        
        
        folder_iterable_dict = {path: os.path.join(self.root, path) for path in folder_iterable}
        new_folders_to_be_created = {path: fullpath for path, fullpath in folder_iterable_dict.items() if fullpath not in existing_folders}
        
        root_folder = self.scheduler.GetFolder(self.root)
        
        for folder in new_folders_to_be_created.keys():
            root_folder.CreateFolder(folder)
    
    
    def send_confirmation_email(self):
        pass
    

        
def timing_function(crontime: str, basetime: str=None) -> Iterable(datetime):
    current_time = pd.Timestamp('now')
    base_cron = croniter(basetime, current_time)
    left_window = min(current_time, pd.Timestamp(base_cron.get_next(datetime))) # 24 hr window to next
    right_window = pd.Timestamp(base_cron.get_next(datetime))
    
    
    valid_times = []
    
    job_cron = croniter(crontime, left_window)
    
    max_looping = 100
    n = 0
    
    for epoch in job_cron.all_next(datetime):
        if left_window <= epoch < right_window:
            valid_times.append(epoch)
        else:
            break
        
        n += 1
        if n > max_looping:
            break 
        
    return tuple(valid_times)
    
    
    
    
    
    
 

class Task(object):
 
    
    def __init__(self, folder: str, name: str, active: bool, run_scheduler: str,
                 action:str, args: str, comments: str):
        
        self.name = name 
        self.folder = folder 
        self.active: bool = active
        self.action = action
        self.run_scheduler: str = run_scheduler
        self.args = args
        
        self.comments = comments
        
        self.tasks = {}
        self.errors = {}
 
 
    def get_run_time(self, base_time: str=None):
        if base_time is None:
            base_time = "1 0 * * *" # 12:01 am
        
        valid_runtimes = timing_function(crontime=self.run_scheduler, basetime=base_time)
        
        for runtime in valid_runtimes:
            job_name = f"{self.name} @ {str(runtime)}" # needs to be a distict name, hence a combination of job name and runtime
            self.tasks[job_name] = runtime
        
    
    def register_tasks(self):
        # in db first, get key, then add to args
        db_ID_dict = {}
        
        
        for job_name, job_time in self.tasks.items():
            pass 
            # some code to register them
            # TODO maybe a try except?
            db_ID_dict[job_name] = None # set dictionary the job ID name, to be used later
            
            task = win32com.client.Dispatch('Schedule.Service')
            task.Connect()
            job_location_folder = task.GetFolder(self.folder)
            newtask = task.NewTask(0)
            
            
            trigger = newtask.Triggers.Create(TASK_TRIGGER_TIME)
            trigger.StartBoundary = job_time.isoformat()
            
            action = newtask.Actions.Create(TASK_ACTION_EXEC)
            action.Path = self.action
            action.Arguments = None #format into "-u job_ID"
            
            newtask.RegistrationInfo.Description = comments
            newtask.Settings.Enabled = self.active
         
                        
            job_location_folder.RegisterTaskDefinition(
                job_name,
                newtask,
                TASK_CREATE_OR_UPDATE,
                os.environ['USERNAME'],  # No user
                os.environ['PASSWORD'],  # No password
                TASK_LOGON_INTERACTIVE_TOKEN)
             
    
       