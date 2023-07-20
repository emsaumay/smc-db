import os
import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

SCRIPT_TO_EXECUTE = "execute.py"

class FolderMonitor(FileSystemEventHandler):
    def on_any_event(self, event):
        if event.is_directory:
            return

        if event.event_type in ['created']:
            print(f"Detected {event.event_type} on {event.src_path}")
            execute_script(event.src_path)

def execute_script(file_path):
    try:
        file_name = os.path.basename(file_path)

        subprocess.run(["python", SCRIPT_TO_EXECUTE, file_path, file_name], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing script: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def start_monitoring(path):
    event_handler = FolderMonitor()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    print(f"Monitoring directory: {path}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

if __name__ == "__main__":
    folder_path = "C:\\Users\\sauma\\Desktop\\Workspace\\test"
    start_monitoring(folder_path)
