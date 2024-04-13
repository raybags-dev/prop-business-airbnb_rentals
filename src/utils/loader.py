import threading
import time
from tqdm import tqdm
from colorama import init, Fore, Style

init()


class LoadingEmulator:
    def __init__(self):
        self.loading_thread = None
        self.stopped = False

    def emulate_loading(self, message: str = '', start_loading: bool = False):
        if message:
            print(f'\033[95m{message}\033[95m ⏳')

        if start_loading:
            self.stopped = False
            self.loading_thread = threading.Thread(target=self.update_progress)
            self.loading_thread.start()
        else:
            self.stopped = True
            if self.loading_thread and self.loading_thread.is_alive():
                self.loading_thread.join()

        if not start_loading:
            print('\033[92m\n> Process completed. \033[92m ✔️✔️')

    def update_progress(self):
        with tqdm(total=100, bar_format=f'> \033[96m{{percentage:.0f}}% - (\033[96m{{elapsed}}/{{remaining}})', ncols=100) as pbar:
            while not self.stopped:
                time.sleep(0.1)
                pbar.update(1)
            pbar.close()


loader_instance = LoadingEmulator()


def worker_emulator(message, is_in_progress):
    if is_in_progress:
        loader_instance.emulate_loading(message=message, start_loading=True)
    else:
        loader_instance.emulate_loading(start_loading=False)

