'''Module for deleting files based on directory and age'''
import os, datetime
import config

class Cleaner:
    '''
    Class for deleting files based on directory and age

    Methods:
    --------
    clean(folder, lifespan):
        Deletes all the files in folder
        than are older then lifespan

    task():
        Calls clean
    '''
    def __init__(self):
        pass

    def clean(self, folder, lifespan):
        '''
        Deletes all the files in folder
        than are older then lifespan
        Args:
            folder: folder from which to delete files
            lifespan: maximum age of a file
        '''
        files = os.listdir(folder)

        for file in files:
            creation_date = file.split('_', maxsplit=3)[2]
            creation_time = datetime.datetime.strptime(creation_date, "%Y%m%d%H%M%S").timestamp()

            now_time = datetime.datetime.now().timestamp()

            life = (now_time - creation_time) / 60
            if life > lifespan:
                os.remove(folder + file)
            pass

    def task(self):
        '''Calls clean'''
        self.clean(config.destination_success_path, 30)
        self.clean(config.destination_error_path, 24*60)
        pass

if __name__ == '__main__':
    cleaner = Cleaner()
    cleaner.task()
