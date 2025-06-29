import os
 
def path(file):
        folder_path = os.path.dirname(os.path.abspath(__file__))
        before_data_path = os.path.dirname(folder_path)
        data_path = os.path.join(before_data_path,'Data')
        file_path = os.path.join(data_path, file)
        return file_path