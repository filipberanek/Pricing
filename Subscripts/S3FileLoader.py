import urllib3 

class S3FileReader():

    def __init__(self):
        self.__pool = urllib3.PoolManager()

    def GetFileContent(self,URLPATH):
        data = self.__pool.urlopen('GET',URLPATH)
        return data.data.decode('utf-8')