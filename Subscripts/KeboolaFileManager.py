import urllib3 
import json

# TokenType = 'X-StorageApi-Token'
# APIKey = '314-42522-7BhD9nyW2Zd1uoXCGCfQ9XgKIzOzDqj53lklVEc1'
# 'content-type' = 'application/json'
# tags = pipers

class KeboolaManager:
    def __init__(self, TokenType, APIKey, ContentType):
        self.__header = {
        TokenType : APIKey
        ,'content-type': ContentType
        }
        self.http = urllib3.PoolManager()

    def GetFiles(self, tags, offset, limit):
        URL = 'https://connection.eu-central-1.keboola.com/v2/storage/files?tags[]='+str(tags)+'&offset='+str(offset)+'&limit='+str(limit)
        request = self.http.request('GET',URL, headers=self.__header)
        output = json.loads(request.data.decode('utf-8'))
        return output