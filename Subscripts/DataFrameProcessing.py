from Subscripts import S3FileLoader
from Subscripts import KeboolaFileManager
from Subscripts import Constants
from io import StringIO
import pandas as pd 
import numpy as np 
import datetime
import json

class DataFrameProcessor: 

    def __init__(self):
        # Create keboola instance
        self.KebolaInst = KeboolaFileManager.KeboolaManager('X-StorageApi-Token','314-42522-7BhD9nyW2Zd1uoXCGCfQ9XgKIzOzDqj53lklVEc1','application/json')
        # Create S3FIleLoader Instance 
        self.S3Inst = S3FileLoader.S3FileReader()
        # Create Instance of DF Processor

    def addTS_addFileName(self, DF):
        #DF['FILENAME'] = FileName
        DF['UPLOAD_TS'] = datetime.datetime.now().strftime("%d.%m.%Y-%H:%M:%S")
        return DF

    def PrepareProduct(self):
        #Get products 
        productsURL = self.KebolaInst.GetFiles(Constants.MALLPRODUCTS,Constants.OFFSET,Constants.MAXLISTVALUES)
        productsDF = pd.DataFrame()
        productsStringValue = ''
        for i,prodcutVal in enumerate(productsURL):
            productsDFPart = pd.read_csv(StringIO(self.S3Inst.GetFileContent(prodcutVal['url'])), sep=",")
            productsDFPart = self.addTS_addFileName(productsDFPart)
            if productsDF.shape[0]>0:
                prodcutsDF = pd.concat([productsDF,productsDFPart])
            else: 
                productsDF = productsDFPart
            print("Made {} out of {}".format(i, len(productsURL)))
        print(productsDF.shape)
        productsDF = productsDF[productsDF['PRICE']>0]
        productsDF = productsDF[productsDF['VALID']==True]
        productsDF.drop_duplicates(subset = productsDF.columns,inplace = True)
        print(productsDF.shape)        
        return productsDF

    def PreparePiper(self):
        #Get piper 
        piperURL = self.KebolaInst.GetFiles(Constants.DODAVAETELPIPER,Constants.OFFSET,Constants.MAXLISTVALUES)
        pipersDF = pd.DataFrame()
        piperStringValue = ''
        for i,piperVal in enumerate(piperURL):
            pipersDFPart = pd.read_csv(StringIO(self.S3Inst.GetFileContent(piperVal['url'])), sep=",")
            pipersDFPart = self.addTS_addFileName(pipersDFPart)
            if pipersDF.shape[0]>0:
                pipersDF = pd.concat([pipersDF,pipersDFPart])
            else: 
                pipersDF = pipersDFPart
            print("Made {} out of {}".format(i, len(piperURL)))
        print(pipersDF.shape)
        pipersDF = pipersDF[pipersDF['PRICE']>0]
        pipersDF.drop_duplicates(subset = ['PROD_ID', 'ESHOP', 'PROD_NAME', 'PRICE', 'STOCK'],inplace = True)
        print(pipersDF.shape)
        return pipersDF

    def PrepareWasher(self):
        #Get piper URL
        washerURL = self.KebolaInst.GetFiles(Constants.DODAVATELWASHER,Constants.OFFSET,Constants.MAXLISTVALUES)
        washerDF = pd.DataFrame()
        complet = pd.DataFrame()
        washerStringValue = ''
        for i, washerVal in enumerate(washerURL):
            washersDFPart = pd.read_csv(StringIO(self.S3Inst.GetFileContent(washerVal['url'])), sep=",")
        #washersDFPart = pd.read_csv(StringIO(self.S3Inst.GetFileContent(washerURL[0]['url'])), sep=",")
        #InnerContent = pd.DataFrame(json.loads(washersDFPart['OFFERS'][0])).T.reset_index()
        #from pandas.io.json import json_normalize
        #washersDFPart = washersDFPart.iloc[0:5]
            for index, row in washersDFPart.iterrows(): 
                print (row["PROD_ID"],) 
                print (pd.DataFrame(json.loads(row['OFFERS'])).T)
                inter = pd.DataFrame(json.loads(row['OFFERS'])).T
                inter['PROD_ID'] = row["PROD_ID"]
                if complet.shape[0]>0:
                    complet = pd.concat([complet,inter])
                else:
                    complet = inter
        #test = washersDFPart.apply(lambda x: (pd.DataFrame(json.loads(x['OFFERS'])).T))
        #df2 = washersDFPart.merge(washersDFPart.OFFERS.apply(self.do_the_thing), how = 'left', left_index = True, right_index = True)
        #InnerContent['PROD_ID'] = washersDFPart['PROD_ID'][0]
        print('Output')

    # Check whether record is null, or doesn't contain any real data
    def do_the_thing(self,row):
        if pd.notnull(row) and len(row) > 2:
            # Convert the json structure into a dataframe, one cell at a time in the relevant column
            x = pd.read_json(row)
            # The last bit of this string (after the last =) will be used as a key for the column labels
            #x['key'] = x['key'].apply(lambda x: x.split("=")[-1])
            # Set this new key to be the index
            #y = x.set_index('key')
            # Stack the rows up via a multi-level column index
            #y = y.stack().to_frame().T
            #y = x.stack().to_frame().T
            # Flatten out the multi-level column index
            #y.columns = ['{1}_{0}'.format(*c) for c in y.columns]

            #we don't need to re-index
                # Give the single record the same index number as the parent dataframe (for the merge to work)
                #y.index = [df.index[i]]
            #we don't need to add to a temp df
            # Append this dataframe on sequentially for each row as we go through the loop
            #return y.iloc[0]
            return x.T
        else:
            return pd.Series()
        