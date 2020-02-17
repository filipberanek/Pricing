from Subscripts import S3FileLoader
from Subscripts import KeboolaFileManager
from Subscripts import Constants
from Subscripts import Logging
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
        # Create Instance of logger
        self.Logger = Logging.logging()
        

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
            if productsDF.shape[0]>0:
                prodcutsDF = pd.concat([productsDF,productsDFPart])
            else: 
                productsDF = productsDFPart
            self.Logger.insert_into_log('INFO',"Uploaded {} out of {} products".format(i, len(productsURL)))
        self.Logger.insert_into_log('INFO','Number of products afer upload: {}'.format(productsDF.shape))
        productsDF = productsDF[productsDF['PRICE']>0]
        productsDF = productsDF[productsDF['VALID']==True]
        productsDF.drop_duplicates(subset = productsDF.columns,inplace = True)
        productsDF = self.addTS_addFileName(productsDF)
        self.Logger.insert_into_log('INFO','Number of products afer dropping duplicates: {}'.format(productsDF.shape))       
        return productsDF

    def PreparePiper(self):
        #Get piper 
        piperURL = self.KebolaInst.GetFiles(Constants.DODAVAETELPIPER,Constants.OFFSET,Constants.MAXLISTVALUES)
        pipersDF = pd.DataFrame()
        piperStringValue = ''
        for i,piperVal in enumerate(piperURL):
            pipersDFPart = pd.read_csv(StringIO(self.S3Inst.GetFileContent(piperVal['url'])), sep=",")
            if pipersDF.shape[0]>0:
                pipersDF = pd.concat([pipersDF,pipersDFPart])
            else: 
                pipersDF = pipersDFPart
            self.Logger.insert_into_log('INFO',"Uploaded {} out of {} pipers".format(i, len(piperURL)))
        self.Logger.insert_into_log('INFO','Number of pipers afer upload: {}'.format(pipersDF.shape))
        pipersDF = pipersDF[pipersDF['PRICE']>0]
        pipersDF.drop_duplicates(subset = ['PROD_ID', 'ESHOP', 'PROD_NAME', 'PRICE', 'STOCK'],inplace = True)
        pipersDF = self.addTS_addFileName(pipersDF)
        self.Logger.insert_into_log('INFO','Number of pipers afer dropping duplicates: {}'.format(pipersDF.shape))
        return pipersDF

    def PrepareWasher(self):
        #Get piper URL
        washerURL = self.KebolaInst.GetFiles(Constants.DODAVATELWASHER,Constants.OFFSET,Constants.MAXLISTVALUES)
        washerDF = pd.DataFrame()
        washerStringValue = ''
        for i, washerVal in enumerate(washerURL):
            try:
                washersDFPart = pd.read_csv(StringIO(self.S3Inst.GetFileContent(washerVal['url'])), sep=",")
                #washersDFPart = pd.read_csv(StringIO(self.S3Inst.GetFileContent(washerURL[0]['url'])), sep=",")
                if (washersDFPart.columns == ['PROD_ID', 'OFFERS']).all():
                # Read JSON
                    #Check JSON quality
                    if ((list(json.loads(washersDFPart['OFFERS'][0])[(list(json.loads(washersDFPart['OFFERS'][0]).keys())[0])].keys()) == ['PRICE', 'PROD_NAME', 'STOCK'])):
                        washersDFPart['PRICE']= washersDFPart.OFFERS.apply(lambda x: [val['PRICE'] for val in json.loads(x).values()])
                        washersDFPart['PROD_NAME']=washersDFPart.OFFERS.apply(lambda x: [val['PROD_NAME'] for val in json.loads(x).values()])
                        washersDFPart['STOCK']=washersDFPart.OFFERS.apply(lambda x: [val['STOCK'] for val in json.loads(x).values()])
                        washersDFPart['ESHOP']=washersDFPart.OFFERS.apply(lambda x: [key for key in json.loads(x).keys()])
                        washersDFPart.drop(columns = ['OFFERS'], inplace = True)
                        washersDFPart = washersDFPart.reindex(washersDFPart.index.repeat(washersDFPart.PRICE.str.len()))\
                            .assign(PRICE=np.concatenate(washersDFPart.PRICE.values))\
                                .assign(STOCK=np.concatenate(washersDFPart.STOCK.values))\
                                    .assign(PROD_NAME=np.concatenate(washersDFPart.PROD_NAME.values))\
                                        .assign(ESHOP=np.concatenate(washersDFPart.ESHOP.values))
                    else:
                        raise Exception("In JSON are not all columns: {}".format((json.loads(washersDFPart['OFFERS'][0])[(list(json.loads(washersDFPart['OFFERS'][0]).keys())[0])].keys())))
                washersDFPart = washersDFPart[washersDFPart['PRICE'].astype(int)>0]
                washersDFPart.drop_duplicates(subset = ['PROD_ID', 'ESHOP', 'PROD_NAME', 'PRICE', 'STOCK'],inplace = True)
                if washerDF.shape[0]>0:
                    washerDF = pd.concat([washerDF,washersDFPart])
                else: 
                    washerDF = washersDFPart
                self.Logger.insert_into_log('INFO',"Uploaded {} out of {} pipers".format(i, len(washerDF)))
            except Exception as e :
                self.Logger.insert_into_log('WARNING',"There was an error: {}, during upload of file:  {} ".format(str(e),washerURL))
        self.Logger.insert_into_log('INFO','Number of pipers afer upload: {}'.format(washerDF.shape))
        washerDF.drop_duplicates(subset = ['PROD_ID', 'ESHOP', 'PROD_NAME', 'PRICE', 'STOCK'],inplace = True)
        washerDF = self.addTS_addFileName(washerDF)
        self.Logger.insert_into_log('INFO','Number of pipers afer dropping duplicates: {}'.format(washerDF.shape))
        return washerDF

    def MedianAnalysis(self, MALLPRODUCTS, COMPETITORS):
        # Get just new records
        MALLPRODUCTS = MALLPRODUCTS[MALLPRODUCTS['UPLOAD_TS']==MALLPRODUCTS['UPLOAD_TS'].drop_duplicates().sort_values(ascending = False)[0]]
        COMPETITORS = COMPETITORS[COMPETITORS['UPLOAD_TS']==COMPETITORS['UPLOAD_TS'].drop_duplicates().sort_values(ascending = False)[0]]
        COMPETITORS = COMPETITORS[['PROD_ID','PRICE','UPLOAD_TS']]
        MERGEDFRAME = pd.merge(MALLPRODUCTS,COMPETITORS, left_on = ['PROD_ID'], right_on = ['PROD_ID'], how = 'inner', suffixes=('_MALL','_COMP'))
        OutputAnalysis = {}
        # Get competitors median
        medianPrices = MERGEDFRAME.groupby(by = 'DIVISION').median().reset_index()
        for index, row in medianPrices.iterrows():
            MERGEDFRAME.loc[MERGEDFRAME['DIVISION']==row['DIVISION'],'MEDIAN']=row['PRICE_MALL']
            WHOLEDATASET = MERGEDFRAME[MERGEDFRAME['DIVISION']==row['DIVISION']].shape[0]
            PARTDATASET = MERGEDFRAME[(MERGEDFRAME['DIVISION']==row['DIVISION']) & (MERGEDFRAME['PRICE_MALL']<row['PRICE_COMP'])].shape[0]
            OutputAnalysis[row['DIVISION']] = str(PARTDATASET/WHOLEDATASET)+' %'
        return OutputAnalysis

    def ProductsAnalysis(self, MALLPRODUCTS, COMPETITORS):
        MALLPRODUCTS = MALLPRODUCTS[['PROD_ID','PRICE','UPLOAD_TS']]
        MALLPRODUCTS.sort_values(by = ['PROD_ID','UPLOAD_TS'],ascending = True, inplace=True)
        MALLPRODUCTS.rename(columns = {'PRICE':'PRICE_MALL'}, inplace = True)
        MALLPRODUCTS['UPLOAD_TS'] = MALLPRODUCTS['UPLOAD_TS'].astype('datetime64[ns]').dt.strftime('%d/%m/%Y')
        MALLPRODUCTS.drop_duplicates(inplace = True)
        COMPETITORS = COMPETITORS[['PROD_ID','PRICE','UPLOAD_TS']]
        COMPETITORS.sort_values(by = ['PROD_ID','UPLOAD_TS'],ascending = True, inplace=True)
        COMPETITORS.rename(columns = {'PRICE':'PRICE_COMPETITORS'}, inplace = True)
        COMPETITORS = COMPETITORS.groupby(by = 'PROD_ID').min()
        COMPETITORS['UPLOAD_TS'] = COMPETITORS['UPLOAD_TS'].astype('datetime64[ns]').dt.strftime('%d/%m/%Y')
        COMPETITORS.drop_duplicates(inplace = True)
        COMPETITORS.reset_index(inplace = True)
        MERGEDFRAME = pd.merge(MALLPRODUCTS,COMPETITORS, left_on = ['PROD_ID','UPLOAD_TS'], right_on = ['PROD_ID','UPLOAD_TS'], how = 'inner')
        MERGEDFRAME['CHEAPERDAY'] = 0
        MERGEDFRAME.loc[MERGEDFRAME['PRICE_MALL']<MERGEDFRAME['PRICE_COMPETITORS'],'CHEAPERDAY'] = 1
        MERGEDFRAME = MERGEDFRAME.groupby(by = 'PROD_ID').sum().loc[:,'CHEAPERDAY']
        return MERGEDFRAME

