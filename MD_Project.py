import pandas_datareader as web
import datetime
import pandas as pd
import schedule
import time

start = datetime.datetime(2000, 9, 1)
end = datetime.datetime(2020, 12, 31)
companies = ["HON", "KMB", "AMBU-B.CO", "APT", "MMM"]

def getCompanyInfo():
    totalStockDf = web.DataReader(companies[0], 'yahoo', start, end)
    totalStockDf.insert(0, "Company", companies[0], True) 

    for i in range(1,len(companies)):
        stockDf = web.DataReader(companies[i], 'yahoo', start, end)
        stockDf.insert(0, "Company", companies[i], True) 
        totalStockDf = totalStockDf.append(stockDf)

    return totalStockDf

def saveHdfs(fileToSave):
    #TODO
    pass

def saveStockData():
    print('Default start date is ', start)
    print('Default end date is ', end)
    print('Code of companies are: ', companies)

    df = getCompanyInfo()
    #saveHdfs(df)

    #Ta czesc jest do usuniecia jak bedzie dzialalao saveHDFS
    now = datetime.datetime.now()
    datetoday = now.strftime("%d-%m-%Y %H-%M-%S")
    filename = datetoday + ' stockData.csv'
    df.to_csv(filename)

    print("File was saved")



if __name__ == "__main__":
    schedule.every(5).seconds.do(saveStockData)
    schedule.every().day.at("17:00").do(saveStockData)

    while True:
        schedule.run_pending()
        time.sleep(1)