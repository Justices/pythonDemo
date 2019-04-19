import schedule
import time
def job():
    print "i am working job"

def job1():
    print "i am working on job1 "


schedule.every().minute.do(job_func=job)
schedule.every().day.at("10:45").do(job_func=job1)

while True:
    schedule.run_pending()
