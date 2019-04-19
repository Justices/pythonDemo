from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()


def test():
    print "test"


scheduler.add_job(func=test, trigger='cron', day='*', hour="14-20/4", minute="46")

if __name__ == '__main__':
    scheduler.start()
    while True:
        pass

