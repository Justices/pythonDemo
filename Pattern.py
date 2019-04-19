from logger import logger
param = dict()
param['test'] = 'dd'
param['test1'] = 'ddd'

for key in param.keys():
    print key,param[key]

param_tuple = ('234','3434')
print param_tuple.count('234')

for item in param_tuple:
    print item

logger.info("recorder the loggging")