from redis import StrictRedis

# str  set/get
redis_demo = StrictRedis()
redis_demo.set('foo', 'bar')
print redis_demo.get('foo')

# hash object hmset/hgetall
redis_demo.hset('car:1', 'name', 'bmw')
print redis_demo.hget('name', 'car:1')
redis_demo.hset('car:1', 'price', '100')
print redis_demo.hget('price', 'car:1')

print redis_demo.hgetall("car:1")

# transaction and pipe line
pipe = redis_demo.pipeline()
pipe.set('foo', '12344')
pipe.get('foo')
result = pipe.execute()
print result

pipe_1 = redis_demo.pipeline(transaction=False)
result_1 = pipe_1.set('foo1', 'bar1').get('foo1').execute()
print result_1
