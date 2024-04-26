# -*- coding: utf-8 -*-
import random
import simpy
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

# Підключення до бази даних MongoDB
client = MongoClient("mongodb+srv://marianvitaliiabelei:<password>@cleaner.fuznyym.mongodb.net/?retryWrites=true&w=majority&appName=Cleaner")
db = client["dry_cleaner"]
suits_collection = db["suits"]
jackets_collection = db["jackets"]
pants_collection = db["pants"]
operators_collection = db["operators"]
queue_collection = db["queue"]

# Визначення параметрів моделі
MEAN_ARRIVAL_TIME = 10

# Функція для генерації випадкового часу з експоненціальним розподілом
def exponential(mean):
    return random.expovariate(1 / mean)

# Функція для обробки костюмів оператором
def operator_process(env, operator_id, queue, mean_service_time, damage_probability):
    while True:
        suit_id = yield queue.get()
        suit = suits_collection.find_one({"_id": ObjectId(suit_id)})
        jacket_id = suit["_jacket_id"]
        pants_id = suit["_pants_id"]

        service_time = exponential(mean_service_time)
        yield env.timeout(service_time)

        is_damaged = random.random() < damage_probability
        if operator_id == 2:
            jackets_collection.update_one({"_id": jacket_id}, {"$set": {"state": "damaged" if is_damaged else "processed"}})
        elif operator_id == 3:
            pants_collection.update_one({"_id": pants_id}, {"$set": {"state": "damaged" if is_damaged else "processed"}})

        suits_collection.update_one({"_id": suit["_id"]}, {"$set": {"status": "processed"}})


operators = list()
# Функція для моделювання процесу обробки костюмів
def dry_cleaning_process(env):
    operator1_queue = simpy.Store(env)
    operator2_queue = simpy.Store(env)
    operator3_queue = simpy.Store(env)
    operator4_queue = simpy.Store(env)
    operator5_queue = simpy.Store(env)

    operators = list(operators_collection.find())
    for operator in operators:
        operator_id = operator["_id"]
        mean_service_time = operator["average_work_time"]
        damage_probability = operator["damage_probability"]
        if operator_id == 1:
            env.process(operator_process(env, operator_id, operator1_queue, mean_service_time, damage_probability))
        elif operator_id == 2:
            env.process(operator_process(env, operator_id, operator2_queue, mean_service_time, damage_probability))
        elif operator_id == 3:
            env.process(operator_process(env, operator_id, operator3_queue, mean_service_time, damage_probability))
        elif operator_id == 4:
            env.process(operator_process(env, operator_id, operator4_queue, mean_service_time, damage_probability))
        elif operator_id == 5:
            env.process(operator_process(env, operator_id, operator5_queue, mean_service_time, damage_probability))

    while True:
        yield env.timeout(exponential(MEAN_ARRIVAL_TIME))
        client = db["clients"].aggregate([{"$sample": {"size": 1}}]).next()
        jacket_id = jackets_collection.insert_one({"state": "waiting"}).inserted_id
        pants_id = pants_collection.insert_one({"state": "waiting"}).inserted_id
        suit_id = suits_collection.insert_one({
            "_client_id": client["_id"],
            "status": "waiting",
            "arrival_date": datetime.now(),
            "_jacket_id": jacket_id,
            "_pants_id": pants_id
        }).inserted_id

        queue_collection.update_one({"to_operator_id": 1}, {"$push": {"list": suit_id}})
        yield operator1_queue.put(suit_id)
        jacket = jackets_collection.find_one({"_id": jacket_id})
        pants = pants_collection.find_one({"_id": pants_id})

        if jacket["state"] == "waiting":
            queue_collection.update_one({"to_operator_id": 2}, {"$push": {"list": jacket_id}})
            yield operator2_queue.put(suit_id)
        if pants["state"] == "waiting":
            queue_collection.update_one({"to_operator_id": 3}, {"$push": {"list": pants_id}})
            yield operator3_queue.put(suit_id)

        jacket = jackets_collection.find_one({"_id": jacket_id})
        pants = pants_collection.find_one({"_id": pants_id})

        if jacket["state"] == "damaged" or pants["state"] == "damaged":
            yield operator5_queue.put(suit_id)
        else:
            yield operator4_queue.put(suit_id)


env = simpy.Environment()
env.process(dry_cleaning_process(env))
env.run(until=1000)


suits = list(suits_collection.find())
undamaged_suits = [s for s in suits if 
                   jackets_collection.find_one({"_id": s["_jacket_id"]}) is not None and 
                   jackets_collection.find_one({"_id": s["_jacket_id"]})["state"] != "damaged" and 
                   pants_collection.find_one({"_id": s["_pants_id"]}) is not None and 
                   pants_collection.find_one({"_id": s["_pants_id"]})["state"] != "damaged"]

damaged_suits = [s for s in suits if 
                 (jackets_collection.find_one({"_id": s["_jacket_id"]}) is not None and 
                  jackets_collection.find_one({"_id": s["_jacket_id"]})["state"] == "damaged") or 
                 (pants_collection.find_one({"_id": s["_pants_id"]}) is not None and 
                  pants_collection.find_one({"_id": s["_pants_id"]})["state"] == "damaged")]


print(f"Максимальний час перебування непошкоджених костюмів: {max(s['arrival_date'] for s in undamaged_suits):.2f} хвилин")
print(f"Середній час перебування непошкоджених костюмів: {sum(s['arrival_date'] for s in undamaged_suits) / len(undamaged_suits):.2f} хвилин")

print(f"Максимальний час перебування пошкоджених костюмів: {max(s['arrival_date'] for s in damaged_suits):.2f} хвилин")
print(f"Середній час перебування пошкоджених костюмів: {sum(s['arrival_date'] for s in damaged_suits) / len(damaged_suits):.2f} хвилин")

operator_utilization = {}
for operator in operators:
    operator_id = operator["_id"]
    total_service_time = sum(exponential(operator["average_work_time"]) for s in suits if s['status'] == 'processed')
    operator_utilization[operator_id] = total_service_time / env.now

print("Коефіцієнти завантаження операторів:")
for operator_id, utilization in operator_utilization.items():
    print(f"Оператор {operator_id}: {utilization:.2f}")